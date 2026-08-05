require "time"
require "active_support"
require "active_support/isolated_execution_state"
require "active_support/core_ext/object/blank"

require "wide_event/version"
require "wide_event/store/envelope"
require "wide_event/store/query_result"
require "wide_event/store/client"
require "wide_event/store/sender"
require "wide_event/store/formatter"
require "wide_event/configuration"
require "wide_event/registry"
require "wide_event/sinks/otel_span"
require "wide_event/sinks/log_line"
require "wide_event/sinks/memory"
require "wide_event/sinks/store"
require "wide_event/middleware"
require "wide_event/span_counter_processor"
require "wide_event/subscribers"
require "wide_event/job_instrumentation"

# Wide-event accumulator: one flat attribute hash per unit of work (HTTP
# request or job execution), flushed to the configured sink by
# WideEvent::Middleware / WideEvent::JobInstrumentation. Safe no-op when no
# unit of work is open.
module WideEvent
  KEY = :wide_event_attributes
  PROCESS_START = Process.clock_gettime(Process::CLOCK_MONOTONIC)

  class << self
    def config
      @config ||= Configuration.new
    end

    def configure
      yield config
      config
    end

    def reset_configuration!
      @config&.shutdown_store_sender!
      @config = Configuration.new
    end

    # Best-effort flush of any in-flight store delivery: gives the sender's
    # worker thread up to `timeout` seconds to drain its queue before the
    # process exits. A no-op when no sink has been resolved yet (nothing to
    # flush) or when the resolved sink has no shutdown hook. Never raises.
    def shutdown!(timeout: 2.0)
      return nil unless config.resolved?
      sink = config.resolved_sink
      sink.shutdown(timeout: timeout) if sink.respond_to?(:shutdown)
      nil
    rescue StandardError => e
      handle_error(e, "shutdown")
      nil
    end

    def enabled?
      config.enabled
    end

    def with
      previous = store
      current = {}
      ActiveSupport::IsolatedExecutionState[KEY] = current
      yield current
    ensure
      ActiveSupport::IsolatedExecutionState[KEY] = previous
    end

    def active?
      !store.nil?
    end

    def set(attrs)
      registry_check(attrs.keys)
      store&.merge!(attrs)
      nil
    rescue StandardError => e
      handle_error(e, "set")
      nil
    end

    def count(dep, duration_ms)
      s = store
      return if s.nil?
      registry_check([ "stats.#{dep}_count", "stats.#{dep}_duration_ms" ])
      s["stats.#{dep}_count"] = s.fetch("stats.#{dep}_count", 0) + 1
      s["stats.#{dep}_duration_ms"] = (s.fetch("stats.#{dep}_duration_ms", 0.0) + duration_ms).round(2)
      nil
    rescue StandardError => e
      handle_error(e, "count")
      nil
    end

    def phase(name)
      start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      yield
    ensure
      elapsed_ms = (Process.clock_gettime(Process::CLOCK_MONOTONIC) - start) * 1000.0
      begin
        set("#{name}.duration_ms" => elapsed_ms.round(2))
      rescue StandardError => e
        handle_error(e, "phase")
        nil
      end
    end

    def error!(slug:, exception: nil, expected: false)
      attrs = { "error" => true, "exception.slug" => slug, "exception.expected" => expected }
      if exception
        attrs["exception.type"] = exception.class.name
        attrs["exception.message"] = exception.message.to_s[0, 500]
      end
      set(attrs)
    rescue StandardError => e
      handle_error(e, "error!")
      nil
    end

    def attributes
      store&.dup
    end

    # Read-only view of the live store for hot-path handlers that would
    # otherwise dup per event (e.g. every cache read). Callers must not
    # mutate: writes go through set/count/error!.
    def peek
      store
    end

    def uptime_attributes
      uptime = Process.clock_gettime(Process::CLOCK_MONOTONIC) - PROCESS_START
      { "uptime_sec" => uptime.round, "uptime_sec_log10" => Math.log10([ uptime, 1 ].max).round(3) }
    end

    # Hands a finished wide event to the configured sink. Never raises into
    # the caller.
    def flush(attrs)
      return if attrs.nil? || attrs.empty?
      config.resolved_sink&.flush(attrs)
      nil
    rescue StandardError => e
      handle_error(e, "flush")
      nil
    end

    # Wires the pieces that need the host's OpenTelemetry SDK configured:
    # notification subscribers and, for the OTel sink, the span counter on
    # the global tracer provider. Called by the railtie after the app's
    # initializers; call it manually from a non-Rails setup.
    def install!
      Subscribers.subscribe!
      if config.sink == :otel && defined?(OpenTelemetry) &&
         OpenTelemetry.respond_to?(:tracer_provider) &&
         OpenTelemetry.tracer_provider.respond_to?(:add_span_processor)
        OpenTelemetry.tracer_provider.add_span_processor(SpanCounterProcessor.new)
      end
      nil
    end

    # Sanitize and write a wide-event hash onto an OTel span. `span` may be
    # nil (OTel inactive). Never raises into the caller.
    def flush_hash(hash, span)
      return if hash.nil? || hash.empty? || span.nil?
      span.add_attributes(sanitize(hash))
    rescue StandardError => e
      handle_error(e, "flush")
      nil
    end

    def sanitize(attrs)
      attrs.each_with_object({}) do |(k, v), out|
        key = k.to_s
        case v
        when String, Integer, Float, true, false then out[key] = v
        when Symbol then out[key] = v.to_s
        when Time, DateTime, ActiveSupport::TimeWithZone then out[key] = v.to_time.utc.iso8601(3)
        when nil then nil # drop
        else out[key] = v.to_s[0, 300]
        end
      end
    end

    # Telemetry must never raise into app code: every public entry point
    # funnels its rescues here, and a broken handler is itself swallowed.
    def handle_error(exception, message)
      config.error_handler&.call(exception, message)
      nil
    rescue StandardError
      nil
    end

    private

    def store
      ActiveSupport::IsolatedExecutionState[KEY]
    end

    def registry_check(keys)
      return unless config.strict
      registry = config.registry
      return if registry.nil?
      keys.each { |key| registry.track(key.to_s) }
    end
  end
end
