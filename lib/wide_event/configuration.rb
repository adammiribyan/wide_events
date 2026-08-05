module WideEvent
  class Configuration
    # Reports through OTel's error handler when it's loaded; silent otherwise.
    DEFAULT_ERROR_HANDLER = lambda do |exception, message|
      if defined?(OpenTelemetry) && OpenTelemetry.respond_to?(:handle_error)
        OpenTelemetry.handle_error(exception: exception, message: "wide_event #{message}")
      end
    end

    # Recurring (scheduled) Solid Queue executions have a RecurringExecution
    # row pointing at the underlying job. One indexed query per execution.
    SOLID_QUEUE_DETECTOR = lambda do |job|
      return false unless defined?(SolidQueue::RecurringExecution)
      job.provider_job_id.present? && SolidQueue::RecurringExecution.exists?(job_id: job.provider_job_id)
    rescue StandardError
      false
    end

    attr_accessor :enabled, :max_cache_attrs, :span_scopes, :strict,
                  :logger, :error_handler, :scheduled_job_detector,
                  :store_url, :store_service, :store_environment,
                  :store_ingest_token, :store_query_token
    attr_reader :sink, :registry_path

    # A resolved store sink is unusable until its configuration
    # (url/service/environment/ingest token) is valid; falls back to this
    # silent no-op so a misconfigured store never raises into a flush.
    NOOP_STORE_SINK = Object.new.tap { |sink| sink.define_singleton_method(:flush) { |_attrs| nil } }.freeze

    def initialize
      @store_url = ENV["WIDE_EVENTS_URL"]
      @enabled = @store_url.present? || ENV["OTEL_EXPORTER_OTLP_ENDPOINT"].present?
      @sink = @store_url.present? ? :store : :otel
      @resolved_sink = nil
      @store_service = ENV["WIDE_EVENTS_SERVICE"]
      @store_environment = ENV["WIDE_EVENTS_ENVIRONMENT"]
      @store_ingest_token = ENV["WIDE_EVENTS_INGEST_TOKEN"]
      @store_query_token = ENV["WIDE_EVENTS_QUERY_TOKEN"]
      @store_sender = nil
      @max_cache_attrs = 10
      @span_scopes = {
        "OpenTelemetry::Instrumentation::PG" => "postgres_query",
        "OpenTelemetry::Instrumentation::Net::HTTP" => "http_call"
      }
      @strict = false
      @registry_path = nil
      @registry = nil
      @logger = nil
      @error_handler = DEFAULT_ERROR_HANDLER
      @scheduled_job_detector = SOLID_QUEUE_DETECTOR
    end

    def sink=(value)
      @sink = value
      @resolved_sink = nil
    end

    def resolved_sink
      @resolved_sink ||= case @sink
      when :otel then Sinks::OtelSpan.new
      when :log then Sinks::LogLine.new(logger)
      when :store then build_store_sink
      else @sink
      end
    end

    # True once #resolved_sink has actually built (or attempted to build) a
    # sink; lets WideEvent.shutdown! avoid constructing one from scratch
    # just to shut it down.
    def resolved?
      !@resolved_sink.nil?
    end

    def registry_path=(path)
      @registry_path = path
      @registry = nil
    end

    def registry
      return nil if @registry_path.nil?
      @registry ||= Registry.load(resolved_registry_path)
    end

    # Stops the store sender's worker thread (if one was ever built) so
    # tests and process shutdown never leak the background thread.
    def shutdown_store_sender!(timeout: WideEvent::Store::Sender::SHUTDOWN_TIMEOUT)
      @store_sender&.shutdown(timeout: timeout)
      @store_sender = nil
    end

    private

    def build_store_sink
      # A prior resolution (e.g. `config.sink = :store` twice, or
      # re-assigning store_* fields and re-resolving) may have already
      # built and started a sender; shut it down before replacing it so
      # its worker thread and queue don't strand themselves, ownerless and
      # retrying forever.
      shutdown_store_sender!

      raise ArgumentError, "WIDE_EVENTS_SERVICE is required for the store sink" if @store_service.to_s.strip.empty?
      raise ArgumentError, "WIDE_EVENTS_ENVIRONMENT is required for the store sink" if @store_environment.to_s.strip.empty?

      client = WideEvent::Store::Client.new(url: @store_url, ingest_token: @store_ingest_token)
      @store_sender = WideEvent::Store::Sender.new(client: client, service: @store_service, environment: @store_environment)
      Sinks::Store.new(sender: @store_sender)
    rescue StandardError => e
      # This instance's own handler, not the module-level WideEvent.handle_error
      # funnel: build_store_sink runs on whatever Configuration instance is
      # resolving (in production that's always WideEvent.config, but tests
      # also resolve standalone instances directly).
      report_configuration_error(e)
      NOOP_STORE_SINK
    end

    def report_configuration_error(exception)
      error_handler&.call(exception, "store_sink_configuration")
    rescue StandardError
      nil
    end

    # `registry_path` stays relative so the generated doc can commit it;
    # absolute paths win inside Pathname#join and pass through as-is.
    def resolved_registry_path
      root = Rails.root if defined?(Rails) && Rails.respond_to?(:root)
      root ? root.join(@registry_path).to_s : @registry_path
    end
  end
end
