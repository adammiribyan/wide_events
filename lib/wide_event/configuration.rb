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
                  :logger, :error_handler, :scheduled_job_detector
    attr_reader :sink, :registry_path

    def initialize
      @enabled = !ENV["OTEL_EXPORTER_OTLP_ENDPOINT"].to_s.empty?
      @sink = :otel
      @resolved_sink = nil
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
      else @sink
      end
    end

    def registry_path=(path)
      @registry_path = path
      @registry = nil
    end

    def registry
      return nil if @registry_path.nil?
      @registry ||= Registry.load(resolved_registry_path)
    end

    private

    # `registry_path` stays relative (it's committed into the generated doc's
    # header); resolution against Rails.root happens only here, at open time.
    # An absolute path wins inside Pathname#join, so apps pointing elsewhere
    # keep working.
    def resolved_registry_path
      root = Rails.root if defined?(Rails) && Rails.respond_to?(:root)
      root ? root.join(@registry_path).to_s : @registry_path
    end
  end
end
