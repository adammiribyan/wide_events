require "rails/railtie"

module WideEvent
  class Railtie < Rails::Railtie
    # Below ActionDispatch::Executor on purpose: the executor clears
    # IsolatedExecutionState on completion; above it the store would be gone
    # before the flush. With OTel Rack instrumentation at the top of the
    # stack, the flush still lands inside the root span.
    initializer "wide_event.middleware" do |app|
      app.middleware.insert_after ActionDispatch::Executor, WideEvent::Middleware
    end

    initializer "wide_event.active_job" do
      ActiveSupport.on_load(:active_job) { include WideEvent::JobInstrumentation }
    end

    initializer "wide_event.registry" do |app|
      path = app.root.join("config", "wide_event", "registry.yml")
      WideEvent.config.registry_path ||= path.to_s if path.exist?
    end

    # After the app's own initializers, so the host's OpenTelemetry::SDK
    # configuration (and any WideEvent.configure block) has already run.
    config.after_initialize do
      if WideEvent.enabled?
        WideEvent.install!
      elsif Rails.env.development?
        Rails.logger&.info(
          "wide_events: installed but disabled (OTEL_EXPORTER_OTLP_ENDPOINT is not set). " \
          "To see events locally, set `config.enabled = true` and `config.sink = :log` " \
          "in config/initializers/wide_events.rb."
        )
      end
    end

    rake_tasks do
      load File.expand_path("tasks/registry.rake", __dir__)
    end
  end
end
