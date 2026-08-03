WideEvent.configure do |config|
  # Enabled wherever OTEL_EXPORTER_OTLP_ENDPOINT is set, flushing onto the
  # OTel root span. Uncomment to emit JSON log lines instead (no tracing
  # required):
  # config.enabled = true
  # config.sink = :log

  # Track attributes against config/wide_event/registry.yml in tests; pair
  # with assert_registered_wide_event_attributes to enforce the schema.
  config.strict = Rails.env.test?
end
