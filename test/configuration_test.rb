require "test_helper"
require "tempfile"
require "tmpdir"
require "fileutils"

class ConfigurationTest < ActiveSupport::TestCase
  test "disabled by default without an OTLP endpoint" do
    config = WideEvent::Configuration.new
    assert_equal false, config.enabled
  end

  test "enabled by default when OTEL_EXPORTER_OTLP_ENDPOINT is set" do
    original = ENV["OTEL_EXPORTER_OTLP_ENDPOINT"]
    ENV["OTEL_EXPORTER_OTLP_ENDPOINT"] = "http://localhost:4318"
    assert_equal true, WideEvent::Configuration.new.enabled
  ensure
    ENV["OTEL_EXPORTER_OTLP_ENDPOINT"] = original
  end

  test "defaults" do
    config = WideEvent::Configuration.new
    assert_equal 10, config.max_cache_attrs
    assert_equal "postgres_query", config.span_scopes["OpenTelemetry::Instrumentation::PG"]
    assert_equal "http_call", config.span_scopes["OpenTelemetry::Instrumentation::Net::HTTP"]
    assert_equal false, config.strict
    assert_nil config.registry_path
  end

  test "sink :otel resolves to the OTel span sink" do
    config = WideEvent::Configuration.new
    config.sink = :otel
    assert_kind_of WideEvent::Sinks::OtelSpan, config.resolved_sink
  end

  test "sink :log resolves to the log line sink" do
    config = WideEvent::Configuration.new
    config.sink = :log
    assert_kind_of WideEvent::Sinks::LogLine, config.resolved_sink
  end

  test "a custom sink object is used as-is" do
    sink = Object.new
    config = WideEvent::Configuration.new
    config.sink = sink
    assert_same sink, config.resolved_sink
  end

  test "WideEvent.enabled? reflects configuration" do
    WideEvent.configure { |c| c.enabled = false }
    assert_equal false, WideEvent.enabled?
    WideEvent.configure { |c| c.enabled = true }
    assert_equal true, WideEvent.enabled?
  end

  test "a relative registry_path resolves against Rails.root" do
    Dir.mktmpdir do |dir|
      FileUtils.mkdir_p(File.join(dir, "config", "wide_event"))
      File.write(File.join(dir, "config", "wide_event", "registry.yml"), "report.format:\n  type: string\n")
      with_rails_root(dir) do
        config = WideEvent::Configuration.new
        config.registry_path = "config/wide_event/registry.yml"
        assert config.registry.registered?("report.format")
      end
    end
  end

  test "an absolute registry_path is opened as-is even with Rails.root set" do
    Tempfile.create([ "registry", ".yml" ]) do |f|
      f.write("report.format:\n  type: string\n")
      f.flush
      Dir.mktmpdir do |dir|
        with_rails_root(dir) do
          config = WideEvent::Configuration.new
          config.registry_path = f.path
          assert config.registry.registered?("report.format")
        end
      end
    end
  end

  test "scheduled_job_detector defaults to the Solid Queue detector" do
    config = WideEvent::Configuration.new
    assert_respond_to config.scheduled_job_detector, :call
    job = Struct.new(:provider_job_id).new(nil)
    assert_equal false, config.scheduled_job_detector.call(job)
  end

  test "store_* defaults come from the WIDE_EVENTS_* environment variables" do
    with_env(
      "WIDE_EVENTS_URL" => "https://store.example.com",
      "WIDE_EVENTS_SERVICE" => "checkout",
      "WIDE_EVENTS_ENVIRONMENT" => "production",
      "WIDE_EVENTS_INGEST_TOKEN" => "ingest-token",
      "WIDE_EVENTS_QUERY_TOKEN" => "query-token"
    ) do
      config = WideEvent::Configuration.new
      assert_equal "https://store.example.com", config.store_url
      assert_equal "checkout", config.store_service
      assert_equal "production", config.store_environment
      assert_equal "ingest-token", config.store_ingest_token
      assert_equal "query-token", config.store_query_token
      assert_equal true, config.enabled
      assert_equal :store, config.sink
    end
  end

  test "without WIDE_EVENTS_URL the store sink is not selected and store fields are nil" do
    with_env("WIDE_EVENTS_URL" => nil, "WIDE_EVENTS_SERVICE" => nil, "WIDE_EVENTS_INGEST_TOKEN" => nil) do
      config = WideEvent::Configuration.new
      assert_nil config.store_url
      assert_nil config.store_service
      assert_nil config.store_ingest_token
      assert_equal :otel, config.sink
    end
  end

  test "sink :store resolves to a store sink and constructs its own sender" do
    config = WideEvent::Configuration.new
    config.store_url = "https://store.example.com"
    config.store_service = "checkout"
    config.store_environment = "test"
    config.store_ingest_token = "token"
    config.sink = :store

    assert_kind_of WideEvent::Sinks::Store, config.resolved_sink
  ensure
    config.shutdown_store_sender!
  end

  test "re-resolving :store shuts down the previously built sender instead of leaking its worker thread" do
    config = WideEvent::Configuration.new
    config.store_url = "https://store.example.com"
    config.store_service = "checkout"
    config.store_environment = "test"
    config.store_ingest_token = "token"
    config.sink = :store

    config.resolved_sink
    first_sender = config.instance_variable_get(:@store_sender)
    first_thread = first_sender.instance_variable_get(:@thread)
    assert first_thread.alive?, "the first sender's worker thread should be running"

    config.sink = :store # any assignment clears @resolved_sink, forcing a rebuild on the next call
    config.resolved_sink
    second_sender = config.instance_variable_get(:@store_sender)

    refute_same first_sender, second_sender
    refute first_thread.alive?, "the previous sender's worker thread should have been shut down, not leaked"
  ensure
    config.shutdown_store_sender!
  end

  test "sink :store without a service falls back to a no-op sink and reports through handle_error" do
    config = WideEvent::Configuration.new
    config.store_url = "https://store.example.com"
    config.store_ingest_token = "token"
    config.store_environment = "test"
    config.sink = :store

    errors = []
    config.error_handler = ->(e, msg) { errors << [ e.class, msg ] }

    assert_nothing_raised { config.resolved_sink.flush({ "duration_ms" => 1.0 }) }
    assert_equal [ [ ArgumentError, "store_sink_configuration" ] ], errors
  end

  test "sink :store without an ingest token falls back to a no-op sink and reports through handle_error" do
    # Client no longer validates ingest_token presence at construction (a
    # query-only Client legitimately has none) — build_store_sink must
    # keep enforcing it itself for the ingest path.
    config = WideEvent::Configuration.new
    config.store_url = "https://store.example.com"
    config.store_service = "checkout"
    config.store_environment = "test"
    config.store_ingest_token = nil
    config.sink = :store

    errors = []
    config.error_handler = ->(e, msg) { errors << [ e.class, msg ] }

    assert_nothing_raised { config.resolved_sink.flush({ "duration_ms" => 1.0 }) }
    assert_equal [ [ ArgumentError, "store_sink_configuration" ] ], errors
  end

  test "resolved? is false until resolved_sink has actually been called" do
    config = WideEvent::Configuration.new
    refute config.resolved?
    config.resolved_sink
    assert config.resolved?
  end

  test "shutdown_store_sender! is a no-op when no store sink was ever built" do
    config = WideEvent::Configuration.new
    assert_nothing_raised { config.shutdown_store_sender! }
  end

  private

  def with_env(vars)
    originals = vars.keys.to_h { |k| [ k, ENV[k] ] }
    vars.each { |k, v| ENV[k] = v }
    yield
  ensure
    originals.each { |k, v| ENV[k] = v }
  end
end
