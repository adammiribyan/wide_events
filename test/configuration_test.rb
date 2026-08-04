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
end
