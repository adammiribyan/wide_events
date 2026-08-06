require "test_helper"
require "open3"
require "opentelemetry/sdk"

class RailtieTest < ActiveSupport::TestCase
  BOOT_SCRIPT = <<~RUBY
    require "rails"
    require "active_job/railtie"
    require "wide_events"

    class Dummy < Rails::Application
      config.eager_load = false
      config.logger = Logger.new(nil)
      config.secret_key_base = "test"
      config.active_job.queue_adapter = :test
    end
    Dummy.initialize!

    stack = Dummy.middleware.middlewares
    executor = stack.index(ActionDispatch::Executor)
    wide_event = stack.index(WideEvent::Middleware)
    raise "middleware not in stack" if wide_event.nil?
    raise "middleware must sit directly below the executor" unless wide_event == executor + 1
    raise "job instrumentation missing" unless ActiveJob::Base.include?(WideEvent::JobInstrumentation)
    puts "BOOT OK"
  RUBY

  NOTICE_BOOT_SCRIPT = <<~RUBY
    require "rails"
    require "active_job/railtie"
    require "wide_events"

    class Dummy < Rails::Application
      config.eager_load = false
      config.logger = Logger.new(STDOUT)
      config.secret_key_base = "test"
      config.active_job.queue_adapter = :test
    end
    Dummy.initialize!
  RUBY

  test "boots with a one-time notice in development when disabled" do
    output, status = Open3.capture2e(
      { "OTEL_EXPORTER_OTLP_ENDPOINT" => nil, "RAILS_ENV" => "development" },
      "bundle", "exec", "ruby", "-e", NOTICE_BOOT_SCRIPT,
      chdir: File.expand_path("..", __dir__)
    )
    assert status.success?, output
    assert_match(/wide_events: installed but disabled/, output)
  end

  test "boots without the notice outside development" do
    output, status = Open3.capture2e(
      { "OTEL_EXPORTER_OTLP_ENDPOINT" => nil, "RAILS_ENV" => "production" },
      "bundle", "exec", "ruby", "-e", NOTICE_BOOT_SCRIPT,
      chdir: File.expand_path("..", __dir__)
    )
    assert status.success?, output
    refute_match(/wide_events: installed but disabled/, output)
  end

  test "railtie inserts the middleware below the executor and instruments ActiveJob" do
    output, status = Open3.capture2e(
      { "OTEL_EXPORTER_OTLP_ENDPOINT" => nil },
      "bundle", "exec", "ruby", "-e", BOOT_SCRIPT,
      chdir: File.expand_path("..", __dir__)
    )
    assert status.success?, output
    assert_includes output, "BOOT OK"
  end

  test "install! registers the span counter on the global tracer provider" do
    previous = OpenTelemetry.tracer_provider
    OpenTelemetry.tracer_provider = OpenTelemetry::SDK::Trace::TracerProvider.new
    WideEvent.install!
    tracer = OpenTelemetry.tracer_provider.tracer("OpenTelemetry::Instrumentation::PG")
    WideEvent.with do
      tracer.in_span("SELECT") { }
      assert_equal 1, WideEvent.attributes["stats.postgres_query_count"]
    end
  ensure
    OpenTelemetry.tracer_provider = previous
  end

  SHUTDOWN_BOOT_SCRIPT = <<~RUBY
    require "rails"
    require "wide_events"

    def WideEvent.shutdown!(timeout: 2.0)
      puts "SHUTDOWN CALLED"
    end

    class Dummy < Rails::Application
      config.eager_load = false
      config.logger = Logger.new(nil)
      config.secret_key_base = "test"
    end
    Dummy.initialize!
    puts "BOOT OK"
  RUBY

  test "railtie registers WideEvent.shutdown! with at_exit" do
    output, status = Open3.capture2e(
      { "OTEL_EXPORTER_OTLP_ENDPOINT" => nil },
      "bundle", "exec", "ruby", "-e", SHUTDOWN_BOOT_SCRIPT,
      chdir: File.expand_path("..", __dir__)
    )
    assert status.success?, output
    assert_includes output, "BOOT OK"
    assert_includes output, "SHUTDOWN CALLED"
  end

  test "install! skips span processor registration for non-otel sinks" do
    WideEvent.configure { |c| c.sink = :log }
    previous = OpenTelemetry.tracer_provider
    OpenTelemetry.tracer_provider = OpenTelemetry::SDK::Trace::TracerProvider.new
    WideEvent.install!
    tracer = OpenTelemetry.tracer_provider.tracer("OpenTelemetry::Instrumentation::PG")
    WideEvent.with do
      tracer.in_span("SELECT") { }
      assert_nil WideEvent.attributes["stats.postgres_query_count"]
    end
  ensure
    OpenTelemetry.tracer_provider = previous
  end
end
