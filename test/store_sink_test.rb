require "test_helper"

class StoreSinkTest < ActiveSupport::TestCase
  CLOCK = -> { Time.utc(2026, 8, 5, 12) }
  UUID = -> { "018f6f50-2f9d-7fb0-a258-6d9f95f20c01" }

  class FakeSender
    attr_reader :enqueued

    def initialize
      @enqueued = []
    end

    def enqueue(event_json)
      @enqueued << event_json
      true
    end
  end

  test "flush builds the canonical envelope and only calls sender.enqueue" do
    sender = FakeSender.new
    sink = WideEvent::Sinks::Store.new(sender: sender, clock: CLOCK, uuid: UUID, env: {})
    sink.flush({ "duration_ms" => 12.5, "report.format" => "pdf" })

    assert_equal 1, sender.enqueued.length
    event = JSON.parse(sender.enqueued.first)
    assert_equal "018f6f50-2f9d-7fb0-a258-6d9f95f20c01", event.fetch("id")
    assert_equal 12.5, event.fetch("duration_ms")
    assert_equal "pdf", event.fetch("attributes").fetch("report.format")
  end

  test "flush never raises, even when the envelope is invalid" do
    sender = FakeSender.new
    sink = WideEvent::Sinks::Store.new(sender: sender, clock: CLOCK, uuid: UUID, env: {})
    errors = []
    WideEvent.configure { |c| c.error_handler = ->(e, msg) { errors << [ e.class, msg ] } }

    assert_nothing_raised { sink.flush({ "report.format" => "pdf" }) } # missing duration_ms
    assert_equal [ [ ArgumentError, "store_sink_flush" ] ], errors
    assert_empty sender.enqueued
  end

  test "flush never raises when the sender itself raises" do
    sender = Object.new.tap { |s| s.define_singleton_method(:enqueue) { |_json| raise "boom" } }
    sink = WideEvent::Sinks::Store.new(sender: sender, clock: CLOCK, uuid: UUID, env: {})
    assert_nothing_raised { sink.flush({ "duration_ms" => 1.0 }) }
  end

  test "shutdown delegates to the sender" do
    calls = []
    sender = Object.new.tap { |s| s.define_singleton_method(:shutdown) { |timeout:| calls << timeout } }
    sink = WideEvent::Sinks::Store.new(sender: sender)
    sink.shutdown(timeout: 1.5)
    assert_equal [ 1.5 ], calls
  end

  test ":store resolves through Configuration when fully configured" do
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

  test ":store falls back to a silent no-op sink and reports through handle_error when misconfigured" do
    config = WideEvent::Configuration.new
    config.store_url = "https://store.example.com"
    config.store_service = nil # missing
    config.store_environment = "test"
    config.store_ingest_token = "token"
    config.sink = :store

    errors = []
    config.error_handler = ->(e, msg) { errors << [ e.class, msg ] }

    sink = config.resolved_sink
    assert_nothing_raised { sink.flush({ "duration_ms" => 1.0 }) }
    assert_equal [ [ ArgumentError, "store_sink_configuration" ] ], errors
  end

  test "WIDE_EVENTS_URL present defaults enabled and sink to :store" do
    with_env("WIDE_EVENTS_URL" => "https://store.example.com") do
      config = WideEvent::Configuration.new
      assert_equal true, config.enabled
      assert_equal :store, config.sink
    end
  end

  test "without WIDE_EVENTS_URL the sink defaults to :otel" do
    with_env("WIDE_EVENTS_URL" => nil) do
      config = WideEvent::Configuration.new
      assert_equal :otel, config.sink
    end
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
