require "test_helper"
require "json"

class StoreEnvelopeTest < ActiveSupport::TestCase
  CLOCK = -> { Time.utc(2026, 8, 5, 12) }
  UUID = -> { "018f6f50-2f9d-7fb0-a258-6d9f95f20c01" }

  def build(attrs, env: {}, clock: CLOCK, uuid: UUID)
    WideEvent::Store::Envelope.build(attrs, clock: clock, uuid: uuid, env: env)
  end

  test "preserves sanitized primitive attribute types" do
    json = build(
      { "duration_ms" => 12.5, "report.format" => "pdf", "error" => false },
      env: { "KAMAL_VERSION" => "abc123", "HOSTNAME" => "web-1" }
    )
    assert_equal "request", JSON.parse(json).fetch("kind")
    assert_equal 12.5, JSON.parse(json).fetch("duration_ms")
  end

  test "attributes hash keeps primitive types and excludes the promoted duration_ms" do
    event = JSON.parse(build({ "duration_ms" => 1.0, "report.format" => "pdf", "error" => false, "count" => 3 }))
    assert_equal({ "report.format" => "pdf", "error" => false, "count" => 3 }, event.fetch("attributes"))
  end

  test "drops nil attributes instead of sending null" do
    event = JSON.parse(build({ "duration_ms" => 1.0, "user.id" => nil, "kept" => "yes" }))
    refute event.fetch("attributes").key?("user.id")
    assert_equal "yes", event.fetch("attributes").fetch("kept")
  end

  test "derives kind job from the presence of job.class" do
    event = JSON.parse(build({ "duration_ms" => 1.0, "job.class" => "ReportJob" }))
    assert_equal "job", event.fetch("kind")
  end

  test "derives kind request when job.class is absent" do
    event = JSON.parse(build({ "duration_ms" => 1.0 }))
    assert_equal "request", event.fetch("kind")
  end

  test "sets deterministic id, occurred_at, and duration_ms from the injected clock and uuid" do
    event = JSON.parse(build({ "duration_ms" => 42.7 }))
    assert_equal "018f6f50-2f9d-7fb0-a258-6d9f95f20c01", event.fetch("id")
    assert_equal "2026-08-05T12:00:00.000Z", event.fetch("occurred_at")
    assert_equal 42.7, event.fetch("duration_ms")
  end

  test "prefers KAMAL_CONTAINER_NAME over HOSTNAME for source" do
    event = JSON.parse(build({ "duration_ms" => 1.0 },
      env: { "KAMAL_CONTAINER_NAME" => "app-web-1", "HOSTNAME" => "web-1" }))
    assert_equal "app-web-1", event.fetch("source")
  end

  test "falls back to HOSTNAME for source when KAMAL_CONTAINER_NAME is absent" do
    event = JSON.parse(build({ "duration_ms" => 1.0 }, env: { "HOSTNAME" => "web-1" }))
    assert_equal "web-1", event.fetch("source")
  end

  test "falls back to Socket.gethostname when no env identity is set" do
    event = JSON.parse(build({ "duration_ms" => 1.0 }, env: {}))
    assert_equal Socket.gethostname, event.fetch("source")
  end

  test "omits deployment when KAMAL_VERSION is absent" do
    event = JSON.parse(build({ "duration_ms" => 1.0 }, env: {}))
    refute event.key?("deployment")
  end

  test "sets deployment from KAMAL_VERSION when present" do
    event = JSON.parse(build({ "duration_ms" => 1.0 }, env: { "KAMAL_VERSION" => "abc123" }))
    assert_equal "abc123", event.fetch("deployment")
  end

  test "raises when duration_ms is missing" do
    assert_raises(ArgumentError) { build({ "report.format" => "pdf" }) }
  end

  test "does not mutate the input hash" do
    attrs = { "duration_ms" => 1.0, "kept" => "yes" }.freeze
    build(attrs)
    assert_equal({ "duration_ms" => 1.0, "kept" => "yes" }, attrs)
  end

  test "returns a frozen string" do
    assert build({ "duration_ms" => 1.0 }).frozen?
  end

  test "rejects an event with more than 256 attributes, reachable through handle_error" do
    huge = { "duration_ms" => 1.0 }
    257.times { |i| huge["attr#{i}"] = i }

    errors = []
    WideEvent.configure { |c| c.error_handler = ->(e, message) { errors << [ e, message ] } }

    begin
      build(huge)
      flunk "expected build to raise"
    rescue StandardError => e
      WideEvent.handle_error(e, "store_envelope")
    end

    assert_equal 1, errors.length
    assert_kind_of ArgumentError, errors.first.first
  end

  test "rejects an event whose canonical JSON exceeds 64 KiB" do
    huge_attrs = { "duration_ms" => 1.0, "blob" => "x" * 70_000 }
    assert_raises(ArgumentError) { build(huge_attrs) }
  end
end
