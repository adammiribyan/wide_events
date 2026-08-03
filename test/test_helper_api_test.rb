require "test_helper"
require "tempfile"
require "wide_event/test_helper"

class TestHelperApiTest < ActiveSupport::TestCase
  include WideEvent::TestHelper

  test "assert_wide_event asserts value equality and returns the attributes" do
    attrs = assert_wide_event("report.format" => "pdf") do
      WideEvent.set("report.format" => "pdf", "extra" => 1)
    end
    assert_equal 1, attrs["extra"]
  end

  test "assert_wide_event with bare keys asserts presence" do
    assert_wide_event("x.duration_ms") do
      WideEvent.phase("x") { :ok }
    end
  end

  test "assert_wide_event fails on a missing key" do
    assert_raises(Minitest::Assertion) do
      assert_wide_event("nope") { }
    end
  end

  test "assert_wide_event fails on a wrong value" do
    assert_raises(Minitest::Assertion) do
      assert_wide_event("a" => 2) { WideEvent.set("a" => 1) }
    end
  end

  test "capture_wide_events collects flushed events and restores the sink" do
    events = capture_wide_events do
      WideEvent.with do |attrs|
        WideEvent.set("a" => 1)
        WideEvent.flush(attrs)
      end
    end
    assert_equal [ { "a" => 1 } ], events
    assert_kind_of WideEvent::Sinks::OtelSpan, WideEvent.config.resolved_sink,
      "original sink must be restored"
  end

  test "capture_wide_events enables instrumentation and restores it after an exception" do
    WideEvent.configure { |config| config.enabled = false }

    assert_raises(RuntimeError) do
      capture_wide_events do
        assert WideEvent.enabled?, "capture must enable request and job instrumentation"
        raise "boom"
      end
    end

    refute WideEvent.enabled?, "capture must restore the previous enabled state"
  end

  test "assert_registered_wide_event_attributes fails when strict mode collected violations" do
    Tempfile.create([ "registry", ".yml" ]) do |f|
      f.write("known:\n  type: string\n")
      f.flush
      WideEvent.configure do |c|
        c.registry_path = f.path
        c.strict = true
      end
      WideEvent.with { WideEvent.set("unknown.attr" => 1) }
      error = assert_raises(Minitest::Assertion) { assert_registered_wide_event_attributes }
      assert_match(/unknown\.attr/, error.message)
    end
  end

  test "assert_registered_wide_event_attributes passes without a registry or violations" do
    assert_registered_wide_event_attributes
  end

  test "assert_registered_wide_event_attributes drains violations so it can run in a teardown" do
    Tempfile.create([ "registry", ".yml" ]) do |f|
      f.write("known:\n  type: string\n")
      f.flush
      WideEvent.configure do |c|
        c.registry_path = f.path
        c.strict = true
      end
      WideEvent.with { WideEvent.set("unknown.attr" => 1) }
      assert_raises(Minitest::Assertion) { assert_registered_wide_event_attributes }
      assert_registered_wide_event_attributes
    end
  end
end
