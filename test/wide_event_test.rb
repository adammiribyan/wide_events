require "test_helper"

class WideEventTest < ActiveSupport::TestCase
  test "set is a no-op outside a unit of work" do
    assert_nil WideEvent.attributes
    WideEvent.set("a" => 1)
    assert_nil WideEvent.attributes
  end

  test "with opens a store, yields it, and restores the previous store" do
    result = WideEvent.with do |attrs|
      WideEvent.set("a" => 1)
      assert_equal({ "a" => 1 }, WideEvent.attributes)
      assert_same attrs, attrs # the yielded hash is the live store
      WideEvent.with do
        WideEvent.set("b" => 2)
        assert_equal({ "b" => 2 }, WideEvent.attributes)
      end
      assert_equal({ "a" => 1 }, WideEvent.attributes)
      :done
    end
    assert_equal :done, result
    assert_nil WideEvent.attributes
  end

  test "count accumulates totals" do
    WideEvent.with do
      WideEvent.count("postgres_query", 1.5)
      WideEvent.count("postgres_query", 2.5)
      attrs = WideEvent.attributes
      assert_equal 2, attrs["stats.postgres_query_count"]
      assert_in_delta 4.0, attrs["stats.postgres_query_duration_ms"]
    end
  end

  test "phase records a duration and returns the block result" do
    WideEvent.with do
      assert_equal :ok, WideEvent.phase("report_generation") { :ok }
      assert_operator WideEvent.attributes["report_generation.duration_ms"], :>=, 0
    end
  end

  test "error! sets error attributes" do
    WideEvent.with do
      WideEvent.error!(slug: "err-test-thing-failed", exception: ArgumentError.new("x" * 600), expected: true)
      attrs = WideEvent.attributes
      assert_equal true, attrs["error"]
      assert_equal "err-test-thing-failed", attrs["exception.slug"]
      assert_equal "ArgumentError", attrs["exception.type"]
      assert_equal 500, attrs["exception.message"].length
      assert_equal true, attrs["exception.expected"]
    end
  end

  test "flush_hash sanitizes values and writes to the span" do
    span = Struct.new(:sink) { def add_attributes(h) = self.sink = h }.new
    WideEvent.flush_hash(
      { "s" => "x", "i" => 1, "f" => 1.5, "b" => true, "sym" => :y,
        "t" => Time.utc(2026, 7, 5, 12), "n" => nil, "o" => Object.new },
      span
    )
    captured = span.sink
    assert_equal "x", captured["s"]
    assert_equal 1, captured["i"]
    assert_in_delta 1.5, captured["f"]
    assert_equal true, captured["b"]
    assert_equal "y", captured["sym"]
    assert_equal "2026-07-05T12:00:00.000Z", captured["t"]
    refute captured.key?("n")
    assert_kind_of String, captured["o"]
  end

  test "flush_hash is safe with nil span and raising span" do
    assert_nothing_raised { WideEvent.flush_hash({ "a" => 1 }, nil) }
    boom = Object.new.tap { |o| def o.add_attributes(_) = raise("boom") }
    assert_nothing_raised { WideEvent.flush_hash({ "a" => 1 }, boom) }
  end

  test "uptime_attributes shape" do
    attrs = WideEvent.uptime_attributes
    assert_kind_of Integer, attrs["uptime_sec"]
    assert_kind_of Float, attrs["uptime_sec_log10"]
  end

  test "set swallows errors when the store cannot be merged into" do
    WideEvent.with do |attrs|
      attrs.freeze
      assert_nothing_raised { WideEvent.set("a" => 1) }
    end
  end

  test "phase re-raises exceptions from the yielded block but never raises from its own telemetry" do
    WideEvent.with do |attrs|
      assert_raises(ArgumentError) do
        WideEvent.phase("x") { raise ArgumentError }
      end

      attrs.freeze
      assert_nothing_raised do
        assert_raises(ArgumentError) do
          WideEvent.phase("x") { raise ArgumentError }
        end
      end
    end
  end

  test "flush delegates to the configured sink" do
    sink = WideEvent::Sinks::Memory.new
    WideEvent.configure { |c| c.sink = sink }
    WideEvent.flush({ "a" => 1 })
    assert_equal [ { "a" => 1 } ], sink.events
  end

  test "flush skips empty hashes and never raises" do
    sink = Object.new.tap { |o| def o.flush(_) = raise("boom") }
    WideEvent.configure { |c| c.sink = sink }
    assert_nothing_raised { WideEvent.flush({ "a" => 1 }) }
    memory = WideEvent::Sinks::Memory.new
    WideEvent.configure { |c| c.sink = memory }
    WideEvent.flush({})
    WideEvent.flush(nil)
    assert_empty memory.events
  end

  test "internal errors are routed to the configured error handler" do
    seen = []
    WideEvent.configure { |c| c.error_handler = ->(e, msg) { seen << [ e.class, msg ] } }
    WideEvent.with do |attrs|
      attrs.freeze
      WideEvent.set("a" => 1)
    end
    assert_equal [ [ FrozenError, "set" ] ], seen
  end

  test "a raising error handler is itself swallowed" do
    WideEvent.configure { |c| c.error_handler = ->(_e, _msg) { raise "handler boom" } }
    WideEvent.with do |attrs|
      attrs.freeze
      assert_nothing_raised { WideEvent.set("a" => 1) }
    end
  end
end
