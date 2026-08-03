require "test_helper"
require "opentelemetry/sdk"

class SpanCounterProcessorTest < ActiveSupport::TestCase
  setup do
    @provider = OpenTelemetry::SDK::Trace::TracerProvider.new
    @provider.add_span_processor(WideEvent::SpanCounterProcessor.new)
  end

  test "counts pg child spans into stats when a wide event is active" do
    pg_tracer = @provider.tracer("OpenTelemetry::Instrumentation::PG")
    WideEvent.with do
      pg_tracer.in_span("SELECT reports") { }
      pg_tracer.in_span("SELECT reports") { }
      attrs = WideEvent.attributes
      assert_equal 2, attrs["stats.postgres_query_count"]
      assert_operator attrs["stats.postgres_query_duration_ms"], :>=, 0
    end
  end

  # Magnitude guard: live spans carry Integer epoch-nanosecond timestamps
  # (verified empirically on opentelemetry-sdk 1.x), so the ns->ms division
  # must land in real-milliseconds territory. A units mistake in either
  # direction (treating Time-subtraction seconds as ns, or forgetting the
  # division) is off by >=1e6 and fails this assertion.
  test "duration lands in milliseconds, not seconds or nanoseconds" do
    pg_tracer = @provider.tracer("OpenTelemetry::Instrumentation::PG")
    start = Time.now
    WideEvent.with do
      span = pg_tracer.start_span("SELECT reports", start_timestamp: start)
      span.finish(end_timestamp: start + 0.05)
      assert_in_delta 50.0, WideEvent.attributes["stats.postgres_query_duration_ms"], 1.0
    end
  end

  test "ignores unknown scopes and is silent without an active wide event" do
    other = @provider.tracer("SomethingElse")
    WideEvent.with do
      other.in_span("x") { }
      assert_nil WideEvent.attributes["stats.postgres_query_count"]
    end
    pg = @provider.tracer("OpenTelemetry::Instrumentation::PG")
    assert_nothing_raised { pg.in_span("SELECT") { } } # no store open
  end

  test "scope map is configurable" do
    WideEvent.configure do |c|
      c.span_scopes = c.span_scopes.merge("OpenTelemetry::Instrumentation::Redis" => "redis_command")
    end
    redis_tracer = @provider.tracer("OpenTelemetry::Instrumentation::Redis")
    WideEvent.with do
      redis_tracer.in_span("GET") { }
      assert_equal 1, WideEvent.attributes["stats.redis_command_count"]
    end
  end
end
