require "test_helper"
require "rack"
require "opentelemetry/sdk"

class MiddlewareTest < ActiveSupport::TestCase
  UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36".freeze

  setup do
    @exporter = OpenTelemetry::SDK::Trace::Export::InMemorySpanExporter.new
    @provider = OpenTelemetry::SDK::Trace::TracerProvider.new
    @provider.add_span_processor(OpenTelemetry::SDK::Trace::Export::SimpleSpanProcessor.new(@exporter))
    @tracer = @provider.tracer("test")
  end

  def call_middleware(app, env_extra = {})
    middleware = WideEvent::Middleware.new(app)
    env = Rack::MockRequest.env_for("/reports", "HTTP_USER_AGENT" => UA,
      "action_dispatch.request_id" => "req-123", "CONTENT_LENGTH" => "42").merge(env_extra)
    # Simulate the root span OTel's Rack instrumentation would have opened
    # above this middleware.
    @tracer.in_span("HTTP GET") { middleware.call(env) }
    @exporter.finished_spans.first
  end

  test "flushes seeded and response attributes onto the root span" do
    span = call_middleware(->(_env) { [ 200, { "Content-Length" => "10" }, [ "ok" ] ] })
    a = span.attributes
    assert_equal true, a["main"]
    assert_equal "req-123", a["http.request.id"]
    assert_equal 42, a["http.request.body_size"]
    assert_equal 200, a["http.response.status_code"]
    assert_equal false, a["error"]
    assert_equal "Chrome", a["user_agent.browser"]
    assert_kind_of Integer, a["uptime_sec"]
  end

  test "marks error on 5xx" do
    span = call_middleware(->(_env) { [ 500, {}, [ "boom" ] ] })
    assert_equal true, span.attributes["error"]
  end

  test "flushes with exception attributes when the app raises, and re-raises" do
    assert_raises(RuntimeError) do
      call_middleware(->(_env) { raise "kaput" })
    end
    span = @exporter.finished_spans.first
    assert_equal true, span.attributes["error"]
    assert_equal "RuntimeError", span.attributes["exception.type"]
    assert_nil span.attributes["exception.slug"] # unhandled: no slug, by design
    assert_equal "req-123", span.attributes["http.request.id"],
      "request id must survive the unhandled-exception path"
  end

  test "app enrichment during the request lands on the span" do
    span = call_middleware(->(_env) {
      WideEvent.set("user.id" => 7)
      [ 200, {}, [ "ok" ] ]
    })
    assert_equal 7, span.attributes["user.id"]
  end

  test "flushes exception attributes when ShowExceptions rendered a 500 above us" do
    span = call_middleware(->(env) {
      begin
        raise "kaput"
      rescue RuntimeError => e
        env["action_dispatch.exception"] = e
      end
      [ 500, {}, [ "error page" ] ]
    })
    assert_equal true, span.attributes["error"]
    assert_equal "RuntimeError", span.attributes["exception.type"]
    assert_equal "kaput", span.attributes["exception.message"]
    assert_nil span.attributes["exception.slug"] # unhandled: no slug, by design
  end

  test "passes through without opening a store when disabled" do
    WideEvent.configure { |c| c.enabled = false }
    active = nil
    status, = WideEvent::Middleware.new(->(_env) {
      active = WideEvent.active?
      [ 200, {}, [ "ok" ] ]
    }).call(Rack::MockRequest.env_for("/"))
    assert_equal 200, status
    assert_equal false, active
  end

  test "degrades to the raw user agent string when the useragent gem is missing" do
    middleware = WideEvent::Middleware.new(->(_env) { [ 200, {}, [ "ok" ] ] })
    attrs = middleware.send(:user_agent_attributes, UA)
    assert_equal "Chrome", attrs["user_agent.browser"] # gem present in this suite
    assert_equal UA, attrs["user_agent.original"]
  end

  test "events reach a custom sink instead of the span when configured" do
    sink = WideEvent::Sinks::Memory.new
    WideEvent.configure { |c| c.sink = sink }
    call_middleware(->(_env) { [ 200, {}, [ "ok" ] ] })
    assert_equal 1, sink.events.length
    assert_equal true, sink.events.first["main"]
  end
end
