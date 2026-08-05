begin
  require "useragent"
rescue LoadError
  # Optional: without the useragent gem only user_agent.original is set.
end

module WideEvent
  # Opens the wide-event accumulator for each request and flushes it to the
  # configured sink as the response unwinds. With the OTel sink, the flush
  # target is the root span opened by OTel's Rack instrumentation at the top
  # of the stack. Must be inserted BELOW ActionDispatch::Executor: the
  # executor clears IsolatedExecutionState on completion, so above it the
  # store would be gone before the flush. The railtie handles placement.
  class Middleware
    def initialize(app)
      @app = app
    end

    def call(env)
      return @app.call(env) unless WideEvent.enabled?

      WideEvent.with do |attrs|
        seed(env)
        start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        begin
          status, headers, body = @app.call(env)
          ex = env["action_dispatch.exception"]
          WideEvent.set(
            "http.response.status_code" => status.to_i,
            "error" => status.to_i >= 500 || attrs["error"] == true || !ex.nil?
          )
          if ex
            # Rendered by ActionDispatch::ShowExceptions before this
            # middleware's rescue ever fires: error attrs WITHOUT a slug,
            # same convention as the unhandled-exception path below.
            WideEvent.set("error" => true, "exception.type" => ex.class.name,
                          "exception.message" => ex.message.to_s[0, 500])
          end
          [ status, headers, body ]
        rescue Exception => e
          # Unhandled exception: error attrs WITHOUT a slug: the standing
          # "instrument this rescue" query keys off the missing slug.
          WideEvent.set("error" => true, "exception.type" => e.class.name,
                        "exception.message" => e.message.to_s[0, 500])
          raise
        ensure
          # Read at flush time, on every path (success AND unhandled
          # exception): ActionDispatch::RequestId runs deeper in the stack
          # and mutates env in place, so the id doesn't exist at seed time
          # but does by the time any outcome unwinds back to here.
          WideEvent.set("http.request.id" => env["action_dispatch.request_id"] || env["HTTP_X_REQUEST_ID"])
          WideEvent.set("duration_ms" => ((Process.clock_gettime(Process::CLOCK_MONOTONIC) - start) * 1000.0).round(2))
          WideEvent.flush(attrs)
        end
      end
    end

    private

    def seed(env)
      WideEvent.set({
        "main" => true,
        "http.request.body_size" => env["CONTENT_LENGTH"]&.to_i
      }.merge(user_agent_attributes(env["HTTP_USER_AGENT"])).merge(WideEvent.uptime_attributes))
    end

    def user_agent_attributes(ua)
      return {} if ua.nil? || ua.empty?
      return { "user_agent.original" => ua[0, 300] } unless defined?(UserAgent)

      parsed = UserAgent.parse(ua)
      {
        "user_agent.original" => ua[0, 300],
        "user_agent.browser" => parsed.browser,
        "user_agent.browser_version" => parsed.version.to_s,
        "user_agent.os" => parsed.os,
        "user_agent.platform" => parsed.platform
      }
    rescue StandardError
      { "user_agent.original" => ua[0, 300] }
    end
  end
end
