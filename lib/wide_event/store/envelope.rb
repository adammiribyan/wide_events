require "json"
require "socket"

module WideEvent
  module Store
    # Renders one flushed wide-event hash as the canonical event JSON the
    # DuckDB store's protocol decoder expects (store/internal/protocol/events.go):
    # an `id`, UTC RFC3339-with-milliseconds `occurred_at`, `kind`
    # ("request"/"job"), required `duration_ms`, optional `deployment`/
    # `source`, and an `attributes` map of sanitized primitives.
    #
    # Pure and synchronous: no I/O, no mutation of the input, and no
    # WideEvent.handle_error rescue of its own — callers that send events
    # over the network (Task 8) rescue StandardError around #build and route
    # it through WideEvent.handle_error themselves.
    module Envelope
      MAX_ATTRIBUTES = 256
      MAX_EVENT_BYTES = 64 * 1024

      class << self
        def build(attrs, clock:, uuid:, env:)
          attributes = WideEvent.sanitize(attrs)
          duration_ms = attributes.delete("duration_ms")
          raise ArgumentError, "duration_ms is required" unless duration_ms.is_a?(Numeric)

          if attributes.size > MAX_ATTRIBUTES
            raise ArgumentError, "event has #{attributes.size} attributes, max #{MAX_ATTRIBUTES}"
          end

          event = {
            "id" => uuid.call,
            "occurred_at" => clock.call.utc.iso8601(3),
            "kind" => attributes["job.class"] ? "job" : "request",
            "duration_ms" => duration_ms,
            "deployment" => deployment(env),
            "source" => source(env),
            "attributes" => attributes
          }.compact

          json = JSON.generate(event)
          if json.bytesize > MAX_EVENT_BYTES
            raise ArgumentError, "event is #{json.bytesize} bytes, max #{MAX_EVENT_BYTES}"
          end
          json.freeze
        end

        private

        def deployment(env)
          value = env["KAMAL_VERSION"]
          value if value.present?
        end

        def source(env)
          [ env["KAMAL_CONTAINER_NAME"], env["HOSTNAME"] ].find(&:present?) || Socket.gethostname
        end
      end
    end
  end
end
