require "securerandom"

module WideEvent
  module Sinks
    # Delivers the wide event to the DuckDB store: builds the canonical
    # event envelope (WideEvent::Store::Envelope) and hands the resulting
    # JSON to the bounded, PID-aware sender. Never performs network I/O on
    # the calling (application) thread and never changes the custom-sink
    # interface: #flush(attrs) is the only method other sinks need.
    class Store
      def initialize(sender:, clock: -> { Time.now }, uuid: -> { SecureRandom.uuid }, env: ENV)
        @sender = sender
        @clock = clock
        @uuid = uuid
        @env = env
      end

      def flush(attrs)
        json = ::WideEvent::Store::Envelope.build(attrs, clock: @clock, uuid: @uuid, env: @env)
        @sender.enqueue(json)
        nil
      rescue StandardError => e
        WideEvent.handle_error(e, "store_sink_flush")
        nil
      end

      # Delegates to the sender's best-effort shutdown flush.
      def shutdown(timeout: ::WideEvent::Store::Sender::SHUTDOWN_TIMEOUT)
        @sender.shutdown(timeout: timeout)
      end
    end
  end
end
