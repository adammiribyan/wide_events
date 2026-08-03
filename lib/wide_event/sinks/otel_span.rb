module WideEvent
  module Sinks
    # Writes the wide event onto the current OTel span: the root span of
    # the unit of work, since flush happens as the request/job unwinds.
    class OtelSpan
      def flush(attrs)
        return unless defined?(OpenTelemetry::Trace)
        WideEvent.flush_hash(attrs, OpenTelemetry::Trace.current_span)
      end
    end
  end
end
