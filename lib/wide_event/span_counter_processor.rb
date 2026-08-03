module WideEvent
  # Rolls finished dependency child spans up into the active wide event's
  # stats.* attributes: what makes "this request ran 742 queries" visible
  # without opening a trace. Instrumentation scopes map to dependency names
  # via config.span_scopes. Registered by the railtie when OTel is active.
  class SpanCounterProcessor
    def on_start(span, parent_context); end

    def on_finish(span)
      return unless WideEvent.active?

      dep = WideEvent.config.span_scopes[span.instrumentation_scope&.name]
      return unless dep

      duration_ms = (span.end_timestamp - span.start_timestamp) / 1_000_000.0
      WideEvent.count(dep, duration_ms.round(2))
    rescue StandardError => e
      WideEvent.handle_error(e, "span counter")
    end

    def force_flush(timeout: nil)
      OpenTelemetry::SDK::Trace::Export::SUCCESS
    end

    def shutdown(timeout: nil)
      OpenTelemetry::SDK::Trace::Export::SUCCESS
    end
  end
end
