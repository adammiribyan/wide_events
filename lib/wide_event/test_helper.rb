module WideEvent
  # Minitest assertions for wide-event instrumentation. Require from your
  # test helper and include in ActiveSupport::TestCase:
  #
  #   require "wide_event/test_helper"
  #   class ActiveSupport::TestCase
  #     include WideEvent::TestHelper
  #   end
  module TestHelper
    # Runs the block inside an open wide event and asserts on the
    # accumulated attributes. Bare string arguments assert presence;
    # keyword-style pairs assert equality. Returns the attributes for
    # further assertions.
    #
    #   assert_wide_event("pdf_render.duration_ms", "report.format" => "pdf") do
    #     report.render
    #   end
    def assert_wide_event(*keys, **pairs, &block)
      attrs = WideEvent.with do |store|
        block.call
        store.dup
      end
      keys.each do |key|
        assert attrs.key?(key), "expected wide event to have #{key.inspect}, got keys #{attrs.keys.inspect}"
      end
      pairs.each do |key, value|
        assert_equal value, attrs[key], "wide event attribute #{key.inspect}"
      end
      attrs
    end

    # Swaps the sink for an in-memory one for the duration of the block and
    # returns every wide event flushed inside it. Use around code that runs
    # its own unit of work (jobs, full request dispatch).
    def capture_wide_events(&block)
      original = WideEvent.config.sink
      sink = Sinks::Memory.new
      WideEvent.config.sink = sink
      block.call
      sink.events
    ensure
      WideEvent.config.sink = original
    end

    # Fails if strict mode saw any attribute that isn't declared in the
    # registry since the last check, then resets the tally. The reset is what
    # makes a global teardown hook practical: the test that set the
    # undeclared attribute fails; later tests stay green.
    #
    #   class ActiveSupport::TestCase
    #     include WideEvent::TestHelper
    #     teardown { assert_registered_wide_event_attributes }
    #   end
    def assert_registered_wide_event_attributes
      registry = WideEvent.config.registry
      violations = registry ? registry.violations : []
      registry&.clear_violations!
      assert violations.empty?,
        "unregistered wide-event attributes #{violations.sort.inspect} - declare them in #{WideEvent.config.registry_path}"
    end
  end
end
