module WideEvent
  module Sinks
    # Captures flushed wide events in memory, for tests.
    class Memory
      attr_reader :events

      def initialize
        @events = []
      end

      def flush(attrs)
        @events << attrs.dup
      end

      def clear
        @events.clear
      end
    end
  end
end
