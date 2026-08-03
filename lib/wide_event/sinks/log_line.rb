require "json"
require "logger"

module WideEvent
  module Sinks
    # Emits the wide event as one JSON log line: the canonical-log-line
    # deployment for apps that don't run tracing.
    class LogLine
      def initialize(logger = nil)
        @logger = logger
      end

      def flush(attrs)
        line = { "timestamp" => Time.now.utc.iso8601(3) }.merge(WideEvent.sanitize(attrs))
        logger.info(JSON.generate(line))
      end

      private

      def logger
        @logger ||= WideEvent.config.logger ||
          (defined?(Rails) && Rails.respond_to?(:logger) && Rails.logger) ||
          Logger.new($stdout)
      end
    end
  end
end
