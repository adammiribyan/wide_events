module WideEvent
  # Copies measurements Rails already makes into the active wide event via
  # ActiveSupport::Notifications. Subscribed once by the railtie.
  module Subscribers
    def self.subscribe!
      return if @subscribed

      @subscribed = true

      ActiveSupport::Notifications.subscribe("process_action.action_controller") do |event|
        next unless WideEvent.active?

        payload = event.payload
        WideEvent.set("http.route.controller" => "#{payload[:controller]}##{payload[:action]}")
        WideEvent.set("db.duration_ms" => payload[:db_runtime].round(2)) if payload[:db_runtime]
        WideEvent.set("view.duration_ms" => payload[:view_runtime].round(2)) if payload[:view_runtime]
      rescue StandardError => e
        WideEvent.handle_error(e, "process_action")
      end

      ActiveSupport::Notifications.subscribe("cache_read.active_support") do |event|
        next unless WideEvent.active?

        prefix = event.payload[:key].to_s.split(%r{[/:]}).first.to_s.gsub(/[^a-z0-9_]+/i, "_")[0, 30].downcase
        next if prefix.empty?

        key = "cache.#{prefix}"
        attrs = WideEvent.peek
        # Cap distinct cache namespaces, but keep refreshing ones already
        # tracked: otherwise a prefix freezes at whatever value it had
        # when the cap-th distinct namespace appeared.
        next if !attrs.key?(key) && attrs.keys.count { |k| k.start_with?("cache.") } >= WideEvent.config.max_cache_attrs

        WideEvent.set(key => event.payload[:hit] ? true : false)
      rescue StandardError => e
        WideEvent.handle_error(e, "cache_read")
      end
    end
  end
end
