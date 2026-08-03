require "test_helper"

class SubscribersTest < ActiveSupport::TestCase
  # subscribe! is process-global; subscribing once for this test class is
  # harmless for the rest of the suite because every handler no-ops unless a
  # wide event is active.
  SUBSCRIBED = begin
    WideEvent::Subscribers.subscribe!
    true
  end

  test "process_action copies rails' own timings" do
    WideEvent.with do
      ActiveSupport::Notifications.instrument("process_action.action_controller",
        controller: "ReportsController", action: "index", db_runtime: 12.345, view_runtime: 6.789) { }
      attrs = WideEvent.attributes
      assert_equal "ReportsController#index", attrs["http.route.controller"]
      assert_in_delta 12.35, attrs["db.duration_ms"]
      assert_in_delta 6.79, attrs["view.duration_ms"]
    end
  end

  test "process_action tolerates nil runtimes" do
    WideEvent.with do
      ActiveSupport::Notifications.instrument("process_action.action_controller",
        controller: "X", action: "y", db_runtime: nil, view_runtime: nil) { }
      assert_nil WideEvent.attributes["db.duration_ms"]
    end
  end

  test "cache_read records normalized hit/miss booleans, capped" do
    WideEvent.with do
      ActiveSupport::Notifications.instrument("cache_read.active_support",
        key: "views/reports/123-20260705", hit: true) { }
      assert_equal true, WideEvent.attributes["cache.views"]

      12.times do |i|
        ActiveSupport::Notifications.instrument("cache_read.active_support",
          key: "prefix#{i}/x", hit: false) { }
      end
      cache_keys = WideEvent.attributes.keys.count { |k| k.start_with?("cache.") }
      assert_operator cache_keys, :<=, WideEvent.config.max_cache_attrs
    end
  end

  test "cache_read keeps refreshing already-tracked prefixes after the cap" do
    WideEvent.with do
      ActiveSupport::Notifications.instrument("cache_read.active_support",
        key: "views/1", hit: true) { }
      assert_equal true, WideEvent.attributes["cache.views"]

      12.times do |i|
        ActiveSupport::Notifications.instrument("cache_read.active_support",
          key: "filler#{i}/x", hit: false) { }
      end

      ActiveSupport::Notifications.instrument("cache_read.active_support",
        key: "views/2", hit: false) { }
      assert_equal false, WideEvent.attributes["cache.views"],
        "tracked prefix must keep updating after the cap is reached"
    end
  end

  test "cache_read respects a custom cap" do
    WideEvent.configure { |c| c.max_cache_attrs = 2 }
    WideEvent.with do
      5.times do |i|
        ActiveSupport::Notifications.instrument("cache_read.active_support",
          key: "prefix#{i}/x", hit: false) { }
      end
      assert_equal 2, WideEvent.attributes.keys.count { |k| k.start_with?("cache.") }
    end
  end

  test "cache_read lowercases key prefixes" do
    WideEvent.with do
      ActiveSupport::Notifications.instrument("cache_read.active_support",
        key: "User/5", hit: false) { }
      assert_equal false, WideEvent.attributes["cache.user"]
    end
  end

  test "handlers no-op without an active wide event" do
    assert_nothing_raised do
      ActiveSupport::Notifications.instrument("process_action.action_controller",
        controller: "X", action: "y", db_runtime: 1.0) { }
    end
  end
end
