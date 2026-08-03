ENV["TZ"] = "UTC"

require "active_support"
require "active_support/test_case"
require "active_support/core_ext"
require "active_support/testing/autorun"

require "wide_events"

ActiveSupport::TestCase.test_order = :random

class ActiveSupport::TestCase
  setup do
    WideEvent.reset_configuration!
    WideEvent.configure do |config|
      config.enabled = true
      config.sink = :otel
    end
  end
end
