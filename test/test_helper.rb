ENV["TZ"] = "UTC"

require "active_support"
require "active_support/test_case"
require "active_support/core_ext"
require "active_support/testing/autorun"

require "pathname"

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

  # The gem's suite has no Rails app; registry-path resolution only needs
  # `Rails.root`.
  def with_rails_root(dir)
    defined_here = !defined?(Rails)
    Object.const_set(:Rails, Module.new) if defined_here
    raise "Rails.root is already defined; stubbing it here would clobber it" if Rails.respond_to?(:root)
    Rails.define_singleton_method(:root) { Pathname.new(dir) }
    yield
  ensure
    Rails.singleton_class.remove_method(:root)
    Object.send(:remove_const, :Rails) if defined_here
  end
end
