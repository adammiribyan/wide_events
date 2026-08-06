ENV["TZ"] = "UTC"

# The suite reads UTF-8 fixtures and generated files (skills/*/SKILL.md among
# them) through plain File.read, which tags content with the default external
# encoding. Under a non-UTF-8 locale (LANG unset in CI containers, cron, or
# `env -i`) that default is US-ASCII, and every regex match against those
# bytes raises "invalid byte sequence in US-ASCII". Pin both encodings here so
# test outcomes never depend on the invoking shell's locale.
#
# Only default_external is pinned: that is what tags File.read results, and
# leaving default_internal nil keeps IO from transcoding, which the integration
# tests rely on when they move gzip bodies over sockets.
Encoding.default_external = Encoding::UTF_8

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
