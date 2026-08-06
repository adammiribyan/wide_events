require "wide_event/setup/checker"

# `bin/rails wide_events:setup:check` — the idempotent resume command for
# the setting-up-wide-events-store skill and for any human re-running setup
# by hand. It is correct with no flags whether it runs before the first
# `kamal setup`, while waiting on DNS to propagate, or against an
# already-deployed store: WideEvent::Setup::Checker re-derives host,
# hostname, service, and tokens from the app's own generated files.
#
# Guarded so re-`load`ing this file (the railtie does so once per process,
# but tests `load` it once per example) doesn't warn about reassigning the
# constants below.
unless defined?(WideEvent::Tasks::Setup)
  module WideEvent
    module Tasks
      module Setup
        def self.run(stdout: $stdout, root: default_root)
          checker = WideEvent::Setup::Checker.new(root: root)
          result = checker.run
          stdout.puts "wide_events setup: #{result.state}"
          stdout.puts result.message if result.message && !result.message.empty?
          abort("wide_events setup check failed - see the message above") if result.state == "failed"
        end

        def self.default_root
          defined?(Rails) && Rails.respond_to?(:root) ? Rails.root.to_s : Dir.pwd
        end
        private_class_method :default_root
      end
    end
  end
end

namespace :wide_events do
  namespace :setup do
    desc "Check (and resume) the Wide Events store setup: Kamal version/config, the host directory, DNS, and the deployed endpoint"
    task check: :environment do
      WideEvent::Tasks::Setup.run
    end
  end
end
