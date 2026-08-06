require "rails/generators/base"
require "generators/wide_events/skill_installer"

module WideEvents
  module Generators
    class InstallGenerator < Rails::Generators::Base
      include WideEvents::Generators::SkillInstaller

      source_root File.expand_path("templates", __dir__)

      def create_initializer
        copy_file "initializer.rb", "config/initializers/wide_events.rb"
      end

      def create_registry
        copy_file "registry.yml", "config/wide_event/registry.yml"
      end

      def install_skills
        copy_wide_events_skills
      end

      # Idempotent: a second run of this generator must not duplicate the
      # section in an AGENTS.md it already added it to.
      def add_agents_section
        section = File.read(File.expand_path("templates/agents_md_section.md", __dir__))
        path = File.join(destination_root, "AGENTS.md")
        if File.exist?(path)
          if File.read(path).include?(section.strip)
            say_status :identical, "AGENTS.md"
          else
            append_to_file "AGENTS.md", "\n#{section}"
          end
        else
          create_file "AGENTS.md", section
        end
      end

      def wire_test_helper
        path = File.join(destination_root, "test/test_helper.rb")
        content = File.exist?(path) ? File.read(path) : ""
        if content.match?(%r{require "rails/test_help"\n}) && content.match?(/class TestCase\n/)
          inject_into_file "test/test_helper.rb", "require \"wide_event/test_helper\"\n",
            after: %r{require "rails/test_help"\n}
          inject_into_file "test/test_helper.rb", <<~RUBY.indent(4),
            include WideEvent::TestHelper
            teardown { assert_registered_wide_event_attributes }
          RUBY
            after: /class TestCase\n/
        else
          say_status :skip, "test/test_helper.rb (not found or unrecognized) - wire the test helper manually:", :yellow
          say <<~MSG
                require "wide_event/test_helper"   # after rails/test_help
                include WideEvent::TestHelper      # inside ActiveSupport::TestCase
                teardown { assert_registered_wide_event_attributes }
          MSG
        end
      end
    end
  end
end
