require "rails/generators/base"

module WideEvents
  module Generators
    class InstallGenerator < Rails::Generators::Base
      source_root File.expand_path("templates", __dir__)

      def create_initializer
        copy_file "initializer.rb", "config/initializers/wide_events.rb"
      end

      def create_registry
        copy_file "registry.yml", "config/wide_event/registry.yml"
      end

      def add_agents_section
        section = File.read(File.expand_path("templates/agents_md_section.md", __dir__))
        if File.exist?(File.join(destination_root, "AGENTS.md"))
          append_to_file "AGENTS.md", "\n#{section}"
        else
          create_file "AGENTS.md", section
        end
      end
    end
  end
end
