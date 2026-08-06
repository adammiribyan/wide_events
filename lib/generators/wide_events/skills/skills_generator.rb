require "rails/generators/base"
require "generators/wide_events/skill_installer"

module WideEvents
  module Generators
    # Copies the agent skills shipped with the gem into .claude/skills/ so
    # coding agents pick up the instrumentation, debugging, and store-setup
    # conventions. A standalone way to add or refresh them later; a fresh
    # `wide_events:install` already installs the same skills (see
    # InstallGenerator), via the same WideEvents::Generators::SkillInstaller
    # routine.
    class SkillsGenerator < Rails::Generators::Base
      include WideEvents::Generators::SkillInstaller

      source_root SkillInstaller::SKILLS_ROOT

      def copy_skills
        copy_wide_events_skills
      end
    end
  end
end
