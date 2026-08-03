require "rails/generators/base"

module WideEvents
  module Generators
    # Copies the agent skills shipped with the gem into .claude/skills/ so
    # coding agents pick up the instrumentation and debugging conventions.
    class SkillsGenerator < Rails::Generators::Base
      SKILLS_ROOT = File.expand_path("../../../../skills", __dir__)

      source_root SKILLS_ROOT

      def copy_skills
        Dir[File.join(SKILLS_ROOT, "*", "SKILL.md")].sort.each do |path|
          name = File.basename(File.dirname(path))
          copy_file File.join(name, "SKILL.md"), ".claude/skills/#{name}/SKILL.md"
        end
      end
    end
  end
end
