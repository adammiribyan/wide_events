module WideEvents
  module Generators
    # Copies every bundled agent skill (skills/*/SKILL.md) into
    # .claude/skills/<name>/SKILL.md. Shared by InstallGenerator (so a fresh
    # `wide_events:install` sets up all three skills in one pass) and
    # SkillsGenerator (a standalone way to add or refresh them later), so
    # both stay byte-for-byte consistent and a new bundled skill only has to
    # be wired up in one place.
    #
    # `copy_file` accepts an absolute source path regardless of the
    # including generator's own `source_root`, so this mixes into either
    # generator unchanged.
    module SkillInstaller
      SKILLS_ROOT = File.expand_path("../../../skills", __dir__)

      def copy_wide_events_skills
        Dir[File.join(SKILLS_ROOT, "*", "SKILL.md")].sort.each do |path|
          name = File.basename(File.dirname(path))
          copy_file path, ".claude/skills/#{name}/SKILL.md"
        end
      end
    end
  end
end
