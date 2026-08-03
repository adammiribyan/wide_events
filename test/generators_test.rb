require "test_helper"
require "rails/generators"
require "rails/generators/test_case"
require "generators/wide_events/install/install_generator"
require "generators/wide_events/skills/skills_generator"

class InstallGeneratorTest < Rails::Generators::TestCase
  tests WideEvents::Generators::InstallGenerator
  destination File.expand_path("../tmp/generators", __dir__)
  setup :prepare_destination

  test "creates the initializer and starter registry" do
    run_generator
    assert_file "config/initializers/wide_events.rb", /WideEvent\.configure/
    assert_file "config/wide_event/registry.yml", /Attributes your app sets/
  end

  test "creates AGENTS.md with the wide-events section when absent" do
    run_generator
    assert_file "AGENTS.md", /wide event/i
  end

  test "appends the wide-events section to an existing AGENTS.md" do
    FileUtils.mkdir_p(destination_root)
    File.write(File.join(destination_root, "AGENTS.md"), "# My app\n")
    run_generator
    assert_file "AGENTS.md", /# My app/
    assert_file "AGENTS.md", /wide event/i
  end
end

class SkillsGeneratorTest < Rails::Generators::TestCase
  tests WideEvents::Generators::SkillsGenerator
  destination File.expand_path("../tmp/generators", __dir__)
  setup :prepare_destination

  test "installs both agent skills" do
    run_generator
    assert_file ".claude/skills/instrumenting-wide-events/SKILL.md", /^---\nname: instrumenting-wide-events/
    assert_file ".claude/skills/debugging-with-wide-events/SKILL.md", /^---\nname: debugging-with-wide-events/
  end
end
