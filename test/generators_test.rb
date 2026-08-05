require "test_helper"
require "rails/generators"
require "rails/generators/test_case"
require "generators/wide_events/install/install_generator"
require "generators/wide_events/skills/skills_generator"
require "generators/wide_events/store/store_generator"

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

  DEFAULT_TEST_HELPER = <<~RUBY
    ENV["RAILS_ENV"] ||= "test"
    require_relative "../config/environment"
    require "rails/test_help"

    module ActiveSupport
      class TestCase
        parallelize(workers: :number_of_processors)
        fixtures :all
      end
    end
  RUBY

  test "wires WideEvent::TestHelper into a default test_helper.rb" do
    FileUtils.mkdir_p(File.join(destination_root, "test"))
    File.write(File.join(destination_root, "test/test_helper.rb"), DEFAULT_TEST_HELPER)
    run_generator
    assert_file "test/test_helper.rb", %r{require "rails/test_help"\nrequire "wide_event/test_helper"}
    assert_file "test/test_helper.rb", /class TestCase\n    include WideEvent::TestHelper/
    assert_file "test/test_helper.rb", /include WideEvent::TestHelper\n    teardown \{ assert_registered_wide_event_attributes \}/
  end

  test "prints manual instructions when test_helper.rb is missing" do
    output = run_generator
    assert_no_file "test/test_helper.rb"
    assert_match(/wide_event\/test_helper/, output)
    assert_match(/teardown \{ assert_registered_wide_event_attributes \}/, output)
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

class StoreGeneratorIsRegisteredTest < ActiveSupport::TestCase
  test "the store generator is discoverable under the wide_events namespace" do
    assert_equal WideEvents::Generators::StoreGenerator,
      Rails::Generators.find_by_namespace("wide_events:store")
  end
end
