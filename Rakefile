require "bundler/gem_tasks"
require "minitest/test_task"

Minitest::TestTask.create do |t|
  t.libs << "test" << "lib"
  # test/integration compiles the Go store and spawns real processes; it has
  # its own command (see CONTRIBUTING.md) and its own CI job, so it stays out
  # of the default suite.
  t.test_globs = Dir["test/**/*_test.rb"].reject { |path| path.start_with?("test/integration/") }
end

task default: :test
