require "bundler/gem_tasks"
require "minitest/test_task"

Minitest::TestTask.create do |t|
  t.libs << "test" << "lib"
  t.test_globs = [ "test/**/*_test.rb" ]
end

task default: :test
