source "https://rubygems.org"
git_source(:github) { |repo| "https://github.com/#{repo}.git" }

gemspec

gem "rake"
gem "minitest"
gem "appraisal"

# Soft dependencies exercised by the test suite.
gem "activejob"
gem "railties"
gem "opentelemetry-sdk"
gem "useragent"

group :rubocop do
  gem "rubocop-rails-omakase", require: false
end
