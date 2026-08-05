require_relative "lib/wide_event/version"

Gem::Specification.new do |spec|
  spec.name = "wide_events"
  spec.version = WideEvent::VERSION
  spec.authors = [ "Adam Miribyan" ]
  spec.email = [ "adam@adam.am" ]

  spec.summary = "One wide telemetry event per request or job, on the OTel span you already export."
  spec.description = "Collects everything a Rails app knows about each request or job execution " \
                     "into one flat, high-cardinality event: route, user, build, query counts, " \
                     "cache hits, feature flags, phase timings, errors. Flushes onto the OpenTelemetry " \
                     "root span or a JSON log line. Ships a checkable attribute registry, test helpers, " \
                     "and agent skills for writing and querying instrumentation."
  spec.homepage = "https://github.com/adammiribyan/wide_events"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.2"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["changelog_uri"] = "#{spec.homepage}/blob/main/CHANGELOG.md"
  spec.metadata["bug_tracker_uri"] = "#{spec.homepage}/issues"
  spec.metadata["documentation_uri"] = "https://rubydoc.info/gems/wide_events"
  spec.metadata["rubygems_mfa_required"] = "true"

  spec.files = Dir["{lib,skills}/**/*", "LICENSE.txt", "README.md", "CHANGELOG.md"].select { |f| File.file?(f) }
  spec.require_paths = [ "lib" ]

  spec.add_dependency "activesupport", ">= 7.1"
  spec.add_dependency "rack", ">= 2.2"
  spec.add_dependency "csv"
end
