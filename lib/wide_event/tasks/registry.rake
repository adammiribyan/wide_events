namespace :wide_events do
  namespace :registry do
    desc "Validate the wide-event attribute registry"
    task check: :environment do
      registry = WideEvent.config.registry
      abort("No wide-event registry at #{WideEvent.config.registry_path.inspect}: run `bin/rails generate wide_events:install`") if registry.nil?
      errors = registry.validate
      abort("wide-event registry invalid:\n  #{errors.join("\n  ")}") unless errors.empty?
      puts "wide-event registry OK (#{registry.entries.size} entries)"
    end

    desc "Generate the human-readable registry doc (default: docs/wide-events-registry.md)"
    task docs: :environment do
      registry = WideEvent.config.registry
      abort("No wide-event registry at #{WideEvent.config.registry_path.inspect}: run `bin/rails generate wide_events:install`") if registry.nil?
      path = ENV["WIDE_EVENTS_DOCS_PATH"] || "docs/wide-events-registry.md"
      require "fileutils"
      FileUtils.mkdir_p(File.dirname(path))
      header = "# Wide events attribute registry\n\n" \
               "Generated from `#{WideEvent.config.registry_path}` by `rake wide_events:registry:docs`. " \
               "Do not edit by hand.\n\n"
      File.write(path, header + registry.to_markdown)
      puts "wrote #{path}"
    end
  end
end
