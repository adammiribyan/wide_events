require "pathname"

namespace :wide_events do
  namespace :registry do
    # The generated doc gets committed, so its header must not carry the
    # machine-specific absolute path that `registry_path` holds.
    relative_to_root = lambda do |path|
      root = Rails.root if defined?(Rails) && Rails.respond_to?(:root)
      return path.to_s if root.nil?
      relative = Pathname.new(path.to_s).relative_path_from(Pathname.new(root.to_s)).to_s
      relative == ".." || relative.start_with?("../") ? path.to_s : relative
    rescue ArgumentError
      path.to_s
    end

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
               "Generated from `#{relative_to_root.call(WideEvent.config.registry_path)}` by `rake wide_events:registry:docs`. " \
               "Do not edit by hand.\n\n"
      File.write(path, header + registry.to_markdown)
      puts "wrote #{path}"
    end
  end
end
