require "yaml"

module WideEvent
  # The attribute schema: every wide-event attribute is declared in a YAML
  # registry (gem defaults merged with the app's registry.yml). Keys
  # containing `*` are globs for dynamic attributes (feature_flag.*).
  # In strict mode the accumulator reports every key it sees via #track;
  # unregistered keys collect in #violations for the test suite to assert on.
  class Registry
    DEFAULTS_PATH = File.expand_path("registry/defaults.yml", __dir__)

    class << self
      def defaults
        new(load_file(DEFAULTS_PATH))
      end

      def load(path)
        new(load_file(DEFAULTS_PATH).merge(load_file(path)))
      end

      private

      def load_file(path)
        YAML.safe_load_file(path) || {}
      end
    end

    attr_reader :entries

    def initialize(entries)
      @entries = entries
      @exact = entries.keys.reject { |k| k.include?("*") }.to_set
      @globs = entries.keys.select { |k| k.include?("*") }
      @violations = Set.new
      @mutex = Mutex.new
    end

    def registered?(key)
      @exact.include?(key) || @globs.any? { |glob| File.fnmatch(glob, key) }
    end

    def track(key)
      return if registered?(key)
      @mutex.synchronize { @violations << key }
      nil
    end

    def violations
      @violations.to_a
    end

    def validate
      entries.filter_map do |key, entry|
        unless entry.is_a?(Hash) && entry["type"]
          "#{key}: entry must be a mapping with at least a `type` field"
        end
      end
    end

    def to_markdown
      lines = [ "| Attribute | Type | Set by | PII | Notes |", "|---|---|---|---|---|" ]
      entries.each do |key, entry|
        entry = {} unless entry.is_a?(Hash)
        lines << "| `#{key}` | #{Array(entry["type"]).join(", ")} | #{entry["set_by"]} | #{entry["pii"]} | #{entry["notes"]} |"
      end
      lines.join("\n") + "\n"
    end
  end
end
