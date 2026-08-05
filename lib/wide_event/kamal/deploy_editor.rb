require "yaml"
require "erb"

module WideEvent
  module Kamal
    # Applies the wide_events Kamal accessory to a host application's
    # config/deploy.yml without disturbing unrelated comments, ordering, or
    # sections. Detection of structure and conflicts uses Psych.safe_load;
    # every actual mutation is a targeted line insertion/replacement on the
    # original text so byte-identical content survives everywhere we did not
    # intentionally touch it.
    #
    # Anything the line-based patcher cannot safely reason about (ERB, YAML
    # aliases/merge keys, tabs, duplicate root keys, non-mapping env/
    # accessories/aliases, an existing non-owned wide_events accessory,
    # conflicting WIDE_EVENTS_* values, or any other shape this editor was
    # not built to understand) causes a refusal: the original content is
    # returned completely unchanged, alongside a complete mergeable snippet
    # the caller can hand to a human or an agent.
    class DeployEditor
      MINIMUM_VERSION = "2.10.0"
      BEGIN_MARKER = "# BEGIN wide_events store"
      END_MARKER = "# END wide_events store"

      CLEAR_VARS = %w[WIDE_EVENTS_URL WIDE_EVENTS_SERVICE WIDE_EVENTS_ENVIRONMENT].freeze
      SECRET_VARS = %w[WIDE_EVENTS_INGEST_TOKEN WIDE_EVENTS_QUERY_TOKEN].freeze

      ALIAS_TELEMETRY_KEY = "telemetry"
      ALIAS_TELEMETRY_LINE = 'telemetry: app exec -i --reuse "bin/rails wide_events:sql"'
      ALIAS_BACKUP_KEY = "wide-events-backup"
      ALIAS_BACKUP_LINE = 'wide-events-backup: accessory exec wide_events "wide-events admin backup"'

      ACCESSORY_TEMPLATE_PATH = File.expand_path(
        "../../generators/wide_events/store/templates/accessory.yml.erb", __dir__
      )

      ROOT_KEY_RE = /\A([A-Za-z_][A-Za-z0-9_.-]*):(.*)\n?\z/

      Result = Struct.new(:content, :changed, :snippet, :conflicts, keyword_init: true) do
        def changed?
          !!changed
        end
      end

      # Raised internally to unwind straight to a refusal; never escapes #apply.
      Refusal = Class.new(StandardError)

      def initialize(content, service:, host:, hostname:, environment: "production", retention_days: 30, image: nil)
        @original = content.to_s
        @service = service
        @host = host
        @hostname = hostname
        @environment = environment
        @retention_days = retention_days
        @image = image || "ghcr.io/adammiribyan/wide-events-store:#{WideEvent::VERSION}"
      end

      def apply
        preflight!
        lines = @original.lines
        changed = false

        changed |= apply_minimum_version(lines)

        env_range = root_block_range(lines, "env")
        changed |= apply_env_in_place(lines, env_range) if env_range

        aliases_range = root_block_range(lines, "aliases")
        changed |= apply_aliases_in_place(lines, aliases_range) if aliases_range

        accessories_range = root_block_range(lines, "accessories")
        changed |= apply_accessories_in_place(lines, accessories_range) if accessories_range

        changed |= append_env_section(lines) unless env_range
        changed |= append_aliases_section(lines) unless aliases_range
        changed |= append_accessories_section(lines) unless accessories_range

        new_content = lines.join
        verify_parseable!(new_content)

        Result.new(content: new_content, changed: changed, snippet: nil, conflicts: [])
      rescue Refusal => e
        Result.new(content: @original, changed: false, snippet: build_snippet, conflicts: [ e.message ])
      end

      private

      # ---- Preflight -----------------------------------------------------

      def preflight!
        raise Refusal, "config/deploy.yml contains ERB tags" if @original.include?("<%")
        raise Refusal, "config/deploy.yml contains tab characters" if @original.include?("\t")

        parsed = safe_parse(@original)
        raise Refusal, "config/deploy.yml's root document is not a mapping" unless parsed.is_a?(Hash)
        raise Refusal, "config/deploy.yml has duplicate root keys" if duplicate_root_keys?(@original)

        raise_if_inline_value!(parsed, "env")
        raise_if_inline_value!(parsed, "accessories")
        raise_if_inline_value!(parsed, "aliases")

        %w[env accessories aliases].each do |key|
          value = parsed[key]
          next if value.nil?

          raise Refusal, "config/deploy.yml's #{key} key is not a mapping" unless value.is_a?(Hash)
        end

        env = parsed["env"] || {}
        if env.key?("clear") && !env["clear"].is_a?(Hash)
          raise Refusal, "config/deploy.yml's env.clear key is not a mapping"
        end
        if env.key?("secret") && !env["secret"].is_a?(Array)
          raise Refusal, "config/deploy.yml's env.secret key is not a sequence"
        end

        clear = env["clear"] || {}
        CLEAR_VARS.each do |key|
          next unless clear.key?(key)

          raise Refusal, "config/deploy.yml already sets #{key} to a different value" if clear[key].to_s != clear_value_for(key)
        end

        accessories = parsed["accessories"] || {}
        if accessories.key?("wide_events") && !owned_accessory?(@original)
          raise Refusal, "config/deploy.yml already defines an accessories.wide_events block wide_events did not create"
        end

        minimum_version = parsed["minimum_version"]
        if minimum_version && !parseable_version?(minimum_version)
          raise Refusal, "config/deploy.yml's minimum_version (#{minimum_version.inspect}) is not a recognizable version"
        end

        parsed
      end

      def safe_parse(content)
        YAML.safe_load(content, permitted_classes: [], aliases: false)
      rescue Psych::Exception, ArgumentError => e
        raise Refusal, "config/deploy.yml could not be safely parsed (#{e.message})"
      end

      def raise_if_inline_value!(parsed, key)
        return unless parsed.key?(key)

        line = @original.lines.find { |l| (m = ROOT_KEY_RE.match(l)) && m[1] == key }
        return unless line

        remainder = ROOT_KEY_RE.match(line)[2].to_s.strip
        return if remainder.empty? || remainder.start_with?("#")

        raise Refusal, "config/deploy.yml's #{key} key uses an inline value this editor cannot merge into"
      end

      def duplicate_root_keys?(content)
        keys = content.lines.filter_map { |l| (m = ROOT_KEY_RE.match(l)) && m[1] }
        keys.size != keys.uniq.size
      end

      def parseable_version?(value)
        Gem::Version.new(value.to_s)
        true
      rescue ArgumentError
        false
      end

      def owned_accessory?(content)
        lines = content.lines
        range = root_block_range(lines, "accessories")
        return true unless range

        child_range = child_block_range(lines, range, "wide_events")
        return true unless child_range

        start_r, = child_range
        preceding = lines[0...start_r].reverse.find { |l| !l.strip.empty? }
        preceding&.strip == BEGIN_MARKER
      end

      def clear_value_for(key)
        case key
        when "WIDE_EVENTS_URL" then "https://#{@hostname}"
        when "WIDE_EVENTS_SERVICE" then @service.to_s
        when "WIDE_EVENTS_ENVIRONMENT" then @environment.to_s
        end
      end

      # ---- Line-range helpers ---------------------------------------------

      def child_key_regex(indent)
        /\A {#{indent}}([A-Za-z_][A-Za-z0-9_.-]*):(.*)\n?\z/
      end

      def root_block_range(lines, key)
        start_idx = lines.find_index { |l| (m = ROOT_KEY_RE.match(l)) && m[1] == key }
        return nil unless start_idx

        rest = lines[(start_idx + 1)..] || []
        offset = rest.find_index { |l| ROOT_KEY_RE.match(l) }
        end_idx = offset ? start_idx + 1 + offset : lines.length
        [ start_idx, end_idx ]
      end

      def child_block_range(lines, range, key, indent: 2)
        start_r, end_r = range
        regex = child_key_regex(indent)
        rel_start = ((start_r + 1)...end_r).find { |i| (m = regex.match(lines[i])) && m[1] == key }
        return nil unless rel_start

        rel_end = ((rel_start + 1)...end_r).find { |i| regex.match(lines[i]) }
        rel_end ||= end_r
        [ rel_start, rel_end ]
      end

      def mapping_keys(lines, range, indent: 2)
        regex = child_key_regex(indent)
        (range[0] + 1...range[1]).filter_map { |i| (m = regex.match(lines[i])) && m[1] }
      end

      def sequence_items(lines, range, indent: 4)
        regex = /\A {#{indent}}- (.+?)\s*\z/
        (range[0] + 1...range[1]).filter_map { |i| (m = regex.match(lines[i])) && m[1] }
      end

      def insert_before(lines, idx, block)
        lines.insert(idx, *block)
      end

      # A block's naive end boundary (the next real key at the same level,
      # or EOF) absorbs any trailing blank lines and standalone comments that
      # actually describe whatever comes *after* this block. Trims those off
      # so insertions land immediately after the block's real content
      # instead of after comments that belong to the next section.
      def content_end(lines, range)
        start_r, end_r = range
        idx = end_r
        while idx > start_r + 1
          previous = lines[idx - 1].strip
          break unless previous.empty? || previous.start_with?("#")

          idx -= 1
        end
        idx
      end

      def append_root_block(lines, block)
        if lines.any? && !lines.last.end_with?("\n")
          lines[-1] = "#{lines.last}\n"
        end
        lines << "\n" unless lines.empty? || lines.last == "\n"
        lines.concat(block)
      end

      # ---- env: clear/secret merge ---------------------------------------

      def apply_env_in_place(lines, _range)
        range = root_block_range(lines, "env")
        changed = ensure_env_clear(lines, range)
        range = root_block_range(lines, "env")
        changed |= ensure_env_secret(lines, range)
        changed
      end

      def ensure_env_clear(lines, env_range)
        clear_range = child_block_range(lines, env_range, "clear")
        if clear_range.nil?
          block = [
            "  #{BEGIN_MARKER}\n",
            "  clear:\n",
            *CLEAR_VARS.map { |k| "    #{k}: #{clear_value_for(k)}\n" },
            "  #{END_MARKER}\n"
          ]
          insert_before(lines, content_end(lines, env_range), block)
          return true
        end

        existing = mapping_keys(lines, clear_range, indent: 4)
        missing = CLEAR_VARS.reject { |k| existing.include?(k) }
        return false if missing.empty?

        block = [ "    #{BEGIN_MARKER}\n", *missing.map { |k| "    #{k}: #{clear_value_for(k)}\n" }, "    #{END_MARKER}\n" ]
        insert_before(lines, content_end(lines, clear_range), block)
        true
      end

      def ensure_env_secret(lines, env_range)
        secret_range = child_block_range(lines, env_range, "secret")
        if secret_range.nil?
          block = [
            "  #{BEGIN_MARKER}\n",
            "  secret:\n",
            *SECRET_VARS.map { |k| "    - #{k}\n" },
            "  #{END_MARKER}\n"
          ]
          insert_before(lines, content_end(lines, env_range), block)
          return true
        end

        existing = sequence_items(lines, secret_range, indent: 4)
        missing = SECRET_VARS.reject { |k| existing.include?(k) }
        return false if missing.empty?

        block = [ "    #{BEGIN_MARKER}\n", *missing.map { |k| "    - #{k}\n" }, "    #{END_MARKER}\n" ]
        insert_before(lines, content_end(lines, secret_range), block)
        true
      end

      def append_env_section(lines)
        block = [
          "#{BEGIN_MARKER}\n",
          "env:\n",
          "  clear:\n",
          *CLEAR_VARS.map { |k| "    #{k}: #{clear_value_for(k)}\n" },
          "  secret:\n",
          *SECRET_VARS.map { |k| "    - #{k}\n" },
          "#{END_MARKER}\n"
        ]
        append_root_block(lines, block)
        true
      end

      # ---- aliases merge ---------------------------------------------------

      def apply_aliases_in_place(lines, range)
        existing = mapping_keys(lines, range, indent: 2)
        missing_lines = []
        missing_lines << ALIAS_TELEMETRY_LINE unless existing.include?(ALIAS_TELEMETRY_KEY)
        missing_lines << ALIAS_BACKUP_LINE unless existing.include?(ALIAS_BACKUP_KEY)
        return false if missing_lines.empty?

        block = [ "  #{BEGIN_MARKER}\n", *missing_lines.map { |l| "  #{l}\n" }, "  #{END_MARKER}\n" ]
        insert_before(lines, content_end(lines, range), block)
        true
      end

      def append_aliases_section(lines)
        block = [
          "#{BEGIN_MARKER}\n",
          "aliases:\n",
          "  #{ALIAS_TELEMETRY_LINE}\n",
          "  #{ALIAS_BACKUP_LINE}\n",
          "#{END_MARKER}\n"
        ]
        append_root_block(lines, block)
        true
      end

      # ---- accessories.wide_events insertion -------------------------------

      def apply_accessories_in_place(lines, range)
        child_range = child_block_range(lines, range, "wide_events")
        return false if child_range

        block = [ "  #{BEGIN_MARKER}\n", *indented(wide_events_template_lines, 2), "  #{END_MARKER}\n" ]
        insert_before(lines, content_end(lines, range), block)
        true
      end

      def append_accessories_section(lines)
        block = [
          "#{BEGIN_MARKER}\n",
          "accessories:\n",
          *indented(wide_events_template_lines, 2),
          "#{END_MARKER}\n"
        ]
        append_root_block(lines, block)
        true
      end

      def wide_events_template_lines
        template = File.read(ACCESSORY_TEMPLATE_PATH)
        rendered = ERB.new(template, trim_mode: "-").result_with_hash(
          image: @image,
          host: @host,
          hostname: @hostname,
          service: @service,
          environment: @environment,
          retention_days: @retention_days,
          local_directory: "/var/lib/#{@service}-wide-events"
        )
        rendered.lines
      end

      def indented(lines_arr, spaces)
        prefix = " " * spaces
        lines_arr.map { |l| l.strip.empty? ? l : "#{prefix}#{l}" }
      end

      # ---- Snippet for refusal ---------------------------------------------

      def build_snippet
        lines = []
        lines << "minimum_version: #{MINIMUM_VERSION}"
        lines << ""
        lines << "env:"
        lines << "  clear:"
        CLEAR_VARS.each { |k| lines << "    #{k}: #{clear_value_for(k)}" }
        lines << "  secret:"
        SECRET_VARS.each { |k| lines << "    - #{k}" }
        lines << ""
        lines << "accessories:"
        lines.concat(indented(wide_events_template_lines, 2).map(&:chomp))
        lines << ""
        lines << "aliases:"
        lines << "  #{ALIAS_TELEMETRY_LINE}"
        lines << "  #{ALIAS_BACKUP_LINE}"
        "#{lines.join("\n")}\n"
      end

      # ---- Post-mutation safety net ----------------------------------------

      def verify_parseable!(content)
        YAML.safe_load(content, permitted_classes: [], aliases: false)
      rescue Psych::Exception => e
        raise "wide_events internal error: generated config/deploy.yml is not valid YAML (#{e.message})"
      end

      def apply_minimum_version(lines)
        idx = lines.find_index { |l| (m = ROOT_KEY_RE.match(l)) && m[1] == "minimum_version" }
        if idx
          m = ROOT_KEY_RE.match(lines[idx])
          existing = m[2].to_s.split("#").first.to_s.strip.gsub(/\A["']|["']\z/, "")
          if existing.empty? || Gem::Version.new(existing) < Gem::Version.new(MINIMUM_VERSION)
            lines[idx] = "minimum_version: #{MINIMUM_VERSION}\n"
            true
          else
            false
          end
        else
          lines.unshift("#{BEGIN_MARKER}\n", "minimum_version: #{MINIMUM_VERSION}\n", "#{END_MARKER}\n", "\n")
          true
        end
      end
    end
  end
end
