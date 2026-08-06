require "wide_event/store/client"
require "wide_event/store/formatter"

# `bin/rails wide_events:sql` — an interactive prompt or a piped SQL runner
# against the store's read-only POST /v1/query. Split out as a plain module
# (rather than inline task code, per registry.rake's style) purely because
# the piped/interactive/format branching needs to be independently
# testable without going through Rake::Task#invoke.
#
# Guarded so re-`load`ing this file (the railtie does so once per process,
# but tests `load` it once per example via `with_rake`) doesn't warn about
# reassigning the constants below.
unless defined?(WideEvent::Tasks::Store)
  module WideEvent
    module Tasks
      module Store
        FORMATS = %w[table json csv].freeze
        DEFAULT_FORMAT = "table"
        EXIT_WORDS = %w[quit exit].freeze
        PROMPT = "wide_events> "

        # `interactive:` defaults to `stdin.tty?`; `readline:` defaults to
        # the standard-library Readline. Both are overridable so tests can
        # drive the interactive branch without a real terminal.
        def self.run(stdin: $stdin, stdout: $stdout, readline: nil, interactive: nil)
          config = WideEvent.config
          abort(setup_guidance) if config.store_url.to_s.strip.empty? || config.store_query_token.to_s.strip.empty?

          client = WideEvent::Store::Client.new(
            url: config.store_url,
            ingest_token: config.store_ingest_token,
            query_token: config.store_query_token
          )
          format = resolve_format
          run_interactively = interactive.nil? ? stdin.tty? : interactive

          if run_interactively
            interactive_loop(client, format, stdout, readline || default_readline)
          else
            run_piped(client, format, stdin, stdout)
          end
        rescue WideEvent::Store::Client::Error, ArgumentError => e
          abort("wide_events:sql: #{e.message}")
        end

        def self.setup_guidance
          "wide_events:sql requires WIDE_EVENTS_URL and WIDE_EVENTS_QUERY_TOKEN to be set " \
            "(see config/initializers/wide_events.rb, or run `bin/rails generate wide_events:install`)."
        end
        private_class_method :setup_guidance

        def self.resolve_format
          format = ENV["WIDE_EVENTS_FORMAT"].to_s.downcase
          FORMATS.include?(format) ? format : DEFAULT_FORMAT
        end
        private_class_method :resolve_format

        def self.default_readline
          require "readline"
          Readline
        end
        private_class_method :default_readline

        def self.run_piped(client, format, stdin, stdout)
          sql = stdin.read.to_s.strip
          return if sql.empty?
          execute_and_render(client, sql, format, stdout)
        end
        private_class_method :run_piped

        # Accumulates lines until one ends with `;`, then runs that buffer
        # as a single statement — this is what makes multiline SQL work.
        # `quit` or `exit` typed alone at a fresh prompt (and EOF, i.e.
        # `readline` returning nil) end the session instead. A query error
        # prints and continues the loop rather than ending the session, so
        # one typo doesn't lose the rest of an interactive investigation.
        def self.interactive_loop(client, format, stdout, readline)
          buffer = +""
          loop do
            prompt = buffer.empty? ? PROMPT : ""
            line = readline.readline(prompt, true)
            break if line.nil?

            stripped = line.strip
            break if buffer.empty? && EXIT_WORDS.include?(stripped.downcase)

            buffer << line << "\n"
            next unless stripped.end_with?(";")

            sql = buffer.strip.sub(/;\z/, "").strip
            buffer = +""
            next if sql.empty?

            begin
              execute_and_render(client, sql, format, stdout)
            rescue WideEvent::Store::Client::Error, ArgumentError => e
              stdout.puts "error: #{e.message}"
            end
          end
        end
        private_class_method :interactive_loop

        def self.execute_and_render(client, sql, format, stdout)
          result = client.query(sql, format: :json)
          case format
          when "json" then WideEvent::Store::Formatter.json(result, io: stdout)
          when "csv" then WideEvent::Store::Formatter.csv(result, io: stdout)
          else WideEvent::Store::Formatter.table(result, io: stdout)
          end
        end
        private_class_method :execute_and_render
      end
    end
  end
end

namespace :wide_events do
  desc "Run SQL against the wide-events store (interactive prompt, or piped stdin)"
  task sql: :environment do
    WideEvent::Tasks::Store.run
  end
end
