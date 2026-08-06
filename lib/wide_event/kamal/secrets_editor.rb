require "securerandom"
require "fileutils"

module WideEvent
  module Kamal
    # Plans, then durably writes, the ingest/query token files a Kamal
    # deployment needs plus their references in .kamal/secrets and
    # .gitignore. #apply never touches the filesystem beyond reading and
    # stat-ing the two token files; it only decides what should exist.
    # Callers persist the plan with Result#write! once every other preflight
    # (such as WideEvent::Kamal::DeployEditor#apply) has also succeeded, so a
    # rejected deploy edit never leaves secret files behind.
    class SecretsEditor
      INGEST_TOKEN_PATH = ".kamal/wide-events-ingest-token"
      QUERY_TOKEN_PATH = ".kamal/wide-events-query-token"
      SECRETS_PATH = ".kamal/secrets"
      GITIGNORE_PATH = ".gitignore"

      SECRETS_LINES = [
        "WIDE_EVENTS_INGEST_TOKEN=$(cat #{INGEST_TOKEN_PATH})",
        "WIDE_EVENTS_QUERY_TOKEN=$(cat #{QUERY_TOKEN_PATH})"
      ].freeze

      GITIGNORE_LINES = [ INGEST_TOKEN_PATH, QUERY_TOKEN_PATH ].freeze

      # Raised when a token file exists but is empty or readable by group/
      # world, since either state means the value can no longer be trusted
      # as a private secret.
      class UnsafeTokenFile < StandardError; end

      Result = Struct.new(:root, :ingest_token, :query_token, :ingest_token_existing, :query_token_existing,
        keyword_init: true) do
        def write!
          write_token_file(INGEST_TOKEN_PATH, ingest_token) unless ingest_token_existing
          write_token_file(QUERY_TOKEN_PATH, query_token) unless query_token_existing
          ensure_lines(SECRETS_PATH, SECRETS_LINES)
          ensure_lines(GITIGNORE_PATH, GITIGNORE_LINES)
          self
        end

        private

        def write_token_file(relative_path, value)
          path = File.join(root, relative_path)
          FileUtils.mkdir_p(File.dirname(path))
          File.write(path, "#{value}\n")
          File.chmod(0600, path)
        end

        def ensure_lines(relative_path, required_lines)
          path = File.join(root, relative_path)
          existing = File.exist?(path) ? File.read(path) : ""
          existing_lines = existing.lines.map { |l| l.chomp("\n") }
          missing = required_lines.reject { |l| existing_lines.include?(l) }
          return if missing.empty?

          FileUtils.mkdir_p(File.dirname(path))
          content = existing.dup
          content << "\n" unless content.empty? || content.end_with?("\n")
          missing.each { |l| content << "#{l}\n" }
          File.write(path, content)
        end
      end

      def initialize(root:)
        @root = root
      end

      def apply
        ingest = plan_token(INGEST_TOKEN_PATH)
        query = plan_token(QUERY_TOKEN_PATH)
        Result.new(
          root: @root,
          ingest_token: ingest.fetch(:value),
          query_token: query.fetch(:value),
          ingest_token_existing: ingest.fetch(:existing),
          query_token_existing: query.fetch(:existing)
        )
      end

      private

      def plan_token(relative_path)
        path = File.join(@root, relative_path)
        return { value: SecureRandom.hex(32), existing: false } unless File.exist?(path)

        content = File.read(path).strip
        if content.empty?
          raise UnsafeTokenFile, "#{relative_path} exists but is empty; remove it or restore its value"
        end

        mode = File.stat(path).mode & 0o777
        if mode & 0o077 != 0
          raise UnsafeTokenFile,
            format("%s is group- or world-readable (mode %04o); fix with `chmod 0600 %s`", relative_path, mode,
              relative_path)
        end

        { value: content, existing: true }
      end
    end
  end
end
