require "test_helper"
require "wide_event/kamal/secrets_editor"

class KamalSecretsEditorTest < ActiveSupport::TestCase
  test "generates two distinct 64-character hex tokens and writes them mode 0600" do
    Dir.mktmpdir do |dir|
      WideEvent::Kamal::SecretsEditor.new(root: dir).apply.write!

      ingest_path = File.join(dir, ".kamal/wide-events-ingest-token")
      query_path = File.join(dir, ".kamal/wide-events-query-token")

      ingest = File.read(ingest_path).strip
      query = File.read(query_path).strip

      assert_match(/\A[0-9a-f]{64}\z/, ingest)
      assert_match(/\A[0-9a-f]{64}\z/, query)
      refute_equal ingest, query

      assert_equal 0o600, File.stat(ingest_path).mode & 0o777
      assert_equal 0o600, File.stat(query_path).mode & 0o777
    end
  end

  test "adds stable references to .kamal/secrets and .gitignore without exposing values" do
    Dir.mktmpdir do |dir|
      WideEvent::Kamal::SecretsEditor.new(root: dir).apply.write!

      secrets = File.read(File.join(dir, ".kamal/secrets"))
      assert_includes secrets, "WIDE_EVENTS_INGEST_TOKEN=$(cat .kamal/wide-events-ingest-token)"
      assert_includes secrets, "WIDE_EVENTS_QUERY_TOKEN=$(cat .kamal/wide-events-query-token)"

      gitignore = File.read(File.join(dir, ".gitignore"))
      assert_includes gitignore, ".kamal/wide-events-ingest-token"
      assert_includes gitignore, ".kamal/wide-events-query-token"
    end
  end

  test "never regenerates or overwrites an existing token file" do
    Dir.mktmpdir do |dir|
      WideEvent::Kamal::SecretsEditor.new(root: dir).apply.write!
      ingest_path = File.join(dir, ".kamal/wide-events-ingest-token")
      original = File.read(ingest_path)

      WideEvent::Kamal::SecretsEditor.new(root: dir).apply.write!

      assert_equal original, File.read(ingest_path)
    end
  end

  test "does not duplicate secrets or gitignore lines on repeat application" do
    Dir.mktmpdir do |dir|
      2.times { WideEvent::Kamal::SecretsEditor.new(root: dir).apply.write! }

      secrets_lines = File.readlines(File.join(dir, ".kamal/secrets"))
      gitignore_lines = File.readlines(File.join(dir, ".gitignore"))

      assert_equal 1, secrets_lines.count { |l| l.include?("WIDE_EVENTS_INGEST_TOKEN") }
      assert_equal 1, secrets_lines.count { |l| l.include?("WIDE_EVENTS_QUERY_TOKEN") }
      assert_equal 1, gitignore_lines.count { |l| l.strip == ".kamal/wide-events-ingest-token" }
      assert_equal 1, gitignore_lines.count { |l| l.strip == ".kamal/wide-events-query-token" }
    end
  end

  test "preserves unrelated existing lines in .kamal/secrets and .gitignore" do
    Dir.mktmpdir do |dir|
      FileUtils.mkdir_p(File.join(dir, ".kamal"))
      File.write(File.join(dir, ".kamal/secrets"), "KAMAL_REGISTRY_PASSWORD=$(cat .kamal/registry-password)\n")
      File.write(File.join(dir, ".gitignore"), "/log/*\n/tmp/*\n")

      WideEvent::Kamal::SecretsEditor.new(root: dir).apply.write!

      secrets = File.read(File.join(dir, ".kamal/secrets"))
      assert_includes secrets, "KAMAL_REGISTRY_PASSWORD=$(cat .kamal/registry-password)"
      assert_includes secrets, "WIDE_EVENTS_INGEST_TOKEN=$(cat .kamal/wide-events-ingest-token)"

      gitignore = File.read(File.join(dir, ".gitignore"))
      assert_includes gitignore, "/log/*"
      assert_includes gitignore, ".kamal/wide-events-ingest-token"
    end
  end

  test "aborts without writing anything when a token file exists but is empty" do
    Dir.mktmpdir do |dir|
      FileUtils.mkdir_p(File.join(dir, ".kamal"))
      empty_path = File.join(dir, ".kamal/wide-events-ingest-token")
      File.write(empty_path, "")
      File.chmod(0o600, empty_path)

      assert_raises(WideEvent::Kamal::SecretsEditor::UnsafeTokenFile) do
        WideEvent::Kamal::SecretsEditor.new(root: dir).apply
      end

      refute File.exist?(File.join(dir, ".kamal/wide-events-query-token"))
      refute File.exist?(File.join(dir, ".kamal/secrets"))
      refute File.exist?(File.join(dir, ".gitignore"))
    end
  end

  test "aborts without writing anything when a token file is group- or world-readable" do
    Dir.mktmpdir do |dir|
      FileUtils.mkdir_p(File.join(dir, ".kamal"))
      readable_path = File.join(dir, ".kamal/wide-events-ingest-token")
      File.write(readable_path, "a" * 64)
      File.chmod(0o644, readable_path)

      assert_raises(WideEvent::Kamal::SecretsEditor::UnsafeTokenFile) do
        WideEvent::Kamal::SecretsEditor.new(root: dir).apply
      end

      refute File.exist?(File.join(dir, ".kamal/wide-events-query-token"))
    end
  end
end
