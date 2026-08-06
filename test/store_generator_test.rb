require "test_helper"
require "rails/generators"
require "rails/generators/test_case"
require "yaml"
require "fileutils"
require "generators/wide_events/store/store_generator"
require "wide_event/version"

class StoreGeneratorTest < Rails::Generators::TestCase
  tests WideEvents::Generators::StoreGenerator
  destination File.expand_path("../tmp/generators", __dir__)
  setup :prepare_destination

  FIXTURES_ROOT = File.expand_path("fixtures/kamal", __dir__)
  DEFAULT_ARGS = %w[--host=203.0.113.20 --hostname=telemetry.example.com --retention-days=30].freeze

  def install_deploy_yml(fixture_name)
    FileUtils.mkdir_p(File.join(destination_root, "config"))
    FileUtils.cp(File.join(FIXTURES_ROOT, fixture_name), File.join(destination_root, "config/deploy.yml"))
  end

  test "adds the accessory, durable tokens, and aliases in one pass" do
    install_deploy_yml("default_deploy.yml")
    run_generator DEFAULT_ARGS

    assert_file "config/deploy.yml" do |content|
      parsed = YAML.safe_load(content)
      assert_equal "ghcr.io/adammiribyan/wide-events-store:#{WideEvent::VERSION}",
        parsed.dig("accessories", "wide_events", "image")
      assert_equal "1000:1000", parsed.dig("accessories", "wide_events", "directories", 0, "owner")
      assert_equal "203.0.113.20", parsed.dig("accessories", "wide_events", "host")
      assert_equal "telemetry.example.com", parsed.dig("accessories", "wide_events", "proxy", "host")
      assert_equal 'app exec -i --reuse "bin/rails wide_events:sql"', parsed.dig("aliases", "telemetry")
    end

    ingest_path = File.join(destination_root, ".kamal/wide-events-ingest-token")
    query_path = File.join(destination_root, ".kamal/wide-events-query-token")
    assert_match(/\A[0-9a-f]{64}\z/, File.read(ingest_path).strip)
    assert_match(/\A[0-9a-f]{64}\z/, File.read(query_path).strip)
    assert_equal 0o600, File.stat(ingest_path).mode & 0o777

    secrets = File.read(File.join(destination_root, ".kamal/secrets"))
    assert_includes secrets, "WIDE_EVENTS_INGEST_TOKEN=$(cat .kamal/wide-events-ingest-token)"

    gitignore = File.read(File.join(destination_root, ".gitignore"))
    assert_includes gitignore, ".kamal/wide-events-ingest-token"
  end

  test "is idempotent: a second run keeps deploy.yml and tokens unchanged" do
    install_deploy_yml("default_deploy.yml")
    run_generator DEFAULT_ARGS
    deploy_after_first = File.read(File.join(destination_root, "config/deploy.yml"))
    ingest_after_first = File.read(File.join(destination_root, ".kamal/wide-events-ingest-token"))

    run_generator DEFAULT_ARGS

    assert_equal deploy_after_first, File.read(File.join(destination_root, "config/deploy.yml"))
    assert_equal ingest_after_first, File.read(File.join(destination_root, ".kamal/wide-events-ingest-token"))
  end

  test "derives the service name from the existing deploy.yml" do
    install_deploy_yml("default_deploy.yml")
    run_generator DEFAULT_ARGS

    assert_file "config/deploy.yml" do |content|
      parsed = YAML.safe_load(content)
      assert_equal "my_app", parsed.dig("env", "clear", "WIDE_EVENTS_SERVICE")
      assert_equal "/var/lib/my_app-wide-events", parsed.dig("accessories", "wide_events", "directories", 0, "local")
    end
  end

  test "refuses without touching any file when config/deploy.yml conflicts" do
    install_deploy_yml("conflicting_deploy.yml")
    original = File.read(File.join(destination_root, "config/deploy.yml"))

    output = run_generator DEFAULT_ARGS

    assert_equal original, File.read(File.join(destination_root, "config/deploy.yml"))
    assert_match(/wide_events:/, output)
    assert_no_file ".kamal/wide-events-ingest-token"
    assert_no_file ".kamal/secrets"
  end

  test "errors without touching any file when config/deploy.yml is missing" do
    output = run_generator DEFAULT_ARGS

    assert_match(/kamal init/, output)
    assert_no_file "config/deploy.yml"
    assert_no_file ".kamal/wide-events-ingest-token"
  end

  test "errors and writes nothing when --host is invalid" do
    install_deploy_yml("default_deploy.yml")
    output = run_generator %w[--host=not_a_valid_host! --hostname=telemetry.example.com --retention-days=30]

    assert_match(/--host/, output)
    original = File.read(File.join(FIXTURES_ROOT, "default_deploy.yml"))
    assert_equal original, File.read(File.join(destination_root, "config/deploy.yml"))
    assert_no_file ".kamal/wide-events-ingest-token"
  end

  test "errors and writes nothing when --hostname includes a scheme" do
    install_deploy_yml("default_deploy.yml")
    output = run_generator %w[--host=203.0.113.20 --hostname=https://telemetry.example.com --retention-days=30]

    assert_match(/--hostname/, output)
    assert_no_file ".kamal/wide-events-ingest-token"
  end

  test "errors and writes nothing when --retention-days is not a positive integer" do
    install_deploy_yml("default_deploy.yml")
    output = run_generator %w[--host=203.0.113.20 --hostname=telemetry.example.com --retention-days=0]

    assert_match(/--retention-days/, output)
    assert_no_file ".kamal/wide-events-ingest-token"
  end

  test "refuses cleanly instead of crashing when config/deploy.yml is a shape the editor cannot patch" do
    FileUtils.mkdir_p(File.join(destination_root, "config"))
    original = "service: my_app\nenv:\n  secret:\n  - RAILS_MASTER_KEY\n"
    File.write(File.join(destination_root, "config/deploy.yml"), original)

    output = run_generator DEFAULT_ARGS

    assert_match(/wide_events:/, output)
    assert_equal original, File.read(File.join(destination_root, "config/deploy.yml"))
    assert_no_file ".kamal/wide-events-ingest-token"
    assert_no_file ".kamal/secrets"
  end

  test "updates the pinned image and settings when regenerated after a gem upgrade" do
    install_deploy_yml("default_deploy.yml")
    run_generator DEFAULT_ARGS
    old_content = File.read(File.join(destination_root, "config/deploy.yml"))
    old_parsed = YAML.safe_load(old_content)
    assert_equal "203.0.113.20", old_parsed.dig("accessories", "wide_events", "host")

    run_generator %w[--host=198.51.100.9 --hostname=telemetry.example.com --retention-days=90]

    new_parsed = YAML.safe_load(File.read(File.join(destination_root, "config/deploy.yml")))
    assert_equal "198.51.100.9", new_parsed.dig("accessories", "wide_events", "host")
    assert_equal 90, new_parsed.dig("accessories", "wide_events", "env", "clear", "WIDE_EVENTS_RETENTION_DAYS")
  end
end
