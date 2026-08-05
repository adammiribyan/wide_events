require "test_helper"
require "yaml"
require "wide_event/kamal/deploy_editor"

class KamalDeployEditorTest < ActiveSupport::TestCase
  FIXTURES_ROOT = File.expand_path("fixtures/kamal", __dir__)

  def default_deploy
    File.read(File.join(FIXTURES_ROOT, "default_deploy.yml"))
  end

  def existing_sections_deploy
    File.read(File.join(FIXTURES_ROOT, "existing_sections.yml"))
  end

  def conflicting_deploy
    File.read(File.join(FIXTURES_ROOT, "conflicting_deploy.yml"))
  end

  def editor(content, **overrides)
    WideEvent::Kamal::DeployEditor.new(
      content,
      service: overrides.fetch(:service, "myapp"),
      host: overrides.fetch(:host, "203.0.113.20"),
      hostname: overrides.fetch(:hostname, "telemetry.example.com"),
      environment: overrides.fetch(:environment, "production"),
      retention_days: overrides.fetch(:retention_days, 30),
      image: overrides.fetch(:image, "ghcr.io/adammiribyan/wide-events-store:0.1.4")
    )
  end

  test "adds structured owned directory and minimum Kamal version idempotently" do
    first = editor(default_deploy).apply
    second = editor(first.content).apply
    assert_equal first.content, second.content
    parsed = YAML.safe_load(first.content)
    assert_equal "2.10.0", parsed.fetch("minimum_version")
    assert_equal "1000:1000", parsed.dig("accessories", "wide_events", "directories", 0, "owner")
  end

  test "refuses ERB and prints a complete snippet without partial writes" do
    result = editor("service: <%= ENV.fetch('SERVICE') %>\n").apply
    refute result.changed?
    assert_includes result.snippet, "wide_events:"
  end

  test "preserves unrelated comments and ordering" do
    result = editor(default_deploy).apply
    assert_includes result.content, "# Name of your application. Used to uniquely configure containers."
    assert_includes result.content, "# accessories:\n#   db:\n#     image: mysql:8.0\n#     host: 192.168.0.2\n"
    assert_includes result.content, "service: my_app\n"
  end

  test "sets the four app env variables under the existing env key" do
    result = editor(default_deploy).apply
    parsed = YAML.safe_load(result.content)
    assert_equal "https://telemetry.example.com", parsed.dig("env", "clear", "WIDE_EVENTS_URL")
    assert_equal "myapp", parsed.dig("env", "clear", "WIDE_EVENTS_SERVICE")
    assert_equal "production", parsed.dig("env", "clear", "WIDE_EVENTS_ENVIRONMENT")
    assert_includes parsed.dig("env", "secret"), "WIDE_EVENTS_INGEST_TOKEN"
    assert_includes parsed.dig("env", "secret"), "WIDE_EVENTS_QUERY_TOKEN"
    assert_includes parsed.dig("env", "secret"), "RAILS_MASTER_KEY"
  end

  test "adds telemetry and wide-events-backup aliases without disturbing existing ones" do
    result = editor(default_deploy).apply
    parsed = YAML.safe_load(result.content)
    assert_equal 'app exec -i --reuse "bin/rails wide_events:sql"', parsed.dig("aliases", "telemetry")
    assert_equal 'accessory exec wide_events "wide-events admin backup"', parsed.dig("aliases", "wide-events-backup")
    assert_equal 'app exec --interactive --reuse "bin/rails console"', parsed.dig("aliases", "console")
  end

  test "pins the exact store image" do
    result = editor(default_deploy, image: "ghcr.io/adammiribyan/wide-events-store:0.1.4").apply
    parsed = YAML.safe_load(result.content)
    assert_equal "ghcr.io/adammiribyan/wide-events-store:0.1.4", parsed.dig("accessories", "wide_events", "image")
  end

  test "merges into existing sections without disturbing an unrelated accessory or a higher minimum_version" do
    first = editor(existing_sections_deploy).apply
    second = editor(first.content).apply
    assert_equal first.content, second.content

    parsed = YAML.safe_load(first.content)
    assert_equal "2.12.0", parsed.fetch("minimum_version")
    assert_equal "info", parsed.dig("env", "clear", "RAILS_LOG_LEVEL")
    assert_equal "mysql:8.0", parsed.dig("accessories", "db", "image")
    assert_equal "my_app", parsed.dig("accessories", "db", "env", "clear", "MYSQL_DATABASE")
    assert_equal "myapp", parsed.dig("env", "clear", "WIDE_EVENTS_SERVICE")
    assert_equal "1000:1000", parsed.dig("accessories", "wide_events", "directories", 0, "owner")
    assert_equal 'app exec --interactive --reuse "bin/rails console"', parsed.dig("aliases", "console")
    assert_equal 'app exec -i --reuse "bin/rails wide_events:sql"', parsed.dig("aliases", "telemetry")
  end

  test "refuses a conflicting WIDE_EVENTS_* value and leaves the file byte-for-byte unchanged" do
    result = editor(conflicting_deploy).apply
    refute result.changed?
    assert_equal conflicting_deploy, result.content
    assert_includes result.snippet, "wide_events:"
    assert result.conflicts.any? { |c| c.include?("WIDE_EVENTS_SERVICE") }
  end

  test "refuses tabs and leaves the file byte-for-byte unchanged" do
    content = "service: my_app\nenv:\n\tclear: {}\n"
    result = editor(content).apply
    refute result.changed?
    assert_equal content, result.content
  end

  test "refuses duplicate root keys and leaves the file byte-for-byte unchanged" do
    content = "service: my_app\nservice: other_app\n"
    result = editor(content).apply
    refute result.changed?
    assert_equal content, result.content
  end

  test "refuses non-mapping env and leaves the file byte-for-byte unchanged" do
    content = "service: my_app\nenv: not_a_mapping\n"
    result = editor(content).apply
    refute result.changed?
    assert_equal content, result.content
  end

  test "refuses YAML aliases and merge keys and leaves the file byte-for-byte unchanged" do
    content = "defaults: &defaults\n  a: 1\nenv:\n  <<: *defaults\n"
    result = editor(content).apply
    refute result.changed?
    assert_equal content, result.content
  end

  test "refuses an existing non-owned wide_events accessory and leaves the file byte-for-byte unchanged" do
    content = <<~YAML
      service: my_app
      accessories:
        wide_events:
          image: someone-elses-image:latest
    YAML
    result = editor(content).apply
    refute result.changed?
    assert_equal content, result.content
  end

  test "never lowers an already-current minimum_version" do
    content = "minimum_version: 2.10.0\nservice: my_app\n"
    result = editor(content).apply
    parsed = YAML.safe_load(result.content)
    assert_equal "2.10.0", parsed.fetch("minimum_version")
  end

  test "raises a lower minimum_version to 2.10.0" do
    content = "minimum_version: 2.4.0\nservice: my_app\n"
    result = editor(content).apply
    parsed = YAML.safe_load(result.content)
    assert_equal "2.10.0", parsed.fetch("minimum_version")
  end
end
