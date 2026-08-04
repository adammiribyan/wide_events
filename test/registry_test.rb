require "test_helper"
require "tempfile"

class RegistryTest < ActiveSupport::TestCase
  APP_YAML = <<~YAML
    report.format:
      type: string
      set_by: ReportsController#create
      pii: none
      notes: pdf or csv
    "feature_flag.*":
      type: [boolean, string]
      set_by: feature flag evaluation
      pii: none
  YAML

  def with_registry(yaml = APP_YAML)
    Tempfile.create([ "registry", ".yml" ]) do |f|
      f.write(yaml)
      f.flush
      yield WideEvent::Registry.load(f.path)
    end
  end

  test "gem defaults cover the attributes the gem itself sets" do
    registry = WideEvent::Registry.defaults
    %w[main error uptime_sec http.request.id http.response.status_code
       user_agent.browser db.duration_ms job.class job.queue_latency_ms
       exception.slug exception.message].each do |key|
      assert registry.registered?(key), "#{key} must be registered by default"
    end
    assert registry.registered?("stats.postgres_query_count"), "stats.* glob"
    assert registry.registered?("cache.views"), "cache.* glob"
  end

  test "app entries merge over defaults" do
    with_registry do |registry|
      assert registry.registered?("report.format")
      assert registry.registered?("main"), "defaults must survive the merge"
      assert_equal "pdf or csv", registry.entries["report.format"]["notes"]
    end
  end

  test "glob entries match dynamic keys" do
    with_registry do |registry|
      assert registry.registered?("feature_flag.release-enable-overview-redesign")
      refute registry.registered?("feature_flag"), "glob must not match its own prefix"
      refute registry.registered?("totally.unknown")
    end
  end

  test "track records unregistered keys once and keeps registered keys out" do
    with_registry do |registry|
      registry.track("report.format")
      registry.track("unknown.attr")
      registry.track("unknown.attr")
      assert_equal [ "unknown.attr" ], registry.violations
    end
  end

  test "validate reports entries that are not hashes with a type" do
    with_registry("bad.attr:\nworse.attr: 12\n") do |registry|
      errors = registry.validate
      assert_equal 2, errors.length
      assert_match(/bad\.attr/, errors.join)
    end
  end

  test "to_markdown renders the registry as a table" do
    with_registry do |registry|
      doc = registry.to_markdown
      assert_match(/\| Attribute \|/, doc)
      assert_match(/\| `report\.format` \| string \| ReportsController#create \| none \| pdf or csv \|/, doc)
      assert_match(/`feature_flag\.\*`.*boolean, string/, doc)
      assert_match(/`main`/, doc, "defaults appear in the generated doc")
    end
  end

  test "to_markdown keeps a folded-scalar note on one row" do
    yaml = <<~YAML
      report.format:
        type: string
        set_by: ReportsController#create
        pii: none
        notes: >
          pdf or csv, whichever the
          requesting user picked
    YAML
    with_registry(yaml) do |registry|
      doc = registry.to_markdown
      assert_match(
        /^\| `report\.format` \| string \| ReportsController#create \| none \| pdf or csv, whichever the requesting user picked \|$/,
        doc
      )
      doc.each_line do |line|
        assert_equal 6, line.count("|"), "malformed table row: #{line.inspect}"
      end
    end
  end

  test "to_markdown escapes pipes so a value cannot open extra columns" do
    with_registry("report.format:\n  type: string\n  notes: pdf | csv\n") do |registry|
      assert_match(/\| `report\.format` \| string \|  \|  \| pdf \\\| csv \|/, registry.to_markdown)
    end
  end

  test "strict mode tracks unregistered attributes set during a unit of work" do
    Tempfile.create([ "registry", ".yml" ]) do |f|
      f.write(APP_YAML)
      f.flush
      WideEvent.configure do |c|
        c.registry_path = f.path
        c.strict = true
      end
      WideEvent.with do
        WideEvent.set("report.format" => "pdf", "unknown.attr" => 2)
        WideEvent.count("postgres_query", 1.0)
        WideEvent.phase("unregistered_phase") { :ok }
      end
      assert_equal [ "unknown.attr", "unregistered_phase.duration_ms" ],
        WideEvent.config.registry.violations.sort
    end
  end

  test "strict mode is inert when no registry is configured" do
    WideEvent.configure { |c| c.strict = true }
    assert_nothing_raised do
      WideEvent.with { WideEvent.set("anything.goes" => 1) }
    end
  end
end
