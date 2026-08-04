require "test_helper"
require "rake"
require "tempfile"
require "tmpdir"
require "fileutils"
require "pathname"

class TasksTest < ActiveSupport::TestCase
  def with_rake
    rake = Rake::Application.new
    Rake.application = rake
    Rake::Task.define_task(:environment)
    load File.expand_path("../lib/wide_event/tasks/registry.rake", __dir__)
    yield rake
  end

  def with_rails_root(dir)
    defined_here = !defined?(Rails)
    Object.const_set(:Rails, Module.new) if defined_here
    raise "Rails.root is already defined; stubbing it here would clobber it" if Rails.respond_to?(:root)
    Rails.define_singleton_method(:root) { Pathname.new(dir) }
    yield
  ensure
    Rails.singleton_class.remove_method(:root)
    Object.send(:remove_const, :Rails) if defined_here
  end

  def with_registry_file(yaml)
    Tempfile.create([ "registry", ".yml" ]) do |f|
      f.write(yaml)
      f.flush
      WideEvent.configure { |c| c.registry_path = f.path }
      yield
    end
  end

  test "registry:check passes on a valid registry" do
    with_registry_file("report.format:\n  type: string\n") do
      with_rake do |rake|
        assert_output(/registry OK/) { rake["wide_events:registry:check"].invoke }
      end
    end
  end

  test "registry:check aborts on invalid entries" do
    with_registry_file("bad.attr:\n") do
      with_rake do |rake|
        error = assert_raises(SystemExit) do
          capture_io { rake["wide_events:registry:check"].invoke }
        end
        refute error.success?
      end
    end
  end

  test "registry:check aborts when no registry is configured" do
    with_rake do |rake|
      assert_raises(SystemExit) do
        capture_io { rake["wide_events:registry:check"].invoke }
      end
    end
  end

  test "registry:docs writes the generated table" do
    with_registry_file("report.format:\n  type: string\n  notes: pdf or csv\n") do
      Dir.mktmpdir do |dir|
        ENV["WIDE_EVENTS_DOCS_PATH"] = File.join(dir, "registry.md")
        with_rake do |rake|
          capture_io { rake["wide_events:registry:docs"].invoke }
        end
        doc = File.read(File.join(dir, "registry.md"))
        assert_match(/Generated from/, doc)
        assert_match(/`report\.format`/, doc)
        assert_match(/`main`/, doc, "gem defaults included")
      ensure
        ENV.delete("WIDE_EVENTS_DOCS_PATH")
      end
    end
  end

  test "registry:docs names the registry relative to the app root" do
    Dir.mktmpdir do |dir|
      registry_path = File.join(dir, "config", "wide_event", "registry.yml")
      FileUtils.mkdir_p(File.dirname(registry_path))
      File.write(registry_path, "report.format:\n  type: string\n")
      WideEvent.configure { |c| c.registry_path = registry_path }
      ENV["WIDE_EVENTS_DOCS_PATH"] = File.join(dir, "registry.md")
      with_rails_root(dir) do
        with_rake { |rake| capture_io { rake["wide_events:registry:docs"].invoke } }
      end
      doc = File.read(File.join(dir, "registry.md"))
      assert_match(%r{Generated from `config/wide_event/registry\.yml`}, doc)
      refute_includes doc, dir, "the committed doc must not carry a machine-specific path"
    ensure
      ENV.delete("WIDE_EVENTS_DOCS_PATH")
    end
  end

  test "registry:docs falls back to the raw path outside the app root" do
    with_registry_file("report.format:\n  type: string\n") do
      outside = WideEvent.config.registry_path
      Dir.mktmpdir do |dir|
        ENV["WIDE_EVENTS_DOCS_PATH"] = File.join(dir, "registry.md")
        with_rails_root(File.join(dir, "app")) do
          with_rake { |rake| capture_io { rake["wide_events:registry:docs"].invoke } }
        end
        assert_includes File.read(File.join(dir, "registry.md")), "Generated from `#{outside}`"
      ensure
        ENV.delete("WIDE_EVENTS_DOCS_PATH")
      end
    end
  end
end
