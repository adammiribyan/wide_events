require "rails/generators/base"
require "yaml"
require "tempfile"
require "fileutils"
require "ipaddr"
require "wide_event/version"
require "wide_event/kamal/deploy_editor"
require "wide_event/kamal/secrets_editor"

module WideEvents
  module Generators
    # Wires the bundled wide_events store container into a host app's Kamal
    # deployment: an accessory in config/deploy.yml plus durable ingest/query
    # tokens under .kamal/. Delegates every structural decision to
    # WideEvent::Kamal::DeployEditor and WideEvent::Kamal::SecretsEditor so
    # this class only handles option parsing, Thor plumbing, and applying
    # both edits as a single transaction: nothing is written to disk unless
    # every preflight check on both editors has already succeeded.
    class StoreGenerator < Rails::Generators::Base
      source_root File.expand_path("templates", __dir__)

      DNS_NAME_RE = /\A(?=.{1,253}\z)[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*\z/

      class_option :host, type: :string, desc: "SSH host/IP of the store VM (IP address or DNS name)"
      class_option :hostname, type: :string, desc: "Public DNS hostname for the telemetry endpoint (no scheme/path)"
      class_option :"retention-days", type: :string, default: "30", desc: "Event retention window, in days"
      class_option :service, type: :string, desc: "Kamal service name (defaults to config/deploy.yml's service)"
      class_option :environment, type: :string, default: "production", desc: "WIDE_EVENTS_ENVIRONMENT value"

      def generate_accessory
        host = options["host"]
        hostname = options["hostname"]
        retention_days = options["retention-days"]

        return unless valid_options?(host, hostname, retention_days)

        deploy_path = File.join(destination_root, "config/deploy.yml")
        unless File.exist?(deploy_path)
          say_status :error, "config/deploy.yml not found - run `kamal init` first, then re-run this generator", :red
          return
        end

        service = options["service"] || default_service_name(deploy_path)
        image = "ghcr.io/adammiribyan/wide-events-store:#{WideEvent::VERSION}"

        deploy_result = WideEvent::Kamal::DeployEditor.new(
          File.read(deploy_path),
          service: service,
          host: host,
          hostname: hostname,
          environment: options["environment"],
          retention_days: retention_days.to_i,
          image: image
        ).apply

        if deploy_result.conflicts.any?
          say_status :refused, "config/deploy.yml was left unchanged - merge this by hand:", :red
          say deploy_result.snippet
          return
        end

        begin
          secrets_result = WideEvent::Kamal::SecretsEditor.new(root: destination_root).apply
        rescue WideEvent::Kamal::SecretsEditor::UnsafeTokenFile => e
          say_status :error, e.message, :red
          return
        end

        if deploy_result.changed?
          atomic_write(deploy_path, deploy_result.content)
          say_status :update, "config/deploy.yml"
        else
          say_status :identical, "config/deploy.yml"
        end

        secrets_result.write!
        say_status :kamal, "wide_events store accessory ready - run `kamal setup` then `kamal telemetry`"
      end

      private

      def valid_options?(host, hostname, retention_days)
        unless present?(host) && valid_host?(host)
          say_status :error, "--host must be an IP address or DNS name, got #{host.inspect}", :red
          return false
        end

        unless present?(hostname) && valid_hostname?(hostname)
          say_status :error, "--hostname must be a bare DNS name without scheme or path, got #{hostname.inspect}", :red
          return false
        end

        unless positive_integer?(retention_days)
          say_status :error, "--retention-days must be a positive integer, got #{retention_days.inspect}", :red
          return false
        end

        true
      end

      def present?(value)
        !value.nil? && !value.strip.empty?
      end

      def valid_host?(value)
        ip_address?(value) || DNS_NAME_RE.match?(value)
      end

      def valid_hostname?(value)
        return false if value.include?("/") || value.include?(":") || value.include?("@")

        DNS_NAME_RE.match?(value)
      end

      def ip_address?(value)
        IPAddr.new(value)
        true
      rescue IPAddr::Error
        false
      end

      def positive_integer?(value)
        value.to_s.match?(/\A[1-9][0-9]*\z/)
      end

      def default_service_name(deploy_path)
        parsed = YAML.safe_load(File.read(deploy_path), permitted_classes: [], aliases: false)
        service = parsed.is_a?(Hash) ? parsed["service"] : nil
        service || File.basename(destination_root)
      rescue Psych::Exception
        File.basename(destination_root)
      end

      def atomic_write(path, content)
        dir = File.dirname(path)
        mode = File.exist?(path) ? File.stat(path).mode & 0o777 : 0o644
        tmp = Tempfile.new(".wide_events_deploy", dir)
        tmp.write(content)
        tmp.close
        File.chmod(mode, tmp.path)
        FileUtils.mv(tmp.path, path)
      end
    end
  end
end
