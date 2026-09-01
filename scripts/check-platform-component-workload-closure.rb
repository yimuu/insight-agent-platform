#!/usr/bin/env ruby
# frozen_string_literal: true

require "open3"
require "yaml"

ROOT = File.expand_path("..", __dir__)
CHARTS = %w[
  insight-platform-gateway
  insight-platform-orchestration-worker
  insight-platform-registry-validation-worker
  insight-platform-model-worker
  insight-platform-capability-native-worker
  insight-platform-capability-remote-worker
  insight-platform-context-worker
  insight-platform-remote-context-worker
  insight-platform-mcp-host
  insight-platform-sandbox
  insight-platform-artifact
  insight-platform-security-egress
].freeze
EXPECTED_COUNTS = {
  "management_api" => 1,
  "runtime_api" => 1,
  "scheduler_recovery" => 1,
  "model_worker" => 1,
  "capability_native_worker" => 1,
  "capability_remote_worker" => 1,
  "registry_validation_worker" => 1,
  "context_worker" => 3,
  "context_dataset_worker" => 1,
  "mcp_host" => 4,
  "sandbox_dispatcher" => 1,
  "opensandbox_server" => 1,
  "opensandbox_controller" => 1,
  "artifact_gateway" => 1,
  "artifact_data_worker" => 1,
  "artifact_maintenance" => 1,
  "egress_secret_broker" => 2
}.freeze
DIGEST_IMAGE = /\A[^\s@]+@sha256:[0-9a-f]{64}\z/
CONFIG_DIGEST = /\Asha256:[0-9a-f]{64}\z/
CONFIG_DIGEST_ANNOTATION = "insight.platform/deployment-config-digest"

documents = CHARTS.flat_map do |chart|
  path = File.join(ROOT, "deploy", "helm", chart)
  rendered, error, status = Open3.capture3("helm", "template", chart.delete_prefix("insight-platform-"), path)
  abort("#{chart} failed to render: #{error}") unless status.success?

  YAML.load_stream(rendered).compact
end
failures = []
workloads = documents.select { |item| %w[Deployment DaemonSet].include?(item["kind"]) }
workloads = workloads.select do |item|
  item.dig("spec", "template", "metadata", "labels", "insight.platform/component-role")
end
by_role = workloads.group_by do |item|
  item.dig("spec", "template", "metadata", "labels", "insight.platform/component-role")
end

EXPECTED_COUNTS.each do |role, count|
  actual = by_role.fetch(role, []).length
  failures << "#{role} expected #{count} workload pools, found #{actual}" unless actual == count
end
(by_role.keys - EXPECTED_COUNTS.keys).each { |role| failures << "unknown component role #{role}" }

pdbs = documents.select { |item| item["kind"] == "PodDisruptionBudget" }
hpas = documents.select { |item| item["kind"] == "HorizontalPodAutoscaler" }
identities = {}
deployment_config_digests = []
by_role.each do |role, role_workloads|
  digests = []
  role_workloads.each do |workload|
    namespace = workload.dig("metadata", "namespace")
    name = workload.dig("metadata", "name")
    labels = workload.dig("spec", "template", "metadata", "labels") || {}
    config_digest = workload.dig("spec", "template", "metadata", "annotations", CONFIG_DIGEST_ANNOTATION)
    failures << "#{role}/#{name} deployment configuration digest is invalid" unless CONFIG_DIGEST.match?(config_digest.to_s)
    deployment_config_digests << config_digest
    pod = workload.dig("spec", "template", "spec") || {}
    account = pod["serviceAccountName"]
    identity = [namespace, account]
    previous = identities[identity]
    failures << "#{role}/#{name} shares ServiceAccount with #{previous}" if previous
    identities[identity] = "#{role}/#{name}"
    kubernetes_clients = %w[opensandbox_server opensandbox_controller]
    expected_automount = kubernetes_clients.include?(role)
    unless pod["automountServiceAccountToken"] == expected_automount
      failures << "#{role}/#{name} ServiceAccount token policy drifted"
    end

    containers = pod["containers"] || []
    failures << "#{role}/#{name} has no containers" if containers.empty?
    containers.each do |container|
      image = container["image"]
      failures << "#{role}/#{name}/#{container['name']} image is mutable" unless DIGEST_IMAGE.match?(image.to_s)
      digests << image.to_s.split("@").last if DIGEST_IMAGE.match?(image.to_s)
      %w[requests limits].each do |side|
        resources = container.dig("resources", side) || {}
        missing = %w[cpu memory ephemeral-storage] - resources.keys
        failures << "#{role}/#{name}/#{container['name']} #{side} misses #{missing.join(',')}" unless missing.empty?
      end
    end

    matching_pdbs = pdbs.select do |pdb|
      next false unless pdb.dig("metadata", "namespace") == namespace

      selector = pdb.dig("spec", "selector", "matchLabels") || {}
      !selector.empty? && selector.all? { |key, value| labels[key] == value }
    end
    failures << "#{role}/#{name} requires exactly one PDB" unless matching_pdbs.length == 1
    next unless workload["kind"] == "Deployment"

    matching_hpas = hpas.select do |hpa|
      target = hpa.dig("spec", "scaleTargetRef") || {}
      hpa.dig("metadata", "namespace") == namespace &&
        target == {"apiVersion" => "apps/v1", "kind" => "Deployment", "name" => name}
    end
    failures << "#{role}/#{name} requires exactly one HPA" unless matching_hpas.length == 1
  end
  failures << "#{role} workload pools use different candidate image digests" unless digests.uniq.length == 1
end
failures << "workload pools do not share one CandidateManifest deployment configuration digest" unless deployment_config_digests.uniq.length == 1

workload_namespaces = workloads.map { |item| item.dig("metadata", "namespace") }.uniq
workload_namespaces.each do |namespace|
  defaults = documents.select do |item|
    item["kind"] == "NetworkPolicy" && item.dig("metadata", "namespace") == namespace &&
      item.dig("metadata", "name") == "default-deny" && item.dig("spec", "podSelector") == {} &&
      (item.dig("spec", "policyTypes") || []).sort == %w[Egress Ingress] &&
      !item.dig("spec", "ingress") && !item.dig("spec", "egress")
  end
  failures << "#{namespace} requires exactly one bidirectional default-deny" unless defaults.length == 1
end

abort(failures.map { |failure| "component workload closure: #{failure}" }.join("\n")) unless failures.empty?

puts "Platform ComponentRole workload closure passed (16 roles, #{workloads.length} isolated pools)."
