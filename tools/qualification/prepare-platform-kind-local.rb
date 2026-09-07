#!/usr/bin/env ruby
# frozen_string_literal: true

require "digest"
require "fileutils"
require "json"
require "optparse"
require "open3"
require "time"
require "yaml"

options = {}
OptionParser.new do |parser|
  parser.on("--seed-runtime PATH") { |value| options[:seed_runtime] = value }
  parser.on("--worker-binaries PATH") { |value| options[:worker_binaries] = value }
  parser.on("--qualification-tool PATH") { |value| options[:qualification_tool] = value }
  parser.on("--output PATH") { |value| options[:output] = value }
  parser.on("--git-commit SHA") { |value| options[:git_commit] = value }
  parser.on("--platform-image-digest DIGEST") { |value| options[:platform_image_digest] = value }
  parser.on("--platform-image-repository REPOSITORY") do |value|
    options[:platform_image_repository] = value
  end
  parser.on("--sandbox-runner-image-digest DIGEST") do |value|
    options[:sandbox_runner_image_digest] = value
  end
  parser.on("--sandbox-runner-image-repository REPOSITORY") do |value|
    options[:sandbox_runner_image_repository] = value
  end
  parser.on("--postgres-cidr CIDR") { |value| options[:postgres_cidr] = value }
  parser.on("--nats-cidr CIDR") { |value| options[:nats_cidr] = value }
  parser.on("--localstack-pod-cidr CIDR") { |value| options[:localstack_pod_cidr] = value }
  parser.on("--localstack-service-cidr CIDR") { |value| options[:localstack_service_cidr] = value }
  parser.on("--kubernetes-api-service-cidr CIDR") { |value| options[:kubernetes_api_service_cidr] = value }
  parser.on("--kubernetes-api-endpoint-cidr CIDR") { |value| options[:kubernetes_api_endpoint_cidr] = value }
  parser.on("--kubernetes-api-endpoint-port PORT", Integer) do |value|
    options[:kubernetes_api_endpoint_port] = value
  end
  parser.on("--kms-key-arn ARN") { |value| options[:kms_key_arn] = value }
  parser.on("--readiness-secret-arn ARN") { |value| options[:readiness_secret_arn] = value }
end.parse!

required = %i[
  seed_runtime worker_binaries qualification_tool output git_commit platform_image_digest platform_image_repository
  sandbox_runner_image_digest sandbox_runner_image_repository postgres_cidr nats_cidr
  localstack_pod_cidr localstack_service_cidr kubernetes_api_service_cidr
  kubernetes_api_endpoint_cidr kubernetes_api_endpoint_port kms_key_arn readiness_secret_arn
]
missing = required.reject { |name| options[name] }
abort "missing required options: #{missing.join(', ')}" unless missing.empty?

DIGEST = /\Asha256:[0-9a-f]{64}\z/
COMMIT = /\A[0-9a-f]{40}\z/
abort "platform image digest is invalid" unless DIGEST.match?(options[:platform_image_digest])
abort "Sandbox runner image digest is invalid" unless DIGEST.match?(options[:sandbox_runner_image_digest])
abort "platform image repository is invalid" unless
  %r{\A[a-z0-9.-]+(?::[0-9]+)?/[a-z0-9._/-]+\z}.match?(options[:platform_image_repository]) ||
    %r{\A[a-z0-9._/-]+\z}.match?(options[:platform_image_repository])
abort "Sandbox runner image repository is invalid" unless
  %r{\A[a-z0-9.-]+(?::[0-9]+)?/[a-z0-9._/-]+\z}.match?(options[:sandbox_runner_image_repository]) ||
    %r{\A[a-z0-9._/-]+\z}.match?(options[:sandbox_runner_image_repository])
abort "git commit is invalid" unless COMMIT.match?(options[:git_commit])
abort "Kubernetes API endpoint port is invalid" unless (1..65_535).cover?(options[:kubernetes_api_endpoint_port])

source = File.join(options[:seed_runtime], "config")
output = File.expand_path(options[:output])
config_output = File.join(output, "configs")
values_output = File.join(output, "helm-values")
FileUtils.mkdir_p(config_output)
FileUtils.mkdir_p(values_output)

localstack = "https://localhost.localstack.cloud:443"
artifact_gateway_endpoint = "https://insight-platform-artifact-gateway.platform-artifacts.svc:8080/"
artifact_data_endpoint = "https://insight-platform-artifact-data-worker.platform-artifacts.svc:9443/"
egress_endpoint = "https://l4-security-insight-platform-security-egress-egress.platform-egress.svc:8443/"
security_authority_endpoint = "https://l4-security-insight-platform-security-egress-security-authority.platform-security-authority.svc:9443"
mcp_host_endpoint = "https://insight-platform-mcp-host.platform-mcp-host.svc:9443/"
mcp_resource_endpoint = "https://insight-platform-mcp-resource-host.platform-mcp-host.svc:9543/"

def normalize(value)
  case value
  when Hash
    value.keys.sort.to_h { |key| [key, normalize(value.fetch(key))] }
  when Array
    value.map { |item| normalize(item) }
  else
    value
  end
end

def canonical_json(value)
  JSON.generate(normalize(value))
end

def digest(value)
  "sha256:#{Digest::SHA256.hexdigest(canonical_json(value))}"
end

def load_config(source, name)
  path = File.join(source, name)
  abort "seed configuration must be a bounded physical file: #{path}" unless File.file?(path) && !File.symlink?(path) && File.size(path).between?(1, 4_194_304)

  JSON.parse(File.read(path, 4_194_305))
end

def tool_json(tool, *arguments)
  result, error, status = Open3.capture3(tool, *arguments)
  abort "owning deployment tool rejected Kind composition: #{error}" unless status.success?
  JSON.parse(result)
end

def executable_digest(directory, binary)
  path = File.join(directory, binary)
  abort "actual image executable missing: #{binary}" unless File.file?(path) && !File.symlink?(path) && File.size(path).positive?
  "sha256:#{Digest::SHA256.file(path).hexdigest}"
end

workers = tool_json(options[:qualification_tool], "print-worker-executables")
worker_manifests = File.join(output, "worker-manifests")
worker_configs = File.join(output, "worker-configs")
FileUtils.mkdir_p(worker_manifests)
FileUtils.mkdir_p(worker_configs)
digests = {}
write_config = lambda do |name, value|
  worker = workers.find { |item| "#{item.fetch('binary').delete_prefix('platform-')}.json" == name }
  if worker
    manifest = worker.fetch("manifest_pointer").delete_prefix("/").split("/").reduce(value) { |node, key| node.fetch(key) }
    abort "seed must contain a current owning worker manifest: #{name}" unless manifest.fetch("manifest_version") == 2
    manifest["worker_build_digest"] = executable_digest(options[:worker_binaries], worker.fetch("binary"))
    File.write(File.join(worker_manifests, "#{worker.fetch('binary')}.json"), canonical_json(manifest))
    File.write(File.join(worker_configs, "#{worker.fetch('binary')}.json"), canonical_json(value))
  elsif name == "history-maintenance.json"
    value["executable_digest"] = executable_digest(options[:worker_binaries], "platform-history-maintenance")
  end
  File.write(File.join(config_output, name), JSON.pretty_generate(value) << "\n")
  digests[name] = digest(value)
end

catalog = load_config(source, "artifact-gateway.json").fetch("artifact_provider_catalog")
kms = catalog.fetch("kms_key_bindings").fetch(0)
kms["endpoint"] = localstack
kms["key_id"] = options[:kms_key_arn]
kms.delete("kms_binding_digest")
kms["kms_binding_digest"] = digest(kms.merge("provider" => "aws_kms"))
storage = catalog.fetch("s3_storage_bindings").fetch(0)
storage["endpoint"] = localstack
storage["kms_binding_digest"] = kms.fetch("kms_binding_digest")
storage.delete("storage_binding_digest")
storage["storage_binding_digest"] = digest(storage.merge("backend" => "s3"))
catalog["write_storage_binding_digest"] = storage.fetch("storage_binding_digest")

artifact_gateway = load_config(source, "artifact-gateway.json")
artifact_gateway["listen_address"] = "0.0.0.0:8080"
artifact_gateway["observability_listen_address"] = "0.0.0.0:9090"
artifact_gateway["artifact_provider_catalog"] = catalog
write_config.call("artifact-gateway.json", artifact_gateway)

artifact_data = load_config(source, "artifact-data.json")
artifact_data["controller_listen_address"] = "0.0.0.0:9443"
artifact_data["observability_listen_address"] = "0.0.0.0:9090"
artifact_data["artifact_provider_catalog"] = catalog
write_config.call("artifact-data-worker.json", artifact_data)

artifact_maintenance = load_config(source, "artifact-maintenance.json")
artifact_maintenance["listen_address"] = "0.0.0.0:8081"
artifact_maintenance["artifact_provider_catalog"] = catalog
write_config.call("artifact-maintenance.json", artifact_maintenance)

orchestration = load_config(source, "orchestration.json")
orchestration["observability_listen_address"] = "0.0.0.0:9090"
orchestration.fetch("artifact")["endpoint"] = artifact_data_endpoint
write_config.call("orchestration-worker.json", orchestration)

authority = load_config(source, "security-authority.json")
authority["listen_address"] = "0.0.0.0:9443"
authority["observability_listen_address"] = "0.0.0.0:9090"
write_config.call("security-authority.json", authority)

egress = load_config(source, "egress-broker.json")
egress["listen_address"] = "0.0.0.0:8443"
egress["observability_listen_address"] = "0.0.0.0:9090"
egress["security_authority_endpoint"] = security_authority_endpoint
egress.fetch("mcp_state_keys")["projected_secret_root"] = "/etc/insight/mcp-state-keys"
egress.fetch("mcp_state_keys").fetch("keys").fetch(0)["key_material_path"] = "/etc/insight/mcp-state-keys/current"
provider = egress.fetch("secret_provider_catalog").fetch("providers").fetch(0)
provider["secrets_endpoint"] = localstack
provider["kms_endpoint"] = localstack
provider["kms_key_arn"] = options[:kms_key_arn]
provider["readiness_secret_id"] = options[:readiness_secret_arn]
provider.delete("provider_config_digest")
provider["provider_config_digest"] = digest(provider)
write_config.call("egress-broker.json", egress)

model = load_config(source, "model-worker.json")
model["observability_listen_address"] = "0.0.0.0:9090"
model["egress_endpoint"] = egress_endpoint
model.fetch("live_delta")["servers"] = ["tls://nats.platform-deps.svc:4222"]
write_config.call("model-worker.json", model)

remote_context = load_config(source, "context-remote.json")
remote_context["observability_listen_address"] = "0.0.0.0:9090"
remote_context["egress_endpoint"] = egress_endpoint
write_config.call("remote-context-worker.json", remote_context)

capability_remote = load_config(source, "capability-remote.json")
capability_remote["observability_listen_address"] = "0.0.0.0:9090"
capability_remote.fetch("egress")["endpoint"] = egress_endpoint
remote_mcp_enabled = !capability_remote.fetch("installed_mcp_codecs").empty?
raise "MCP codec/Host configuration mismatch" unless remote_mcp_enabled == !capability_remote["mcp_host"].nil?
capability_remote.fetch("mcp_host")["endpoint"] = mcp_host_endpoint if remote_mcp_enabled
write_config.call("capability-remote-worker.json", capability_remote)

mcp_host = load_config(source, "mcp-host.json")
mcp_host["listen_address"] = "0.0.0.0:9443"
mcp_host["observability_listen_address"] = "0.0.0.0:9090"
mcp_host.fetch("egress")["endpoint"] = egress_endpoint
write_config.call("mcp-host.json", mcp_host)

mcp_resource = load_config(source, "mcp-resource-host.json")
mcp_resource["listen_address"] = "0.0.0.0:9543"
mcp_resource["observability_listen_address"] = "0.0.0.0:9190"
mcp_resource.fetch("egress")["endpoint"] = egress_endpoint
write_config.call("mcp-resource-host.json", mcp_resource)

mcp_discovery = load_config(source, "mcp-discovery-worker.json")
mcp_discovery["observability_listen_address"] = "0.0.0.0:9290"
mcp_discovery.fetch("egress")["endpoint"] = egress_endpoint
mcp_discovery.fetch("artifact_data_worker")["endpoint"] = artifact_data_endpoint
write_config.call("mcp-discovery-worker.json", mcp_discovery)

mcp_subscription = load_config(source, "mcp-subscription-worker.json")
mcp_subscription["observability_listen_address"] = "0.0.0.0:9390"
mcp_subscription.fetch("egress")["endpoint"] = egress_endpoint
write_config.call("mcp-subscription-worker.json", mcp_subscription)

mcp_cleanup = load_config(source, "mcp-cleanup-worker.json")
mcp_cleanup["observability_listen_address"] = "0.0.0.0:9090"
mcp_cleanup["egress_endpoint"] = egress_endpoint
write_config.call("mcp-cleanup-worker.json", mcp_cleanup)

context_dataset = load_config(source, "context-dataset-worker.json")
context_dataset["observability_listen_address"] = "0.0.0.0:9290"
context_dataset.fetch("artifact_data_worker")["endpoint"] = artifact_data_endpoint
write_config.call("context-dataset-worker.json", context_dataset)

context_subscription = load_config(source, "subscription-context-worker.json")
context_subscription["observability_listen_address"] = "0.0.0.0:9190"
context_subscription.fetch("host")["endpoint"] = mcp_resource_endpoint
write_config.call("subscription-context-worker.json", context_subscription)

callback = load_config(source, "callback-api.json")
callback["listen_address"] = "0.0.0.0:8080"
callback["observability_listen_address"] = "0.0.0.0:9090" if callback.key?("observability_listen_address")
callback["egress_endpoint"] = egress_endpoint
callback.fetch("oauth_state")["key_directory"] = "/etc/insight/oauth-state-keys"
callback.fetch("oauth_state").fetch("keys").fetch(0)["key_material_path"] = "/etc/insight/oauth-state-keys/current"
write_config.call("callback-api.json", callback)

context_native = load_config(source, "context-native.json")
context_native["observability_listen_address"] = "0.0.0.0:9090"
write_config.call("context-worker.json", context_native)

capability_native = load_config(source, "capability-native.json")
capability_native["observability_listen_address"] = "0.0.0.0:9090"
write_config.call("capability-native-worker.json", capability_native)

registry = load_config(source, "registry-validation.json")
registry["observability_listen_address"] = "0.0.0.0:9090"
write_config.call("registry-validation-worker.json", registry)

management_gateway = load_config(source, "gateway-management.json")
management_gateway["listen_address"] = "0.0.0.0:8080"
management_gateway.fetch("artifact_gateway")["endpoint"] = artifact_gateway_endpoint
write_config.call("management-gateway.json", management_gateway)

runtime_gateway = load_config(source, "gateway-runtime.json")
runtime_gateway["listen_address"] = "0.0.0.0:8080"
runtime_gateway.fetch("artifact_gateway")["endpoint"] = artifact_gateway_endpoint
write_config.call("runtime-gateway.json", runtime_gateway)

outbox = load_config(source, "outbox-worker.json")
outbox["observability_listen_address"] = "0.0.0.0:9090"
outbox["nats_servers"] = ["tls://nats.platform-deps.svc:4222"]
write_config.call("outbox-worker.json", outbox)
history = load_config(source, "history-maintenance.json")
history["observability_listen_address"] = "0.0.0.0:9090"
write_config.call("history-maintenance.json", history)

# The chart owns Sandbox runtime composition; this local profile supplies its required manifest.
# Read the same physical chart inputs and hash the canonical runtime contract used by Helm.
sandbox_defaults = YAML.load_file(File.expand_path("../../deploy/helm/insight-platform-sandbox/values.yaml", __dir__))
runtime = sandbox_defaults.fetch("runtimeContract")
sandbox_runtime = {
  "schema_version" => 1, "provider" => "open_sandbox_kubernetes",
  "opensandbox_server_release_digest" => sandbox_defaults.fetch("images").fetch("server").fetch("digest"),
  "batchsandbox_controller_digest" => sandbox_defaults.fetch("images").fetch("controller").fetch("digest"),
  "lifecycle_schema_digest" => runtime.fetch("lifecycleSchemaDigest"),
  "batchsandbox_crd_digest" => runtime.fetch("batchSandboxCrdDigest"),
  "kubernetes_provider_template_digest" => runtime.fetch("kubernetesProviderTemplateDigest"),
  "runner_protocol_digest" => runtime.fetch("runnerProtocolDigest"),
  "container_runtime_digest" => runtime.fetch("containerRuntimeDigest"),
  "network_policy_digest" => runtime.fetch("networkPolicyDigest")
}
sandbox_worker = workers.find { |worker| worker.fetch("binary") == "platform-sandbox-dispatcher" }
abort "owning Sandbox worker missing" unless sandbox_worker
sandbox_catalog_input = File.join(output, "sandbox-catalog-input.json")
File.write(sandbox_catalog_input, "{}")
sandbox_manifest = {
  "manifest_version" => 2, "worker_role" => sandbox_worker.fetch("worker_role"), "work_class" => sandbox_worker.fetch("work_class"),
  "adapter_runtime_digest" => digest(sandbox_runtime),
  "worker_build_digest" => executable_digest(options[:worker_binaries], "platform-sandbox-dispatcher"),
  "execution_capabilities" => tool_json(options[:qualification_tool], "print-worker-execution-capabilities", "platform-sandbox-dispatcher", sandbox_catalog_input),
  "protocol_version" => 1, "max_concurrency" => sandbox_defaults.dig("dispatcher", "worker", "maximumConcurrency"),
  "critical_control_reserved_slots" => sandbox_defaults.dig("dispatcher", "worker", "criticalControlReservedSlots")
}
File.unlink(sandbox_catalog_input)
digests["sandbox-worker-manifest.json"] = digest(sandbox_manifest)

resource_bytes = File.binread(File.expand_path('../../deploy/kind/workload-resources.json', __dir__), 4097)
abort 'Kind resource profile exceeds byte limit' if resource_bytes.bytesize > 4096
# A plain object is intentional: JSON versions may insert directly into Hash subclasses,
# bypassing an overridden []= and silently accepting duplicate fields.
unique_fields = Class.new do
  def initialize
    @fields = {}
  end

  def []=(key, value)
    abort 'duplicate Kind resource profile field' if @fields.key?(key)
    @fields[key] = value
  end

  def to_h
    @fields
  end
end
resource_document = JSON.parse(resource_bytes, object_class: unique_fields, create_additions: false)
abort 'invalid Kind resource profile' unless resource_document.is_a?(unique_fields)
resource_profile = resource_document.to_h
resource_keys = %w[schema_version rust_service_cpu_request_millicores maximum_steady_platform_cpu_millicores]
abort 'invalid Kind resource profile' unless resource_profile.keys.sort == resource_keys.sort &&
  resource_profile['schema_version'].is_a?(Integer) &&
  resource_profile['schema_version'] == 1 &&
  resource_profile['rust_service_cpu_request_millicores'].is_a?(Integer) &&
  (1..250).cover?(resource_profile['rust_service_cpu_request_millicores']) &&
  resource_profile['maximum_steady_platform_cpu_millicores'].is_a?(Integer) &&
  (1..4000).cover?(resource_profile['maximum_steady_platform_cpu_millicores'])
local_cpu_resources = {'requests' => {'cpu' => "#{resource_profile.fetch('rust_service_cpu_request_millicores')}m"}}

deployment_config_digest = digest(
  "schema_version" => 1,
  "profile" => "kind-local-mechanics",
  "git_commit" => options[:git_commit],
  "platform_image_repository" => options[:platform_image_repository],
  "platform_image_digest" => options[:platform_image_digest],
  "sandbox_runner_image_repository" => options[:sandbox_runner_image_repository],
  "sandbox_runner_image_digest" => options[:sandbox_runner_image_digest],
  "configuration_digests" => digests,
  "resource_profile" => resource_profile
)

common = {
  "image" => {
    "repository" => options[:platform_image_repository],
    "digest" => options[:platform_image_digest]
  },
  "candidate" => {"deploymentConfigDigest" => deployment_config_digest},
  "resources" => local_cpu_resources
}
two_replicas = {"minReplicas" => 2, "maxReplicas" => 2}
postgres = [options[:postgres_cidr]]
localstack_cidrs = [options[:localstack_pod_cidr], options[:localstack_service_cidr]]

values = {
  "sandbox" => {
    "dispatcher" => {"workerManifest" => sandbox_manifest},
    "global" => {"deploymentConfigDigest" => deployment_config_digest},
    "images" => {
      "platform" => {
        "repository" => options[:platform_image_repository],
        "digest" => options[:platform_image_digest]
      },
      "sandboxRunner" => {
        "repository" => options[:sandbox_runner_image_repository],
        "digest" => options[:sandbox_runner_image_digest]
      }
    },
    "monitoring" => {"enabled" => false},
    "networkPolicy" => {
      "postgresCidrs" => postgres,
      "kubernetesApiCidrs" => [
        options[:kubernetes_api_service_cidr], options[:kubernetes_api_endpoint_cidr]
      ],
      "kubernetesApiPorts" => [443, options[:kubernetes_api_endpoint_port]]
    }
  },
  "artifact" => common.merge(
    "autoscaling" => two_replicas,
    "roles" => {
      "gateway" => {"resources" => local_cpu_resources, "config" => {"digest" => digests.fetch("artifact-gateway.json")}},
      "data-worker" => {"resources" => local_cpu_resources, "config" => {"digest" => digests.fetch("artifact-data-worker.json")}},
      "maintenance" => {"resources" => local_cpu_resources, "config" => {"digest" => digests.fetch("artifact-maintenance.json")}}
    },
    "networkPolicy" => {
      "postgresCidrs" => postgres,
      "storageProviderCidrs" => localstack_cidrs,
      "kmsProviderCidrs" => localstack_cidrs,
      "workloadIdentityCidrs" => localstack_cidrs
    }
  ),
  "callback" => common.merge(
    "autoscaling" => two_replicas,
    "config" => {"digest" => digests.fetch("callback-api.json")},
    "ingress" => {"enabled" => false},
    "networkPolicy" => {"postgresCidrs" => postgres}
  ),
  "capability-native" => common.merge(
    "replicas" => 2,
    "autoscaling" => two_replicas,
    "monitoring" => {"enabled" => false},
    "config" => {"digest" => digests.fetch("capability-native-worker.json")},
    "networkPolicy" => {"postgresCidrs" => postgres}
  ),
  "capability-remote" => common.merge(
    "autoscaling" => two_replicas,
    "config" => {"digest" => digests.fetch("capability-remote-worker.json")},
    "mcpHost" => {"enabled" => remote_mcp_enabled},
    "networkPolicy" => {"postgresCidrs" => postgres}
  ),
  "context" => common.merge(
    "replicas" => 2,
    "autoscaling" => two_replicas,
    "monitoring" => {"enabled" => false},
    "config" => {"digest" => digests.fetch("context-worker.json")},
    "networkPolicy" => {"postgresCidrs" => postgres},
    "datasetPool" => {
      "resources" => local_cpu_resources,
      "enabled" => true,
      "autoscaling" => two_replicas,
      "config" => {"digest" => digests.fetch("context-dataset-worker.json")},
      "artifactDataWorker" => {"namespace" => "platform-artifacts", "port" => 9443}
    },
    "subscriptionPool" => {
      "resources" => local_cpu_resources,
      "enabled" => true,
      "autoscaling" => two_replicas,
      "config" => {"digest" => digests.fetch("subscription-context-worker.json")}
    }
  ),
  "gateway" => common.merge(
    "ingress" => {"enabled" => false},
    "networkPolicy" => {"postgresCidrs" => postgres},
    "runEventCursorKey" => {
      "digest" => "sha256:#{Digest::SHA256.file(File.join(options[:seed_runtime], 'run-event-cursor-key')).hexdigest}"
    },
    "roles" => {
      "management-api" => {
        "resources" => local_cpu_resources,
        "replicas" => 2,
        "autoscaling" => two_replicas,
        "config" => {"digest" => digests.fetch("management-gateway.json")}
      },
      "runtime-api" => {
        "resources" => local_cpu_resources,
        "replicas" => 2,
        "autoscaling" => two_replicas,
        "config" => {"digest" => digests.fetch("runtime-gateway.json")}
      }
    }
  ),
  "mcp" => common.merge(
    "autoscaling" => two_replicas,
    "config" => {"digest" => digests.fetch("mcp-host.json")},
    "resourcePool" => {
      "resources" => local_cpu_resources,
      "autoscaling" => two_replicas,
      "config" => {"digest" => digests.fetch("mcp-resource-host.json")},
      "postgresCidrs" => postgres
    },
    "discoveryPool" => {
      "resources" => local_cpu_resources,
      "autoscaling" => two_replicas,
      "config" => {"digest" => digests.fetch("mcp-discovery-worker.json")},
      "postgresCidrs" => postgres,
      "artifactNamespace" => "platform-artifacts",
      "artifactPort" => 9443
    },
    "subscriptionPool" => {
      "resources" => local_cpu_resources,
      "autoscaling" => two_replicas,
      "config" => {"digest" => digests.fetch("mcp-subscription-worker.json")},
      "postgresCidrs" => postgres
    }
  ),
  "mcp-cleanup" => common.merge(
    "autoscaling" => two_replicas,
    "config" => {"digest" => digests.fetch("mcp-cleanup-worker.json")},
    "networkPolicy" => {"postgresCidrs" => postgres}
  ),
  "model" => common.merge(
    "autoscaling" => two_replicas,
    "config" => {"digest" => digests.fetch("model-worker.json")},
    "networkPolicy" => {"postgresCidrs" => postgres, "natsCidrs" => [options[:nats_cidr]]}
  ),
  "outbox" => common.merge(
    "autoscaling" => two_replicas,
    "config" => {"digest" => digests.fetch("outbox-worker.json")},
    "networkPolicy" => {"postgresCidrs" => postgres, "natsCidrs" => [options[:nats_cidr]]}
  ),
  "history" => common.merge(
    "autoscaling" => two_replicas,
    "config" => {"digest" => digests.fetch("history-maintenance.json")},
    "networkPolicy" => {"postgresCidrs" => postgres}
  ),
  "orchestration" => common.merge(
    "autoscaling" => two_replicas,
    "config" => {"digest" => digests.fetch("orchestration-worker.json")},
    "networkPolicy" => {"postgresCidrs" => postgres}
  ),
  "registry" => common.merge(
    "replicas" => 2,
    "autoscaling" => two_replicas,
    "monitoring" => {"enabled" => false},
    "config" => {"digest" => digests.fetch("registry-validation-worker.json")},
    "networkPolicy" => {"postgresCidrs" => postgres}
  ),
  "remote-context" => common.merge(
    "autoscaling" => two_replicas,
    "config" => {"digest" => digests.fetch("remote-context-worker.json")},
    "networkPolicy" => {"postgresCidrs" => postgres}
  ),
  "security" => common.merge(
    "egress" => {
      "resources" => local_cpu_resources,
      "autoscaling" => two_replicas,
      "config" => {"digest" => digests.fetch("egress-broker.json")}
    },
    "securityAuthority" => {
      "resources" => local_cpu_resources,
      "autoscaling" => two_replicas,
      "config" => {"digest" => digests.fetch("security-authority.json")}
    },
    "networkPolicy" => {
      "postgresCidrs" => postgres,
      "secretProviderCidrs" => localstack_cidrs,
      "externalDestinationCidrs" => localstack_cidrs,
      "workloadIdentityCidrs" => localstack_cidrs
    }
  )
}

values.each do |name, value|
  File.write(File.join(values_output, "#{name}.yaml"), YAML.dump(value))
end

# Check the final rendered Sandbox config, including the independently computed runtime digest.
sandbox_chart = File.expand_path("../../deploy/helm/insight-platform-sandbox", __dir__)
rendered, error, status = Open3.capture3("helm", "template", "insight-sandbox", sandbox_chart, "--values", File.join(values_output, "sandbox.yaml"))
abort "Sandbox chart rejected Kind configuration: #{error}" unless status.success?
sandbox_configmap = YAML.load_stream(rendered).compact.find { |item| item["kind"] == "ConfigMap" && item.dig("metadata", "name") == "sandbox-dispatcher-config" }
sandbox_config = JSON.parse(sandbox_configmap.fetch("data").fetch("config.json"))
abort "Sandbox runtime identity differs between composition and chart" unless sandbox_config.fetch("runtime_contract_digest") == sandbox_manifest.fetch("adapter_runtime_digest")
File.write(File.join(worker_manifests, "platform-sandbox-dispatcher.json"), canonical_json(sandbox_manifest))
File.write(File.join(worker_configs, "platform-sandbox-dispatcher.json"), canonical_json(sandbox_config))
evidence = tool_json(options[:qualification_tool], "validate-worker-deployment", worker_manifests, worker_configs, options[:worker_binaries], options[:platform_image_digest])
File.write(File.join(output, "worker-executable-evidence.json"), JSON.pretty_generate(evidence) << "\n")
history_evidence = tool_json(options[:qualification_tool], "validate-history-maintenance-deployment", File.join(config_output, "history-maintenance.json"), File.join(options[:worker_binaries], "platform-history-maintenance"), options[:platform_image_digest])
File.write(File.join(output, "history-maintenance-executable-evidence.json"), JSON.pretty_generate(history_evidence) << "\n")

role_replicas = {
  "artifact_data_worker" => 2,
  "artifact_gateway" => 2,
  "artifact_maintenance" => 2,
  "capability_native_worker" => 2,
  "capability_remote_worker" => 2,
  "context_worker" => 8,
  "egress_secret_broker" => 4,
  "management_api" => 2,
  "mcp_host" => 12,
  "model_worker" => 2,
  "outbox_worker" => 2,
  "history_maintenance" => 2,
  "opensandbox_controller" => 1,
  "opensandbox_server" => 1,
  "registry_validation_worker" => 2,
  "runtime_api" => 2,
  "sandbox_dispatcher" => 1,
  "scheduler_recovery" => 2
}
component_images = role_replicas.keys.to_h do |role|
  digest_value = case role
                 when "opensandbox_controller"
                   "sha256:a9a5f73c1785ebd955336ffa313973a35c1a1b662cb7afc4ea82d92021b3532a"
                 when "opensandbox_server"
                   "sha256:ae8dfbb277f40a39ff01ef35e5e1c10675acfe0fa9db15259b8f323e5efab778"
                 else
                   options[:platform_image_digest]
                 end
  [role, digest_value]
end
local_candidate = {
  "component_images" => component_images,
  "deployment_config_digest" => deployment_config_digest
}
local_capacity = {
  "deployment_config_digest" => deployment_config_digest,
  "replicas" => role_replicas.to_h do |role, replicas|
    [role, {"min_replicas" => replicas, "max_replicas" => replicas}]
  end,
  "hpa" => role_replicas.to_h do |role, _|
    target = role.start_with?("opensandbox_") || role == "sandbox_dispatcher" ? 8_000 : 7_000
    [role, {"target_utilization_basis_points" => target}]
  end
}

File.write(File.join(output, "digests.json"), JSON.pretty_generate(digests) << "\n")
File.write(
  File.join(output, "local-workload-candidate.json"),
  JSON.pretty_generate(local_candidate) << "\n"
)
File.write(
  File.join(output, "local-workload-capacity.json"),
  JSON.pretty_generate(local_capacity) << "\n"
)
File.write(
  File.join(output, "environment.json"),
  JSON.pretty_generate(
    "schema_version" => 2,
    "kind" => "insight.platform/kind-local-mechanics/v2",
    "production" => false,
    "git_commit" => options[:git_commit],
    "platform_image_repository" => options[:platform_image_repository],
    "platform_image_digest" => options[:platform_image_digest],
    "sandbox_runner_image_repository" => options[:sandbox_runner_image_repository],
    "sandbox_runner_image_digest" => options[:sandbox_runner_image_digest],
    "deployment_config_digest" => deployment_config_digest,
    "generated_at" => Time.now.utc.iso8601(6)
  ) << "\n"
)

puts deployment_config_digest
