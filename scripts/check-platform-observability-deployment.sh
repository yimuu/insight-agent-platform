#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
chart="$root/deploy/helm/insight-platform-observability"
rendered=$(mktemp)
trap 'rm -f "$rendered"' EXIT

helm lint "$chart" >/dev/null
helm template platform "$chart" >"$rendered"

for mutation in \
  '--set alerts.runbookBaseUrl=http://unsafe.example/runbook' \
  '--set-json alerts.labels=null' \
  '--set alerts.maximumFailureRatio=1' \
  '--set alerts.maximumRecoveryFailureRatio=1' \
  '--set alerts.maximumDependencyFailureRatio=1' \
  '--set alerts.minimumDependencyObservations=0' \
  '--set alerts.minimumRecoveryRate=0' \
  '--set alerts.maximumDueJobLagSeconds=0' \
  '--set alerts.maximumExpiredLeaseLagSeconds=0' \
  '--set alerts.maximumDueOutboxLagSeconds=0' \
  '--set alerts.maximumExpiredOutboxClaimLagSeconds=0' \
  '--set alerts.outboxDeadFor=' \
  '--set alerts.operationalCapacityExhaustedFor=' \
  '--set-json dashboard.labels=null'; do
  # shellcheck disable=SC2086
  if helm template platform "$chart" $mutation >/dev/null 2>&1; then
    echo "observability deployment: invalid values were accepted: $mutation" >&2
    exit 1
  fi
done

ruby -rjson -ryaml - "$rendered" "$root/docs/runbooks/platform-v2-observability.md" "$root" <<'RUBY'
documents = YAML.load_stream(File.read(ARGV.fetch(0))).compact
runbook = File.read(ARGV.fetch(1))
root = ARGV.fetch(2)
failures = []
rules = documents.select { |document| document["kind"] == "PrometheusRule" }
dashboards = documents.select { |document| document["kind"] == "ConfigMap" }
failures << "must render one PrometheusRule and one dashboard" unless rules.length == 1 && dashboards.length == 1

alerts = rules.flat_map { |document| document.dig("spec", "groups").to_a.flat_map { |group| group["rules"].to_a } }
expected = %w[
  InsightPlatformCriticalControlPermitsExhausted
  InsightPlatformDependencyFailureRatioHigh
  InsightPlatformDueOutboxLagHigh
  InsightPlatformDurableJobLagHigh
  InsightPlatformExpiredLeaseRecoveryLagHigh
  InsightPlatformExpiredOutboxClaimLagHigh
  InsightPlatformHttpFailureRatioHigh
  InsightPlatformHttpLatencyHigh
  InsightPlatformOperationalCapacityExhausted
  InsightPlatformOutboxDeadEventsPresent
  InsightPlatformRecoveryFailureRatioHigh
  InsightPlatformTelemetryMissing
  InsightPlatformWorkloadNotReady
]
failures << "symptom-first alert inventory drifted" unless alerts.map { |alert| alert["alert"] }.sort == expected
alerts.each do |alert|
  name = alert.fetch("alert")
  url = alert.dig("annotations", "runbook_url").to_s
  failures << "#{name} lacks a stable runbook URL" unless url.start_with?("https://")
  failures << "#{name} lacks a checked-in runbook section" unless runbook.include?("## #{name}")
  expression = alert.fetch("expr").to_s
  %w[tenant_id principal_id resource_id run_id job_id url token secret].each do |forbidden|
    failures << "#{name} uses forbidden label #{forbidden}" if expression.include?(forbidden)
  end
end

unless dashboards.empty?
  begin
    dashboard = JSON.parse(dashboards.first.dig("data", "insight-platform-runtime.json").to_s)
    failures << "dashboard lacks the eight minimum symptom panels" unless dashboard.fetch("panels", []).length >= 8
    dependency_panels = dashboard.fetch("panels", []).select { |panel| panel.fetch("title", "").include?("Dependency") }
    failures << "dashboard lacks the fixed dependency outcome panel" unless dependency_panels.any? do |panel|
      panel.fetch("targets", []).any? do |target|
        expression = target.fetch("expr", "")
        expression.include?("insight_platform_dependency_observations_total") &&
          expression.include?("component_role, dependency, outcome")
      end
    end
  rescue JSON::ParserError => error
    failures << "dashboard JSON is invalid: #{error.message}"
  end
end

production_egress_clients = %w[
  crates/platform-callback-api/src/main.rs
  crates/platform-capability-worker/src/remote_main.rs
  crates/platform-context-worker/src/remote_main.rs
  crates/platform-mcp-cleanup-worker/src/main.rs
  crates/platform-mcp-service/src/main.rs
  crates/platform-mcp-service/src/resource_main.rs
  crates/platform-model-worker/src/main.rs
]
production_egress_clients.each do |relative|
  source = File.read(File.join(root, relative))
  failures << "#{relative} lacks the required Egress dependency observer" unless source.include?("EgressBrokerGrpcClient::new_with_observer(")
  failures << "#{relative} uses the no-op Egress client constructor" if source.include?("EgressBrokerGrpcClient::new(")
end

allowed_noop_clients = %w[
  crates/platform-egress-rpc/src/lib.rs
  crates/platform-postgres/tests/phase4_mcp_oauth.rs
  crates/platform-sandbox-microvm/src/main.rs
]
Dir.glob(File.join(root, "crates/**/*.rs")).each do |path|
  next unless File.read(path).include?("EgressBrokerGrpcClient::new(")
  relative = path.delete_prefix("#{root}/")
  failures << "unexpected no-op Egress client constructor in #{relative}" unless allowed_noop_clients.include?(relative)
end

unless failures.empty?
  warn failures.map { |failure| "observability deployment: #{failure}" }.join("\n")
  exit 1
end
puts "Platform observability dashboard/alert contract passed."
RUBY
