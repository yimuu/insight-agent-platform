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
  '--set alerts.minimumRecoveryRate=0' \
  '--set-json dashboard.labels=null'; do
  # shellcheck disable=SC2086
  if helm template platform "$chart" $mutation >/dev/null 2>&1; then
    echo "observability deployment: invalid values were accepted: $mutation" >&2
    exit 1
  fi
done

ruby -rjson -ryaml - "$rendered" "$root/docs/runbooks/platform-v2-observability.md" <<'RUBY'
documents = YAML.load_stream(File.read(ARGV.fetch(0))).compact
runbook = File.read(ARGV.fetch(1))
failures = []
rules = documents.select { |document| document["kind"] == "PrometheusRule" }
dashboards = documents.select { |document| document["kind"] == "ConfigMap" }
failures << "must render one PrometheusRule and one dashboard" unless rules.length == 1 && dashboards.length == 1

alerts = rules.flat_map { |document| document.dig("spec", "groups").to_a.flat_map { |group| group["rules"].to_a } }
expected = %w[
  InsightPlatformCriticalControlPermitsExhausted
  InsightPlatformHttpFailureRatioHigh
  InsightPlatformHttpLatencyHigh
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
  rescue JSON::ParserError => error
    failures << "dashboard JSON is invalid: #{error.message}"
  end
end

unless failures.empty?
  warn failures.map { |failure| "observability deployment: #{failure}" }.join("\n")
  exit 1
end
puts "Platform observability dashboard/alert contract passed."
RUBY
