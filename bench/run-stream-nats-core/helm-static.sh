#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
chart="$repo_root/deploy/archive/helm/insight-agent-platform"
profile="$chart/values-nats-core-qualification.yaml"
scratch=$(mktemp -d "${TMPDIR:-/tmp}/insight-nats-helm.XXXXXX")
trap 'rm -rf "$scratch"' EXIT

for command in helm rg; do
  command -v "$command" >/dev/null 2>&1 || {
    printf '%s is required\n' "$command" >&2
    exit 2
  }
done

helm template qualification "$chart" >"$scratch/default.yaml"
helm template qualification "$chart" -f "$profile" >"$scratch/nats.yaml"

rg -q 'topology: single_runtime' "$scratch/default.yaml"
rg -q 'type: in_memory' "$scratch/default.yaml"
rg -q 'topology: distributed' "$scratch/nats.yaml"
rg -q 'type: nats_core' "$scratch/nats.yaml"
rg -q 'name: INSIGHT_RUN_STREAM_NATS_CREDENTIALS' "$scratch/nats.yaml"
rg -q 'secretKeyRef:' "$scratch/nats.yaml"
rg -q 'mountPath: /var/run/secrets/insight-nats' "$scratch/nats.yaml"
if rg -q 'BEGIN NATS USER JWT|BEGIN USER NKEY SEED' "$scratch/nats.yaml"; then
  printf 'rendered NATS credential material into the Helm manifest\n' >&2
  exit 1
fi

expect_failure() {
  local name=$1
  local expected=$2
  shift 2
  if helm template qualification "$chart" "$@" >"$scratch/$name.log" 2>&1; then
    printf 'Helm negative case unexpectedly rendered: %s\n' "$name" >&2
    return 1
  fi
  rg -q "$expected" "$scratch/$name.log"
}

expect_failure distributed-in-memory \
  'distributed requires broker=nats_core' \
  --set runtime.runStream.topology=distributed \
  --set runtime.runStream.broker=in_memory
expect_failure replicas \
  'requires replicaCount=1' \
  --set replicaCount=2
expect_failure missing-credential-secret \
  'credentials existingSecret and secretKey are required' \
  -f "$profile" \
  --set-string runtime.runStream.natsCore.credentials.existingSecret=
expect_failure tls-disabled \
  'tls.required must be true' \
  -f "$profile" \
  --set runtime.runStream.natsCore.tls.required=false
expect_failure inline-userinfo \
  'explicit nats:// or tls:// host:port URLs' \
  -f "$profile" \
  --set-string 'runtime.runStream.natsCore.servers[0]=tls://user:secret@nats.invalid:4222'

printf '%s\n' '{"passed":true,"default_backend":"in_memory","nats_profile":"strict-secret-and-tls","negative_cases":5}'
