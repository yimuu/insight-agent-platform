#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
namespace=${BENCH_NAMESPACE:-insight-bench}
name=${STREAM_FIXTURE_NAME:-terminal-stream-mock}
image=${STREAM_FIXTURE_IMAGE:-python:3.12-alpine}
tenant_keyring_secret=${TENANT_ARTIFACT_KEYRING_SECRET:-terminal-tenant-keyring}
tenant_key_version=${TENANT_ARTIFACT_KEY_VERSION:-qualification-v1}
tenant_key_hex=${TENANT_ARTIFACT_KEY_HEX:-}

command -v kubectl >/dev/null 2>&1 || {
  printf 'kubectl is required\n' >&2
  exit 2
}
[[ "$tenant_keyring_secret" =~ ^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$ ]] || {
  printf 'TENANT_ARTIFACT_KEYRING_SECRET is not a valid Secret name\n' >&2
  exit 2
}
[[ "$tenant_key_version" =~ ^[A-Za-z0-9._-]{1,64}$ ]] || {
  printf 'TENANT_ARTIFACT_KEY_VERSION is invalid\n' >&2
  exit 2
}
if [[ -n "$tenant_key_hex" &&
      ! "$tenant_key_hex" =~ ^[0-9a-f]{64}$ ]]; then
  printf 'TENANT_ARTIFACT_KEY_HEX must be exactly 64 lowercase hex digits\n' >&2
  exit 2
fi

kubectl create namespace "$namespace" --dry-run=client -o yaml |
  kubectl apply -f -

if [[ -n "$tenant_key_hex" ]] ||
   ! kubectl -n "$namespace" get secret "$tenant_keyring_secret" \
     >/dev/null 2>&1; then
  if [[ -z "$tenant_key_hex" ]]; then
    command -v openssl >/dev/null 2>&1 || {
      printf 'openssl is required to generate a new tenant keyring\n' >&2
      exit 2
    }
    tenant_key_hex=$(openssl rand -hex 32)
  fi
  keyring_file=$(mktemp)
  cleanup_keyring_file() {
    rm -f "$keyring_file"
  }
  trap cleanup_keyring_file EXIT INT TERM
  umask 077
  printf '{"%s":"%s"}\n' "$tenant_key_version" "$tenant_key_hex" \
    >"$keyring_file"
  kubectl -n "$namespace" create secret generic "$tenant_keyring_secret" \
    --from-file=keyring.json="$keyring_file" \
    --dry-run=client -o yaml |
    kubectl apply -f -
  tenant_key_hex=
  cleanup_keyring_file
  trap - EXIT INT TERM
else
  command -v python3 >/dev/null 2>&1 || {
    printf 'python3 is required to validate the existing tenant keyring\n' >&2
    exit 2
  }
  kubectl -n "$namespace" get secret "$tenant_keyring_secret" -o json |
    python3 -c '
import base64
import json
import re
import sys

version = sys.argv[1]
secret = json.load(sys.stdin)
encoded = secret.get("data", {}).get("keyring.json")
if not isinstance(encoded, str):
    raise SystemExit("existing tenant keyring has no keyring.json")
keyring = json.loads(base64.b64decode(encoded, validate=True))
value = keyring.get(version)
if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
    raise SystemExit("existing tenant keyring lacks the requested valid version")
' "$tenant_key_version"
fi
printf 'Tenant Artifact keyring Secret %s/%s is ready (active version %s).\n' \
  "$namespace" "$tenant_keyring_secret" "$tenant_key_version"

kubectl -n "$namespace" create configmap "$name" \
  --from-file=mock-openai-server.py="$script_dir/mock-openai-server.py" \
  --dry-run=client -o yaml |
  kubectl apply -f -

kubectl -n "$namespace" apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${name}
  labels:
    app.kubernetes.io/name: ${name}
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: ${name}
  template:
    metadata:
      labels:
        app.kubernetes.io/name: ${name}
    spec:
      containers:
        - name: mock-openai
          image: ${image}
          command: ["python3", "/fixture/mock-openai-server.py"]
          ports:
            - name: http
              containerPort: 8080
          readinessProbe:
            httpGet: {path: /health, port: http}
          resources:
            requests: {cpu: 10m, memory: 16Mi}
            limits: {cpu: 100m, memory: 64Mi}
          securityContext:
            allowPrivilegeEscalation: false
            capabilities: {drop: ["ALL"]}
          volumeMounts:
            - {name: fixture, mountPath: /fixture, readOnly: true}
      volumes:
        - name: fixture
          configMap: {name: ${name}}
---
apiVersion: v1
kind: Service
metadata:
  name: ${name}
spec:
  selector:
    app.kubernetes.io/name: ${name}
  ports:
    - {name: http, port: 8080, targetPort: http}
EOF

kubectl -n "$namespace" rollout status "deployment/$name" --timeout=120s
printf 'Set Helm value: --set models.terminalStreamBaseUrl=http://%s.%s.svc.cluster.local:8080/v1\n' \
  "$name" "$namespace"
