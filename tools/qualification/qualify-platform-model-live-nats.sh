#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
[[ $# = 0 ]] || { echo "usage: qualify-platform-model-live-nats.sh" >&2; exit 2; }
: "${PLATFORM_TEST_NATS_URL:?real mTLS NATS endpoint is required}"
for name in PLATFORM_TEST_NATS_CA_PATH PLATFORM_TEST_NATS_CERT_PATH PLATFORM_TEST_NATS_KEY_PATH; do
  path=${!name:-}
  [[ "$path" = /* && -f "$path" && -r "$path" ]] || { echo "$name must name a readable absolute fixture file" >&2; exit 2; }
done
cd "$root"
cargo test --locked -p insight-platform-model-worker --lib \
  tests::real_tls_nats_publishes_live_delta -- --ignored --exact --nocapture
