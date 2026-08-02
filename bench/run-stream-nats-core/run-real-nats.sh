#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
stamp=$(date -u +%Y%m%dT%H%M%SZ)
result_dir=${1:-"$repo_root/bench/results/run-stream-nats-core-$stamp"}
profile=${QUALIFICATION_PROFILE:-smoke}
nats_image=${NATS_IMAGE:-nats:2.12.4-alpine}
nats_box_image=${NATS_BOX_IMAGE:-natsio/nats-box:0.18.0}
plain_container="insight-nats-plain-${RANDOM}-$$"
secure_container="insight-nats-secure-${RANDOM}-$$"
port_base=${NATS_QUALIFICATION_PORT_BASE:-$((44000 + ($$ % 500) * 4))}
plain_port=$port_base
plain_monitor_port=$((port_base + 1))
secure_port=$((port_base + 2))
secure_monitor_port=$((port_base + 3))
scratch=$(mktemp -d "${TMPDIR:-/tmp}/insight-nats-core.XXXXXX")
mkdir -p "$result_dir"

cleanup() {
  docker rm -f "$plain_container" "$secure_container" >/dev/null 2>&1 || true
  rm -rf "$scratch"
}
trap cleanup EXIT

for command in cargo curl docker jq openssl rg rustc; do
  command -v "$command" >/dev/null 2>&1 || {
    printf '%s is required\n' "$command" >&2
    exit 2
  }
done
if [[ "$profile" != smoke && "$profile" != full ]]; then
  printf 'QUALIFICATION_PROFILE must be smoke or full\n' >&2
  exit 2
fi

docker run -d --name "$plain_container" \
  -p "127.0.0.1:$plain_port:4222" -p "127.0.0.1:$plain_monitor_port:8222" \
  "$nats_image" -m 8222 >"$result_dir/plain-container-id.txt"

mkdir -p "$scratch/tls" "$scratch/nsc" "$scratch/foreign"
openssl req -x509 -newkey rsa:2048 -sha256 -days 1 -nodes \
  -subj '/CN=localhost' \
  -addext 'subjectAltName=DNS:localhost,IP:127.0.0.1' \
  -keyout "$scratch/tls/server-key.pem" \
  -out "$scratch/tls/server.pem" >/dev/null 2>&1
openssl req -x509 -newkey rsa:2048 -sha256 -days 1 -nodes \
  -subj '/CN=wrong-qualification-ca' \
  -keyout "$scratch/tls/wrong-key.pem" \
  -out "$scratch/tls/wrong-ca.pem" >/dev/null 2>&1

nsc() {
  docker run --rm -v "$scratch/nsc:/nsc" "$nats_box_image" nsc -H /nsc "$@"
}
foreign_nsc() {
  docker run --rm -v "$scratch/foreign:/nsc" "$nats_box_image" nsc -H /nsc "$@"
}

nsc add operator --name Insight --sys >/dev/null
nsc add account --name APP >/dev/null
nsc add user --account APP --name combined \
  --allow-pub 'insight.qualification.run_stream.v1.*' \
  --allow-sub 'insight.qualification.run_stream.v1.*' >/dev/null
nsc add user --account APP --name publisher-strict \
  --allow-pub 'insight.qualification.run_stream.v1.*' --deny-sub '>' >/dev/null
nsc add user --account APP --name subscriber-strict \
  --allow-sub 'insight.qualification.run_stream.v1.*' --deny-pub '>' >/dev/null
nsc generate config --mem-resolver --force \
  --config-file /nsc/nats-operator.conf >/dev/null

foreign_nsc add operator --name Foreign >/dev/null
foreign_nsc add account --name APP >/dev/null
foreign_nsc add user --account APP --name outsider >/dev/null

cat >"$scratch/nats-server.conf" <<EOF
include ./nsc/nats-operator.conf
port: 4222
http: 8222
tls {
  cert_file: /qualification/tls/server.pem
  key_file: /qualification/tls/server-key.pem
  timeout: 2
}
EOF

docker run -d --name "$secure_container" \
  -p "127.0.0.1:$secure_port:4222" -p "127.0.0.1:$secure_monitor_port:8222" \
  -v "$scratch:/qualification:ro" \
  "$nats_image" -c /qualification/nats-server.conf \
  >"$result_dir/secure-container-id.txt"

for endpoint in \
  "http://127.0.0.1:$plain_monitor_port/varz" \
  "http://127.0.0.1:$secure_monitor_port/varz"; do
  for _ in $(seq 1 100); do
    if curl --fail --silent "$endpoint" >/dev/null; then
      break
    fi
    sleep 0.1
  done
  curl --fail --silent "$endpoint" >/dev/null
done

(
  cd "$repo_root"
  env \
    TEST_NATS_URL="nats://127.0.0.1:$plain_port" \
    TEST_NATS_MONITOR_URL="http://127.0.0.1:$plain_monitor_port" \
    TEST_NATS_DOCKER_CONTAINER="$plain_container" \
    cargo test -p insight-runtime --lib nats_run_stream::tests --locked \
      -- --test-threads=1 --nocapture
) 2>&1 | tee "$result_dir/plain-integration.log"

combined="$scratch/nsc/creds/Insight/APP/combined.creds"
publisher="$scratch/nsc/creds/Insight/APP/publisher-strict.creds"
subscriber="$scratch/nsc/creds/Insight/APP/subscriber-strict.creds"
outsider="$scratch/foreign/creds/Foreign/APP/outsider.creds"
for credential in "$combined" "$publisher" "$subscriber" "$outsider"; do
  test -s "$credential"
done

(
  cd "$repo_root"
  env \
    TEST_NATS_TLS_URL="tls://127.0.0.1:$secure_port" \
    TEST_NATS_TLS_CA="$scratch/tls/server.pem" \
    TEST_NATS_TLS_WRONG_CA="$scratch/tls/wrong-ca.pem" \
    TEST_NATS_COMBINED_CREDS="$combined" \
    TEST_NATS_PUBLISHER_CREDS="$publisher" \
    TEST_NATS_SUBSCRIBER_CREDS="$subscriber" \
    TEST_NATS_FOREIGN_CREDS="$outsider" \
    cargo test -p insight-runtime --lib \
      real_nats_tls_credentials_and_subject_acl_fail_closed_when_configured \
      --locked -- --nocapture
) 2>&1 | tee "$result_dir/security-integration.log"

curl --fail --silent "http://127.0.0.1:$plain_monitor_port/varz" \
  >"$result_dir/plain-varz-before-soak.json"
curl --fail --silent "http://127.0.0.1:$plain_monitor_port/connz" \
  >"$result_dir/plain-connz-before-soak.json"
(
  cd "$repo_root"
  cargo tree -p async-nats -e features --locked
) >"$result_dir/async-nats-features.txt"
if rg -q 'feature "(jetstream|service|object-store|kv)"' \
  "$result_dir/async-nats-features.txt"; then
  printf 'forbidden async-nats persistence/service feature enabled\n' >&2
  exit 1
fi

(
  cd "$repo_root"
  cargo build -p insight-runtime --example nats_run_stream_soak --locked
)
soak_binary="$repo_root/target/debug/examples/nats_run_stream_soak"

run_soak() {
  local name=$1
  local duration=$2
  local runs=$3
  local tick_ms=$4
  local restart_delay=$((duration / 4))
  local output="$result_dir/$name.ndjson"
  (
    sleep "$restart_delay"
    docker restart "$plain_container" >/dev/null
  ) &
  local restart_pid=$!
  env \
    TEST_NATS_URL="nats://127.0.0.1:$plain_port" \
    TEST_NATS_NAMESPACE=qualification \
    SOAK_DURATION_SECONDS="$duration" \
    SOAK_RUNS="$runs" \
    SOAK_TICK_MILLIS="$tick_ms" \
    "$soak_binary" | tee "$output"
  wait "$restart_pid"
  jq -e 'select(.result == "passed")' "$output" >/dev/null
}

if [[ "$profile" == smoke ]]; then
  run_soak smoke 12 5 100
else
  for round in $(seq -w 1 20); do
    run_soak "burst-$round" 10 50 50
  done
  run_soak mixed-30m 1800 50 250
  run_soak soak-2h 7200 50 1000
fi

leak_window_verified=false
if [[ "$profile" == full ]]; then
  jq -s '
    def median: sort | .[(length / 2 | floor)];
    map(select(.resources != null)) as $samples
    | ($samples | map(.elapsed_seconds) | max) as $last_elapsed
    | ($samples | map(select(.elapsed_seconds >= ($last_elapsed - 1800)))) as $window
    | ($window[0:5]) as $first
    | ($window[-5:]) as $last
    | ($first | map(.resources.rss_bytes) | median) as $rss_first
    | ($last | map(.resources.rss_bytes) | median) as $rss_last
    | ($first | map(.resources.publisher_tasks) | median) as $publisher_tasks_first
    | ($last | map(.resources.publisher_tasks) | median) as $publisher_tasks_last
    | ($first | map(.resources.subscriber_tasks) | median) as $subscriber_tasks_first
    | ($last | map(.resources.subscriber_tasks) | median) as $subscriber_tasks_last
    | ($first | map(.resources.publisher_pending_messages) | median) as $pending_messages_first
    | ($last | map(.resources.publisher_pending_messages) | median) as $pending_messages_last
    | ($first | map(.resources.publisher_pending_bytes) | median) as $pending_bytes_first
    | ($last | map(.resources.publisher_pending_bytes) | median) as $pending_bytes_last
    | ([67108864, ($rss_first * 0.20)] | max) as $rss_allowance
    | {
        samples: ($samples | length),
        last_30m_samples: ($window | length),
        rss_bytes: {first_median: $rss_first, last_median: $rss_last, allowance: $rss_allowance},
        publisher_tasks: {first_median: $publisher_tasks_first, last_median: $publisher_tasks_last},
        subscriber_tasks: {first_median: $subscriber_tasks_first, last_median: $subscriber_tasks_last},
        publisher_pending_messages: {first_median: $pending_messages_first, last_median: $pending_messages_last},
        publisher_pending_bytes: {first_median: $pending_bytes_first, last_median: $pending_bytes_last},
        final_active_subscriptions: ($last | map(.resources.active_subscriptions)),
        passed: (
          ($window | length) >= 50
          and $rss_last <= ($rss_first + $rss_allowance)
          and $publisher_tasks_last == $publisher_tasks_first
          and $subscriber_tasks_last == $subscriber_tasks_first
          and $pending_messages_last <= ($pending_messages_first + 50)
          and $pending_bytes_last <= ($pending_bytes_first + 1048576)
          and all($last[]; .resources.active_subscriptions == 50)
        )
      }
  ' "$result_dir/soak-2h.ndjson" >"$result_dir/soak-2h-leak-report.json"
  jq -e '.passed == true' "$result_dir/soak-2h-leak-report.json" >/dev/null
  leak_window_verified=true
fi

curl --fail --silent "http://127.0.0.1:$plain_monitor_port/varz" \
  >"$result_dir/plain-varz-after-soak.json"
curl --fail --silent "http://127.0.0.1:$plain_monitor_port/connz" \
  >"$result_dir/plain-connz-after-soak.json"
connections_before=$(jq -r '.connections' "$result_dir/plain-varz-before-soak.json")
connections_after=$(jq -r '.connections' "$result_dir/plain-varz-after-soak.json")
if (( connections_after > connections_before )); then
  printf 'NATS client connection count leaked across qualification: before=%s after=%s\n' \
    "$connections_before" "$connections_after" >&2
  exit 1
fi

git_revision=$(git -C "$repo_root" rev-parse HEAD)
nats_version=$(jq -r '.version' "$result_dir/plain-varz-after-soak.json")
jq -n \
  --arg completed_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg profile "$profile" \
  --arg git_revision "$git_revision" \
  --arg rustc "$(rustc --version)" \
  --arg cargo "$(cargo --version)" \
  --arg nats_version "$nats_version" \
  --argjson leak_window_verified "$leak_window_verified" \
  --argjson connections_before "$connections_before" \
  --argjson connections_after "$connections_after" \
  '{
    passed: true,
    completed_at: $completed_at,
    profile: $profile,
    git_revision: $git_revision,
    rustc: $rustc,
    cargo: $cargo,
    nats_server: $nats_version,
    fixed_images: {
      nats: "nats:2.12.4-alpine",
      nats_box: "natsio/nats-box:0.18.0"
    },
    core_only: true,
    tls_credentials_acl: true,
    last_30m_leak_window_verified: $leak_window_verified,
    nats_connections: {before_soak: $connections_before, after_soak: $connections_after},
    raw_credentials_captured: false
  }' >"$result_dir/report.json"
printf 'Core NATS qualification evidence: %s\n' "$result_dir"
