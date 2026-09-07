#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
[[ $# = 0 ]] || { echo "usage: qualify-platform-artifact-provider.sh" >&2; exit 2; }
: "${PLATFORM_TEST_AWS_ENDPOINT:?real HTTPS S3/KMS fixture endpoint is required}"
: "${PLATFORM_TEST_S3_BUCKET:?versioned fixture bucket is required}"
: "${PLATFORM_TEST_KMS_KEY_ID:?complete fixture KMS key ARN is required}"
cd "$root"
cargo test --locked -p insight-platform-artifact-broker --lib \
  aws::tests::real_https_s3_and_kms_round_trip_exact_generation -- --ignored --exact --nocapture
