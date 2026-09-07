#!/usr/bin/env bash
set -euo pipefail

if ! docker info --format '{{json .DriverStatus}}' | \
  jq -e 'any(.[]; . == ["driver-type", "io.containerd.snapshotter.v1"])' >/dev/null; then
  printf 'Kind OCI qualification requires Docker with the containerd image store enabled\n' >&2
  exit 1
fi
