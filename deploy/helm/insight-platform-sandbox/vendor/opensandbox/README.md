# OpenSandbox source pin

This directory vendors the lifecycle schema from the Accepted CR-216 source pin:

- repository: `https://github.com/opensandbox-group/OpenSandbox`
- Server tag: `server/v0.2.3`
- exact commit: `c39b814f36ded4c61d5ac6f9332ee4dfbab86c00`
- Controller application: `v0.2.0`
- Controller chart source version: `0.2.1`
- execd tag: `docker/execd/v1.1.0`
- execd source commit: `48b0215f1bd097b31d0f022a44640e00c11ac49d`
- execd multi-platform index: `sha256:6cf7dba2f21f0b536e100563d841ac58a9f31c2b0a081b7ac76796a24d6f47e2`

The three CRD templates under `templates/crds/` are copied byte-for-byte from
`kubernetes/charts/opensandbox-controller/templates/crds/` at that commit. The Platform does not
patch or rebuild OpenSandbox Server, Controller, or execd. Platform-owned templates, RBAC,
NetworkPolicy, and admission wrap the official binaries to close unused Pool, Snapshot, task,
ingress, egress-sidecar, secure-runtime, and general-exec surfaces.
