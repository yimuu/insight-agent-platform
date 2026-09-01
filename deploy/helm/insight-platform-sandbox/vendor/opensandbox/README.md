# OpenSandbox source pin

This directory vendors the lifecycle schema from the Accepted CR-216 source pin:

- repository: `https://github.com/opensandbox-group/OpenSandbox`
- Server tag: `server/v0.2.3`
- exact commit: `c39b814f36ded4c61d5ac6f9332ee4dfbab86c00`
- Controller application: `v0.2.0`
- Controller chart source version: `0.2.1`
- execd: `v1.0.22`

The three CRD templates under `templates/crds/` are copied byte-for-byte from
`kubernetes/charts/opensandbox-controller/templates/crds/` at that commit. The Platform does not
patch or rebuild OpenSandbox Server, Controller, or execd. Platform-owned templates, RBAC,
NetworkPolicy, and admission wrap the official binaries to close unused Pool, Snapshot, task,
ingress, egress-sidecar, secure-runtime, and general-exec surfaces.
