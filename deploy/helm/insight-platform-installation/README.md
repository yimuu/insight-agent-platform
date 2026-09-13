# Local Kubernetes installation

Prepare configuration once, then start the complete platform with `helm upgrade --install`.
The chart uses the same finite Rust installer as Compose. No host Python controller is required.
This remains a single-node development topology with node-bound ReadWriteOnce volumes.

## Prepare configuration

Select runtime and Console images by `REPOSITORY@sha256:DIGEST`, accessible to the cluster.
The installation name is also the namespace. The following commands run only configuration
factories, without starting dependencies or accessing a Kubernetes API:

```sh
RUNTIME_DIGEST="${RUNTIME_IMAGE##*@}"
docker run --rm --network none --read-only --cap-drop ALL \
  --entrypoint /usr/local/bin/platform-installation "$RUNTIME_IMAGE" \
  kubernetes-input my-platform "$RUNTIME_DIGEST" > input.json
```

Set model destinations in the public input before installation. Optional remote Context destinations
must be supplied to the input factory so it derives the complete role closure; see the
[installation guide](../../../docs/current/installation.md). The chart does not patch process JSON.

```sh
INPUT_FILE="$(pwd)/input.json"
docker run --rm --network none --read-only --cap-drop ALL \
  --user "$(id -u):$(id -g)" \
  --mount "type=bind,source=$INPUT_FILE,target=/input.json,readonly" \
  --entrypoint /usr/local/bin/platform-installation "$RUNTIME_IMAGE" \
  helm-values --input /input.json \
  --runtime-image "$RUNTIME_IMAGE" --console-image "$CONSOLE_IMAGE" > values.json
```

## Start and observe

Use your explicitly selected Kubernetes context and choose the node that will hold all local PVCs:

```sh
helm upgrade --install installation deploy/helm/insight-platform-installation \
  --namespace my-platform --create-namespace -f values.json \
  --set node="$NODE_NAME" --wait --wait-for-jobs --timeout 15m
kubectl -n my-platform get pods,jobs
```

`storageClass` optionally selects a provisioner; its default is the cluster's default storage class.
Provisioning requires new empty private volume directories. The install Job prepares configuration,
publishes its input-bound preparation gate, bootstraps the ordinary OpenBao server, provisions the
platform and publishes completion. Dependencies wait for the first gate; serving init containers
wait for completion. These public files convey no business authority. Serving binaries still check
their actual role configuration and installed schema.

Only administrative Jobs mount installation-private state. Each serving role mounts its own
read-only files, and the public gates have a separate PVC. Pods have no Kubernetes API token or
workload-management role. S3 and OpenBao receive only their own dependency files and persistent data.
TLS CA and hostname verification remain enabled. The static seal and private local volumes are a
development trust boundary, not production HSM isolation.

Repeating the Helm command starts a new finite install Job, which verifies existing state rather
than repeating provisioning writes. A failed or interrupted bootstrap retains its one-time intent;
retries may observe or finish root-token revocation, but never repeat uncertain initialization.
Keep original PVCs and Job evidence. Changed input, credentials or configuration are rejected.
PVCs and completed Jobs have Helm keep policies. Uninstalling a release does not erase its data.

## Explicit administrator session

Ordinary installation never signs a session. Create a Job from the suspended management template:

```sh
umask 077
kubectl -n my-platform create job installation-session-manual \
  --from=cronjob/installation-session
kubectl -n my-platform wait --for=condition=Ready pod \
  -l job-name=installation-session-manual --timeout=90s
SESSION_POD=$(kubectl -n my-platform get pod -l job-name=installation-session-manual \
  -o jsonpath='{.items[0].metadata.name}')
kubectl -n my-platform cp "$SESSION_POD:/delivery/session-token" ./session-token
kubectl -n my-platform cp "$SESSION_POD:/delivery/result.json" ./session-info.json
kubectl -n my-platform cp "$SESSION_POD:/delivery/public-ca.pem" ./public-ca.pem
kubectl -n my-platform delete job installation-session-manual
```

Readiness is published only after the delivery files are complete. The Job retains them in a
private ephemeral memory volume for up to 15 minutes; token bytes never enter logs or Helm notes.
The safe result file contains tenant, endpoint and expiry. Use the local token file with Console
or the public CLI. Run one delivery at a time. If copying is interrupted, continue from the same
Job before deleting it. Explicitly repeat this procedure to renew a session.

For independent public CA export, use the same procedure with
`--from=cronjob/installation-public-trust`, a distinct Job name, and copy only `public-ca.pem` and
`result.json`. This template mounts installation state read-only, contacts no provider and does
not issue a session. The result's certificate digest hashes the exact PEM file bytes.

## Access and qualification

Console can be port-forwarded to its configured origin. Signed object URLs retain the exact
`s3.NAME.svc.cluster.local:8333` host. Browser object access needs matching DNS routing and explicit
trust in the public CA. Console forwarding alone does not establish that route, and the chart
never changes OS trust or disables TLS verification.

Rendering tests and actual deployment evidence are separate. The disposable test harness is
`tools/qualification/qualify-platform-installation-kind.py --runtime-image REPOSITORY@DIGEST
--console-image REPOSITORY@DIGEST --report /absolute/unused-report.json`. Python is used for this
contributor test harness only, not ordinary startup. It creates an isolated Kind cluster, validates
OCI image graphs, invokes native Helm commands and checks repeated installation, serving Pod
recovery, TLS rejection, read-only verification and explicit delivery. It preserves failed fixtures
and only reports a pass after cleaning its own successful cluster. Existing clusters are untouched.
