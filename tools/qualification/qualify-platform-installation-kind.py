#!/usr/bin/env python3
"""Run the installation Helm consumer on a new disposable, explicitly owned Kind cluster.

The host's existing Kubernetes configuration and clusters are never consulted. Only
the selected, locally present product images and pinned dependency images are used.
Failures retain the owned cluster and private evidence; only full success cleans up.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import selectors
import shutil
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
NODE_IMAGE = "kindest/node@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0"
KIND_VERSION = "v0.33.0"
SUPPORTED_PLATFORMS = {"linux/amd64", "linux/arm64"}
SDK_OPERATIONS = frozenset({
    "kms_list_keys", "kms_describe_key", "kms_list_resource_tags", "kms_create_key",
    "s3_head_bucket", "s3_get_bucket_tagging", "s3_get_bucket_versioning", "s3_get_bucket_cors",
    "s3_create_bucket", "s3_put_bucket_tagging", "s3_put_bucket_versioning", "s3_put_bucket_cors",
    "secrets_describe_secret", "secrets_create_secret", "secrets_get_secret_value",
})
SDK_FAILURES = frozenset({"timeout", "dispatch", "service", "invalid_response", "unknown"})
S3_OPERATIONS = frozenset(operation for operation in SDK_OPERATIONS if operation.startswith("s3_"))
# Raw handshake output stays inside this bounded, read-only Pod command. Only
# exact OpenSSL verification categories cross exec; an I/O failure proves none.
LINUX_TLS_PROBE = '''{
  timeout -s KILL 10s openssl s_client -connect "$1:8333" -servername "$1" \\
    -verify_hostname "$2" -CAfile "$3" -no-CApath -no-CAstore \\
    -verify_return_error -min_protocol TLSv1.2 -brief </dev/null 2>&1
  printf '\\nqualification_exit=%s\\n' "$?"
} | awk '
/^Verification: OK$/ { verified++ }
/^verify error:num=(19|20|21):/ { issuer++; next }
/^verify error:num=62:/ { hostname++; next }
/^verify error:num=/ { other++ }
/^qualification_exit=[0-9]+$/ { exits++; split($0, parts, "="); code=parts[2] }
END { if (exits != 1) code=255;
  printf "tls_probe exit=%d verified=%d issuer=%d hostname=%d other=%d\\n", code, verified, issuer, hostname, other }
'
'''


class QualificationFailure(Exception):
    pass


def plan_consumer():
    spec = importlib.util.spec_from_file_location("kind_helm_consumer", ROOT/"tools/qualification/installation_support.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def tls_probe_evidence(data, expected):
    match = re.fullmatch(rb"tls_probe exit=([0-9]{1,3}) verified=([0-9]) issuer=([0-9]) hostname=([0-9]) other=([0-9])\n", data)
    if match is None:
        raise QualificationFailure("linux_tls_evidence_invalid")
    code, verified, issuer, hostname, other = map(int, match.groups())
    valid = {"trusted": (code == 0 and verified == 1 and issuer == hostname == other == 0),
             "issuer": (code == 1 and issuer == 1 and verified == hostname == other == 0),
             "hostname": (code == 1 and hostname == 1 and verified == issuer == other == 0)}
    if not valid.get(expected, False):
        raise QualificationFailure("linux_tls_verification_did_not_match")


def sdk_diagnostic_entries(data):
    """Discard raw logs; these observations never establish an external write outcome."""
    if len(data) > 16_384:
        raise QualificationFailure("sdk_diagnostic_bytes_exceeded")
    entries = []
    for line in data.split(b"\n"):
        match = re.fullmatch(rb"installation_(aws|s3) operation=([a-z0-9_]+) failure=([a-z_]+)", line)
        if match is None:
            continue
        provider, operation, failure = (value.decode("ascii") for value in match.groups())
        operations = S3_OPERATIONS if provider == "s3" else SDK_OPERATIONS
        if operation not in operations or failure not in SDK_FAILURES:
            continue
        if len(entries) == 32:
            raise QualificationFailure("sdk_diagnostic_count_exceeded")
        entries.append({"operation": operation, "failure": failure})
    return entries


# Exact owning InstallationError variants; raw stderr is never report evidence.
INSTALLATION_ERRORS = frozenset({
    "InvalidInput", "InvalidEndpoint", "InvalidRoleClosure", "InvalidPath",
    "UnsupportedTopology", "IdentityDrift", "ConfigurationDrift", "ForeignState",
    "CredentialInvalid", "PrerequisiteUnavailable", "SchemaMismatch",
    "ExternalOutcomeUnknown", "Conflict", "Incomplete",
})


def installation_diagnostic_entries(data):
    if len(data) > 16_384:
        raise QualificationFailure("installation_diagnostic_bytes_exceeded")
    entries = []
    for line in data.split(b"\n"):
        match = re.fullmatch(rb"installation ([A-Za-z]+)", line)
        if match is not None and match[1].decode("ascii") in INSTALLATION_ERRORS:
            if len(entries) == 32:
                raise QualificationFailure("installation_diagnostic_count_exceeded")
            entries.append(match[1].decode("ascii"))
    return entries


def completed_controller_replacement(pods, previous_uid):
    """A replacement is complete after the old Pod's termination grace period.

    Kubernetes may report both Pods Ready while the deleted Pod is terminating.
    Do not take immutable-file evidence until only the live replacement remains.
    """
    if len(pods) != 1:
        return None
    pod = pods[0]
    metadata = pod.get("metadata", {})
    if metadata.get("uid") in (None, previous_uid) or metadata.get("deletionTimestamp") is not None:
        return None
    if not any(condition.get("type") == "Ready" and condition.get("status") == "True"
               for condition in pod.get("status", {}).get("conditions", [])):
        return None
    return pod


def run(arguments, *, timeout=300, environment=None):
    process = subprocess.Popen(arguments, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, env=environment, start_new_session=True)
    output = bytearray()
    received = 0
    deadline = time.monotonic()+timeout
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            selector.register(process.stderr, selectors.EVENT_READ)
            while selector.get_map():
                remaining = deadline-time.monotonic()
                if remaining <= 0:
                    raise QualificationFailure("command_timeout:"+Path(arguments[0]).name)
                for key, _ in selector.select(min(remaining, 1)):
                    chunk = os.read(key.fd, 65_536)
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    received += len(chunk)
                    if received > 8_388_608:
                        raise QualificationFailure("command_output_exceeded")
                    if key.fileobj is process.stdout:
                        output.extend(chunk)
        if process.wait(timeout=max(.01, deadline-time.monotonic())):
            raise QualificationFailure("command_failed:"+Path(arguments[0]).name)
        return bytes(output)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
        process.stdout.close()
        process.stderr.close()


def write(path, data, mode=0o600):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    with os.fdopen(descriptor, "wb") as file:
        file.write(data)
        file.flush()
        os.fsync(file.fileno())


def json_write(path, value):
    write(path, json.dumps(value, sort_keys=True, separators=(",", ":")).encode()+b"\n")


def client_shim(node, kubeconfig, context):
    """Render a private executable for one already-owned node and exact host context."""
    return f'''#!{sys.executable}
import os, sys
arguments = sys.argv[1:]
expected = ["--kubeconfig", {str(kubeconfig)!r}, "--context", {context!r}]
if arguments[:4] != expected or any(a in ("--kubeconfig", "--context", "--server", "--token", "--client-key", "--client-certificate") or a.startswith(("--kubeconfig=", "--context=", "--server=", "--token=")) for a in arguments[4:]):
    raise SystemExit(64)
os.execvp("docker", ["docker", "exec", "-i", {node!r}, "/usr/bin/kubectl", "--kubeconfig", "/etc/kubernetes/admin.conf", *arguments[4:]])
'''


def archive_identity(path, expected_digest, platform):
    """Verify the exported descriptor and its selected manifest/config/layer bytes.

    Docker's platform export can retain the original multi-platform index. Other
    platform descriptors do not assert that their bytes are in this archive.
    The selected platform's complete graph must be present and content verified.
    """
    if platform not in SUPPORTED_PLATFORMS:
        raise QualificationFailure("unsupported_image_platform")
    expected_os, expected_arch = platform.split("/")
    with tarfile.open(path, "r:") as archive:
        members = {}
        for member in archive:
            if len(members) >= 100_000 or member.name in members or member.name.startswith("/") or ".." in Path(member.name).parts or not (member.isfile() or member.isdir()):
                raise QualificationFailure("unsafe_image_archive")
            members[member.name] = member
        def data(name, maximum=16_777_216):
            member = members.get(name)
            if member is None or not member.isfile() or member.size > maximum:
                raise QualificationFailure("image_blob_missing_or_large")
            return archive.extractfile(member).read(maximum+1)
        def blob(descriptor, *, parsed=True):
            digest = descriptor["digest"]
            if not re.fullmatch(r"sha256:[a-f0-9]{64}", digest):
                raise QualificationFailure("image_digest_invalid")
            member = members.get("blobs/sha256/"+digest[7:])
            if member is None or member.size != descriptor["size"] or not member.isfile():
                raise QualificationFailure("image_blob_missing")
            hasher = hashlib.sha256()
            output = bytearray()
            with archive.extractfile(member) as stream:
                while chunk := stream.read(1_048_576):
                    hasher.update(chunk)
                    if parsed:
                        output.extend(chunk)
                        if len(output) > 16_777_216:
                            raise QualificationFailure("image_json_exceeded")
            if "sha256:"+hasher.hexdigest() != digest:
                raise QualificationFailure("image_blob_digest_differs")
            return json.loads(output) if parsed else None
        index = json.loads(data("index.json"))
        roots = [item for item in index["manifests"] if item["digest"] == expected_digest]
        if not roots:
            raise QualificationFailure("export_does_not_prove_selected_image")
        selected = roots[0]
        value = blob(selected)
        depth = 0
        while "manifests" in value:
            depth += 1
            if depth > 8:
                raise QualificationFailure("image_index_depth_exceeded")
            matching = [item for item in value["manifests"] if item.get("platform", {}).get("os") == expected_os and item.get("platform", {}).get("architecture") == expected_arch]
            if len(matching) != 1:
                raise QualificationFailure("image_platform_ambiguous")
            selected = matching[0]
            value = blob(selected)
        config = blob(value["config"])
        if config.get("os") != expected_os or config.get("architecture") != expected_arch:
            raise QualificationFailure("image_platform_differs")
        for layer in value["layers"]:
            blob(layer, parsed=False)
        return {"image_digest": expected_digest, "manifest_digest": selected["digest"],
                "config_digest": value["config"]["digest"], "platform": platform}


class Fixture:
    def __init__(self, runtime, console, directory):
        self.directory = directory
        self.nonce = uuid.uuid4().hex[:12]
        self.name = "insight-installation-"+self.nonce
        self.node = self.name+"-control-plane"
        self.namespace = "installation-"+self.nonce
        self.kubeconfig = directory/"kubeconfig"
        self.context = "kind-"+self.name
        self.runtime, self.console = runtime, console
        self.started = False
        self.report = {"schema_version": 1, "cluster": self.name, "namespace": self.namespace,
                       "node_image": NODE_IMAGE, "images": {}, "checks": [], "cleaned": False}
        self.environment = {key: value for key, value in os.environ.items()
                            if not key.startswith(("KUBE", "HELM_KUBE", "HELM_DRIVER", "KIND_"))}
        self.environment.update(KUBECONFIG=str(self.kubeconfig), KIND_EXPERIMENTAL_PROVIDER="docker", HELM_DRIVER="secret")

    def progress(self, value):
        self.report["stage"] = value
        print(value, flush=True)

    def docker(self, *arguments, timeout=300):
        return run(["docker", *arguments], timeout=timeout, environment=self.environment)

    def owned_node(self):
        result = json.loads(self.docker("inspect", self.node))[0]
        if result["Config"]["Labels"].get("io.x-k8s.kind.cluster") != self.name:
            raise QualificationFailure("node_ownership_differs")
        return result

    def kube(self, *arguments, timeout=300):
        return run([str(self.directory/"bin/kubectl"), "--kubeconfig", str(self.kubeconfig),
                    "--context", self.context, *arguments], timeout=timeout, environment=self.environment)

    def begin(self):
        if run(["kind", "version"], environment=self.environment).decode().split()[:2] != ["kind", KIND_VERSION]:
            raise QualificationFailure("kind_version_differs")
        node = json.loads(self.docker("image", "inspect", NODE_IMAGE))[0]
        runtime = json.loads(self.docker("image", "inspect", self.runtime))[0]
        console = json.loads(self.docker("image", "inspect", self.console))[0]
        if any(image.get("Descriptor", {}).get("digest") != reference.split("@", 1)[1] for image, reference in ((runtime, self.runtime), (console, self.console))):
            raise QualificationFailure("containerd_image_store_with_repository_descriptors_required")
        self.platform = runtime["Os"]+"/"+runtime["Architecture"]
        if self.platform not in SUPPORTED_PLATFORMS or any(image["Os"]+"/"+image["Architecture"] != self.platform for image in (node, console)):
            raise QualificationFailure("selected_image_platforms_differ")
        self.report["platform"] = self.platform
        # This image includes a client matching the pinned Kubernetes server.
        version = json.loads(self.docker("run", "--rm", "--network", "none", "--read-only",
                             "--entrypoint", "/usr/bin/kubectl", NODE_IMAGE, "version", "--client", "--output=json"))
        if version["clientVersion"]["gitVersion"] != "v1.35.8":
            raise QualificationFailure("node_client_version_differs")
        self.report["kubectl_version"] = version["clientVersion"]["gitVersion"]
        config = self.directory/"kind.json"
        json_write(config, {"kind": "Cluster", "apiVersion": "kind.x-k8s.io/v1alpha4",
                    "networking": {"apiServerAddress": "127.0.0.1", "apiServerPort": 0},
                    "nodes": [{"role": "control-plane", "extraMounts": [{"hostPath": str(self.directory), "containerPath": str(self.directory)}]}]})
        if getattr(self, "subnet", None):
            import ipaddress
            subnet = ipaddress.ip_network(self.subnet, strict=True)
            if subnet.version != 4 or not subnet.is_private:
                raise QualificationFailure("private_fixture_subnet_required")
            self.fixture_network = self.name+"-network"
            self.docker("network", "create", "--subnet", str(subnet), "--label", "insight.qualification="+self.name, self.fixture_network)
            self.environment["KIND_EXPERIMENTAL_DOCKER_NETWORK"] = self.fixture_network
        self.progress("creating isolated pinned Kind cluster "+self.name)
        self.started = True  # Preserve the exact ownership intent if creation loses its response.
        run(["kind", "create", "cluster", "--name", self.name, "--image", NODE_IMAGE,
             "--config", str(config), "--kubeconfig", str(self.kubeconfig), "--wait", "180s"], timeout=240, environment=self.environment)
        self.owned_node()
        os.chmod(self.kubeconfig, 0o600)
        binary = self.directory/"bin"
        binary.mkdir(mode=0o700)
        # A fixed-node client shim preserves exec streams and refuses every other
        # kubeconfig/context. Host fixture files are mounted at the same private path.
        shim = client_shim(self.node, self.kubeconfig, self.context)
        write(binary/"kubectl", shim.encode(), 0o500)
        self.environment["PATH"] = str(binary)+os.pathsep+os.environ["PATH"]
        self.kube("wait", "--for=condition=Ready", "node/"+self.node, "--timeout=90s", timeout=100)
        self.report["checks"].append("isolated_cluster_matching_client")

    def import_image(self, image, key):
        info = json.loads(self.docker("image", "inspect", image))[0]
        expected = image.split("@", 1)[1]
        if info.get("Descriptor", {}).get("digest") != expected:
            raise QualificationFailure("local_image_descriptor_differs")
        archive = self.directory/(key+".tar")
        self.progress("verifying and importing "+key+" image")
        # Retain the original index bytes. Docker's --platform export replaces
        # the archive root with its child, which cannot prove the selected index.
        self.docker("image", "save", image, "--output", str(archive), timeout=300)
        identity = archive_identity(archive, expected, self.platform)
        repository = image.split("@", 1)[0]
        self.docker("exec", "--privileged", self.node, "ctr", "--namespace=k8s.io", "images", "import", "--platform", self.platform,
                    "--digests", "--base-name", repository, "--snapshotter=overlayfs", str(archive), timeout=300)
        listing = self.docker("exec", self.node, "ctr", "--namespace=k8s.io", "images", "list").decode()
        rows = [line.split() for line in listing.splitlines()[1:]]
        candidates = [row[0] for row in rows if len(row) >= 3 and row[2] == expected]
        if not candidates:
            raise QualificationFailure("imported_descriptor_missing")
        first = repository.split("/", 1)[0]
        normalized = repository if "." in first or ":" in first or first == "localhost" else "docker.io/"+(repository if "/" in repository else "library/"+repository)
        references = {image: expected, normalized+"@"+expected: expected}
        # containerd also creates an image for the archive layout index. CRI can
        # select that same-config descriptor; normalize its name without changing
        # its target, as the existing Kind OCI qualification does.
        for row in rows:
            if row[0].startswith(repository+"@"):
                if row[0].split("@", 1)[1] != row[2]:
                    raise QualificationFailure("imported_alias_digest_differs")
                references[normalized+"@"+row[2]] = row[2]
        for reference, target in references.items():
            existing = [row for row in rows if row[0] == reference]
            if existing and existing[0][2] != target:
                raise QualificationFailure("imported_reference_digest_differs")
            if not existing:
                sources = [row[0] for row in rows if row[2] == target]
                if not sources:
                    raise QualificationFailure("imported_platform_descriptor_missing")
                self.docker("exec", self.node, "ctr", "--namespace=k8s.io", "images", "tag", sources[0], reference)
        verified = json.loads(self.docker("exec", self.node, "ctr", "--namespace=k8s.io", "content", "get", identity["manifest_digest"]))
        if verified["config"]["digest"] != identity["config_digest"]:
            raise QualificationFailure("imported_config_digest_differs")
        self.report["images"][key] = dict(reference=image, **identity)
        archive.unlink()

    def setup(self):
        path = self.directory/"input.json"
        producer = ("run", "--rm", "--network", "none", "--read-only", "--cap-drop", "ALL",
                    "--security-opt", "no-new-privileges:true", "--user", f"{os.geteuid()}:{os.getegid()}")
        declaration = self.docker(*producer, "--entrypoint", "/usr/local/bin/platform-installation",
                                  self.runtime, "kubernetes-input", self.namespace, self.runtime.split("@", 1)[1])
        consumer = plan_consumer()
        document = consumer.decode(declaration)
        write(path, declaration)
        raw = self.docker(*producer, "--mount", f"type=bind,source={path},target=/installation-input/input.json,readonly",
                          "--entrypoint", "/usr/local/bin/platform-installation", self.runtime,
                          "helm-values", "--input", "/installation-input/input.json",
                          "--runtime-image", self.runtime, "--console-image", self.console)
        envelope = consumer.decode(raw)
        if set(envelope) != {"plan"}:
            raise QualificationFailure("values_envelope_differs")
        plan = consumer.validate_plan(envelope["plan"])
        if (plan["input"] != document or plan["namespace"] != self.namespace
                or plan["runtime_image"] != self.runtime or plan["console_image"] != self.console):
            raise QualificationFailure("selected_runtime_plan_identity_differs")
        json_write(self.directory/"producer-plan.json", plan)
        # These references are produced by the selected runtime. Their actual
        # descriptor graphs are then checked by the same OCI import as products.
        images = [(self.runtime, "runtime"), (self.console, "console")]
        images += [(image, name) for name, image in sorted(plan["dependencies"].items())]
        for image, key in images:
            self.import_image(image, key)
        self.installation = self.directory/"installation"
        self.installation.mkdir(mode=0o700)
        self.plan = plan
        self.values = self.directory/"values.json"
        json_write(self.values, {"plan": plan, "node": self.node, "storageClass": ""})

    def operation(self, name):
        self.progress("actual declarative Helm "+name)
        if name == "up":
            return run(["helm", "upgrade", "--install", "installation", str(ROOT/"deploy/helm/insight-platform-installation"),
                        "--namespace", self.namespace, "--create-namespace", "--values", str(self.values),
                        "--wait", "--wait-for-jobs", "--timeout", "900s"], timeout=930, environment=self.environment)
        if name == "verify":
            source = json.loads(self.kube("get", "job", "installation-1", "-n", self.namespace, "-o", "json"))
            spec = source["spec"]["template"]["spec"]
            container = spec["containers"][0]
            container["args"][0] = container["args"][0].replace(" install --input ", " verify --input ")
            job = {"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": "verification-"+uuid.uuid4().hex[:12], "namespace": self.namespace},
                   "spec": {"backoffLimit": 0, "activeDeadlineSeconds": 300, "template": {"spec": spec}}}
            path = self.directory/"verification.json"
            json_write(path, job)
            self.kube("create", "-f", str(path))
            self.kube("wait", "--for=condition=Complete", "job/"+job["metadata"]["name"], "-n", self.namespace, "--timeout=300s", timeout=310)
            return b""
        if name in ("session", "public-trust"):
            job = name+"-"+uuid.uuid4().hex[:12]
            self.kube("create", "job", job, "--from=cronjob/installation-"+name, "-n", self.namespace)
            self.kube("wait", "--for=condition=Ready", "pod", "--selector", "job-name="+job, "-n", self.namespace, "--timeout=90s", timeout=100)
            pods = json.loads(self.kube("get", "pods", "-n", self.namespace, "--selector", "job-name="+job, "-o", "json"))["items"]
            if len(pods) != 1:
                raise QualificationFailure("session_pod_ambiguous")
            pod = pods[0]["metadata"]["name"]
            for source, destination in [("result.json", name+"-result.json"), ("public-ca.pem", "public-ca.pem")]+([("session-token", "session-token")] if name == "session" else []):
                self.kube("cp", "-n", self.namespace, pod+":/delivery/"+source, str(self.installation/destination))
                os.chmod(self.installation/destination, 0o600)
            self.kube("delete", "job", job, "-n", self.namespace, "--wait=true")
            return b""
        raise QualificationFailure("unknown_fixture_operation")

    def snapshot(self):
        deployments = json.loads(self.kube("get", "deployments", "--namespace", self.namespace, "--output=json"))["items"]
        claims = json.loads(self.kube("get", "pvc", "--namespace", self.namespace, "--output=json"))["items"]
        plan = self.plan
        jobs = json.loads(self.kube("get", "pods", "-n", self.namespace, "--selector", "job-name=installation-1", "-o", "json"))["items"]
        if len(jobs) != 1:
            raise QualificationFailure("installation_completion_ambiguous")
        completion = json.loads(jobs[0]["status"]["containerStatuses"][0]["state"]["terminated"]["message"])
        if completion.get("input_digest") != plan["input_digest"] or completion.get("phase") != "ready":
            raise QualificationFailure("installation_completion_differs")
        file_digests = {}
        for name in [process["name"] for process in plan["processes"]]+["console"]:
            pods = json.loads(self.kube("get", "pods", "--namespace", self.namespace, "--selector", "insight.platform/process="+name, "--output=json"))["items"]
            if len(pods) != 1 or not any(c.get("type") == "Ready" and c.get("status") == "True" for c in pods[0].get("status", {}).get("conditions", [])):
                raise QualificationFailure("serving_file_evidence_pod_not_ready")
            # The process reads only its own immutable mount. Hash output contains
            # no environment values, credential bytes or installation-root files.
            command = ["sha256sum", "/run/insight/console/config.json"] if name == "console" else ["find", "/run/insight/role", "-type", "f", "-exec", "sha256sum", "{}", "+"]
            hashes = self.kube("exec", "--namespace", self.namespace, pods[0]["metadata"]["name"], "--container", "console" if name == "console" else "process", "--", *command)
            lines = hashes.decode().splitlines()
            prefix = "/run/insight/console/" if name == "console" else "/run/insight/role/"
            if not 1 <= len(lines) <= 128 or any(not re.fullmatch(r"[a-f0-9]{64}  "+re.escape(prefix)+r"[a-zA-Z0-9/._-]+", line) for line in lines):
                raise QualificationFailure("serving_file_evidence_invalid")
            file_digests[name] = hashlib.sha256("\n".join(sorted(lines)).encode()).hexdigest()
        return {"identity_digest": completion["identity_digest"], "role_file_digests": file_digests,
                "deployments": {item["metadata"]["name"]: {"uid": item["metadata"]["uid"], "generation": item["metadata"]["generation"],
                    "spec_digest": hashlib.sha256(json.dumps(item["spec"], sort_keys=True).encode()).hexdigest()} for item in deployments},
                "pvcs": {item["metadata"]["name"]: item["metadata"]["uid"] for item in claims}}

    def qualify(self):
        self.operation("up")
        self.qualify_tls()
        first = self.snapshot()
        self.operation("up")
        if self.snapshot() != first:
            raise QualificationFailure("repeated_helm_install_changed_workloads_or_identity")
        self.operation("session")
        token_digest = hashlib.sha256((self.installation/"session-token").read_bytes()).hexdigest()
        ca = (self.installation/"public-ca.pem").read_bytes()
        self.operation("public-trust")
        trust = json.loads((self.installation/"public-trust-result.json").read_bytes())
        if (trust["input_digest"] != self.plan["input_digest"] or trust["identity_digest"] != first["identity_digest"]
                or trust["certificate_pem"].encode() != ca or trust["certificate_sha256"] != "sha256:"+hashlib.sha256(ca).hexdigest()
                or (self.installation/"public-ca.pem").read_bytes() != ca
                or hashlib.sha256((self.installation/"session-token").read_bytes()).hexdigest() != token_digest):
            raise QualificationFailure("public_trust_changed_identity_or_session")
        self.report["checks"].append("readonly_public_trust_delivery_preserves_session")
        self.operation("verify")
        if self.snapshot() != first:
            raise QualificationFailure("readonly_verify_changed_installation")
        self.report["checks"].extend(["single_helm_install_ready_without_host_orchestration", "repeated_helm_install_preserves_identity", "readonly_verify_preserves_identity_workloads_volumes"])
        self.progress("actual controller recovery of one serving Pod")
        pods = json.loads(self.kube("get", "pods", "--namespace", self.namespace, "--selector", "insight.platform/process=gateway-runtime", "--output=json"))["items"]
        if len(pods) != 1:
            raise QualificationFailure("runtime_pod_ambiguous")
        before = pods[0]["metadata"]
        options = self.directory/"restart-delete.json"
        json_write(options, {"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": {"uid": before["uid"]}})
        self.kube("delete", "--raw", f"/api/v1/namespaces/{self.namespace}/pods/{before['name']}", "--filename", str(options))
        recovered = self.wait_controller_replacement(before["uid"])
        if self.snapshot() != first:
            raise QualificationFailure("controller_recovery_changed_installation")
        self.report["restart"] = {"old_pod_uid": before["uid"], "new_pod_uid": recovered["metadata"]["uid"]}
        self.report["checks"].append("controller_new_pod_ready_same_identity_and_configuration")
        token = self.installation/"session-token"
        previous = hashlib.sha256(token.read_bytes()).digest()
        self.operation("session")
        metadata = token.lstat()
        if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != 0o600 or metadata.st_uid != os.getuid() or metadata.st_nlink != 1 or hashlib.sha256(token.read_bytes()).digest() == previous:
            raise QualificationFailure("explicit_session_delivery_invalid")
        self.report["checks"].append("explicit_session_renewal_private_file")
        self.report["installation"] = first

    def wait_controller_replacement(self, previous_uid):
        deadline = time.monotonic()+120
        while True:
            remaining = deadline-time.monotonic()
            if remaining <= 0:
                raise QualificationFailure("controller_recovery_timeout")
            pods = json.loads(self.kube("get", "pods", "--namespace", self.namespace, "--selector",
                                        "insight.platform/process=gateway-runtime", "--output=json",
                                        timeout=min(remaining, 15)))["items"]
            if time.monotonic() >= deadline:
                raise QualificationFailure("controller_recovery_timeout")
            recovered = completed_controller_replacement(pods, previous_uid)
            if recovered is not None:
                return recovered
            time.sleep(1)

    def qualify_tls(self):
        self.progress("actual installed public CA and Linux OpenSSL 3 TLS verification")
        items = json.loads(self.kube("get", "pods", "--namespace", self.namespace,
                                    "--selector", "insight.platform/process=artifact-gateway", "--output=json"))["items"]
        if (len(items) != 1 or items[0]["metadata"].get("deletionTimestamp") is not None
                or items[0]["metadata"].get("namespace") != self.namespace
                or not any(container.get("name") == "process" and container.get("image") == self.runtime
                           for container in items[0].get("spec", {}).get("containers", []))
                or not any(status.get("name") == "process" and status.get("ready") is True
                           for status in items[0].get("status", {}).get("containerStatuses", []))):
            raise QualificationFailure("tls_evidence_pod_not_ready")
        command = ("exec", "--namespace", self.namespace, items[0]["metadata"]["name"], "--container", "process", "--")
        version = self.kube(*command, "openssl", "version", timeout=10)
        version_match = re.fullmatch(rb"OpenSSL (3\.[0-9]{1,3}\.[0-9]{1,3})[ -~]{0,160}\n", version)
        if version_match is None:
            raise QualificationFailure("linux_openssl3_required")
        host = "s3."+self.namespace+".svc.cluster.local"
        ca = "/run/insight/role/credentials/ca.pem"
        for expected, hostname, trust in [("trusted", host, ca), ("issuer", host, "/etc/ssl/certs/ca-certificates.crt"),
                                          ("hostname", "wrong.installation.invalid", ca)]:
            data = self.kube(*command, "/bin/sh", "-c", LINUX_TLS_PROBE, "installation-tls",
                             host, hostname, trust, timeout=15)
            tls_probe_evidence(data, expected)
        self.report["tls"] = {"openssl": "OpenSSL "+version_match[1].decode(), "exact_dns_verified": True,
                              "unknown_ca_rejected": True, "wrong_san_rejected": True}
        self.report["checks"].append("installed_public_ca_linux_openssl3_exact_dns_and_rejections")

    def failure_sdk_diagnostics(self, items):
        entries = []
        owner_errors = []
        for pod in items:
            metadata = pod.get("metadata", {})
            if (metadata.get("namespace") != self.namespace or pod.get("status", {}).get("phase") != "Failed"
                    or not any(ref.get("kind") == "Job" and ref.get("name", "").startswith("installation-") and ref.get("controller") is True
                               for ref in metadata.get("ownerReferences", []))):
                continue
            containers = pod.get("spec", {}).get("containers", [])
            if len(containers) != 1 or containers[0].get("image") != self.runtime or containers[0].get("name") != "installation":
                continue
            data = self.kube("logs", metadata["name"], "-n", self.namespace, "-c", "installation",
                             "--tail=64", "--limit-bytes=16384", "--request-timeout=5s", timeout=10)
            entries.extend(sdk_diagnostic_entries(data))
            owner_errors.extend(installation_diagnostic_entries(data))
        return {"status": "observed" if entries or owner_errors else "unknown", "entries": entries,
                "installation_errors": owner_errors}

    def diagnostics(self):
        try:
            items = json.loads(self.kube("get", "pods", "--namespace", self.namespace, "--output=json"))["items"]
            safe = []
            for item in items:
                statuses = []
                for status in item.get("status", {}).get("containerStatuses", []):
                    state = status.get("state", {})
                    statuses.append({"name": status["name"], "restarts": status.get("restartCount"), "ready": status.get("ready"),
                                     "waiting": state.get("waiting", {}).get("reason"), "exit": state.get("terminated", {}).get("exitCode")})
                safe.append({"name": item["metadata"]["name"], "phase": item.get("status", {}).get("phase"), "containers": statuses})
            self.report["failure_pods"] = safe
            print(json.dumps({"failure_pods": safe}), flush=True)
            try:
                self.report["failure_sdk"] = self.failure_sdk_diagnostics(items)
            except Exception:
                self.report["failure_sdk"] = {"status": "unavailable", "entries": []}
            print(json.dumps({"failure_sdk": self.report["failure_sdk"]}), flush=True)
        except Exception:
            self.report["diagnostics"] = "unavailable"

    def close(self):
        if self.started:
            def owned():
                return self.docker("container", "ls", "--all", "--filter", "label=io.x-k8s.kind.cluster="+self.name, "--format", "{{.Names}}").decode().splitlines()
            nodes = owned()
            if nodes:
                if nodes != [self.node]:
                    raise QualificationFailure("cluster_node_inventory_differs")
                self.owned_node()
                run(["kind", "delete", "cluster", "--name", self.name, "--kubeconfig", str(self.kubeconfig)], timeout=120, environment=self.environment)
            if owned():
                raise QualificationFailure("owned_cluster_cleanup_incomplete")
        if getattr(self, "fixture_network", None):
            network = json.loads(self.docker("network", "inspect", self.fixture_network))[0]
            if network.get("Labels", {}).get("insight.qualification") != self.name or network.get("Containers"):
                raise QualificationFailure("fixture_network_ownership_differs")
            self.docker("network", "rm", self.fixture_network)
        self.report["cleaned"] = True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-image", required=True)
    parser.add_argument("--console-image", required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--subnet", help="Explicit non-overlapping private subnet for this disposable fixture")
    arguments = parser.parse_args()
    if not arguments.report.is_absolute() or not arguments.report.parent.is_dir() or arguments.report.exists() or arguments.report.is_symlink():
        raise QualificationFailure("unused_absolute_report_file_required")
    for image in (arguments.runtime_image, arguments.console_image):
        if not re.fullmatch(r"[a-z0-9._/-]+@sha256:[a-f0-9]{64}", image):
            raise QualificationFailure("immutable_repository_image_required")
    os.umask(0o077)
    temporary = Path(tempfile.mkdtemp(prefix="insight-installation-kind-")).resolve()
    fixture = Fixture(arguments.runtime_image, arguments.console_image, temporary)
    fixture.subnet = getattr(arguments, "subnet", None)
    error = None
    try:
        fixture.begin()
        fixture.setup()
        fixture.qualify()
    except (Exception, KeyboardInterrupt) as failure:
        error = type(failure).__name__+":"+(str(failure) if isinstance(failure, QualificationFailure) else "bounded_failure")
        try:
            fixture.diagnostics()
        except (Exception, KeyboardInterrupt):
            fixture.report["diagnostics"] = "unavailable"
    if error is None:
        try:
            fixture.close()
            shutil.rmtree(temporary)
        except (Exception, KeyboardInterrupt):
            error = "qualification_cleanup_failed"
    fixture.report["result"] = "PASS" if error is None else "FAIL"
    if error is not None:
        # First-init Requested/Unknown evidence must survive a failed run. A new
        # run is not permission to recreate or erase this installation's state.
        fixture.report.update(failure=error, cleaned=False, evidence_directory=str(temporary))
    json_write(arguments.report, fixture.report)
    print(json.dumps({"result": fixture.report["result"], "cleaned": fixture.report["cleaned"], "report": str(arguments.report),
                      **({"evidence_directory": str(temporary)} if error is not None else {})}), flush=True)
    return 0 if error is None else 1


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda _number, _frame: (_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        raise SystemExit(main())
    except (QualificationFailure, OSError, subprocess.SubprocessError):
        print("isolated_kind_qualification_failed", file=sys.stderr)
        raise SystemExit(1) from None
