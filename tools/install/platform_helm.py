#!/usr/bin/env python3
"""Consume the shared installer on an explicitly selected local Kubernetes cluster.

Only this host wrapper uses operator Kubernetes credentials. Installation and serving
Pods have no API token. A live Job/Pod completion envelope gates every later phase.
"""
from __future__ import annotations

import argparse
import base64
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import selectors
import stat
import subprocess
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
CHART = ROOT / "deploy/helm/insight-platform-installation"
OWNER = "insight.platform/installation"
DIGEST = "insight.platform/input-digest"
MAXIMUM = 1_048_576
_TRUST_SPEC = importlib.util.spec_from_file_location("installation_public_trust", Path(__file__).with_name("public_trust.py"))
TRUST = importlib.util.module_from_spec(_TRUST_SPEC)
_TRUST_SPEC.loader.exec_module(TRUST)


class InstallationFailure(Exception):
    """Messages contain closed diagnostics, never external output or credentials."""


class SessionExpired(InstallationFailure):
    """Installation is ready; only a previously delivered session has expired."""


def failure_message(error):
    if isinstance(error, SessionExpired):
        return ("installation Helm is ready; its delivered session expired (SessionExpired). "
                "Run the same command with operation 'session' and the same installation arguments "
                "to renew explicitly. No new session was issued; private state and cluster resources were retained.")
    return "installation Helm operation failed; its private state and cluster resources were retained"


def decode(data):
    def unique(items):
        result = {}
        for key, value in items:
            if key in result:
                raise InstallationFailure("duplicate JSON field")
            result[key] = value
        return result
    if len(data) > MAXIMUM:
        raise InstallationFailure("installation JSON exceeds its bound")
    try:
        def invalid_constant(_value):
            raise InstallationFailure("non-finite JSON number")
        result = json.loads(data, object_pairs_hook=unique, parse_constant=invalid_constant)
        def bounded(value, depth=0):
            if depth > 32:
                raise InstallationFailure("JSON nesting exceeds its bound")
            if isinstance(value, dict):
                if len(value) > 64:
                    raise InstallationFailure("JSON object exceeds its bound")
                for key, child in value.items():
                    bounded(key, depth+1)
                    bounded(child, depth+1)
            elif isinstance(value, list):
                if len(value) > 256:
                    raise InstallationFailure("JSON array exceeds its bound")
                for child in value: bounded(child, depth+1)
            elif isinstance(value, str) and len(value.encode()) > 65_536:
                raise InstallationFailure("JSON string exceeds its bound")
        bounded(result)
        return result
    except (ValueError, RecursionError) as error:
        raise InstallationFailure("invalid installation JSON") from error


def command(arguments, *, timeout=300, maximum=MAXIMUM):
    """Bound output while receiving it; always kill and reap timed-out children."""
    process = subprocess.Popen(arguments, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                               stderr=subprocess.DEVNULL, start_new_session=True)
    data = bytearray()
    deadline = time.monotonic() + timeout
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise InstallationFailure("operator command timed out; outcome may be unknown")
                for key, _ in selector.select(min(remaining, 1)):
                    chunk = os.read(key.fd, 65_536)
                    if not chunk:
                        selector.unregister(key.fileobj)
                    elif len(data) + len(chunk) > maximum:
                        raise InstallationFailure("operator command output exceeds its bound")
                    else:
                        data.extend(chunk)
        if process.wait(timeout=max(0.01, deadline-time.monotonic())):
            raise InstallationFailure("operator command failed; installation evidence was retained")
        return bytes(data)
    finally:
        if process.poll() is None:
            import signal
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
        process.stdout.close()


def private_directory(path):
    if not path.is_absolute() or ".." in path.parts:
        raise InstallationFailure("an absolute private directory is required")
    for ancestor in [*reversed(path.parents), path]:
        try:
            metadata = ancestor.lstat()
        except FileNotFoundError:
            if ancestor != path:
                raise InstallationFailure("private directory parent is missing") from None
            path.mkdir(mode=0o700)
            metadata = path.lstat()
        if not stat.S_ISDIR(metadata.st_mode):
            raise InstallationFailure("private directory has an unsafe ancestor")
    if stat.S_IMODE(metadata.st_mode) != 0o700 or metadata.st_uid != os.getuid():
        raise InstallationFailure("private directory ownership or permissions differ")


def read_file(path, *, private=False, maximum=MAXIMUM):
    if not path.is_absolute() or ".." in path.parts:
        raise InstallationFailure("an absolute file path is required")
    for ancestor in path.parents:
        if not stat.S_ISDIR(ancestor.lstat().st_mode):
            raise InstallationFailure("file has an unsafe ancestor")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1 or metadata.st_size > maximum:
            raise InstallationFailure("file is not a bounded regular file")
        if private and (stat.S_IMODE(metadata.st_mode) != 0o600 or metadata.st_uid != os.getuid()):
            raise InstallationFailure("file is not private")
        with os.fdopen(os.dup(descriptor), "rb") as file:
            result = file.read(maximum+1)
        if len(result) > maximum:
            raise InstallationFailure("file grew beyond its bound")
        return result
    finally:
        os.close(descriptor)


def persist(path, value, *, immutable=False):
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()+b"\n"
    persist_bytes(path, encoded, immutable=immutable)


def persist_bytes(path, encoded, *, immutable=False):
    if len(encoded) > MAXIMUM:
        raise InstallationFailure("installation state exceeds its bound")
    try:
        path.lstat()
        exists = True
    except FileNotFoundError:
        exists = False
    if exists:
        previous = read_file(path, private=True)
        if immutable:
            if previous != encoded:
                raise InstallationFailure("immutable installation input or topology changed")
            return
    with tempfile.NamedTemporaryFile(prefix=".installation-", dir=path.parent, delete=False) as file:
        temporary = Path(file.name)
        os.fchmod(file.fileno(), 0o600)
        file.write(encoded)
        file.flush()
        os.fsync(file.fileno())
    try:
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def validate_plan(plan):
    fields = {"schema_version", "namespace", "input", "input_digest", "runtime_image", "console_image", "dependencies", "dependency_commands", "dependency_stop_grace_seconds", "processes"}
    if set(plan) != fields or type(plan["schema_version"]) is not int or plan["schema_version"] != 1:
        raise InstallationFailure("unexpected shared installation plan")
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,38}[a-z0-9])?", plan["namespace"]):
        raise InstallationFailure("invalid installation namespace")
    if plan["input"]["name"] != plan["namespace"] or plan["input"]["network"]["topology"] != "kubernetes_local":
        raise InstallationFailure("shared plan topology differs")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", plan["input_digest"]):
        raise InstallationFailure("invalid installation input digest")
    if set(plan["dependencies"]) != {"postgres", "nats", "s3", "openbao"}:
        raise InstallationFailure("invalid dependency closure")
    for image in [plan["runtime_image"], plan["console_image"], *plan["dependencies"].values()]:
        if not re.fullmatch(r"[A-Za-z0-9._/:-]+@sha256:[0-9a-f]{64}", image):
            raise InstallationFailure("repository image digests are required")
    if set(plan["dependency_commands"]) != {"s3"} or not isinstance(plan["dependency_commands"]["s3"], list) or not 1 <= len(plan["dependency_commands"]["s3"]) <= 64 or any(not isinstance(arg, str) or not 1 <= len(arg) <= 512 or "\x00" in arg for arg in plan["dependency_commands"]["s3"]):
        raise InstallationFailure("invalid shared dependency command")
    if (not isinstance(plan["dependency_stop_grace_seconds"], dict) or set(plan["dependency_stop_grace_seconds"]) != {"s3"}
            or type(plan["dependency_stop_grace_seconds"]["s3"]) is not int or plan["dependency_stop_grace_seconds"]["s3"] != 45):
        raise InstallationFailure("invalid shared dependency shutdown grace")
    names = set()
    if not 1 <= len(plan["processes"]) <= 24:
        raise InstallationFailure("invalid process closure")
    for process in plan["processes"]:
        if set(process) != {"name", "binary", "uid", "port", "observability_port", "paths"} or process["uid"] != 10001:
            raise InstallationFailure("invalid physical process declaration")
        if not re.fullmatch(r"[a-z][a-z0-9-]{0,40}", process["name"]) or process["name"] in names or not re.fullmatch(r"platform-[a-z0-9-]+", process["binary"]):
            raise InstallationFailure("invalid process name or executable")
        names.add(process["name"])
        for port in [process["observability_port"], *([] if process["port"] is None else [process["port"]])]:
            if type(port) is not int or not 1 <= port <= 65_535:
                raise InstallationFailure("invalid process port")
        if process["paths"] != {"process": process["name"], "configuration_directory": "/run/insight/role/config", "credential_directory": "/run/insight/role/credentials", "temporary_directory": "/var/lib/insight"}:
            raise InstallationFailure("process paths differ from the shared container policy")
    return plan


def completion(job, pods, *, plan, owner, phase, job_name):
    if job["metadata"]["name"] != job_name or job["metadata"].get("labels", {}).get(OWNER) != owner or job["metadata"].get("annotations", {}).get(DIGEST) != plan["input_digest"]:
        raise InstallationFailure("Job identity differs from the installation")
    if any(c.get("type") == "Failed" and c.get("status") == "True" for c in job.get("status", {}).get("conditions", [])) or job["spec"].get("backoffLimit") != 0 or job.get("status", {}).get("succeeded") != 1 or not any(c.get("type") == "Complete" and c.get("status") == "True" for c in job.get("status", {}).get("conditions", [])):
        raise InstallationFailure("installation Job is not complete")
    owned = [pod for pod in pods if any(ref.get("kind") == "Job" and ref.get("uid") == job["metadata"]["uid"] and ref.get("controller") is True for ref in pod["metadata"].get("ownerReferences", []))]
    if len(owned) != 1:
        raise InstallationFailure("installation Job has an ambiguous Pod result")
    pod = owned[0]
    containers = pod["spec"].get("containers", [])
    statuses = pod.get("status", {}).get("containerStatuses", [])
    if pod.get("status", {}).get("phase") != "Succeeded" or len(containers) != 1 or len(statuses) != 1:
        raise InstallationFailure("installation Pod is not a completed single process")
    container, status = containers[0], statuses[0]
    provider = phase in {"provider-start", "provider-observe"}
    extra = "" if provider else " --output /output" + ("" if phase == "prepare" else " --binaries /usr/local/bin")
    expected = f"exec /usr/local/bin/platform-installation {phase} --input /installation-input/input.json --state /installation/private{extra} > /tmp/installation-result.json" + (" 2>&1" if provider else "")
    if container.get("name") != "installation" or container.get("image") != plan["runtime_image"] or container.get("command") != ["/bin/sh", "-ec"] or container.get("args") != [expected] or container.get("terminationMessagePath") != "/tmp/installation-result.json":
        raise InstallationFailure("installation container command or image differs")
    terminated = status.get("state", {}).get("terminated", {})
    if status.get("name") != "installation" or terminated.get("exitCode") != 0:
        raise InstallationFailure("installation process failed")
    result = decode(terminated.get("message", "").encode())
    field = "mode" if phase == "provider-start" else "phase"
    expected_phase = {"prepare": "prepared", "provider-observe": "provider_ready"}.get(phase, "ready")
    if (set(result) != {"schema_version", field, "input_digest", "identity_digest"}
            or type(result["schema_version"]) is not int or result["schema_version"] != 1
            or (field == "mode" and result[field] not in {"initialize_once", "serve"})
            or (field == "phase" and result[field] != expected_phase)
            or result["input_digest"] != plan["input_digest"]
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", result["identity_digest"])):
        raise InstallationFailure("installation completion envelope is invalid")
    proof = {"job": job_name, "job_uid": job["metadata"]["uid"], "pod": pod["metadata"]["name"], "pod_uid": pod["metadata"]["uid"], "identity_digest": result["identity_digest"]}
    if field == "mode":
        proof["mode"] = result["mode"]
    return proof


def provider_state(value, prepared):
    """Validate only bounded deployment evidence, never infer provider readiness from a phase."""
    def identity(raw):
        return isinstance(raw, str) and bool(re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", raw))
    if not isinstance(value, dict) or set(value) != {"start_intent", "started", "observe_intent", "serve_observe_intent", "ready", "initialization"}:
        raise InstallationFailure("provider lifecycle intent differs")
    for field, phase in (("start_intent", "provider-start"), ("observe_intent", "provider-observe"), ("serve_observe_intent", "provider-observe")):
        intent = value[field]
        if intent is not None and (not isinstance(intent, dict) or set(intent) != {"job", "uid"}
                or not isinstance(intent["job"], str)
                or not re.fullmatch("installation-"+phase+"-[a-f0-9]{16}", intent["job"])
                or (intent["uid"] is not None and not identity(intent["uid"]))):
            raise InstallationFailure("provider owner Job intent differs")
    for field, phase, intent_key in (("started", "provider-start", "start_intent"), ("ready", "provider-observe", "observe_intent")):
        proof = value[field]
        if not isinstance(proof, dict):
            raise InstallationFailure("provider completion proof differs")
        if not proof:
            continue
        fields = {"job", "job_uid", "pod", "pod_uid", "identity_digest"} | ({"mode"} if field == "started" else set())
        intent = value[intent_key]
        if (set(proof) != fields or intent is None or proof["job"] != intent["job"] or proof["job_uid"] != intent["uid"]
                or any(not identity(proof[key]) for key in ("job", "job_uid", "pod", "pod_uid"))
                or not isinstance(proof["identity_digest"], str)
                or not re.fullmatch(r"sha256:[0-9a-f]{64}", proof["identity_digest"])
                or not prepared or proof["identity_digest"] != prepared.get("identity_digest")
                or (field == "started" and proof["mode"] not in {"initialize_once", "serve"})):
            raise InstallationFailure("provider completion identity differs")
    intent = value["initialization"]
    if intent is not None:
        if (not isinstance(intent, dict) or set(intent) != {"nonce", "uid", "deleting", "terminated", "complete"}
                or not isinstance(intent["nonce"], str) or not re.fullmatch(r"[a-f0-9]{32}", intent["nonce"])
                or (intent["uid"] is not None and not identity(intent["uid"]))
                or any(type(intent[key]) is not bool for key in ("deleting", "terminated", "complete"))
                or (intent["complete"] and not intent["terminated"])
                or (intent["terminated"] and not intent["deleting"])
                or (intent["deleting"] and intent["uid"] is None)
                or value["started"].get("mode") != "initialize_once"):
            raise InstallationFailure("provider initializer intent differs")
    if value["ready"] and (not value["started"] or intent is None or intent["uid"] is None):
        raise InstallationFailure("provider readiness has no initialization intent")


def remaining(deadline, maximum):
    value = deadline-time.monotonic()
    if value <= 0:
        raise InstallationFailure("provider operation timed out")
    return min(value, maximum)


class Installation:
    def __init__(self, arguments, plan):
        self.arguments = arguments
        self.plan = plan
        self.directory = arguments.directory
        self.kube = ["kubectl", "--kubeconfig", str(arguments.kubeconfig), "--context", arguments.context]
        self.helm = ["helm", "--kubeconfig", str(arguments.kubeconfig), "--kube-context", arguments.context]
        self.namespace = plan["namespace"]
        self.file = self.directory / "helm-state.json"
        if self.file.exists():
            self.state = decode(read_file(self.file, private=True))
            fields = {"schema_version", "owner", "namespace_uid", "phase", "operation", "prepared", "ready", "pvcs", "provider"}
            if set(self.state) != fields or type(self.state["schema_version"]) is not int or self.state["schema_version"] != 1 or not re.fullmatch(r"[a-f0-9]{32}", self.state["owner"]) or self.state["phase"] not in {"prepare", "provider-start", "dependencies", "provider-observe", "provision", "serving", "verify"}:
                raise InstallationFailure("installation host state differs")
        else:
            self.state = {"schema_version": 1, "owner": uuid.uuid4().hex, "namespace_uid": None, "phase": "prepare", "operation": uuid.uuid4().hex[:16], "prepared": {}, "ready": {}, "pvcs": {}, "provider": {"start_intent": None, "started": {}, "observe_intent": None, "serve_observe_intent": None, "ready": {}, "initialization": None}}
            self.save()
        provider_state(self.state["provider"], self.state["prepared"])

    def save(self):
        persist(self.file, self.state)

    def get(self, kind, name, *, timeout=300):
        output = command([*self.kube, "get", kind, name, "--namespace", self.namespace, "--ignore-not-found", "--output", "json"], timeout=timeout)
        return decode(output) if output.strip() else None

    def namespace_owner(self):
        namespace = self.get("namespace", self.namespace)
        if namespace is None:
            if self.state["namespace_uid"]:
                raise InstallationFailure("owned installation namespace is missing")
            file = self.directory / "namespace.json"
            persist(file, {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": self.namespace, "labels": {OWNER: self.state["owner"]}, "annotations": {DIGEST: self.plan["input_digest"]}}}, immutable=True)
            command([*self.kube, "create", "--filename", str(file)])
            namespace = self.get("namespace", self.namespace)
        metadata = namespace["metadata"]
        if metadata.get("labels", {}).get(OWNER) != self.state["owner"] or metadata.get("annotations", {}).get(DIGEST) != self.plan["input_digest"] or self.state["namespace_uid"] not in {None, metadata["uid"]}:
            raise InstallationFailure("foreign namespace or installation identity; refusing adoption")
        self.state["namespace_uid"] = metadata["uid"]
        self.save()
        self.claims(require_all=bool(self.state["prepared"]))

    def claims(self, *, require_all):
        expected = {"installation-private", "postgres-data", "nats-data", "s3-data", "openbao-data", "dependency-postgres", "dependency-nats", "dependency-s3", "dependency-openbao", "role-console"} | {"role-"+process["name"] for process in self.plan["processes"]}
        items = decode(command([*self.kube, "get", "pvc", "--namespace", self.namespace, "--output", "json"]))["items"]
        actual = {}
        for claim in items:
            metadata = claim["metadata"]
            if metadata["name"] not in expected or metadata.get("labels", {}).get(OWNER) != self.state["owner"] or metadata.get("annotations", {}).get(DIGEST) != self.plan["input_digest"] or metadata.get("annotations", {}).get("insight.platform/namespace-uid") != self.state["namespace_uid"]:
                raise InstallationFailure("foreign PVC in the exclusive installation namespace")
            actual[metadata["name"]] = metadata["uid"]
        if require_all and set(actual) != expected:
            raise InstallationFailure("installation PVC closure is incomplete")
        if any(actual.get(name) != uid for name, uid in self.state["pvcs"].items()):
            raise InstallationFailure("installation PVC identity changed")
        if actual:
            self.state["pvcs"] = actual
            self.save()

    def apply(self, phase, *, new_operation=False, timeout=330, serve_observe_operation=None):
        pending = (phase == "dependencies" and self.state["phase"] in {"provision", "verify"}
                   and self.state["ready"].get("job") != "installation-"+self.state["phase"]+"-"+self.state["operation"])
        if serve_observe_operation is not None:
            intent = self.state["provider"]["serve_observe_intent"]
            if (phase != "provider-observe" or new_operation or not isinstance(serve_observe_operation, str)
                    or not re.fullmatch(r"[a-f0-9]{16}", serve_observe_operation) or intent is None
                    or intent["job"] != "installation-provider-observe-"+serve_observe_operation):
                raise InstallationFailure("serving observation has no matching durable intent")
        elif not pending:
            if self.state["phase"] != phase or new_operation:
                self.state["operation"] = uuid.uuid4().hex[:16]
            self.state["phase"] = phase
        self.save()  # Intent precedes Helm's API effects.
        values = {"plan": self.plan, "phase": phase, "owner": self.state["owner"], "namespaceUID": self.state["namespace_uid"], "node": self.arguments.node, "storageClass": self.arguments.storage_class, "operation": serve_observe_operation or self.state["operation"], "prepared": self.state["prepared"], "ready": self.state["ready"], "providerStarted": self.state["provider"]["started"], "providerReady": self.state["provider"]["ready"]}
        file = self.directory / "helm-values.json"
        persist(file, values)
        command([*self.helm, "upgrade", "--install", "installation", str(CHART), "--namespace", self.namespace, "--values", str(file), "--timeout", str(max(1, int(timeout-5)))+"s"], timeout=timeout)

    def wait_job(self, phase, name, *, timeout=900, allow_failed=False):
        deadline = time.monotonic()+timeout
        job_uid = None
        while True:
            remaining = deadline-time.monotonic()
            if remaining <= 0:
                raise InstallationFailure("installation Job completion timed out")
            job = decode(command([*self.kube, "get", "job", name, "--namespace", self.namespace,
                                  "--output", "json"], timeout=min(15, remaining)))
            if time.monotonic() >= deadline:
                raise InstallationFailure("installation Job completion timed out")
            metadata = job.get("metadata", {})
            uid = metadata.get("uid")
            if metadata.get("name") != name or metadata.get("namespace") != self.namespace or metadata.get("labels", {}).get(OWNER) != self.state["owner"] or metadata.get("annotations", {}).get(DIGEST) != self.plan["input_digest"] or metadata.get("annotations", {}).get("insight.platform/phase") != phase or not isinstance(uid, str) or not uid or job.get("spec", {}).get("backoffLimit") != 0:
                raise InstallationFailure("Job identity differs from the installation")
            if job_uid is not None and uid != job_uid:
                raise InstallationFailure("installation Job UID changed while waiting")
            job_uid = uid
            conditions = job.get("status", {}).get("conditions", [])
            if any(condition.get("type") == "Failed" and condition.get("status") == "True" for condition in conditions):
                if allow_failed:
                    return job
                raise InstallationFailure("installation Job failed")
            if any(condition.get("type") == "Complete" and condition.get("status") == "True" for condition in conditions):
                return job
            remaining = deadline-time.monotonic()
            if remaining <= 0:
                raise InstallationFailure("installation Job completion timed out")
            time.sleep(min(1, remaining))

    def run_job(self, phase):
        self.apply(phase)
        name = f"installation-{phase}-{self.state['operation']}"
        job = self.wait_job(phase, name)
        pods = decode(command([*self.kube, "get", "pods", "--namespace", self.namespace, "--selector", "job-name="+name, "--output", "json"]))["items"]
        proof = completion(job, pods, plan=self.plan, owner=self.state["owner"], phase=phase, job_name=name)
        prior = self.state["prepared"]
        if prior and proof["identity_digest"] != prior["identity_digest"]:
            raise InstallationFailure("installation identity changed between phases")
        self.state["prepared" if phase == "prepare" else "ready"] = proof
        self.save()
        self.claims(require_all=True)

    def provider_pod(self, nonce):
        """One non-restarting provider process; its finalizer retains actual stop evidence."""
        return {"apiVersion": "v1", "kind": "Pod", "metadata": {
            "name": "installation-openbao-"+nonce, "namespace": self.namespace,
            "labels": {OWNER: self.state["owner"], "insight.platform/process": "openbao"},
            "annotations": {DIGEST: self.plan["input_digest"], "insight.platform/provider-nonce": nonce},
            "finalizers": ["insight.platform/provider-initialization-evidence"]},
            "spec": {"automountServiceAccountToken": False, "restartPolicy": "Never",
                     "nodeSelector": {"kubernetes.io/hostname": self.arguments.node},
                     "activeDeadlineSeconds": 600, "terminationGracePeriodSeconds": 30,
                     "containers": [{"name": "openbao", "image": self.plan["dependencies"]["openbao"],
                         "command": ["/usr/bin/bao"],
                         "args": ["server", "-config=/run/insight-openbao/initialize.json"],
                         "securityContext": {"runAsUser": 10001, "runAsGroup": 10001,
                             "allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                             "capabilities": {"drop": ["ALL"]}},
                         "resources": {"requests": {"cpu": "100m", "memory": "128Mi"},
                                       "limits": {"cpu": "2", "memory": "1Gi"}},
                         "volumeMounts": [
                             {"name": "configuration", "mountPath": "/run/insight-openbao", "readOnly": True},
                             {"name": "data", "mountPath": "/var/lib/openbao"},
                             {"name": "temporary", "mountPath": "/tmp"}]}],
                     "volumes": [
                         {"name": "configuration", "persistentVolumeClaim": {"claimName": "dependency-openbao", "readOnly": True}},
                         {"name": "data", "persistentVolumeClaim": {"claimName": "openbao-data"}},
                         {"name": "temporary", "emptyDir": {"medium": "Memory", "sizeLimit": "64Mi"}}]}}

    def check_provider_pod(self, pod, intent):
        expected = self.provider_pod(intent["nonce"])
        metadata = pod.get("metadata", {})
        if (metadata.get("name") != expected["metadata"]["name"]
                or metadata.get("namespace") != self.namespace
                or metadata.get("labels", {}).get(OWNER) != self.state["owner"]
                or metadata.get("annotations", {}).get(DIGEST) != self.plan["input_digest"]
                or metadata.get("annotations", {}).get("insight.platform/provider-nonce") != intent["nonce"]
                or not isinstance(metadata.get("uid"), str) or not metadata["uid"]
                or (intent["uid"] is not None and metadata["uid"] != intent["uid"])
                or metadata.get("ownerReferences")
                or (metadata.get("finalizers", []) != expected["metadata"]["finalizers"]
                    and not (intent["terminated"] and metadata.get("finalizers", []) == []))):
            raise InstallationFailure("provider initialization Pod identity differs")
        spec = pod.get("spec", {})
        if any(spec.get(key) for key in ("initContainers", "ephemeralContainers", "hostPID", "hostIPC", "hostNetwork")):
            raise InstallationFailure("provider initialization process isolation differs")
        for key in ("automountServiceAccountToken", "restartPolicy", "nodeSelector", "activeDeadlineSeconds", "terminationGracePeriodSeconds"):
            if spec.get(key) != expected["spec"][key]:
                raise InstallationFailure("provider initialization lifecycle differs")
        containers = spec.get("containers", [])
        if len(containers) != 1:
            raise InstallationFailure("provider initialization process closure differs")
        for key in ("name", "image", "command", "args", "securityContext", "volumeMounts", "resources"):
            if containers[0].get(key) != expected["spec"]["containers"][0][key]:
                raise InstallationFailure("provider initialization process differs")
        if spec.get("volumes") != expected["spec"]["volumes"]:
            raise InstallationFailure("provider initialization volumes differ")
        statuses = pod.get("status", {}).get("containerStatuses", [])
        if len(statuses) > 1 or any(status.get("name") != "openbao" or type(status.get("restartCount")) is not int or status["restartCount"] != 0 for status in statuses):
            raise InstallationFailure("provider initializer was restarted")
        return statuses[0] if statuses else {}

    def provider_job(self, phase, deadline, *, intent_key=None):
        provider = self.state["provider"]
        key = "start_intent" if phase == "provider-start" else "observe_intent"
        if intent_key is not None:
            if phase != "provider-observe" or intent_key != "serve_observe_intent":
                raise InstallationFailure("invalid serving observation phase")
            key = intent_key
        intent = provider[key]
        first = intent is None
        if first:
            operation = uuid.uuid4().hex[:16]
            if key != "serve_observe_intent":
                self.state.update(phase=phase, operation=operation)
            intent = {"job": "installation-"+phase+"-"+operation, "uid": None}
            provider[key] = intent
            self.save()  # No Job may be created before its exact immutable intent is durable.
        job = self.get("job", intent["job"], timeout=remaining(deadline, 10))
        if job is None:
            if not first:
                raise InstallationFailure("provider owner Job disappeared; outcome is unknown")
            options = {"serve_observe_operation": intent["job"].removeprefix("installation-provider-observe-")} if key == "serve_observe_intent" else {}
            self.apply(phase, timeout=remaining(deadline, 60), **options)
            job = self.get("job", intent["job"], timeout=remaining(deadline, 10))
        if (job is None or job.get("metadata", {}).get("name") != intent["job"]
                or job["metadata"].get("labels", {}).get(OWNER) != self.state["owner"]
                or job["metadata"].get("annotations", {}).get(DIGEST) != self.plan["input_digest"]
                or (intent["uid"] is not None and job["metadata"]["uid"] != intent["uid"])):
            raise InstallationFailure("provider owner Job identity differs")
        intent["uid"] = job["metadata"]["uid"]
        self.save()
        job = self.wait_job(phase, intent["job"], timeout=remaining(deadline, 180), allow_failed=True)
        if job["metadata"]["uid"] != intent["uid"] or time.monotonic() >= deadline:
            raise InstallationFailure("provider owner completion identity or deadline differs")
        pods = decode(command([*self.kube, "get", "pods", "--namespace", self.namespace,
                               "--selector", "job-name="+intent["job"], "--output", "json"],
                              timeout=remaining(deadline, 10)))["items"]
        remaining(deadline, 10)
        failed = any(item.get("type") == "Failed" and item.get("status") == "True" for item in job.get("status", {}).get("conditions", []))
        if failed:
            # Only the two closed read-only observation failures permit another observation Job.
            if phase == "provider-observe" and len(pods) == 1:
                pod = pods[0]
                owners = pod.get("metadata", {}).get("ownerReferences", [])
                statuses = pod.get("status", {}).get("containerStatuses", [])
                containers = pod.get("spec", {}).get("containers", [])
                expected = "exec /usr/local/bin/platform-installation provider-observe --input /installation-input/input.json --state /installation/private > /tmp/installation-result.json 2>&1"
                if (pod.get("metadata", {}).get("namespace") == self.namespace
                        and pod.get("status", {}).get("phase") == "Failed"
                        and any(owner.get("uid") == intent["uid"] and owner.get("kind") == "Job" and owner.get("controller") is True for owner in owners)
                        and len(containers) == 1 and containers[0].get("name") == "installation"
                        and containers[0].get("image") == self.plan["runtime_image"]
                        and containers[0].get("command") == ["/bin/sh", "-ec"] and containers[0].get("args") == [expected]
                        and containers[0].get("terminationMessagePath") == "/tmp/installation-result.json"
                        and len(statuses) == 1 and statuses[0].get("name") == "installation"
                        and statuses[0].get("restartCount", 0) == 0
                        and type(statuses[0].get("state", {}).get("terminated", {}).get("exitCode")) is int
                        and statuses[0]["state"]["terminated"]["exitCode"] != 0
                        and statuses[0].get("state", {}).get("terminated", {}).get("message", "").strip()
                        in {"installation Incomplete", "installation PrerequisiteUnavailable"}):
                    return None
            raise InstallationFailure("provider owner Job failed; initialization permission is not renewed")
        proof = completion(job, pods, plan=self.plan, owner=self.state["owner"], phase=phase, job_name=intent["job"])
        if proof["identity_digest"] != self.state["prepared"]["identity_digest"]:
            raise InstallationFailure("provider owner returned another installation identity")
        return proof

    def provider_stop(self):
        intent = self.state["provider"]["initialization"]
        if intent is None or intent["complete"]:
            return
        name = "installation-openbao-"+intent["nonce"]
        deadline = time.monotonic()+100
        pod = self.get("pod", name, timeout=remaining(deadline, 10))
        if pod is None:
            if intent["terminated"]:
                intent["complete"] = True
                self.save()
                return
            raise InstallationFailure("provider initializer disappeared without termination evidence")
        status = self.check_provider_pod(pod, intent)
        if not intent["deleting"]:
            intent["deleting"] = True
            self.save()
        if not pod["metadata"].get("deletionTimestamp"):
            options = self.directory/"provider-delete.json"
            persist(options, {"apiVersion": "v1", "kind": "DeleteOptions", "gracePeriodSeconds": 30,
                              "preconditions": {"uid": intent["uid"]}})
            command([*self.kube, "delete", "--raw", f"/api/v1/namespaces/{self.namespace}/pods/{name}", "--filename", str(options)], timeout=remaining(deadline, 40))
        while not status.get("state", {}).get("terminated"):
            if time.monotonic() >= deadline:
                raise InstallationFailure("provider initializer did not terminate")
            time.sleep(0.5)
            pod = self.get("pod", name, timeout=remaining(deadline, 10))
            if pod is None:
                raise InstallationFailure("provider initializer disappeared without termination evidence")
            status = self.check_provider_pod(pod, intent)
        if type(status["state"]["terminated"].get("exitCode")) is not int or status["state"]["terminated"]["exitCode"] != 0:
            raise InstallationFailure("provider initializer did not stop cleanly")
        remaining(deadline, 10)
        intent["terminated"] = True
        self.save()
        if pod["metadata"].get("finalizers"):
            patch = self.directory/"provider-finalizer.json"
            persist(patch, [{"op": "test", "path": "/metadata/uid", "value": intent["uid"]},
                            {"op": "test", "path": "/metadata/finalizers", "value": ["insight.platform/provider-initialization-evidence"]},
                            {"op": "replace", "path": "/metadata/finalizers", "value": []}])
            command([*self.kube, "patch", "pod", name, "--namespace", self.namespace,
                     "--type=json", "--patch-file", str(patch)], timeout=remaining(deadline, 15))
        while True:
            pod = self.get("pod", name, timeout=remaining(deadline, 10))
            remaining(deadline, 10)
            if pod is None:
                break
            if pod["metadata"]["uid"] != intent["uid"] or time.monotonic() >= deadline:
                raise InstallationFailure("provider initializer deletion is incomplete")
            time.sleep(0.5)
        intent["complete"] = True
        self.save()

    def ensure_provider(self):
        provider = self.state["provider"]
        deadline = time.monotonic()+180
        if not provider["started"]:
            provider["started"] = self.provider_job("provider-start", deadline)
            self.save()
        if not provider["ready"]:
            if provider["started"]["mode"] != "initialize_once":
                raise InstallationFailure("serving permission has no recorded provider identity")
            self.apply("dependencies", timeout=remaining(deadline, 60))
            first = provider["initialization"] is None
            if first:
                provider["initialization"] = {"nonce": uuid.uuid4().hex, "uid": None,
                    "deleting": False, "terminated": False, "complete": False}
                self.save()
            intent = provider["initialization"]
            name = "installation-openbao-"+intent["nonce"]
            pod = self.get("pod", name, timeout=remaining(deadline, 10))
            if pod is None:
                if not first:
                    raise InstallationFailure("provider initialization outcome is unknown; no replacement is permitted")
                manifest = self.directory/"provider-pod.json"
                persist(manifest, self.provider_pod(intent["nonce"]), immutable=True)
                pod = decode(command([*self.kube, "create", "--filename", str(manifest), "--output", "json"], timeout=remaining(deadline, 30)))
            remaining(deadline, 10)
            self.check_provider_pod(pod, intent)
            intent["uid"] = pod["metadata"]["uid"]
            self.save()
            while True:
                if time.monotonic() >= deadline:
                    raise InstallationFailure("provider initialization observation timed out")
                pod = self.get("pod", name, timeout=remaining(deadline, 10))
                if pod is None:
                    raise InstallationFailure("provider initializer disappeared")
                status = self.check_provider_pod(pod, intent)
                if status.get("state", {}).get("terminated") or intent["deleting"]:
                    raise InstallationFailure("provider initializer stopped before observation completed")
                if not status.get("state", {}).get("running"):
                    time.sleep(0.5)
                    continue
                proof = self.provider_job("provider-observe", deadline)
                current = self.get("pod", name, timeout=remaining(deadline, 10))
                if current is None or not self.check_provider_pod(current, intent).get("state", {}).get("running"):
                    raise InstallationFailure("provider initializer changed during observation")
                if time.monotonic() >= deadline:
                    raise InstallationFailure("provider initialization observation timed out")
                if proof is not None:
                    provider["ready"] = proof
                    self.save()
                    break
                provider["observe_intent"] = None
                self.save()  # Another read-only observation, never another initializer or start Job.
        self.provider_stop()

    def serving_provider_identity(self, deadline):
        """Bind one normal provider process to the frozen input and its Deployment chain."""
        def metadata(value, name):
            result = (value or {}).get("metadata", {})
            if (result.get("name") != name or result.get("namespace") != self.namespace
                    or result.get("labels", {}).get(OWNER) != self.state["owner"]
                    or result.get("deletionTimestamp") is not None
                    or not isinstance(result.get("uid"), str)
                    or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", result["uid"])):
                raise InstallationFailure("normal provider resource identity differs")
            return result

        def read(kind, name):
            value = self.get(kind, name, timeout=remaining(deadline, 10))
            remaining(deadline, 10)
            return value

        # Ordinary Pods intentionally carry no installation input or private-root
        # mount. Their exact immutable ConfigMap and PVC identities bind the input.
        configuration = read("configmap", "installation-input")
        metadata(configuration, "installation-input")
        if (configuration.get("immutable") is not True
                or decode(configuration.get("data", {}).get("input.json", "").encode()) != self.plan["input"]):
            raise InstallationFailure("normal provider input differs")
        for name in ("dependency-openbao", "openbao-data"):
            claim = read("pvc", name)
            meta = metadata(claim, name)
            if (meta["uid"] != self.state["pvcs"].get(name)
                    or meta.get("annotations", {}).get(DIGEST) != self.plan["input_digest"]
                    or meta.get("annotations", {}).get("insight.platform/namespace-uid") != self.state["namespace_uid"]):
                raise InstallationFailure("normal provider volume identity differs")
        labels = {OWNER: self.state["owner"], "insight.platform/process": "openbao"}
        service = read("service", "openbao")
        metadata(service, "openbao")
        ports = service.get("spec", {}).get("ports", [])
        if (service.get("spec", {}).get("selector") != labels
                or service["spec"].get("type", "ClusterIP") != "ClusterIP" or len(ports) != 1
                or ports[0].get("port") != 8200 or ports[0].get("targetPort") != 8200):
            raise InstallationFailure("normal provider Service selector differs")
        deployment = read("deployment", "openbao")
        deployment_meta = metadata(deployment, "openbao")
        desired = deployment.get("spec", {})
        if type(desired.get("replicas")) is not int or desired["replicas"] != 1 or desired.get("selector", {}).get("matchLabels") != labels:
            raise InstallationFailure("normal provider Deployment differs")
        pods = decode(command([*self.kube, "get", "pods", "--namespace", self.namespace, "--selector",
                               OWNER+"="+self.state["owner"]+",insight.platform/process=openbao", "--output", "json"],
                              timeout=remaining(deadline, 10)))["items"]
        remaining(deadline, 10)
        if len(pods) != 1:
            raise InstallationFailure("normal provider Pod is ambiguous")
        pod = pods[0]
        name = pod.get("metadata", {}).get("name", "")
        if not isinstance(name, str) or not re.fullmatch(r"openbao-[a-z0-9-]{1,100}", name):
            raise InstallationFailure("normal provider Pod name differs")
        pod_meta = metadata(pod, name)
        if pod_meta.get("labels", {}).get("insight.platform/process") != "openbao":
            raise InstallationFailure("normal provider Pod selector differs")
        refs = pod_meta.get("ownerReferences", [])
        if (len(refs) != 1 or refs[0].get("kind") != "ReplicaSet" or refs[0].get("controller") is not True
                or not isinstance(refs[0].get("name"), str) or not re.fullmatch(r"openbao-[a-z0-9-]{1,80}", refs[0]["name"])):
            raise InstallationFailure("normal provider Pod has no Deployment owner")
        replica = read("replicaset", refs[0]["name"])
        replica_meta = metadata(replica, refs[0]["name"])
        owners = replica_meta.get("ownerReferences", [])
        if (replica_meta["uid"] != refs[0].get("uid") or len(owners) != 1
                or owners[0].get("kind") != "Deployment" or owners[0].get("name") != "openbao"
                or owners[0].get("uid") != deployment_meta["uid"] or owners[0].get("controller") is not True):
            raise InstallationFailure("normal provider ReplicaSet owner differs")
        for spec in (desired.get("template", {}).get("spec", {}), replica.get("spec", {}).get("template", {}).get("spec", {}), pod.get("spec", {})):
            containers = spec.get("containers", [])
            if (spec.get("automountServiceAccountToken") is not False or len(containers) != 1
                    or any(spec.get(key) for key in ("initContainers", "ephemeralContainers", "hostPID", "hostIPC", "hostNetwork"))):
                raise InstallationFailure("normal provider process isolation differs")
            container = containers[0]
            if (container.get("name") != "openbao" or container.get("image") != self.plan["dependencies"]["openbao"]
                    or container.get("command") != ["/usr/bin/bao"]
                    or container.get("args") != ["server", "-config=/run/insight-openbao/serve.json"]):
                raise InstallationFailure("normal provider serving command differs")
            if container.get("securityContext") != {"runAsUser": 10001, "runAsGroup": 10001,
                    "allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}}:
                raise InstallationFailure("normal provider privileges differ")
            volumes = spec.get("volumes", [])
            expected = {"configuration": {"name": "configuration", "persistentVolumeClaim": {"claimName": "dependency-openbao", "readOnly": True}},
                        "data": {"name": "data", "persistentVolumeClaim": {"claimName": "openbao-data"}},
                        "temporary": {"name": "temporary", "emptyDir": {"medium": "Memory", "sizeLimit": "128Mi"}}}
            mounts = container.get("volumeMounts", [])
            expected_mounts = {"configuration": {"name": "configuration", "mountPath": "/run/insight-openbao", "readOnly": True},
                               "data": {"name": "data", "mountPath": "/var/lib/openbao"},
                               "temporary": {"name": "temporary", "mountPath": "/tmp"}}
            if (len(volumes) != 3 or {item.get("name"): item for item in volumes} != expected
                    or len(mounts) != 3 or {item.get("name"): item for item in mounts} != expected_mounts):
                raise InstallationFailure("normal provider volume scope differs")
        statuses = pod.get("status", {}).get("containerStatuses", [])
        if (pod.get("status", {}).get("phase") != "Running" or len(statuses) != 1
                or statuses[0].get("name") != "openbao" or statuses[0].get("ready") is not True
                or not statuses[0].get("state", {}).get("running")
                or not isinstance(statuses[0].get("containerID"), str)
                or not re.fullmatch(r"[a-z][a-z0-9-]{0,31}://[a-f0-9]{64}", statuses[0]["containerID"])
                or not any(item.get("type") == "Ready" and item.get("status") == "True" for item in pod["status"].get("conditions", []))):
            raise InstallationFailure("normal provider process is not Ready")
        return deployment_meta["uid"], replica_meta["uid"], pod_meta["uid"], statuses[0]["containerID"]

    def observe_serving_provider(self):
        """A new read-only proof for this invocation; never renew initialization permission."""
        provider = self.state["provider"]
        if not provider["ready"] or not provider["initialization"] or not provider["initialization"]["complete"]:
            raise InstallationFailure("normal provider has no completed initialization")
        deadline = time.monotonic()+180
        key = "serve_observe_intent"
        if provider[key] is not None:
            # Even an old Pending Job which completes during recovery cannot prove
            # this invocation's process. Resolve it, then require a new observation.
            self.provider_job("provider-observe", deadline, intent_key=key)
            remaining(deadline, 10)
            provider[key] = None
            self.save()
        original = None
        while True:
            remaining(deadline, 10)
            current = self.serving_provider_identity(deadline)
            if original is None:
                original = current
            elif current != original:
                raise InstallationFailure("normal provider process changed before observation")
            proof = self.provider_job("provider-observe", deadline, intent_key=key)
            if self.serving_provider_identity(deadline) != original:
                raise InstallationFailure("normal provider process changed during observation")
            remaining(deadline, 10)
            provider[key] = None
            self.save()
            if proof is not None:
                return
            time.sleep(min(1, remaining(deadline, 1)))

    def verify_existing_workloads(self):
        """Pure verify never asks Helm to recreate dependencies, role Deployments or input."""
        config = self.get("configmap", "installation-input")
        if config is None or config.get("immutable") is not True or config["metadata"].get("labels", {}).get(OWNER) != self.state["owner"] or decode(config.get("data", {}).get("input.json", "").encode()) != self.plan["input"]:
            raise InstallationFailure("installed input ConfigMap is missing or differs")
        images = {name: image for name, image in self.plan["dependencies"].items()}
        images.update({process["name"]: self.plan["runtime_image"] for process in self.plan["processes"]})
        images["console"] = self.plan["console_image"]
        for name, image in images.items():
            deployment = self.get("deployment", name)
            service = self.get("service", name)
            if deployment is None or service is None or any(obj["metadata"].get("labels", {}).get(OWNER) != self.state["owner"] for obj in (deployment, service)):
                raise InstallationFailure("an installed workload or Service is missing or foreign")
            pod = deployment["spec"]["template"]["spec"]
            containers = pod.get("containers", [])
            if pod.get("automountServiceAccountToken") is not False or len(containers) != 1 or containers[0].get("image") != image or pod.get("initContainers") or pod.get("ephemeralContainers"):
                raise InstallationFailure("installed workload image or container boundary differs")
            volumes = pod.get("volumes", [])
            claims = {volume["persistentVolumeClaim"]["claimName"] for volume in volumes if "persistentVolumeClaim" in volume}
            expected = {name+"-data", "dependency-"+name} if name in self.plan["dependencies"] else {"role-"+name}
            if claims != expected or any(set(volume) not in ({"name", "persistentVolumeClaim"}, {"name", "emptyDir"}) for volume in volumes):
                raise InstallationFailure("installed workload volume scope differs")
            if name not in self.plan["dependencies"]:
                security = containers[0].get("securityContext", {})
                uid = 1000 if name == "console" else 10001
                if security.get("runAsUser") != uid or security.get("runAsGroup") != uid or security.get("allowPrivilegeEscalation") is not False or security.get("readOnlyRootFilesystem") is not True or security.get("privileged", False) or security.get("capabilities", {}).get("drop") != ["ALL"] or security.get("capabilities", {}).get("add"):
                    raise InstallationFailure("installed serving process privileges differ")
                claims = [volume for volume in volumes if "persistentVolumeClaim" in volume]
                if len(claims) != 1 or claims[0]["persistentVolumeClaim"].get("readOnly") is not True:
                    raise InstallationFailure("installed serving credentials are not read-only")
                mounts = [mount for mount in containers[0].get("volumeMounts", []) if mount["name"] == claims[0]["name"]]
                if len(mounts) != 1 or mounts[0].get("readOnly") is not True:
                    raise InstallationFailure("installed serving credential mount differs")
                if name == "console":
                    if mounts[0].get("mountPath") != "/run/insight/console/config.json" or mounts[0].get("subPath") != "config.json":
                        raise InstallationFailure("installed Console mount differs")
                else:
                    process = next(process for process in self.plan["processes"] if process["name"] == name)
                    expected = "set -a; . /run/insight/role/environment; set +a; exec /usr/local/bin/"+process["binary"]
                    if mounts[0].get("mountPath") != "/run/insight/role" or containers[0].get("command") != ["/bin/sh", "-ec"] or containers[0].get("args") != [expected]:
                        raise InstallationFailure("installed serving executable differs")

    def run(self, operation):
        if operation == "verify" and not self.state["ready"]:
            raise InstallationFailure("verify requires a completed installation")
        self.namespace_owner()
        if operation == "verify":
            self.verify_existing_workloads()
        if operation == "resume" and self.state["phase"] in {"prepare", "provision", "verify"}:
            phase = self.state["phase"]
            job = self.get("job", f"installation-{phase}-{self.state['operation']}")
            if job is not None:
                metadata = job["metadata"]
                if metadata.get("labels", {}).get(OWNER) != self.state["owner"] or metadata.get("annotations", {}).get(DIGEST) != self.plan["input_digest"]:
                    raise InstallationFailure("failed Job identity differs; refusing recovery")
                conditions = job.get("status", {}).get("conditions", [])
                failed = any(c.get("type") == "Failed" and c.get("status") == "True" for c in conditions)
                complete = any(c.get("type") == "Complete" and c.get("status") == "True" for c in conditions)
                if failed and not complete and not job.get("status", {}).get("active", 0):
                    # Explicit recovery starts a new child against the same fsynced owner journal.
                    # No failed Job is deleted, and the owner decides whether its intent is resumable.
                    self.state["operation"] = uuid.uuid4().hex[:16]
                    self.save()
                elif not complete:
                    raise InstallationFailure("an installation Job may still be running")
        target_phase = "verify" if self.state["ready"] else "provision"
        pending_operation = (self.state["operation"] if self.state["phase"] == target_phase
                             and self.state["ready"].get("job") != "installation-"+target_phase+"-"+self.state["operation"] else None)
        if self.state["ready"]:
            if operation != "verify":
                self.provider_stop()
                self.apply("dependencies")
                for dependency in ("postgres", "nats", "s3", "openbao"):
                    command([*self.kube, "rollout", "status", "deployment/"+dependency, "--namespace", self.namespace, "--timeout=180s"], timeout=200)
                self.observe_serving_provider()
            if pending_operation:
                self.state.update(phase="verify", operation=pending_operation)
                self.save()
            current = "installation-verify-"+self.state["operation"]
            if self.state["phase"] != "verify" or self.state["ready"].get("job") == current:
                self.state["operation"] = uuid.uuid4().hex[:16]
                self.state["phase"] = "verify"
                self.save()
            self.run_job("verify")
        else:
            if operation == "verify":
                raise InstallationFailure("verify requires a completed installation")
            if not self.state["prepared"]:
                self.run_job("prepare")
            self.ensure_provider()
            self.apply("dependencies")
            for dependency in ("postgres", "nats", "s3", "openbao"):
                command([*self.kube, "rollout", "status", "deployment/"+dependency, "--namespace", self.namespace, "--timeout=180s"], timeout=200)
            self.observe_serving_provider()
            if pending_operation:
                self.state.update(phase="provision", operation=pending_operation)
                self.save()
            self.run_job("provision")
        if operation != "verify":
            self.apply("serving")
        for process in [*self.plan["processes"], {"name": "console"}]:
            command([*self.kube, "rollout", "status", "deployment/"+process["name"], "--namespace", self.namespace, "--timeout=180s"], timeout=200)
        print(json.dumps({"schema_version": 1, "phase": "ready", "namespace": self.namespace, "input_digest": self.plan["input_digest"], "identity_digest": self.state["ready"]["identity_digest"]}))

    def public_trust_pod(self, nonce):
        script = "umask 077; /usr/local/bin/platform-installation public-trust --input /installation-input/input.json --state /installation/private > /tmp/public-trust-pending.json; mv /tmp/public-trust-pending.json /tmp/public-trust.json; sleep 60"
        return {"apiVersion": "v1", "kind": "Pod", "metadata": {
            "name": "installation-public-trust-"+nonce, "namespace": self.namespace,
            "labels": {OWNER: self.state["owner"], "insight.platform/public-trust-nonce": nonce},
            "annotations": {DIGEST: self.plan["input_digest"], "insight.platform/identity-digest": self.state["ready"]["identity_digest"]}},
            "spec": {"automountServiceAccountToken": False, "enableServiceLinks": False,
                "restartPolicy": "Never", "activeDeadlineSeconds": 120, "terminationGracePeriodSeconds": 1,
                "nodeSelector": {"kubernetes.io/hostname": self.arguments.node},
                "containers": [{"name": "public-trust", "image": self.plan["runtime_image"],
                    "command": ["/bin/sh", "-ec"], "args": [script],
                    "securityContext": {"runAsUser": 0, "runAsGroup": 0, "allowPrivilegeEscalation": False,
                        "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}},
                    "readinessProbe": {"exec": {"command": ["test", "-f", "/tmp/public-trust.json"]}, "periodSeconds": 1},
                    "resources": {"requests": {"cpu": "25m", "memory": "32Mi"}, "limits": {"cpu": "500m", "memory": "128Mi"}},
                    "volumeMounts": [{"name": "private", "mountPath": "/installation", "readOnly": True},
                        {"name": "input", "mountPath": "/installation-input/input.json", "subPath": "input.json", "readOnly": True},
                        {"name": "temporary", "mountPath": "/tmp"}]}],
                "volumes": [{"name": "private", "persistentVolumeClaim": {"claimName": "installation-private", "readOnly": True}},
                    {"name": "input", "configMap": {"name": "installation-input", "defaultMode": 292}},
                    {"name": "temporary", "emptyDir": {"medium": "Memory", "sizeLimit": "16Mi"}}]}}

    def check_public_trust_pod(self, pod, intent):
        expected = self.public_trust_pod(intent["nonce"])
        metadata = (pod or {}).get("metadata", {})
        if (metadata.get("name") != expected["metadata"]["name"] or metadata.get("namespace") != self.namespace
                or not isinstance(metadata.get("uid"), str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", metadata["uid"])
                or intent["pod_uid"] not in (None, metadata["uid"]) or metadata.get("ownerReferences")
                or any(metadata.get(field, {}).get(key) != value for field in ("labels", "annotations") for key, value in expected["metadata"][field].items())):
            raise InstallationFailure("public trust Pod identity differs")
        actual, wanted = pod.get("spec", {}), expected["spec"]
        if any(actual.get(key) for key in ("initContainers", "ephemeralContainers", "hostPID", "hostIPC", "hostNetwork")):
            raise InstallationFailure("public trust Pod isolation differs")
        for key in ("automountServiceAccountToken", "enableServiceLinks", "restartPolicy", "activeDeadlineSeconds", "terminationGracePeriodSeconds", "nodeSelector", "volumes"):
            if actual.get(key) != wanted[key]:
                raise InstallationFailure("public trust Pod read-only scope differs")
        containers = actual.get("containers", [])
        if len(containers) != 1 or containers[0].get("env") or containers[0].get("envFrom"):
            raise InstallationFailure("public trust Pod process differs")
        for key in ("name", "image", "command", "args", "securityContext", "volumeMounts", "resources"):
            if containers[0].get(key) != wanted["containers"][0][key]:
                raise InstallationFailure("public trust Pod command or scope differs")
        statuses = pod.get("status", {}).get("containerStatuses", [])
        if len(statuses) > 1 or any(item.get("name") != "public-trust" or type(item.get("restartCount")) is not int or item["restartCount"] != 0 for item in statuses):
            raise InstallationFailure("public trust Pod restarted")

    def remove_public_trust_pod(self, intent, deadline):
        name = "installation-public-trust-"+intent["nonce"]
        pod = self.get("pod", name, timeout=remaining(deadline, 10))
        if pod is not None:
            self.check_public_trust_pod(pod, intent)
            options = self.directory/"public-trust-delete.json"
            persist(options, {"apiVersion": "v1", "kind": "DeleteOptions", "gracePeriodSeconds": 1,
                              "preconditions": {"uid": intent["pod_uid"]}})
            command([*self.kube, "delete", "--raw", f"/api/v1/namespaces/{self.namespace}/pods/{name}", "--filename", str(options)], timeout=remaining(deadline, 15))
        while True:
            pod = self.get("pod", name, timeout=remaining(deadline, 10))
            remaining(deadline, 10)
            if pod is None:
                break
            self.check_public_trust_pod(pod, intent)
            time.sleep(min(.5, remaining(deadline, .5)))

    def deliver_public_trust(self):
        if not self.state["ready"]:
            raise InstallationFailure("public trust export requires a completed installation")
        self.namespace_owner()
        deadline = time.monotonic()+120
        path = self.directory/"public-trust-intent.json"
        intent = decode(read_file(path, private=True, maximum=4096)) if path.exists() else None
        if intent is not None:
            if (not isinstance(intent, dict) or set(intent) != {"schema_version", "input_digest", "identity_digest", "nonce", "pod_uid", "complete"}
                    or type(intent["schema_version"]) is not int or intent["schema_version"] != 1
                    or intent["input_digest"] != self.plan["input_digest"] or intent["identity_digest"] != self.state["ready"]["identity_digest"]
                    or not isinstance(intent["nonce"], str) or not re.fullmatch(r"[a-f0-9]{32}", intent["nonce"])
                    or type(intent["complete"]) is not bool or (intent["complete"] and intent["pod_uid"] is None)
                    or (intent["pod_uid"] is not None and (not isinstance(intent["pod_uid"], str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", intent["pod_uid"])))):
                raise InstallationFailure("public trust export intent differs")
            if intent["complete"]:
                self.remove_public_trust_pod(intent, deadline)
                # Prior delivery is only cleanup evidence. A new call reads the owner again.
                intent = None
        first = intent is None
        if first:
            intent = {"schema_version": 1, "input_digest": self.plan["input_digest"], "identity_digest": self.state["ready"]["identity_digest"],
                      "nonce": uuid.uuid4().hex, "pod_uid": None, "complete": False}
            persist(path, intent)
        name = "installation-public-trust-"+intent["nonce"]
        pod = self.get("pod", name, timeout=remaining(deadline, 10))
        if pod is None:
            if not first:
                raise InstallationFailure("public trust Pod disappeared; export outcome is unknown")
            manifest = self.directory/"public-trust-pod.json"
            persist(manifest, self.public_trust_pod(intent["nonce"]))
            pod = decode(command([*self.kube, "create", "--filename", str(manifest), "--output", "json"], timeout=remaining(deadline, 20)))
        self.check_public_trust_pod(pod, intent)
        intent["pod_uid"] = pod["metadata"]["uid"]
        persist(path, intent)
        while True:
            remaining(deadline, 10)
            pod = self.get("pod", name, timeout=remaining(deadline, 10))
            self.check_public_trust_pod(pod, intent)
            if pod["metadata"].get("deletionTimestamp") or pod.get("status", {}).get("phase") in {"Failed", "Succeeded"}:
                raise InstallationFailure("public trust Pod ended before delivery")
            if any(item.get("type") == "Ready" and item.get("status") == "True" for item in pod.get("status", {}).get("conditions", [])):
                break
            time.sleep(min(.5, remaining(deadline, .5)))
        output = command([*self.kube, "exec", "--namespace", self.namespace, name, "--container", "public-trust", "--", "cat", "/tmp/public-trust.json"],
                         timeout=remaining(deadline, 10), maximum=TRUST.MAX_RESPONSE_BYTES)
        self.check_public_trust_pod(self.get("pod", name, timeout=remaining(deadline, 10)), intent)
        remaining(deadline, 10)
        result = TRUST.deliver(self.directory, output, input_digest=self.plan["input_digest"], identity_digest=self.state["ready"]["identity_digest"])
        intent["complete"] = True
        persist(path, intent)
        self.remove_public_trust_pod(intent, deadline)
        print(json.dumps(result, separators=(",", ":")))
        return result

    def session_pod(self, nonce):
        script = "umask 077; /usr/local/bin/platform-installation session --input /installation-input/input.json --state /installation/private > /tmp/session-pending.json; mv /tmp/session-pending.json /tmp/session-result.json; sleep 60"
        return {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "installation-session-"+nonce, "namespace": self.namespace,
                "labels": {OWNER: self.state["owner"], "insight.platform/session-nonce": nonce},
                "annotations": {DIGEST: self.plan["input_digest"], "insight.platform/identity-digest": self.state["ready"]["identity_digest"]}},
            "spec": {"automountServiceAccountToken": False, "enableServiceLinks": False, "restartPolicy": "Never", "activeDeadlineSeconds": 120,
                "terminationGracePeriodSeconds": 1, "nodeSelector": {"kubernetes.io/hostname": self.arguments.node},
                "containers": [{"name": "session", "image": self.plan["runtime_image"], "command": ["/bin/sh", "-ec"], "args": [script],
                    "securityContext": {"runAsUser": 0, "runAsGroup": 0, "allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}},
                    "readinessProbe": {"exec": {"command": ["test", "-f", "/tmp/session-result.json"]}, "periodSeconds": 1},
                    "resources": {"requests": {"cpu": "25m", "memory": "32Mi"}, "limits": {"cpu": "500m", "memory": "128Mi"}},
                    "volumeMounts": [{"name": "private", "mountPath": "/installation"}, {"name": "input", "mountPath": "/installation-input/input.json", "subPath": "input.json", "readOnly": True}, {"name": "temporary", "mountPath": "/tmp"}]}],
                "volumes": [{"name": "private", "persistentVolumeClaim": {"claimName": "installation-private"}}, {"name": "input", "configMap": {"name": "installation-input", "defaultMode": 292}}, {"name": "temporary", "emptyDir": {"medium": "Memory", "sizeLimit": "16Mi"}}]}}

    def check_session_pod(self, pod, journal):
        if pod is None:
            raise InstallationFailure("recorded session Pod disappeared; outcome is unknown")
        expected = self.session_pod(journal["nonce"])
        metadata = pod["metadata"]
        if metadata["name"] != expected["metadata"]["name"] or metadata.get("namespace") != self.namespace or journal["pod_uid"] not in {None, metadata["uid"]}:
            raise InstallationFailure("session Pod identity changed")
        for key, value in expected["metadata"]["labels"].items():
            if metadata.get("labels", {}).get(key) != value:
                raise InstallationFailure("session Pod nonce or owner differs")
        for key, value in expected["metadata"]["annotations"].items():
            if metadata.get("annotations", {}).get(key) != value:
                raise InstallationFailure("session Pod installation identity differs")
        for key in ("automountServiceAccountToken", "enableServiceLinks", "restartPolicy", "activeDeadlineSeconds", "nodeSelector"):
            if pod["spec"].get(key) != expected["spec"][key]:
                raise InstallationFailure("session Pod execution boundary differs")
        if pod["spec"].get("initContainers") or pod["spec"].get("ephemeralContainers"):
            raise InstallationFailure("session Pod has another container")
        if any(pod["spec"].get(field, False) for field in ("hostNetwork", "hostPID", "hostIPC")):
            raise InstallationFailure("session Pod shares host namespaces")
        if len(pod["spec"].get("containers", [])) != 1:
            raise InstallationFailure("session Pod has another container")
        actual = pod["spec"]["containers"][0]
        declared = expected["spec"]["containers"][0]
        for key in ("name", "image", "command", "args", "securityContext", "volumeMounts"):
            if actual.get(key) != declared[key]:
                raise InstallationFailure("session process image, command or mounts differ")
        if actual.get("env") or actual.get("envFrom"):
            raise InstallationFailure("session process has ambient environment overrides")
        volumes = pod["spec"].get("volumes", [])
        if len(volumes) != 3 or {v["name"] for v in volumes} != {v["name"] for v in expected["spec"]["volumes"]}:
            raise InstallationFailure("session volume closure differs")
        for volume in expected["spec"]["volumes"]:
            installed = next(v for v in volumes if v["name"] == volume["name"])
            # Kubernetes may default PVC readOnly:false; only that harmless default is normalized.
            installed = json.loads(json.dumps(installed))
            if installed.get("persistentVolumeClaim", {}).get("readOnly") is False:
                installed["persistentVolumeClaim"].pop("readOnly")
            if installed != volume:
                raise InstallationFailure("session private mount differs")

    def delete_session_pod(self, journal):
        name = "installation-session-"+journal["nonce"]
        pod = self.get("pod", name)
        if pod is None:
            return
        self.check_session_pod(pod, journal)
        if not journal["pod_uid"]:
            raise InstallationFailure("session deletion needs its recorded Pod UID")
        options = self.directory/"session-delete.json"
        persist(options, {"apiVersion": "v1", "kind": "DeleteOptions", "gracePeriodSeconds": 1, "preconditions": {"uid": journal["pod_uid"]}})
        # The API checks UID atomically; a replaced same-name Pod is never deleted.
        command([*self.kube, "delete", "--raw", f"/api/v1/namespaces/{self.namespace}/pods/{name}", "--filename", str(options)], timeout=30)

    def deliver_session(self, *, explicit):
        if not self.state["ready"]:
            raise InstallationFailure("session issuance requires a completed installation")
        self.namespace_owner()
        path = self.directory/"session-delivery.json"
        token_file = self.directory/"session-token"
        journal = decode(read_file(path, private=True)) if path.exists() else None
        if journal is not None:
            fields = {"schema_version", "input_digest", "identity_digest", "nonce", "pod_uid", "complete", "envelope", "token_digest"}
            if set(journal) != fields or type(journal["schema_version"]) is not int or journal["schema_version"] != 1 or type(journal["complete"]) is not bool or journal["input_digest"] != self.plan["input_digest"] or journal["identity_digest"] != self.state["ready"]["identity_digest"] or not re.fullmatch(r"[a-f0-9]{32}", journal["nonce"]):
                raise InstallationFailure("session delivery intent differs")
            if journal["complete"]:
                if not explicit:
                    token = read_file(token_file, private=True, maximum=16_384)
                    if hashlib.sha256(token).hexdigest() != journal["token_digest"]:
                        raise InstallationFailure("delivered session changed")
                    expiry = journal["envelope"]["expires_at_unix_seconds"]
                    if type(expiry) is not int or expiry <= 0:
                        raise InstallationFailure("delivered session expiry is invalid")
                    if expiry <= int(time.time()):
                        raise SessionExpired()
                    self.delete_session_pod(journal)
                    return self.session_result(journal["envelope"], token_file)
                self.delete_session_pod(journal)
                journal = None
            else:
                pod = self.get("pod", "installation-session-"+journal["nonce"])
                if pod is not None:
                    self.check_session_pod(pod, journal)
                    if explicit and pod.get("status", {}).get("phase") in {"Succeeded", "Failed"}:
                        journal["pod_uid"] = pod["metadata"]["uid"]
                        persist(path, journal)
                        self.delete_session_pod(journal)
                        journal = None
        if journal is None:
            journal = {"schema_version": 1, "input_digest": self.plan["input_digest"], "identity_digest": self.state["ready"]["identity_digest"], "nonce": uuid.uuid4().hex, "pod_uid": None, "complete": False, "envelope": None, "token_digest": None}
            persist(path, journal)  # A lost create response must reuse this nonce, never create another.
        name = "installation-session-"+journal["nonce"]
        pod = self.get("pod", name)
        if pod is None:
            if journal["pod_uid"]:
                raise InstallationFailure("recorded session Pod disappeared; outcome is unknown")
            manifest = self.directory/"session-pod.json"
            persist(manifest, self.session_pod(journal["nonce"]))
            pod = decode(command([*self.kube, "create", "--filename", str(manifest), "--output", "json"]))
        self.check_session_pod(pod, journal)
        journal["pod_uid"] = pod["metadata"]["uid"]
        persist(path, journal)
        command([*self.kube, "wait", "--namespace", self.namespace, "--for=condition=Ready", "pod/"+name, "--timeout=90s"], timeout=100)
        pod = self.get("pod", name)
        self.check_session_pod(pod, journal)
        envelope = decode(command([*self.kube, "exec", "--namespace", self.namespace, name, "--container", "session", "--", "cat", "/tmp/session-result.json"], timeout=10, maximum=4096))
        expected_fields = {"schema_version", "session_file", "tenant_id", "endpoint", "expires_at_unix_seconds", "input_digest", "identity_digest"}
        now = int(time.time())
        if set(envelope) != expected_fields or type(envelope["schema_version"]) is not int or envelope["schema_version"] != 1 or envelope["session_file"] != "/installation/private/session-token" or envelope["input_digest"] != self.plan["input_digest"] or envelope["identity_digest"] != self.state["ready"]["identity_digest"] or envelope["endpoint"] != self.plan["input"]["network"]["console_origin"] or type(envelope["expires_at_unix_seconds"]) is not int or not now < envelope["expires_at_unix_seconds"] <= now+900:
            raise InstallationFailure("session delivery envelope is invalid or expired")
        if not isinstance(envelope["tenant_id"], str) or not re.fullmatch(r"ten_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", envelope["tenant_id"]):
            raise InstallationFailure("session tenant identity is not canonical")
        token = command([*self.kube, "exec", "--namespace", self.namespace, name, "--container", "session", "--", "cat", "/installation/private/session-token"], timeout=10, maximum=16_384)
        # Exec has no UID precondition. Reject a same-name replacement observed around delivery.
        self.check_session_pod(self.get("pod", name), journal)
        if not re.fullmatch(rb"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\n", token):
            raise InstallationFailure("session token file has an invalid representation")
        payload = token.split(b".")[1]
        try:
            claims = decode(base64.urlsafe_b64decode(payload+b"="*((-len(payload))%4)))
        except ValueError as error:
            raise InstallationFailure("session token payload is invalid") from error
        if claims.get("tenant_id") != envelope["tenant_id"] or claims.get("exp") != envelope["expires_at_unix_seconds"] or claims.get("exp", 0)-claims.get("iat", 0) != 900:
            raise InstallationFailure("session token differs from its delivery envelope")
        # Authentication remains the Gateway's responsibility; this verifies same-file delivery.
        persist_bytes(token_file, token)
        journal.update(complete=True, envelope=envelope, token_digest=hashlib.sha256(token).hexdigest())
        persist(path, journal)
        self.delete_session_pod(journal)
        return self.session_result(envelope, token_file)

    @staticmethod
    def session_result(envelope, token_file):
        result = dict(envelope, session_file=str(token_file))
        print(json.dumps(result))
        return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("render", "up", "verify", "resume", "session", "public-trust"))
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--directory", type=Path, required=True)
    def image(value):
        if not re.fullmatch(r"[A-Za-z0-9._/:-]+@sha256:[a-f0-9]{64}", value):
            raise argparse.ArgumentTypeError("a repository image digest is required")
        return value
    parser.add_argument("--runtime-image", type=image, required=True)
    parser.add_argument("--console-image", type=image, required=True)
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--context", required=True)
    parser.add_argument("--node", required=True)
    parser.add_argument("--storage-class", default="")
    args = parser.parse_args()
    private_directory(args.directory)
    read_file(args.input, maximum=262_144)
    read_file(args.kubeconfig)
    lock = os.open(args.directory/"lock", os.O_WRONLY|os.O_CREAT|os.O_NOFOLLOW, 0o600)
    try:
        metadata = os.fstat(lock)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1 or metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) != 0o600:
            raise InstallationFailure("invalid installation lock")
        fcntl.flock(lock, fcntl.LOCK_EX|fcntl.LOCK_NB)
        output = command(["docker", "run", "--rm", "--network", "none", "--read-only", "--user", f"{os.geteuid()}:{os.getegid()}", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true", "--mount", f"type=bind,source={args.input},target=/installation-input/input.json,readonly", "--entrypoint", "/usr/local/bin/platform-installation", args.runtime_image, "helm-plan", "--input", "/installation-input/input.json", "--runtime-image", args.runtime_image, "--console-image", args.console_image])
        plan = validate_plan(decode(output))
        persist(args.directory/"helm-plan.json", {"plan": plan, "context": args.context, "kubeconfig": str(args.kubeconfig), "node": args.node, "storage_class": args.storage_class}, immutable=True)
        if args.operation == "render":
            print(args.directory/"helm-plan.json")
            return
        installation = Installation(args, plan)
        if args.operation == "session":
            installation.deliver_session(explicit=True)
        elif args.operation == "public-trust":
            installation.deliver_public_trust()
        else:
            installation.run(args.operation)
            if args.operation in {"up", "resume"}:
                installation.deliver_public_trust()
                installation.deliver_session(explicit=False)
    finally:
        os.close(lock)


if __name__ == "__main__":
    try:
        main()
    except (InstallationFailure, TRUST.PublicTrustFailure, OSError, KeyError, TypeError, ValueError, subprocess.TimeoutExpired) as error:
        # No raw subprocess output, external JSON, DSN or kubeconfig contents are echoed.
        import sys
        print(failure_message(error), file=sys.stderr)
        raise SystemExit(1) from None
