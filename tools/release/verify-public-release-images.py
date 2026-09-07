#!/usr/bin/env python3
"""Prove exact candidate GHCR indexes are anonymously readable before publication."""

import argparse
import datetime
import hashlib
import http.client
import importlib.util
import json
import os
from pathlib import Path
import re
import ssl
import subprocess
import sys
from urllib.parse import urlencode


MAX_FILE_BYTES = 4 * 1024 * 1024
MAX_TOKEN_BYTES = 16_384
MAX_INDEX_BYTES = 4 * 1024 * 1024
MAX_WALL_SECONDS = 60
MAX_SAFE_JSON_INTEGER = 9_007_199_254_740_991
IMAGE_SUFFIXES = {"runtime": "platform-runtime", "sandbox_runner": "platform-sandbox-runner", "console": "platform-console"}
SPEC = importlib.util.spec_from_file_location("candidate", Path(__file__).with_name("prepare-productization-release-candidate.py"))
CANDIDATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CANDIDATE)


class Rejected(ValueError):
    def __init__(self, code):
        super().__init__(code)
        self.code = code


def strict_json(data, maximum):
    if not data or len(data) > maximum:
        raise Rejected("response_size_rejected")

    def pairs(values):
        result = {}
        for key, value in values:
            if key in result or len(result) >= 64:
                raise Rejected("json_rejected")
            result[key] = value
        return result

    def number(_):
        raise Rejected("json_rejected")

    try:
        value = json.loads(data.decode("utf-8"), object_pairs_hook=pairs,
                           parse_float=number, parse_constant=number)
        # Candidate documents, registry tokens and worker reports all have object roots.
        if not isinstance(value, dict):
            raise Rejected("json_rejected")
        return value
    except (ValueError, UnicodeError, RecursionError):
        raise Rejected("json_rejected") from None


def read_json(path):
    CANDIDATE.regular_file(path, "candidate input")
    with path.open("rb") as source:
        return strict_json(source.read(MAX_FILE_BYTES + 1), MAX_FILE_BYTES)


def exact_images(assets, repository, tag, revision):
    if (re.fullmatch(r"[a-z0-9][a-z0-9_.-]*/[a-z0-9][a-z0-9_.-]*", repository) is None
            or CANDIDATE.RELEASE_TAG.fullmatch(tag) is None
            or CANDIDATE.REVISION.fullmatch(revision) is None
            or assets.is_symlink() or not assets.is_dir()):
        raise Rejected("candidate_identity_rejected")
    bundle = read_json(assets / "release-bundle.json")
    metadata = CANDIDATE.closed(read_json(assets / "images.json"), set(IMAGE_SUFFIXES), "images.json")
    if (not isinstance(bundle, dict) or type(bundle.get("schema_version")) is not int
            or bundle["schema_version"] != 1 or bundle.get("version") != tag[1:]
            or bundle.get("git_commit") != revision):
        raise Rejected("candidate_identity_rejected")
    images = bundle.get("images")
    if not isinstance(images, list) or len(images) != 3:
        raise Rejected("candidate_image_closure_rejected")
    result = {}
    for raw in images:
        if not isinstance(raw, dict):
            raise Rejected("candidate_image_closure_rejected")
        name = raw.get("name")
        if not isinstance(name, str) or name not in IMAGE_SUFFIXES or name in result:
            raise Rejected("candidate_image_closure_rejected")
        subject = f"ghcr.io/{repository}/{IMAGE_SUFFIXES[name]}"
        identity, platforms = CANDIDATE.validate_image(raw, name, subject, "linux/amd64")
        expected = {"subject": subject, "index_digest": identity["index_digest"], "platforms": platforms}
        # Reuse the owning image decoder and compare the complete images.json projection.
        if metadata[name] != expected:
            raise Rejected("candidate_image_closure_rejected")
        result[name] = expected
    if set(result) != set(IMAGE_SUFFIXES):
        raise Rejected("candidate_image_closure_rejected")
    return result


def fetch(path, headers, maximum):
    # HTTPSConnection does not consume proxy, netrc, Docker or account credential configuration.
    connection = http.client.HTTPSConnection("ghcr.io", timeout=10, context=ssl.create_default_context())
    try:
        connection.request("GET", path, headers={"Accept-Encoding": "identity", **headers})
        response = connection.getresponse()
        if response.status in (401, 403):
            raise Rejected("anonymous_access_denied")
        if response.status != 200:
            raise Rejected("registry_response_rejected")
        if response.getheader("Content-Encoding") not in (None, "identity"):
            raise Rejected("response_encoding_rejected")
        length = response.getheader("Content-Length")
        if length is not None and (not length.isdecimal() or not 0 < int(length) <= maximum):
            raise Rejected("response_size_rejected")
        body = response.read(maximum + 1)
        if not body or len(body) > maximum or (length is not None and len(body) != int(length)):
            raise Rejected("response_size_rejected")
        return body
    finally:
        connection.close()


def valid_issued_at(value):
    if (not isinstance(value, str) or len(value) > 64
            or re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt][0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?(?:[Zz]|[+-](?:[01][0-9]|2[0-3]):[0-5][0-9])", value) is None):
        return False
    # Validate calendar/zone syntax without imposing the platform's six-microsecond wire form.
    # RFC3339 permits second 60; no local leap-second table becomes token authority.
    calendar = value[:17] + "59" + value[19:] if value[17:19] == "60" else value
    # Fractional syntax was checked above; calendar validation must also work on Python 3.9.
    calendar = re.sub(r"\.[0-9]+", "", calendar, count=1)
    try:
        datetime.datetime.fromisoformat(calendar.replace("t", "T").replace("z", "Z").replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def anonymous_token(response):
    if (not isinstance(response, dict)
            or set(response) - {"token", "access_token", "expires_in", "issued_at"}
            or ("expires_in" in response and (type(response["expires_in"]) is not int
                or not 0 < response["expires_in"] <= MAX_SAFE_JSON_INTEGER))
            or ("issued_at" in response and not valid_issued_at(response["issued_at"]))):
        raise Rejected("anonymous_token_rejected")
    token = response.get("token", response.get("access_token"))
    if (not isinstance(token, str) or not 0 < len(token) <= 8_192
            or re.fullmatch(r"[A-Za-z0-9._~+/-]+=*", token) is None
            or ("token" in response and "access_token" in response and response["token"] != response["access_token"])):
        raise Rejected("anonymous_token_rejected")
    return token


def verify_indexes(images):
    checked = []
    for name in sorted(images):
        image = images[name]
        repository_path = image["subject"].removeprefix("ghcr.io/")
        token_path = "/token?" + urlencode({"service": "ghcr.io", "scope": f"repository:{repository_path}:pull"})
        token_response = strict_json(fetch(token_path, {"Accept": "application/json"}, MAX_TOKEN_BYTES), MAX_TOKEN_BYTES)
        token = anonymous_token(token_response)
        body = fetch(f"/v2/{repository_path}/manifests/{image['index_digest']}", {
            "Authorization": "Bearer " + token,
            "Accept": "application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json",
        }, MAX_INDEX_BYTES)
        if "sha256:" + hashlib.sha256(body).hexdigest() != image["index_digest"]:
            raise Rejected("index_digest_rejected")
        checked.append({"image": image["subject"], "index_digest": image["index_digest"]})
    return {"status": "passed", "evidence": "exact_index_anonymous_accessibility", "indexes": checked}


def arguments(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--assets", required=True, type=Path)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--release-tag", required=True)
    parser.add_argument("--revision", required=True)
    return parser.parse_args(argv)


def worker_main(argv):
    args = arguments(argv)
    try:
        result = verify_indexes(exact_images(args.assets, args.repository, args.release_tag, args.revision))
    except Rejected as error:
        result = {"status": "failed", "reason": error.code}
    except (OSError, http.client.HTTPException):
        result = {"status": "failed", "reason": "registry_transport_failed"}
    except (ValueError, KeyError, TypeError):
        result = {"status": "failed", "reason": "candidate_input_rejected"}
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0 if result["status"] == "passed" else 1


def supervise(command, seconds=MAX_WALL_SECONDS):
    # A separate, credential-free process makes the total bound include DNS, headers and slow reads.
    try:
        result = subprocess.run(command, env={"PATH": os.defpath}, stdin=subprocess.DEVNULL,
                                capture_output=True, timeout=seconds)
    except subprocess.TimeoutExpired:
        raise Rejected("total_deadline_exceeded") from None
    if len(result.stdout) > 4_096 or result.stderr:
        raise Rejected("verification_process_failed")
    report = strict_json(result.stdout, 4_096)
    if not isinstance(report, dict) or report.get("status") not in ("passed", "failed"):
        raise Rejected("verification_process_failed")
    if (result.returncode == 0) != (report["status"] == "passed"):
        raise Rejected("verification_process_failed")
    return report


def main():
    arguments(sys.argv[1:])
    worker = 'import runpy,sys; raise SystemExit(runpy.run_path(sys.argv[1],run_name="anonymous_worker")["worker_main"](sys.argv[2:]))'
    try:
        report = supervise([sys.executable, "-I", "-c", worker, str(Path(__file__).resolve()), *sys.argv[1:]])
    except Rejected as error:
        report = {"status": "failed", "reason": error.code}
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    raise SystemExit(0 if report["status"] == "passed" else 1)


if __name__ == "__main__":
    main()
