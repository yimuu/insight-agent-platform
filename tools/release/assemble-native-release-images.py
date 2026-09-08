#!/usr/bin/env python3
"""Produce and consume the current run's native image and Console build records."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import os
from pathlib import Path
import selectors
import shutil
import signal
import stat
import subprocess
import time

from native_image_contract import (
    BuildIdentity, COMPONENTS, ConsoleBuildRecord, DIGEST, MAX_CONSOLE_ARCHIVE_BYTES,
    MAX_INDEX_BYTES, MAX_RECORD_BYTES, NativeBuildRecord, OCI_INDEX, PLATFORMS,
    bounded_file, integer, load_records, require, strict_json, verify_merged_index,
)


def current_identity() -> BuildIdentity:
    require(os.environ.get("GITHUB_REF_TYPE") == "tag", "release records require a tag workflow")
    result = BuildIdentity(os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_SHA"],
                           os.environ["GITHUB_REF_NAME"], int(os.environ["GITHUB_RUN_ID"]),
                           int(os.environ["GITHUB_RUN_ATTEMPT"]))
    result.validate()
    return result


def write_new(path: Path, payload: bytes) -> None:
    with path.open("xb") as output:
        output.write(payload)


def file_binding(path: Path) -> tuple[int, str]:
    metadata = path.lstat()
    require(stat.S_ISREG(metadata.st_mode) and 0 < metadata.st_size <= MAX_CONSOLE_ARCHIVE_BYTES,
            "Console archive must be a bounded regular file")
    total = 0
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(min(1024 * 1024, MAX_CONSOLE_ARCHIVE_BYTES + 1 - total)):
            total += len(chunk)
            require(total <= MAX_CONSOLE_ARCHIVE_BYTES, "Console archive grew beyond its bound")
            digest.update(chunk)
    require(total == metadata.st_size, "Console archive changed while reading")
    return total, "sha256:" + digest.hexdigest()


def run_bounded(argv: list[str], timeout: float = 120) -> bytes:
    deadline = time.monotonic() + timeout
    buffers = {"stdout": bytearray(), "stderr": bytearray()}
    limits = {"stdout": MAX_INDEX_BYTES, "stderr": 16 * 1024}
    with subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, start_new_session=True) as process:
        completed = False
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ, "stdout")
                selector.register(process.stderr, selectors.EVENT_READ, "stderr")
                while selector.get_map():
                    remaining = deadline - time.monotonic()
                    require(remaining > 0, "image command timed out")
                    for key, _ in selector.select(remaining):
                        chunk = os.read(key.fileobj.fileno(), 8192)
                        if not chunk:
                            selector.unregister(key.fileobj)
                            continue
                        require(len(buffers[key.data]) + len(chunk) <= limits[key.data],
                                "image command output exceeded its bound")
                        buffers[key.data].extend(chunk)
            remaining = deadline - time.monotonic()
            require(remaining > 0, "image command timed out")
            code = process.wait(timeout=remaining)
            completed = True
            if code != 0:
                detail = bytes(buffers["stderr"]).decode("utf-8", errors="replace")
                raise ValueError(f"image command failed ({code}): {detail}")
            return bytes(buffers["stdout"])
        finally:
            if not completed:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait(timeout=5)


def record_native(args: argparse.Namespace, identity: BuildIdentity) -> None:
    require(DIGEST.fullmatch(args.digest) is not None, "invalid native build digest")
    subject = f"ghcr.io/{identity.repository}/{COMPONENTS[args.component]}"
    raw = run_bounded(["docker", "buildx", "imagetools", "inspect", "--raw", f"{subject}@{args.digest}"])
    result = NativeBuildRecord(
        1, identity, args.component, args.platform,
        subject, args.digest,
        args.started_epoch, args.finished_epoch, raw.decode("utf-8"),
    )
    payload = result.encode(identity, int(time.time()))
    args.output_directory.mkdir(parents=True, exist_ok=True)
    write_new(args.output_directory / result.filename, payload)


def record_console(args: argparse.Namespace, identity: BuildIdentity) -> None:
    size, digest = file_binding(args.archive)
    result = ConsoleBuildRecord(1, identity, digest, size, args.started_epoch, args.finished_epoch)
    require(args.archive.name == result.filename, "Console archive name differs from release version")
    write_new(args.output, result.encode(identity, int(time.time())))


def prepare_console(args: argparse.Namespace, identity: BuildIdentity) -> None:
    record = ConsoleBuildRecord.decode(
        bounded_file(args.input_directory / "console-build.json", MAX_RECORD_BYTES),
        identity, int(time.time()),
    )
    with os.scandir(args.input_directory) as entries:
        names = []
        for entry in entries:
            require(len(names) < 2, "Console artifact entry bound exceeded")
            names.append(entry.name)
    require(set(names) == {record.filename, "console-build.json"}, "Console artifact file closure mismatch")
    archive = args.input_directory / record.filename
    require(file_binding(archive) == (record.archive_bytes, record.archive_sha256),
            "Console archive differs from its build record")
    path = Path(__file__).with_name("prepare-productization-release-candidate.py")
    spec = importlib.util.spec_from_file_location("release_candidate", path)
    candidate = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(candidate)
    candidate.checked_artifact(args.input_directory, {
        "path": record.filename, "bytes": record.archive_bytes, "sha256": record.archive_sha256,
    }, "Console build archive")
    candidate.extract_console(archive, args.output_directory)
    args.assets_directory.mkdir(parents=True, exist_ok=True)
    with (args.assets_directory / record.filename).open("xb") as output, archive.open("rb") as source:
        shutil.copyfileobj(source, output, 1024 * 1024)
    write_new(args.timing_directory / "console-build.time",
              f"{record.started_epoch} {record.finished_epoch}\n".encode())


def merge_native(args: argparse.Namespace, identity: BuildIdentity) -> None:
    records = load_records(args.input_directory, identity, int(time.time()))
    # Validate the entire current collection before the first registry mutation.
    args.output_directory.mkdir(parents=True, exist_ok=True)
    for component in ("runtime", "sandbox_runner"):
        selected = [record for record in records if record.component == component]
        subject = selected[0].subject
        metadata_path = args.output_directory / f"{component}-merge.json"
        require(not metadata_path.exists() and not metadata_path.is_symlink(), "merge metadata must be fresh")
        references = [f"{record.subject}@{record.index_digest}" for record in selected]
        run_bounded(["docker", "buildx", "imagetools", "create", "--metadata-file", str(metadata_path),
                     "--tag", f"{subject}:build-{identity.git_commit}", *references])
        metadata = strict_json(bounded_file(metadata_path, MAX_INDEX_BYTES), MAX_INDEX_BYTES)
        require(isinstance(metadata, dict), "invalid merge metadata")
        descriptor = metadata.get("containerimage.descriptor")
        require(isinstance(descriptor, dict) and descriptor.get("mediaType") == OCI_INDEX and
                isinstance(descriptor.get("digest"), str) and DIGEST.fullmatch(descriptor["digest"]) is not None and
                integer(descriptor.get("size"), 1) and descriptor["size"] <= MAX_INDEX_BYTES,
                "merge metadata has no bounded OCI index subject")
        digest = descriptor["digest"]
        raw = run_bounded(["docker", "buildx", "imagetools", "inspect", "--raw", f"{subject}@{digest}"])
        require(len(raw) == descriptor["size"], "merged index size mismatch")
        verify_merged_index(records, identity, component, raw, digest, int(time.time()))
        ready = int(time.time())
        # Stamp completion only after the pushed exact index and all its descriptors are verified.
        start = min(record.started_epoch for record in selected)
        prefix = "runtime" if component == "runtime" else "runner"
        write_new(args.output_directory / f"{component}-index.json", raw)
        write_new(args.output_directory / f"{component}-digest", (digest + "\n").encode())
        write_new(args.timing_directory / f"{prefix}-start", f"{start}\n".encode())
        write_new(args.timing_directory / f"{prefix}-finish", f"{ready}\n".encode())


def main() -> None:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    native = commands.add_parser("record-native")
    native.add_argument("--component", choices=COMPONENTS, required=True)
    native.add_argument("--platform", choices=sorted(PLATFORMS), required=True)
    native.add_argument("--digest", required=True)
    native.add_argument("--output-directory", type=Path, required=True)
    console = commands.add_parser("record-console")
    console.add_argument("--archive", type=Path, required=True)
    console.add_argument("--output", type=Path, required=True)
    for command in (native, console):
        command.add_argument("--started-epoch", type=int, required=True)
        command.add_argument("--finished-epoch", type=int, required=True)
    merge = commands.add_parser("merge-native")
    prepare = commands.add_parser("prepare-console")
    for command in (merge, prepare):
        command.add_argument("--input-directory", type=Path, required=True)
        command.add_argument("--output-directory", type=Path, required=True)
        command.add_argument("--timing-directory", type=Path, required=True)
    prepare.add_argument("--assets-directory", type=Path, required=True)
    args = parser.parse_args()
    handlers = {"record-native": record_native, "record-console": record_console,
                "merge-native": merge_native, "prepare-console": prepare_console}
    handlers[args.command](args, current_identity())


if __name__ == "__main__":
    def terminate(signum: int, _frame: object) -> None:
        # Keep repeated cancellation from interrupting the owned process group's cleanup.
        signal.signal(signum, signal.SIG_IGN)
        raise SystemExit(128 + signum)

    previous = signal.signal(signal.SIGTERM, terminate)
    try:
        main()
    finally:
        signal.signal(signal.SIGTERM, previous)
