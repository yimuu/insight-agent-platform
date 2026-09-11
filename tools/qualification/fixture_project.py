"""Lifecycle for a directory created exclusively by one qualification invocation.

The CLI remains the owner of process identity and Compose teardown. This helper only
binds cleanup to the fresh directory and exports bounded, explicitly selected logs.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys


def identity(project: Path) -> str:
    info = project.lstat()
    if not stat.S_ISDIR(info.st_mode) or project.absolute() != project.resolve():
        raise ValueError("fixture project must be a canonical, non-symlink directory")
    if info.st_uid != os.getuid():
        raise ValueError("fixture project belongs to another user")
    return f"{info.st_dev}:{info.st_ino}"


def export_logs(project: Path, destination: Path) -> None:
    destination = destination.absolute()
    if destination.resolve().is_relative_to(project):
        raise ValueError("diagnostics must be outside the disposable project")
    destination.mkdir(mode=0o700, parents=True, exist_ok=False)
    source = project / ".insight/runtime/logs"
    if not source.exists():
        return
    if source.resolve() != source or not source.is_dir():
        raise ValueError("refusing symlinked runtime logs")
    paths = sorted(path for path in source.iterdir()
                   if re.fullmatch(r"[a-z0-9-]{1,64}\.log", path.name))
    if len(paths) > 64:
        print("runtime diagnostics truncated to 64 log files", file=sys.stderr)
    for path in paths[:64]:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise ValueError("runtime log must be a regular file with one link")
            stream.seek(max(0, os.fstat(stream.fileno()).st_size - 1024 * 1024))
            with (destination / path.name).open("xb") as output:
                output.write(stream.read(1024 * 1024))


def cleanup(project: Path, expected_identity: str, insight: str, project_name: str,
            status: int, keep_failed: bool = False, logs: Path | None = None) -> int:
    if identity(project) != expected_identity:
        raise ValueError("fixture directory identity changed; refusing cleanup")
    state = project / ".insight"
    runtime = state / "runtime"
    for path in (state, runtime):
        if path.is_symlink():
            raise ValueError("refusing symlinked project state")
    manifest = state / "project.json"
    if manifest.exists():
        if manifest.is_symlink():
            raise ValueError("refusing symlinked project manifest")
        if json.loads(manifest.read_bytes()).get("project_name") != project_name:
            raise ValueError("fixture project name changed; refusing cleanup")
    journal = runtime / "processes.json"
    compose = runtime / "compose.yaml"
    installed = journal.exists() or (runtime / "profile.json").exists()
    if installed and (not manifest.exists() or not compose.is_file() or compose.is_symlink()):
        raise ValueError("installed runtime has no owning project or Compose configuration")
    stop_error = None
    try:
        if journal.exists():
            subprocess.run([insight, "qualification-aws", "stop", "--path", str(project)], check=True)
    except (OSError, subprocess.CalledProcessError) as error:
        stop_error = error
    # Include shutdown/drain diagnostics and still export on a failed stop.
    log_error = None
    if logs is not None:
        try:
            export_logs(project, logs)
        except (OSError, ValueError) as error:
            log_error = error
    if stop_error is not None:
        raise stop_error
    if status != 0 and keep_failed:
        if log_error is not None:
            print(f"runtime diagnostics failed: {log_error}", file=sys.stderr)
        print(f"failed fixture explicitly retained at {project}", file=sys.stderr)
        return status
    # Init can fail before runtime installation; no Compose call has occurred then.
    if manifest.exists() and compose.exists():
        if not compose.is_file() or compose.is_symlink():
            raise ValueError("refusing invalid fixture Compose configuration")
        subprocess.run([insight, "qualification-aws", "reset", "--path", str(project),
                        "--confirm", project_name], check=True)
    if identity(project) != expected_identity:
        raise ValueError("fixture directory was replaced during cleanup")
    shutil.rmtree(project)
    if log_error is not None:
        raise ValueError(f"fixture removed but diagnostics failed: {log_error}")
    return status


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["identity", "cleanup"])
    parser.add_argument("--project", type=Path, required=True)
    parser.add_argument("--identity")
    parser.add_argument("--insight-bin")
    parser.add_argument("--project-name")
    parser.add_argument("--status", type=int, default=0)
    parser.add_argument("--keep-failed", action="store_true")
    parser.add_argument("--logs-directory", type=Path)
    args = parser.parse_args()
    try:
        if args.operation == "identity":
            print(identity(args.project))
            return 0
        if not all((args.identity, args.insight_bin, args.project_name)):
            parser.error("cleanup requires identity, insight-bin and project-name")
        return cleanup(args.project, args.identity, args.insight_bin,
                       args.project_name, args.status, args.keep_failed, args.logs_directory)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"fixture cleanup failed: {error}", file=sys.stderr)
        return args.status or 1


def cleanup_all(owned: list[tuple[Path, str]], insight: str, project_name: str) -> None:
    errors = []
    for project, expected_identity in owned:
        if project.exists():
            try:
                cleanup(project, expected_identity, insight, project_name, 1)
            except (OSError, ValueError, subprocess.CalledProcessError) as error:
                errors.append(error)
    if errors:
        raise RuntimeError(f"{len(errors)} fixture cleanup failures; failed directories retained") from errors[0]


if __name__ == "__main__":
    raise SystemExit(main())
