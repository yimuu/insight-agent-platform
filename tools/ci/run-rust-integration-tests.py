#!/usr/bin/env python3
"""Run every ordinary integration target; external qualification has explicit owning harnesses.

The coordinator and its four-process Q1 test require separate fresh current-schema databases:
PLATFORM_TEST_ORCHESTRATION_DATABASE_URL and PLATFORM_TEST_ORCHESTRATION_Q1_DATABASE_URL.
They reject pre-existing tenants before admission; their children use the corresponding
exact authority. Serializing test functions cannot isolate deliberately retained leased Jobs.
The coordinator target also runs its claim-isolation module against the required dedicated
PLATFORM_TEST_ORCHESTRATION_ISOLATION_DATABASE_URL provisioned by CI; it never shares
its intentionally damaged owning rows with the ordinary/Q1 coordinator fixture.
Installation bootstrap requires PLATFORM_TEST_INSTALLATION_DATABASE_URL: a dedicated
current-schema database with no principals or tenant bindings. The ordinary shared
database can already contain either when this target runs; ordering is not isolation.
Run kernel transaction tests require PLATFORM_TEST_RUN_KERNEL_DATABASE_URL: their
same-transaction claim/rollback assertions use a dedicated current-schema database,
independent of other targets' enrolled scheduler partitions.
"""
import argparse
import json
from pathlib import Path
import subprocess

ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "Cargo.toml").is_file())
# These complete targets require external process/Kubernetes fixtures. Their tests remain
# compile-checked and fail on missing configuration when the owning harness selects them.
EXTERNAL_TARGETS = {
    ("insight-platform-opensandbox-client", "kubernetes_l3"),
    ("insight-platform-mcp-service", "process_l3"),
    ("insight-platform-qualification-tests", "productization"),
}


def integration_commands(metadata):
    commands = []
    observed = set()
    workspace = set(metadata["workspace_members"])
    for package in metadata["packages"]:
        if package["id"] not in workspace:
            continue
        for target in package["targets"]:
            if target["kind"] != ["test"]:
                continue
            identity = (package["name"], target["name"])
            observed.add(identity)
            if identity not in EXTERNAL_TARGETS:
                commands.append(["cargo", "test", "--locked", "--all-features", "-p", package["name"], "--test", target["name"]])
    if not EXTERNAL_TARGETS.issubset(observed):
        raise ValueError("external qualification target registry references a missing target")
    return sorted(commands)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--print", action="store_true", dest="print_only")
    args = parser.parse_args()
    metadata = json.loads(subprocess.check_output(["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"], cwd=ROOT))
    commands = integration_commands(metadata)
    if args.print_only:
        print(json.dumps(commands, indent=2))
        return
    for command in commands:
        print("Running ordinary integration target: " + " ".join(command), flush=True)
        subprocess.run(command, cwd=ROOT, check=True)


if __name__ == "__main__":
    main()
