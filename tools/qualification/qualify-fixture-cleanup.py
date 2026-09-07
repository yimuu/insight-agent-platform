#!/usr/bin/env python3
"""Exercise real CLI initialization and isolated Docker teardown without binding ports."""
from pathlib import Path
import argparse
import json
import os
import shutil
import subprocess
import tempfile

from fixture_project import cleanup, cleanup_all, identity


def run(arguments, **kwargs):
    return subprocess.run(arguments, check=True, capture_output=True, text=True, **kwargs).stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--insight-bin", type=Path, required=True)
    args = parser.parse_args()
    insight = str(args.insight_bin.resolve(strict=True))
    workspace = Path(__file__).resolve().parents[2]
    owned = []
    try:
        for provision in (False, True, True):
            project = Path(tempfile.mkdtemp(prefix="insight-cleanup-")).resolve()
            project_identity = identity(project)
            owned.append((project, project_identity))
            run([insight, "init", "--path", str(project), "--name", "cleanup-probe"])
            manifest = json.loads((project / ".insight/project.json").read_bytes())
            tenant = manifest["identity"]["tenant_id"]
            compose_project = "insight-" + tenant.removeprefix("ten_").replace("-", "")
            assert len(compose_project) == 40
            if not provision:
                assert cleanup(project, project_identity, insight, "cleanup-probe", 19) == 19
                assert not project.exists()
                owned.pop()
                continue
            runtime = project / ".insight/runtime"
            runtime.mkdir(exist_ok=True)
            shutil.copyfile(workspace / "deploy/dev/compose.yaml", runtime / "compose.yaml")
            environment = dict(os.environ)
            for key, suffix in [
                ("CONFIG", "nats.conf"), ("CA", "tls/ca.pem"),
                ("SERVER_CERT", "tls/nats-server.pem"), ("SERVER_KEY", "tls/nats-server-key.pem"),
            ]:
                environment[f"INSIGHT_DEV_NATS_{key}_PATH"] = str(runtime / suffix)
            # Create actual containers and volumes, without starting PostgreSQL or reserving ports.
            run(["docker", "compose", "--project-name", compose_project, "--file",
                 str(runtime / "compose.yaml"), "create", "postgres"], env=environment)

        def resources(project):
            manifest = json.loads((project / ".insight/project.json").read_bytes())
            tenant = manifest["identity"]["tenant_id"]
            namespace = "insight-" + tenant.removeprefix("ten_").replace("-", "")
            label = f"label=com.docker.compose.project={namespace}"
            return (
                set(run(["docker", "ps", "--all", "--quiet", "--filter", label]).split()),
                set(run(["docker", "volume", "ls", "--quiet", "--filter", label]).split()),
            )

        first, second = owned
        first_resources, second_resources = resources(first[0]), resources(second[0])
        assert all(first_resources) and all(second_resources)
        assert not first_resources[0] & second_resources[0]
        assert not first_resources[1] & second_resources[1]
        assert cleanup(first[0], first[1], insight, "cleanup-probe", 19) == 19
        assert resources(second[0]) == second_resources
        assert cleanup(second[0], second[1], insight, "cleanup-probe", 0) == 0
        containers = set(run(["docker", "ps", "--all", "--quiet"]).split())
        volumes = set(run(["docker", "volume", "ls", "--quiet"]).split())
        assert not containers & (first_resources[0] | second_resources[0])
        assert not volumes & (first_resources[1] | second_resources[1])
        assert all(not project.exists() for project, _ in owned)
        print("PASS: init failure cleanup and independent container/volume teardown for two fresh projects")
    finally:
        cleanup_all(owned, insight, "cleanup-probe")


if __name__ == "__main__":
    main()
