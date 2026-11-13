#!/usr/bin/env python3
"""Explicit bounded build of the reviewed registry-only command-key adapter."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tomllib

import execution_guards as guards


SOURCE = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--rust-image", required=True, help="already present immutable official Rust image ID")
    options = parser.parse_args()
    if (not options.work_dir.is_absolute() or options.work_dir.exists()
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", options.rust_image)):
        parser.error("require a new absolute work directory and immutable local Rust image ID")
    subject = guards.reviewed_subject(SOURCE)
    image = json.loads(subprocess.check_output(
        ["docker", "image", "inspect", options.rust_image], text=True, timeout=20))[0]
    if image["Os"] != "linux" or image["Architecture"] != "amd64":
        parser.error("this build profile is Linux amd64")
    if not any(value.startswith("rust@sha256:") for value in image["RepoDigests"]):
        parser.error("build image lacks its official Rust repository digest")
    options.work_dir.mkdir(mode=0o700)
    source = options.work_dir / "source"
    source.mkdir(mode=0o700)
    for name in guards.INPUT_NAMES:
        path = source / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes((SOURCE / "consumer" / name).read_bytes())
        path.chmod(0o444)
    if guards.consumer_inputs(source) != subject["inputs"]:
        raise ValueError("staged build inputs differ from the reviewed Git subject")
    home = options.work_dir / "cargo-home"; home.mkdir(mode=0o700)
    target = options.work_dir / "target"; target.mkdir(mode=0o700)
    docker_config = options.work_dir / "docker-config"; docker_config.mkdir(mode=0o700)
    docker = ["docker", "--config", str(docker_config)]
    name = "sdk-rust-command-key-build-" + os.urandom(6).hex()
    owned = None
    started = datetime.now(timezone.utc).isoformat()
    try:
        command = [*docker, "create", "--init", "--read-only", "--name", name,
                   "--cpus", "2", "--memory", "3g", "--pids-limit", "512",
                   "--user", f"{os.getuid()}:{os.getgid()}", "--cap-drop", "ALL",
                   "--security-opt", "no-new-privileges", "--tmpfs", "/tmp:mode=1777,size=256m",
                   "-v", f"{source}:/source:ro", "-v", f"{home}:/cargo-home:rw",
                   "-v", f"{target}:/target:rw", "-e", "CARGO_HOME=/cargo-home",
                   "-e", "CARGO_BUILD_JOBS=2", "-w", "/source", options.rust_image,
                   "timeout", "--kill-after=10s", "900s", "sh", "-ec", guards.BUILD_COMMAND]
        owned = subprocess.check_output(command, text=True, timeout=30).strip()
        subprocess.run([*docker, "start", owned], check=True, stdout=subprocess.DEVNULL, timeout=30)
        status = subprocess.check_output([*docker, "wait", owned], text=True, timeout=930).strip()
        logs = subprocess.check_output([*docker, "logs", owned], stderr=subprocess.STDOUT, timeout=30)
        (options.work_dir / "build.log").write_bytes(logs)
        if status != "0":
            raise ValueError("bounded consumer build failed; no receipt admitted")
        if (guards.consumer_inputs(source) != subject["inputs"]
                or guards.reviewed_subject(SOURCE) != subject):
            raise ValueError("reviewed inputs changed during the build")
        lock = tomllib.loads((source / "Cargo.lock").read_text())
        packages = [row for row in lock["package"] if row["name"] == "durable-workflow"]
        if len(packages) != 1 or packages[0]["source"] != "registry+https://github.com/rust-lang/crates.io-index":
            raise ValueError("consumer lock is not the official registry-only SDK")
        package = packages[0]
        archives = list((home / "registry/cache").glob(f"*/durable-workflow-{package['version']}.crate"))
        if len(archives) != 1 or hashlib.sha256(archives[0].read_bytes()).hexdigest() != package["checksum"]:
            raise ValueError("downloaded SDK archive differs from its lock checksum")
        with tarfile.open(archives[0]) as archive:
            member = archive.getmember(f"durable-workflow-{package['version']}/.cargo_vcs_info.json")
            if not member.isfile() or member.size > 4096:
                raise ValueError("SDK source identity is not bounded regular metadata")
            vcs = json.load(archive.extractfile(member))
        if vcs["git"].get("dirty") or vcs.get("path_in_vcs"):
            raise ValueError("SDK archive has an unqualified source identity")
        package_name = tomllib.loads((source / "Cargo.toml").read_text())["package"]["name"]
        binary = options.work_dir / "consumer"
        # Cargo may hardlink target/debug to target/debug/deps. Materialize one
        # owned executable inode for the held-descriptor runtime identity.
        with open(os.open(binary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o700), "wb") as output:
            with (target / "debug" / package_name).open("rb") as built:
                shutil.copyfileobj(built, output)
        record = guards.executable_record(binary)
        receipt = {"schema": "durable-workflow.command-key-consumer-build.v1",
                   "subject": subject, "inputs": subject["inputs"],
                   "lock_sha256": subject["inputs"]["Cargo.lock"],
                   "crate": {"version": package["version"], "sha256": package["checksum"],
                             "source": vcs["git"]["sha1"]},
                   "build_command": guards.BUILD_COMMAND, "binary_path": str(binary), "binary": record,
                   "image": {"id": image["Id"], "digests": image["RepoDigests"]},
                   "toolchain": logs.decode(errors="replace").splitlines()[:2],
                   "started_at": started, "finished_at": datetime.now(timezone.utc).isoformat()}
        with open(os.open(options.work_dir / "build-receipt.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as output:
            json.dump(receipt, output, sort_keys=True)
        print(json.dumps({"outcome": "pass", "binary_sha256": record["sha256"],
                          "inputs": subject["inputs"], "subject_commit": subject["commit"]}))
    finally:
        if owned is not None:
            subprocess.run([*docker, "rm", "-fv", owned], check=True,
                           stdout=subprocess.DEVNULL, timeout=30)


if __name__ == "__main__":
    main()
