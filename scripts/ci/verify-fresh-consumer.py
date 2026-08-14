#!/usr/bin/env python3
"""Build an isolated exact-version consumer on the crate's declared MSRV."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import tarfile
import tempfile
import tomllib
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
EXPECTED_MSRV = "1.86"


class VerificationError(RuntimeError):
    """The packaged or published crate failed fresh-consumer verification."""


def package_identity(manifest: Path) -> tuple[str, str, str]:
    document = tomllib.loads(manifest.read_text(encoding="utf-8"))
    package = document.get("package")
    if not isinstance(package, dict):
        raise VerificationError("manifest does not contain a package table")

    name = package.get("name")
    version = package.get("version")
    rust_version = package.get("rust-version")
    if not all(isinstance(value, str) and value for value in (name, version)):
        raise VerificationError("manifest package identity is incomplete")
    if rust_version != EXPECTED_MSRV:
        raise VerificationError(
            f"package rust-version must remain {EXPECTED_MSRV}, got {rust_version!r}"
        )
    return name, version, rust_version


def require_msrv_toolchain() -> str:
    rustc = os.environ.get("RUSTC", "rustc")
    result = subprocess.run(
        [rustc, "--version"],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    match = re.fullmatch(r"rustc (\d+\.\d+)\.\d+ .*", result.stdout.strip())
    if match is None:
        raise VerificationError(
            f"could not parse rustc version: {result.stdout.strip()}"
        )
    if match.group(1) != EXPECTED_MSRV:
        raise VerificationError(
            f"fresh consumer must use Rust {EXPECTED_MSRV}, got {match.group(1)}"
        )
    return result.stdout.strip()


def cargo_metadata(manifest: Path) -> dict[str, Any]:
    result = subprocess.run(
        [
            "cargo",
            "metadata",
            "--manifest-path",
            str(manifest),
            "--no-deps",
            "--format-version",
            "1",
        ],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    metadata = json.loads(result.stdout)
    if not isinstance(metadata, dict):
        raise VerificationError("cargo metadata did not return an object")
    return metadata


def extract_package(archive_path: Path, destination: Path) -> Path:
    if not archive_path.is_file():
        raise VerificationError(f"package archive does not exist: {archive_path}")

    with tarfile.open(archive_path, mode="r:gz") as archive:
        members = archive.getmembers()
        if not members:
            raise VerificationError("package archive is empty")
        for member in members:
            member_path = PurePosixPath(member.name)
            if (
                not member_path.parts
                or member_path.is_absolute()
                or ".." in member_path.parts
            ):
                raise VerificationError(
                    f"package archive contains an unsafe path: {member.name}"
                )
            if member.issym() or member.islnk():
                raise VerificationError(
                    f"package archive contains an unsupported link: {member.name}"
                )
        archive.extractall(destination)

    roots = {PurePosixPath(member.name).parts[0] for member in members}
    if len(roots) != 1:
        raise VerificationError("package archive must contain one package root")
    package_root = destination / roots.pop()
    if not (package_root / "Cargo.toml").is_file():
        raise VerificationError("package archive does not contain Cargo.toml")
    return package_root


def render_consumer_manifest(
    package_name: str,
    package_version: str,
    package_path: Path | None,
) -> str:
    dependency: str
    if package_path is None:
        dependency = f'"={package_version}"'
    else:
        escaped_path = (
            str(package_path.resolve()).replace("\\", "\\\\").replace('"', '\\"')
        )
        dependency = f'{{ version = "={package_version}", path = "{escaped_path}" }}'
    return (
        "[package]\n"
        'name = "durable-workflow-msrv-consumer"\n'
        'version = "0.0.0"\n'
        'edition = "2021"\n'
        f'rust-version = "{EXPECTED_MSRV}"\n\n'
        "[dependencies]\n"
        f"{package_name} = {dependency}\n"
    )


def verify_lockfile(
    lock_path: Path,
    package_name: str,
    package_version: str,
    expect_registry_source: bool,
) -> None:
    if not lock_path.is_file():
        raise VerificationError("fresh consumer build did not create Cargo.lock")
    lock = tomllib.loads(lock_path.read_text(encoding="utf-8"))
    packages = lock.get("package")
    if not isinstance(packages, list):
        raise VerificationError("fresh consumer Cargo.lock has no package records")
    matches = [
        package
        for package in packages
        if package.get("name") == package_name
        and package.get("version") == package_version
    ]
    if len(matches) != 1:
        raise VerificationError(
            f"fresh lockfile did not resolve exact {package_name} {package_version}"
        )
    source = matches[0].get("source")
    if expect_registry_source and not (
        isinstance(source, str) and source.startswith("registry+")
    ):
        raise VerificationError("published consumer did not resolve from a registry")
    if not expect_registry_source and source is not None:
        raise VerificationError(
            "packaged consumer did not resolve from the package path"
        )


def verify(manifest: Path, source: str) -> None:
    package_name, package_version, rust_version = package_identity(manifest)
    toolchain = require_msrv_toolchain()

    with tempfile.TemporaryDirectory(prefix="durable-workflow-msrv-") as temp_name:
        temp = Path(temp_name)
        package_path: Path | None = None
        if source == "package":
            metadata = cargo_metadata(manifest)
            target_directory = metadata.get("target_directory")
            if not isinstance(target_directory, str) or not target_directory:
                raise VerificationError(
                    "cargo metadata did not identify a target directory"
                )
            archive_path = (
                Path(target_directory)
                / "package"
                / f"{package_name}-{package_version}.crate"
            )
            package_path = extract_package(archive_path, temp / "package")

        consumer = temp / "consumer"
        (consumer / "src").mkdir(parents=True)
        (consumer / "Cargo.toml").write_text(
            render_consumer_manifest(package_name, package_version, package_path),
            encoding="utf-8",
        )
        (consumer / "src" / "main.rs").write_text("fn main() {}\n", encoding="utf-8")
        lock_path = consumer / "Cargo.lock"
        if lock_path.exists():
            raise VerificationError(
                "consumer directory unexpectedly contains Cargo.lock"
            )

        environment = os.environ.copy()
        environment["CARGO_TARGET_DIR"] = os.environ.get(
            "FRESH_CONSUMER_TARGET_DIR", str(temp / "target")
        )
        environment["CARGO_TERM_COLOR"] = "never"
        subprocess.run(
            ["cargo", "build", "--manifest-path", str(consumer / "Cargo.toml")],
            check=True,
            env=environment,
        )
        verify_lockfile(
            lock_path,
            package_name,
            package_version,
            expect_registry_source=source == "registry",
        )

    print(
        json.dumps(
            {
                "package": package_name,
                "version": package_version,
                "rust_version": rust_version,
                "source": source,
                "toolchain": toolchain,
                "fresh_lockfile": True,
                "outcome": "pass",
            },
            sort_keys=True,
        )
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "source",
        choices=("package", "registry"),
        help="verify the local package archive or the exact registry version",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=ROOT / "Cargo.toml",
        help="source manifest providing the exact package identity",
    )
    arguments = parser.parse_args()
    try:
        verify(arguments.manifest.resolve(), arguments.source)
    except (
        json.JSONDecodeError,
        OSError,
        subprocess.CalledProcessError,
        tarfile.TarError,
        tomllib.TOMLDecodeError,
        VerificationError,
    ) as error:
        print(f"fresh consumer verification failed: {error}", file=os.sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
