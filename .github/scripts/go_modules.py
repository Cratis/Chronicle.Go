#!/usr/bin/env python3
# Copyright (c) Cratis. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for full license information.

"""Read-only module policy and native CI matrix; never creates modules or publishes.

Adapted from Fundamentals.Go v0.2.0 (532d2181c610f67730133f61e768a943379da571),
.github/scripts/go_modules.py, including its corrected exact recipe replacement.
Chronicle adds runtime dependency isolation, explicit profiles and unpublished
source previews. See Documentation/module-policy.md for the policy boundary.
"""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

MODULE = "github.com/cratis/chronicle.go"
NUMBER = r"(?:0|[1-9][0-9]*)"
STABLE = rf"v[01]\.{NUMBER}\.{NUMBER}"
PSEUDO = (rf"v[01]\.{NUMBER}\.{NUMBER}-"
          r"(?:0\.|[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*\.0\.)?[0-9]{14}-[0-9a-f]{12}")
WINDOWS_RESERVED = {"con", "prn", "aux", "nul", *[f"com{i}" for i in range(1, 10)],
                    *[f"lpt{i}" for i in range(1, 10)]}
ROOT_KEYS = {"module", "go", "contracts", "kernelIntegration", "rootDependencies", "nested"}
NESTED_KEYS = {"dir", "kind", "publish", "tagPrefix", "contracts", "kernelIntegration"}


def run(*args, cwd, env=None):
    return subprocess.check_output(args, cwd=cwd, env=env, text=True, timeout=60, stderr=subprocess.PIPE)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON key: {key}")
        result[key] = value
    return result


def regular_file(root, relative):
    """Reject links, absent paths and nonportable casing before invoking Go."""
    path = root
    for part in relative.split("/"):
        if part not in {child.name for child in path.iterdir()}:
            raise ValueError(f"Missing or incorrectly cased path: {relative}")
        path /= part
        if path.is_symlink():
            raise ValueError(f"Symlinks are forbidden in module policy paths: {relative}")
    if not path.is_file() or path.stat().st_nlink != 1 or not path.resolve().is_relative_to(root):
        raise ValueError(f"Expected an independent regular file within the repository: {relative}")
    return path


def configuration(root):
    manifest = regular_file(root, ".github/go-modules.json")
    if manifest.stat().st_size > 64 * 1024:
        raise ValueError("Module policy exceeds 64 KiB")
    config = json.loads(manifest.read_text(encoding="utf-8"), object_pairs_hook=unique_object)
    if not isinstance(config, dict) or set(config) != ROOT_KEYS:
        raise ValueError("Expected exactly the documented root policy keys")
    if config["module"] != MODULE or config["go"] != "1.26":
        raise ValueError("Expected the canonical Chronicle root module and Go 1.26 baseline")
    if config["contracts"] is not True or config["kernelIntegration"] is not True:
        raise ValueError("Root contracts and kernelIntegration profiles must remain enabled")
    dependencies = config["rootDependencies"]
    if (not isinstance(dependencies, list)
            or any(not isinstance(item, str) or not re.fullmatch(r"[a-z0-9][a-z0-9.-]*\.[a-z0-9]+(?:/[a-z0-9][a-z0-9._-]*)+", item)
                   or item.startswith(MODULE) for item in dependencies)
            or len(set(dependencies)) != len(dependencies)):
        raise ValueError("rootDependencies must be unique canonical external module paths")
    entries = config["nested"]
    if not isinstance(entries, list):
        raise ValueError("nested must be a list")
    directories = []
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != NESTED_KEYS:
            raise ValueError("Expected exactly the documented nested policy keys")
        if any(type(entry[key]) is not bool for key in ("publish", "contracts", "kernelIntegration")):
            raise ValueError("Module publication and profile flags must be booleans")
        directory, kind = entry["dir"], entry["kind"]
        if not isinstance(directory, str) or not isinstance(kind, str) or not (
            (kind == "tool" and directory == "tools")
            or (kind == "integration" and re.fullmatch(r"integrations/[a-z][a-z0-9_-]*", directory)
                and directory.split("/")[-1] not in WINDOWS_RESERVED
                and not re.fullmatch(r"v[0-9]+", directory.split("/")[-1]))
            or (kind == "recipe" and directory == "recipes")
        ):
            raise ValueError("Expected tools, integrations/<lowercase-name>, or recipes with matching kind")
        if entry["tagPrefix"] != directory + "/v":
            raise ValueError("tagPrefix must be the canonical directory/v namespace (v0/v1 only)")
        if kind == "recipe" and entry["publish"]:
            raise ValueError("Recipes are always unpublished")
        if entry["contracts"] or entry["kernelIntegration"]:
            raise ValueError("Only the root supports contracts and kernelIntegration profiles")
        if directory in directories:
            raise ValueError("Duplicate nested module directory")
        directories.append(directory)
    return config


def visible_files(root):
    # Tracked files remain visible even inside ignored directories. Untracked work,
    # build outputs and downloaded dependencies are excluded only by Git ignores.
    paths = set(filter(None, run("git", "ls-files", "--cached", "--others", "--exclude-standard", "-z",
                                cwd=root).split("\0")))
    if len({path.casefold() for path in paths}) != len(paths):
        raise ValueError("Paths differing only by case are not portable")
    if any("\\" in path for path in paths):
        raise ValueError("Backslash paths are not portable")
    if (root / "go.work").exists() or (root / "go.work").is_symlink() or any(
        Path(path).name.casefold() in {"go.work", "go.work.sum"} for path in paths
    ):
        raise ValueError("Repository workspaces are forbidden; use GOWORK=off")
    return paths


def go_manifest(root, directory):
    relative = "go.mod" if directory == "." else directory + "/go.mod"
    manifest = regular_file(root, relative)
    if manifest.stat().st_size > 1024 * 1024:
        raise ValueError(f"{directory}: go.mod exceeds 1 MiB")
    checksum = manifest.with_name("go.sum")
    if checksum.exists() or checksum.is_symlink():
        regular_file(root, "go.sum" if directory == "." else directory + "/go.sum")
    # go mod edit -json parses without editing or loading the dependency graph.
    # Ambient flags/toolchain/workspace overrides must not turn policy into I/O.
    env = {**os.environ, "GOWORK": "off", "GOTOOLCHAIN": "local", "GOFLAGS": "",
           "GOPROXY": "off", "GOSUMDB": "off", "GONOPROXY": "none"}
    return json.loads(run("go", "mod", "edit", "-json", cwd=manifest.parent, env=env))


def layout(root):
    root = root.resolve()
    config = configuration(root)
    entries = [{"dir": ".", "kind": "root", "publish": True, "tagPrefix": "v",
                "contracts": config["contracts"], "kernelIntegration": config["kernelIntegration"]},
               *config["nested"]]
    files = visible_files(root)
    expected = {"go.mod" if entry["dir"] == "." else entry["dir"] + "/go.mod" for entry in entries}
    actual = {path for path in files if Path(path).name.casefold() == "go.mod"}
    if actual != expected:
        raise ValueError(f"Module allow-list mismatch: unexpected={sorted(actual - expected)}, missing={sorted(expected - actual)}")
    dependencies = set()
    for entry in entries:
        directory = entry["dir"]
        module = go_manifest(root, directory)
        expected_path = MODULE + ("" if directory == "." else "/" + directory)
        if (module.get("Module") or {}).get("Path") != expected_path:
            raise ValueError(f"{directory}: expected module {expected_path}")
        if not re.fullmatch(r"1\.26(?:\.(?:0|[1-9][0-9]*))?", module.get("Go") or ""):
            raise ValueError(f"{directory}: require the Go 1.26 baseline")
        if module.get("Toolchain") and not re.fullmatch(r"go1\.26\.(?:0|[1-9][0-9]*)", module["Toolchain"]):
            raise ValueError(f"{directory}: toolchain must not raise the Go 1.26 baseline")
        replacements = module.get("Replace") or []
        recipe = entry["kind"] == "recipe" and not entry["publish"]
        if replacements and not recipe:
            raise ValueError(f"{directory}: replace directives are forbidden")
        local_root = "/".join(".." for _ in directory.split("/"))
        for replacement in replacements:
            old, new = replacement["Old"], replacement["New"]
            if (old["Path"] != MODULE or old.get("Version") or new.get("Version")
                    or new["Path"] not in {local_root, local_root + "/"}
                    or (root / directory / new["Path"]).resolve() != root):
                raise ValueError(f"{directory}: recipes may only replace the unversioned canonical root with its relative repository root")
        if len(replacements) > 1:
            raise ValueError(f"{directory}: duplicate local root replacement")
        requirements = module.get("Require") or []
        paths = [requirement["Path"] for requirement in requirements]
        if len(set(paths)) != len(paths):
            raise ValueError(f"{directory}: duplicate module requirement")
        if directory == ".":
            if module.get("Tool"):
                raise ValueError("Root runtime dependency isolation: tool directives belong in a nested module")
            unknown = set(paths) - set(config["rootDependencies"])
            if unknown:
                raise ValueError(f"Root runtime dependency isolation: unexpected={sorted(unknown)}")
            continue
        versions = [requirement["Version"] for requirement in requirements if requirement["Path"] == MODULE]
        if len(versions) != 1:
            raise ValueError(f"{directory}: require exactly one canonical root dependency")
        version = versions[0]
        if entry["publish"]:
            if not re.fullmatch(STABLE, version):
                raise ValueError(f"{directory}: publication requires a stable v0/v1 root tag, not a pseudo-version")
        elif replacements:
            if version != "v0.0.0":
                raise ValueError(f"{directory}: local root recipe requires placeholder v0.0.0")
            continue
        elif not re.fullmatch(rf"(?:{STABLE}|{PSEUDO})", version):
            raise ValueError(f"{directory}: preview requires a stable tag or fetchable pushed root pseudo-version")
        dependencies.add(MODULE + "@" + version)
    return {"module": entries}, sorted(dependencies)


def verify_dependencies(dependencies):
    if not dependencies:
        print("No nested root dependencies to fetch; root CI does not require a root release")
        return
    # A fresh cache outside the checkout proves proxy availability, not a local
    # workspace/cache substitute. Preview pseudo-versions are never release proof.
    with tempfile.TemporaryDirectory(prefix="chronicle-module-dependencies-") as temporary:
        env = {**os.environ, "GOWORK": "off", "GOTOOLCHAIN": "local", "GOFLAGS": "",
               "GOPROXY": "https://proxy.golang.org", "GOSUMDB": "sum.golang.org",
               "GOPRIVATE": "", "GONOPROXY": "", "GONOSUMDB": "",
               "GOMODCACHE": str(Path(temporary) / "cache")}
        for dependency in dependencies:
            result = json.loads(run("go", "mod", "download", "-json", dependency, cwd=temporary, env=env))
            path, version = dependency.split("@")
            if (result.get("Error") or result.get("Path") != path or result.get("Version") != version
                    or not result.get("Sum") or not result.get("GoModSum")):
                raise ValueError(f"Could not verify public root dependency {dependency}")
            print(f"Verified public root dependency: {dependency}")


def check_format(root, directory):
    matrix, _ = layout(root)
    directories = {entry["dir"] for entry in matrix["module"]}
    if directory not in directories:
        raise ValueError("Select an allow-listed module for formatting")
    files = visible_files(root)
    selected = []
    for path in sorted(files):
        if not path.endswith(".go"):
            continue
        owner = next((item for item in directories if item != "." and path.startswith(item + "/")), ".")
        if owner == directory:
            regular_file(root, path)
            selected.append(path)
    # No shell/xargs, and each producer failure propagates. Chunk argv for Windows.
    for start in range(0, len(selected), 50):
        unformatted = run("gofmt", "-l", *selected[start:start + 50], cwd=root)
        if unformatted:
            raise ValueError("Run gofmt on:\n" + unformatted)


def check_tidy(root, directory):
    matrix, _ = layout(root)
    if directory not in {entry["dir"] for entry in matrix["module"]}:
        raise ValueError("Select an allow-listed module for tidy verification")
    # go mod tidy -diff reports a nonzero exit without changing source manifests.
    subprocess.run(["go", "mod", "tidy", "-diff"], cwd=root / directory, check=True, timeout=180,
                   env={**os.environ, "GOWORK": "off", "GOTOOLCHAIN": "local"})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["matrix", "dependencies", "gofmt", "tidy"])
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--module", default=".")
    args = parser.parse_args()
    root = args.root.resolve()
    if args.command == "gofmt":
        check_format(root, args.module)
    elif args.command == "tidy":
        check_tidy(root, args.module)
    else:
        matrix, dependencies = layout(root)
        if args.command == "matrix":
            print(json.dumps(matrix, separators=(",", ":")))
        else:
            verify_dependencies(dependencies)


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as error:
        print(error.stderr or str(error), file=sys.stderr)
        sys.exit(error.returncode if error.returncode > 0 else 1)
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        sys.exit(str(error))
