# Copyright (c) Cratis. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for full license information.

"""Offline temporary Git fixtures use the real Go parser, never dependency loading."""

import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import go_modules as policy

SCRIPT = Path(__file__).with_name("go_modules.py").resolve()
REPO = SCRIPT.parents[2]
MODULE = policy.MODULE
PSEUDO = "v0.0.0-20261003005349-c8bd4b7d0830"


class ModulePolicy(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="chronicle-module-policy-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        subprocess.run(["git", "init", "-q", str(self.root)], check=True, timeout=10)
        self.config = json.loads((REPO / ".github/go-modules.json").read_text())
        self.config["nested"] = []
        self.write("go.mod", f"module {MODULE}\n\ngo 1.26\n")
        self.configure()

    def write(self, name, text):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    def configure(self):
        self.write(".github/go-modules.json", json.dumps(self.config))

    def nested(self, directory="tools", publish=False, version=PSEUDO, extra="", kind=None):
        entry = {"dir": directory, "kind": kind or ("tool" if directory == "tools" else
                  "recipe" if directory == "recipes" else "integration"), "publish": publish,
                 "tagPrefix": directory + "/v", "contracts": False, "kernelIntegration": False}
        self.config["nested"].append(entry)
        self.configure()
        self.write(directory + "/go.mod", f"module {MODULE}/{directory}\ngo 1.26\nrequire {MODULE} {version}\n{extra}")
        return entry

    def matrix(self):
        return policy.layout(self.root)[0]

    def reject(self, message):
        with self.assertRaises((ValueError, OSError, subprocess.SubprocessError)) as caught:
            self.matrix()
        self.assertRegex(str(caught.exception) + (getattr(caught.exception, "stderr", None) or ""), message)

    def test_root_only_cli_emits_json_booleans_and_has_no_release_dependency(self):
        result = subprocess.run([sys.executable, "-B", str(SCRIPT), "matrix", "--root", str(self.root)],
                                check=True, capture_output=True, text=True, timeout=15)
        matrix = json.loads(result.stdout)
        self.assertEqual(matrix["module"], [{"dir": ".", "kind": "root", "publish": True,
                          "tagPrefix": "v", "contracts": True, "kernelIntegration": True}])
        self.assertEqual(policy.layout(self.root)[1], [])

    def test_malformed_json_is_rejected(self):
        self.write(".github/go-modules.json", "{")
        self.reject("Expecting")

    def test_duplicate_json_keys_are_rejected(self):
        text = json.dumps(self.config).replace('"nested": []', '"nested": [], "nested": []')
        self.write(".github/go-modules.json", text)
        self.reject("Duplicate JSON key")

    def test_unknown_root_keys_and_wrong_json_shapes_are_rejected(self):
        for config in [[], None, {**self.config, "command": "echo unsafe"}, {"module": MODULE}]:
            with self.subTest(config=config):
                self.write(".github/go-modules.json", json.dumps(config))
                self.reject("root policy keys")

    def test_root_module_and_minimum_version_policy_are_exact(self):
        for key, value in [("module", "github.com/Cratis/Chronicle.Go"), ("module", MODULE + "/v2"),
                           ("go", "1.27"), ("go", 1.26)]:
            with self.subTest(key=key, value=value):
                config = {**self.config, key: value}
                self.write(".github/go-modules.json", json.dumps(config))
                self.reject("canonical Chronicle")

    def test_root_service_profiles_cannot_be_disabled(self):
        for key in ["contracts", "kernelIntegration"]:
            for value in [False, 1, "true"]:
                with self.subTest(key=key, value=value):
                    self.write(".github/go-modules.json", json.dumps({**self.config, key: value}))
                    self.reject("profiles must remain enabled")

    def test_dependency_policy_requires_unique_canonical_external_paths(self):
        for dependencies in [None, [1], [MODULE + "/tools"], ["EXAMPLE.com/x"], ["../x"],
                             ["example.com/x", "example.com/x"]]:
            with self.subTest(dependencies=dependencies):
                self.write(".github/go-modules.json", json.dumps({**self.config, "rootDependencies": dependencies}))
                self.reject("rootDependencies")

    def test_unknown_nested_keys_and_missing_flags_fail(self):
        entry = self.nested()
        for changed in [{**entry, "command": "echo unsafe"}, {"dir": "tools", "publish": False}]:
            with self.subTest(entry=changed):
                self.config["nested"] = [changed]
                self.configure()
                self.reject("nested policy keys")

    def test_nested_publication_and_profile_flags_are_strict_booleans(self):
        entry = self.nested()
        for key in ["publish", "contracts", "kernelIntegration"]:
            for value in [0, "false", None]:
                with self.subTest(key=key, value=value):
                    self.config["nested"] = [{**entry, key: value}]
                    self.configure()
                    self.reject("must be booleans")

    def test_nested_list_and_entry_types_are_checked(self):
        for entries in [None, {}, [None], ["tools"]]:
            with self.subTest(entries=entries):
                self.config["nested"] = entries
                self.configure()
                self.reject("nested must be a list|nested policy keys")

    def test_missing_real_module_path_is_rejected(self):
        self.nested()
        (self.root / "tools/go.mod").unlink()
        self.reject("missing=.*tools/go.mod")

    def test_unlisted_tracked_or_untracked_module_is_rejected(self):
        self.write("other/go.mod", "module example.com/other\ngo 1.26\n")
        self.reject("unexpected=.*other/go.mod")
        subprocess.run(["git", "add", "other/go.mod"], cwd=self.root, check=True, timeout=10)
        self.write(".gitignore", "other/\n")
        self.reject("unexpected=.*other/go.mod")

    def test_ignored_work_build_and_downloads_are_excluded_not_whole_source_scopes(self):
        self.write(".gitignore", ".ai-work/\nbuild/\nvendor/\n")
        for name in [".ai-work/cache/go.mod", "build/download/go.mod", "vendor/example.com/x/go.mod"]:
            self.write(name, "invalid ignored fixture")
        self.assertEqual(len(self.matrix()["module"]), 1)
        self.write("examples/real/go.mod", "module example.com/real\ngo 1.26\n")
        self.reject("unexpected=.*examples/real/go.mod")

    def test_deleted_tracked_module_is_not_silently_ignored(self):
        subprocess.run(["git", "add", "go.mod"], cwd=self.root, check=True, timeout=10)
        (self.root / "go.mod").unlink()
        self.reject("Missing")

    def test_real_unpublished_tool_preview_is_gated_before_root_release(self):
        self.nested()
        matrix, dependencies = policy.layout(self.root)
        self.assertEqual([entry["dir"] for entry in matrix["module"]], [".", "tools"])
        self.assertFalse(matrix["module"][1]["publish"])
        self.assertEqual(dependencies, [MODULE + "@" + PSEUDO])

    def test_preview_supports_canonical_pseudo_versions_after_stable_or_prerelease_tags(self):
        self.nested()
        versions = [PSEUDO, "v0.1.1-0.20261003005349-c8bd4b7d0830",
                    "v1.0.0-rc.1.0.20261003005349-c8bd4b7d0830"]
        for version in versions:
            with self.subTest(version=version):
                self.write("tools/go.mod", f"module {MODULE}/tools\ngo 1.26\nrequire {MODULE} {version}\n")
                self.assertEqual(policy.layout(self.root)[1], [MODULE + "@" + version])
        self.write("tools/go.mod", f"module {MODULE}/tools\ngo 1.26\nrequire {MODULE} v0.1.1-0-20261003005349-c8bd4b7d0830\n")
        self.reject("preview requires")

    def test_publishable_tools_and_integrations_have_independent_namespaces(self):
        self.nested(publish=True, version="v0.1.0")
        self.nested("integrations/example", publish=True, version="v0.1.0")
        matrix, dependencies = policy.layout(self.root)
        self.assertEqual([entry["tagPrefix"] for entry in matrix["module"]],
                         ["v", "tools/v", "integrations/example/v"])
        self.assertEqual(dependencies, [MODULE + "@v0.1.0"])

    def test_wrong_tag_prefixes_and_version_suffixes_are_rejected(self):
        entry = self.nested()
        for prefix in ["v", "Tools/v", "tools/v2", "tools/v0.1.0", "integrations/tool/v"]:
            with self.subTest(prefix=prefix):
                self.config["nested"] = [{**entry, "tagPrefix": prefix}]
                self.configure()
                self.reject("tagPrefix")

    def test_windows_separators_case_and_escape_directories_are_rejected(self):
        entry = self.nested()
        for directory in ["Tools", "../tools", "/tools", "C:/tools", "tools\\", "tools/../tools",
                          "integrations/CON", "integrations/x/y", "tools/v2", "./tools"]:
            with self.subTest(directory=directory):
                self.config["nested"] = [{**entry, "dir": directory, "tagPrefix": directory + "/v"}]
                self.configure()
                self.reject("matching kind")

    def test_reserved_integration_and_major_version_names_are_rejected_on_all_platforms(self):
        entry = self.nested()
        for name in ["con", "nul", "com1", "lpt9", "v1", "v2", "v10"]:
            with self.subTest(name=name):
                self.config["nested"] = [{**entry, "dir": "integrations/" + name,
                    "kind": "integration", "tagPrefix": "integrations/" + name + "/v"}]
                self.configure()
                self.reject("matching kind")

    def test_unknown_or_contradictory_kinds_are_rejected(self):
        entry = self.nested()
        for kind in ["adapter", "recipe", "integration", None, {}]:
            with self.subTest(kind=kind):
                self.config["nested"] = [{**entry, "kind": kind}]
                self.configure()
                self.reject("matching kind")

    def test_duplicate_module_directories_are_rejected(self):
        entry = self.nested()
        self.config["nested"].append(copy.deepcopy(entry))
        self.configure()
        self.reject("Duplicate nested")

    def test_wrong_or_duplicate_module_identities_are_rejected(self):
        self.nested()
        for name in [MODULE, MODULE + "/TOOLS", MODULE + "/tools/v2", "example.com/tools"]:
            with self.subTest(name=name):
                self.write("tools/go.mod", f"module {name}\ngo 1.26\nrequire {MODULE} {PSEUDO}\n")
                self.reject("expected module")

    def test_nested_service_profiles_fail_instead_of_skipping_named_tests(self):
        entry = self.nested()
        for key in ["contracts", "kernelIntegration"]:
            with self.subTest(key=key):
                self.config["nested"] = [{**entry, key: True}]
                self.configure()
                self.reject("Only the root supports")

    def test_root_workspace_is_rejected_even_if_ignored(self):
        self.write(".gitignore", "go.work\n")
        self.write("go.work", "go 1.26\nuse .\n")
        self.reject("workspaces are forbidden")

    def test_visible_nested_workspace_is_rejected(self):
        self.write("examples/go.work", "go 1.26\n")
        self.reject("workspaces are forbidden")

    def test_symlinked_module_manifest_or_ancestor_is_rejected(self):
        self.nested()
        original = self.root / "tools/go.mod"
        original.rename(self.root / "saved.mod")
        original.symlink_to(self.root / "saved.mod")
        self.reject("Symlinks")
        original.unlink()
        self.write(".gitignore", "saved.mod\n")
        (self.root / "tools").rmdir()
        outside = tempfile.TemporaryDirectory(prefix="chronicle-outside-module-")
        self.addCleanup(outside.cleanup)
        Path(outside.name, "go.mod").write_text(f"module {MODULE}/tools\ngo 1.26\n")
        (self.root / "tools").symlink_to(outside.name, target_is_directory=True)
        # Git never traverses the linked directory, so layout rejects it as missing.
        self.reject("missing=.*tools/go.mod")

    def test_policy_symlink_and_hardlink_are_rejected(self):
        manifest = self.root / ".github/go-modules.json"
        manifest.rename(self.root / "saved.json")
        manifest.symlink_to(self.root / "saved.json")
        self.reject("Symlinks")
        manifest.unlink()
        os.link(self.root / "saved.json", manifest)
        self.reject("independent regular file")

    def test_module_and_checksum_hardlinks_are_rejected(self):
        os.link(self.root / "go.mod", self.root / "saved.mod")
        self.reject("independent regular file")
        (self.root / "saved.mod").unlink()
        self.write("go.sum", "")
        os.link(self.root / "go.sum", self.root / "saved.sum")
        self.reject("independent regular file")

    def test_casefold_collisions_in_git_index_are_rejected(self):
        oid = subprocess.check_output(["git", "hash-object", "-w", "go.mod"], cwd=self.root, text=True).strip()
        for name in ["Name.go", "name.go"]:
            subprocess.run(["git", "update-index", "--add", "--cacheinfo", f"100644,{oid},{name}"],
                           cwd=self.root, check=True, timeout=10)
        self.reject("differing only by case")

    def test_go_mod_filename_case_is_rejected(self):
        self.write("other/Go.Mod", "module example.com/x\ngo 1.26\n")
        self.reject("unexpected=.*Go.Mod")

    def test_root_and_publishable_replacements_are_always_rejected(self):
        for directive in ["replace example.com/x => example.com/y v1.0.0\n",
                          "replace (\nexample.com/x => ../\n)\n"]:
            with self.subTest(directive=directive):
                self.write("go.mod", f"module {MODULE}\ngo 1.26\n{directive}")
                self.reject("replace directives are forbidden")
        self.write("go.mod", f"module {MODULE}\ngo 1.26\n")
        self.nested(publish=True, version="v0.1.0", extra=f"replace {MODULE} => ../\n")
        self.reject("replace directives are forbidden")

    def test_preview_tools_cannot_use_recipe_replacement_exception(self):
        self.nested(extra=f"replace {MODULE} => ../\n")
        self.reject("replace directives are forbidden")

    def test_recipe_exact_canonical_local_root_replacement_is_accepted(self):
        self.nested("recipes", version="v0.0.0")
        for target in ["..", "../"]:
            with self.subTest(target=target):
                self.write("recipes/go.mod", f"module {MODULE}/recipes\ngo 1.26\nrequire {MODULE} v0.0.0\nreplace (\n{MODULE} => {target}\n)\n")
                self.assertEqual(policy.layout(self.root)[1], [])
                self.assertFalse(self.matrix()["module"][1]["publish"])

    def test_recipe_wrong_remote_versioned_third_party_replacements_are_rejected(self):
        self.nested("recipes", version="v0.0.0")
        for directive in ["example.com/x => ../", "example.com/x => example.com/y v1.0.0",
                          f"{MODULE} => example.com/y v1.0.0", f"{MODULE} => {MODULE} v0.1.0",
                          f"{MODULE} v0.0.0 => ../", f"{MODULE} => ../\nreplace example.com/x => ../"]:
            with self.subTest(directive=directive):
                self.write("recipes/go.mod", f"module {MODULE}/recipes\ngo 1.26\nrequire {MODULE} v0.0.0\nreplace {directive}\n")
                self.reject("relative repository root")

    def test_recipe_alternate_absent_escaping_and_windows_targets_are_rejected(self):
        self.nested("recipes", version="v0.0.0")
        for target in ["./", "../missing", "../../", "../recipes/../", str(self.root),
                       "C:/checkout", "//server/share", "..\\"]:
            with self.subTest(target=target):
                self.write("recipes/go.mod", f"module {MODULE}/recipes\ngo 1.26\nrequire {MODULE} v0.0.0\nreplace {MODULE} => {json.dumps(target)}\n")
                self.reject("relative repository root|Windows path")

    def test_recipes_cannot_be_published(self):
        self.nested("recipes", publish=True, version="v0.1.0")
        self.reject("Recipes are always unpublished")

    def test_local_recipe_placeholder_version_is_exact(self):
        self.nested("recipes", version="v0.1.0", extra=f"replace {MODULE} => ../\n")
        self.reject("placeholder v0.0.0")

    def test_publishable_pseudo_prerelease_and_missing_root_dependencies_are_rejected(self):
        self.nested(publish=True, version="v0.1.0")
        for version in [PSEUDO, "v1.0.0-rc.1"]:
            with self.subTest(version=version):
                self.write("tools/go.mod", f"module {MODULE}/tools\ngo 1.26\nrequire {MODULE} {version}\n")
                self.reject("publication requires a stable")
        self.write("tools/go.mod", f"module {MODULE}/tools\ngo 1.26\n")
        self.reject("exactly one canonical root")

    def test_preview_without_replace_requires_a_root_dependency(self):
        self.nested()
        self.write("tools/go.mod", f"module {MODULE}/tools\ngo 1.26\n")
        self.reject("exactly one canonical root")

    def test_all_modules_obey_go_126_and_toolchain_local_baseline(self):
        for version in ["1.25", "1.27", "1.26rc1", ""]:
            with self.subTest(version=version):
                self.write("go.mod", f"module {MODULE}\n" + (f"go {version}\n" if version else ""))
                self.reject("baseline|invalid go version")
        self.write("go.mod", f"module {MODULE}\ngo 1.26\ntoolchain go1.27.1\n")
        self.reject("toolchain must not raise")
        self.write("go.mod", f"module {MODULE}\ngo 1.26.3\n")
        self.assertEqual(len(self.matrix()["module"]), 1)

    def test_oversized_policy_and_go_manifests_are_rejected_before_parsing(self):
        self.write("go.mod", " " * (1024 * 1024 + 1))
        self.reject("go.mod exceeds")
        self.write(".github/go-modules.json", " " * (64 * 1024 + 1))
        self.reject("policy exceeds")

    def test_root_tool_directives_do_not_evade_dependency_isolation(self):
        self.write("go.mod", f"module {MODULE}\ngo 1.26\ntool golang.org/x/tools/cmd/stringer\n")
        self.reject("tool directives belong in a nested module")

    def test_real_go_parser_rejects_invalid_and_unknown_mod_directives(self):
        for extra in ["require (\n", "invented directive\n", "go 1.27\n"]:
            with self.subTest(extra=extra):
                self.write("go.mod", f"module {MODULE}\ngo 1.26\n{extra}")
                with self.assertRaises(subprocess.CalledProcessError):
                    self.matrix()

    def test_root_runtime_dependency_leakage_including_test_only_deps_is_rejected(self):
        for dependency in ["golang.org/x/tools", "go.opentelemetry.io/otel", "github.com/docker/docker",
                           "go.uber.org/fx", MODULE + "/tools", "example.com/broker"]:
            with self.subTest(dependency=dependency):
                self.write("go.mod", f"module {MODULE}\ngo 1.26\nrequire {dependency} v0.1.0 // indirect\n")
                self.reject("Root runtime dependency isolation")

    def test_current_runtime_dependency_manifest_is_accepted(self):
        self.write("go.mod", (REPO / "go.mod").read_text())
        self.assertEqual(len(self.matrix()["module"]), 1)

    def test_go_parser_does_not_write_manifests_or_download(self):
        before = (self.root / "go.mod").read_bytes()
        with patch.dict(os.environ, {"GOFLAGS": "-mod=mod", "GOTOOLCHAIN": "auto", "GOWORK": "/missing/work"}):
            self.matrix()
        self.assertEqual((self.root / "go.mod").read_bytes(), before)
        self.assertFalse((self.root / "go.sum").exists())

    def test_duplicate_root_requirements_fail(self):
        self.nested()
        self.write("tools/go.mod", f"module {MODULE}/tools\ngo 1.26\nrequire (\n{MODULE} {PSEUDO}\n{MODULE} v0.1.0\n)\n")
        self.reject("duplicate module requirement")

    def test_cli_keeps_native_parser_failure_exit_and_diagnostics(self):
        self.write("bin/go", '#!/usr/bin/env python3\nimport sys\nprint("native parser failure", file=sys.stderr)\nsys.exit(37)\n')
        (self.root / "bin/go").chmod(0o755)
        result = subprocess.run([sys.executable, "-B", str(SCRIPT), "matrix", "--root", str(self.root)],
                                capture_output=True, text=True, timeout=15,
                                env={**os.environ, "PATH": str(self.root / "bin") + os.pathsep + os.environ["PATH"]})
        self.assertEqual(result.returncode, 37)
        self.assertIn("native parser failure", result.stderr)
        self.assertEqual(result.stdout, "")

    def test_dependency_verification_propagates_producer_failure(self):
        failure = subprocess.CalledProcessError(17, ["go", "mod", "download"])
        with patch.object(policy, "run", side_effect=failure):
            with self.assertRaises(subprocess.CalledProcessError) as caught:
                policy.verify_dependencies([MODULE + "@" + PSEUDO])
        self.assertEqual(caught.exception.returncode, 17)

    def test_dependency_verification_rejects_error_or_identity_mismatch(self):
        for result in [{"Error": "missing"}, {"Path": MODULE, "Version": "v0.1.0"},
                       {"Path": "example.com/wrong", "Version": PSEUDO, "Sum": "x", "GoModSum": "x"}]:
            with self.subTest(result=result), patch.object(policy, "run", return_value=json.dumps(result)):
                with self.assertRaisesRegex(ValueError, "Could not verify"):
                    policy.verify_dependencies([MODULE + "@" + PSEUDO])

    def test_dependency_verification_uses_fresh_public_cache_without_checkout(self):
        result = {"Path": MODULE, "Version": PSEUDO, "Sum": "h1:x", "GoModSum": "h1:y"}
        with patch.object(policy, "run", return_value=json.dumps(result)) as run:
            policy.verify_dependencies([MODULE + "@" + PSEUDO])
            kwargs = run.call_args.kwargs
            self.assertNotEqual(Path(kwargs["cwd"]), self.root)
            self.assertEqual(kwargs["env"]["GOWORK"], "off")
            self.assertEqual(kwargs["env"]["GOTOOLCHAIN"], "local")
            self.assertEqual(kwargs["env"]["GOPROXY"], "https://proxy.golang.org")
            self.assertTrue(kwargs["env"]["GOMODCACHE"].startswith(kwargs["cwd"]))

    def test_root_dependency_verification_never_requests_a_release(self):
        with patch.object(policy, "run") as run:
            policy.verify_dependencies([])
            run.assert_not_called()

    def test_gofmt_is_per_module_and_producer_failures_propagate(self):
        self.nested()
        self.write("good.go", "package fixture\n")
        self.write("tools/bad.go", "package fixture\nvar x=1\n")
        policy.check_format(self.root, ".")
        with self.assertRaisesRegex(ValueError, "Run gofmt"):
            policy.check_format(self.root, "tools")
        failure = subprocess.CalledProcessError(19, ["gofmt"])
        original = policy.run
        def run(*args, **kwargs):
            if args[0] == "gofmt":
                raise failure
            return original(*args, **kwargs)
        with patch.object(policy, "run", side_effect=run):
            with self.assertRaises(subprocess.CalledProcessError) as caught:
                policy.check_format(self.root, ".")
        self.assertEqual(caught.exception.returncode, 19)

    def test_format_and_tidy_reject_unknown_modules(self):
        for check in [policy.check_format, policy.check_tidy]:
            with self.subTest(check=check), self.assertRaisesRegex(ValueError, "allow-listed"):
                check(self.root, "../other")

    def test_tidy_is_read_only_and_producer_failures_propagate(self):
        before = (self.root / "go.mod").read_bytes()
        failure = subprocess.CalledProcessError(23, ["go", "mod", "tidy", "-diff"])
        with patch.object(policy, "layout", return_value=({"module": [{"dir": "."}]}, [])), \
                patch.object(policy.subprocess, "run", side_effect=failure) as run:
            with self.assertRaises(subprocess.CalledProcessError) as caught:
                policy.check_tidy(self.root, ".")
            self.assertEqual(run.call_args.args[0], ["go", "mod", "tidy", "-diff"])
            self.assertTrue(run.call_args.kwargs["check"])
        self.assertEqual(caught.exception.returncode, 23)
        self.assertEqual((self.root / "go.mod").read_bytes(), before)


class WorkflowPolicy(unittest.TestCase):
    def setUp(self):
        self.workflow = (REPO / ".github/workflows/build.yml").read_text()

    def test_validator_and_matrix_producer_fail_closed(self):
        self.assertIn("set -euo pipefail\n          matrix=$(python3 -B .github/scripts/go_modules.py matrix)", self.workflow)
        self.assertIn("run: python3 -B .github/scripts/go_modules.py dependencies", self.workflow)
        self.assertNotIn("|| true", self.workflow)
        self.assertNotIn("continue-on-error", self.workflow)
        self.assertNotIn("for module", self.workflow)

    def test_matrix_output_is_not_written_after_native_producer_failure(self):
        snippet = self.workflow.split("          set -euo pipefail\n", 1)[1].split("      - name:", 1)[0]
        snippet = "set -euo pipefail\n" + "\n".join(line.strip() for line in snippet.splitlines())
        with tempfile.TemporaryDirectory(prefix="chronicle-matrix-failure-") as temporary:
            output = Path(temporary) / "output"
            result = subprocess.run(["bash", "-c", "python3() { return 17; }\n" + snippet],
                                    capture_output=True, text=True, timeout=10,
                                    env={**os.environ, "GITHUB_OUTPUT": str(output)})
            self.assertEqual(result.returncode, 17)
            self.assertFalse(output.exists())

    def test_each_module_lane_uses_dynamic_matrix_and_native_commands(self):
        for job in ["test", "race", "lint", "vulnerabilities"]:
            section = self.workflow.split("\n  " + job + ":\n", 1)[1].split("\n\n  ", 1)[0]
            self.assertIn("needs: modules", section)
            self.assertIn("fromJSON(needs.modules.outputs.matrix)", section)
            self.assertIn("working-directory: ${{ matrix.module.dir }}", section)
            self.assertIn("cache-dependency-path: ${{ matrix.module.dir }}/go.sum", section)
        for command in ["go mod download", "go mod verify", "go build ./...", "go vet ./...",
                        "go test -count=1 -timeout=2m ./...", "go test -race -count=1 -timeout=3m ./...",
                        "go mod tidy -diff", "govulncheck ./..."]:
            self.assertIn("run: " + command, self.workflow)

    def test_root_check_names_os_go_lanes_and_final_gate_stay_stable(self):
        for name in ["Test ({0}, Go {1})", "Race detector", "Go lint and module hygiene",
                     "Go vulnerability check", "Workflow lint", "Go gate"]:
            self.assertIn(name, self.workflow)
        for lane in ["{os: ubuntu-latest, go: '1.26.x'}", "{os: ubuntu-latest, go: '1.27.x'}",
                     "{os: macos-latest, go: '1.27.x'}", "{os: windows-latest, go: '1.27.x'}"]:
            self.assertIn(lane, self.workflow)
        self.assertIn("needs: [modules, test, race, lint, vulnerabilities, workflows, contracts, integration]", self.workflow)
        self.assertIn("jq -e 'all(.[]; . == \"success\")'", self.workflow)
        self.assertIn("GOTOOLCHAIN: local\n  GOWORK: 'off'", self.workflow)

    def test_root_contract_and_kernel_profiles_are_explicit_and_not_nested_matrix(self):
        self.assertIn("if: fromJSON(needs.modules.outputs.matrix).module[0].contracts", self.workflow)
        self.assertIn("if: fromJSON(needs.modules.outputs.matrix).module[0].kernelIntegration", self.workflow)
        integration = (REPO / ".github/workflows/integration.yml").read_text()
        self.assertIn("./internal/integration ./chronicletest/...", integration)
        self.assertNotIn("matrix", integration)
        self.assertFalse((REPO / ".github/workflows/publish-nested.yml").exists())


if __name__ == "__main__":
    unittest.main()
