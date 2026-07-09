#!/usr/bin/env python3

#
# affected_subprojects.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Determine which Gradle subprojects are affected by a pull request's changed files,
so that CI can skip expensive per-subproject test jobs for subprojects that couldn't
possibly have been affected.

A subproject is "affected" if it was changed directly, or if it (transitively) depends
on a subproject that was changed. The subproject-to-affected-subprojects mapping is not
computed here -- it's read from the output of the `printDependentSubprojects` Gradle task
(see gradle/root.gradle), which precomputes, for every subproject, the full transitive
impact set (itself plus everything that depends on it). Keeping the graph traversal in
Gradle means it can never drift out of sync with the real build.

Any change that isn't clearly confined to a single known subproject -- e.g. a change to
the root build files, the Gradle wrapper, shared CI actions, or a path this script
doesn't recognize -- is treated conservatively as affecting everything. The same applies
to any failure parsing the subproject dependency data or the changed-files list: fail
safe to "run everything", never to "run nothing".

Usage:
    # In CI: subproject dependency data and changed-files list are precomputed, and the CI-specific
    # matrix legs are known, so the plan also reports which of them need to run.
    python build/affected_subprojects.py subproject_deps.json \
        --changed-files-file changed_files.txt \
        --matrix-candidates '["fdb-extensions","fdb-record-layer-core","fdb-record-layer-lucene","yaml-tests"]'

    # Local dry run: diff against origin/main for changed files, using an already
    # generated subproject dependency graph, e.g. via
    # `./gradlew printDependentSubprojects -PsubprojectDeps.output=subproject_deps.json`.
    python build/affected_subprojects.py subproject_deps.json

The plan is written as JSON to --output (default: subproject_plan.json). A human-readable
Markdown rendering of the same plan -- suitable for piping straight into a GitHub Actions step
summary -- is printed to stdout.
"""

import argparse
import json
import os.path
import subprocess
import sys
from collections.abc import Iterable

# Paths that, if touched, mean we can't reason about affected subprojects from the
# dependency graph alone -- e.g. changes to the build itself, the CI plumbing that
# computes this very answer, or the Gradle wrapper. Treat any of these as "run
# everything". Entries are path prefixes (checked against '/'-joined repo-relative
# paths), so a trailing '/' matches a whole directory.
BUILD_AFFECTING_PREFIXES: list[str] = [
    'build.gradle',
    'settings.gradle',
    'gradle.properties',
    'gradle/',
    'gradlew',
    'gradlew.bat',
    '.github/workflows/pull_request.yml',
    'actions/',
    'build/',
]

# Default path for the --output plan file; a build artifact, so it's gitignored.
DEFAULT_OUTPUT_FILE = 'subproject_plan.json'


def parse_subproject_deps(text: str) -> dict[str, list[str]]:
    """
    Parse the output of the `printDependentSubprojects` Gradle task: a JSON object mapping
    each subproject name to the sorted list of subprojects affected by a change to it
    (itself plus every transitive dependent).
    """
    data = json.loads(text)
    if not isinstance(data, dict) or not data:
        raise ValueError('Subproject dependency data must be a non-empty JSON object')
    for subproject, affected in data.items():
        if not isinstance(subproject, str) or not isinstance(affected, list):
            raise ValueError(f'Malformed subproject dependency entry: {subproject!r}: {affected!r}')
    return data


def is_build_affecting(path: str) -> bool:
    """Whether a changed path means we should conservatively run everything."""
    return any(path == prefix or path.startswith(prefix)
               for prefix in BUILD_AFFECTING_PREFIXES)


def map_changed_file_to_subproject(path: str, known_subprojects: Iterable[str]) -> str | None:
    """
    Map a repo-relative changed file path to the subproject it belongs to, based on its
    top-level path component. Returns None if the path isn't under any known subproject.
    """
    head, top_level = path, ''
    while head:
        head, top_level = os.path.split(head)
    return top_level if top_level in known_subprojects else None


def compute_affected(changed_files: list[str], subproject_deps: dict[str, list[str]]) -> dict:
    """
    Decide which subprojects are affected by a set of changed files, given the precomputed
    subproject -> affected-subprojects mapping.

    Returns:
        dict with keys 'run_all' and 'affected'.
    """
    known_subprojects = set(subproject_deps)

    if any(is_build_affecting(path) for path in changed_files):
        # Modified file is part of the build. Run all tests to be conservative
        return {'run_all': True, 'affected': sorted(known_subprojects)}

    affected: set[str] = set()
    for path in changed_files:
        subproject = map_changed_file_to_subproject(path, known_subprojects)
        if subproject is not None:
            # Modified file is in a subproject. Register it and its downstream dependencies as affected.
            # If the file is not in a subproject (e.g., because it is docs or a readme), we ignore it.
            affected.update(subproject_deps[subproject])

    return {'run_all': False, 'affected': sorted(affected)}


def compute_matrix_plan(plan: dict, matrix_candidates: Iterable[str]) -> dict:
    """
    Layer CI matrix planning on top of a base plan from compute_affected(): which of the fixed
    set of subprojects with their own CI matrix leg need to run, and whether any other
    (non-matrix) subproject also needs testing via a combined job.

    Returns:
        plan, with 'matrix' and 'run_other_tests' keys added.
    """
    candidates = set(matrix_candidates)
    if plan['run_all']:
        matrix, run_other_tests = sorted(candidates), True
    else:
        affected = set(plan['affected'])
        matrix, run_other_tests = sorted(affected & candidates), bool(affected - candidates)
    return {**plan, 'matrix': matrix, 'run_other_tests': run_other_tests}


def render_markdown(plan: dict) -> str:
    """Render a plan as a human-readable Markdown summary, e.g. for a GitHub Actions step summary."""
    lines = [
        '### CI Plan',
        '',
        "Based on the set of changed files, the following test plan was made:",
        '',
        f"- All tests need to be run: `{str(plan['run_all']).lower()}`",
        f"- Affected subprojects: `{', '.join(plan['affected']) or '(none)'}`",
    ]
    if 'matrix' in plan:
        lines.append(f"- Subprojects to test in individual jobs: `{', '.join(plan['matrix']) or '(none)'}`")
        lines.append(f"- Remaining subprojects tested in a combined job: `{str(plan['run_other_tests']).lower()}`")
    return '\n'.join(lines) + '\n'


def nonempty_stripped_lines(lines: Iterable[str]) -> list[str]:
    """Strip and return all non-empty lines from an iterable of lines."""
    return list(filter(bool, map(str.strip, lines)))


def get_changed_files(changed_files_file: str | None, base_ref: str, head_ref: str,
                       root_dir: str) -> list[str]:
    """Get the list of changed repo-relative file paths, from a file or from git."""
    if changed_files_file:
        with open(changed_files_file, encoding='utf-8') as f:
            return nonempty_stripped_lines(f)
    result = subprocess.run(
        ['git', 'diff', '--name-only', f'{base_ref}...{head_ref}'],
        cwd=root_dir, capture_output=True, text=True, check=True)
    return nonempty_stripped_lines(result.stdout.splitlines())


def fail_safe_result(reason: str) -> dict:
    """A conservative 'run everything' result, used when we can't determine the graph."""
    print(f'::warning::{reason} -- defaulting to running everything', file=sys.stderr)
    return {'run_all': True, 'affected': []}


def determine_base_plan(args: argparse.Namespace) -> dict:
    """Compute the base plan (before any matrix-specific layering), failing safe on any error."""
    try:
        with open(args.subproject_deps_file, encoding='utf-8') as f:
            subproject_deps = parse_subproject_deps(f.read())
    except (OSError, ValueError, json.JSONDecodeError) as e:
        return fail_safe_result(f'Could not determine subproject dependency graph ({e})')

    try:
        changed_files = get_changed_files(
            args.changed_files_file, args.base_ref, args.head_ref, args.root_dir)
    except (OSError, subprocess.CalledProcessError) as e:
        return fail_safe_result(f'Could not determine changed files ({e})')

    return compute_affected(changed_files, subproject_deps)


def main(argv: list[str]) -> None:
    """Main entry point for the affected subprojects script."""
    parser = argparse.ArgumentParser(
        prog='affected_subprojects',
        description='Determine which Gradle subprojects are affected by a set of changed files'
    )
    parser.add_argument('subproject_deps_file',
                         help='Path to the output of the printDependentSubprojects Gradle task.')
    parser.add_argument('--changed-files-file',
                         help='Path to a newline-separated list of changed repo-relative paths. '
                              'If omitted, uses `git diff --name-only <base>...<head>`.')
    parser.add_argument('--base-ref', default='origin/main',
                         help='Base ref for the local dry-run git diff fallback (default: origin/main)')
    parser.add_argument('--head-ref', default='HEAD',
                         help='Head ref for the local dry-run git diff fallback (default: HEAD)')
    parser.add_argument('--root-dir', default='.',
                         help='Repository root directory (default: current directory)')
    parser.add_argument('--matrix-candidates',
                         help='JSON array of subprojects that have their own CI matrix leg. If given, the '
                              'plan additionally reports which of those need to run and whether any other '
                              'subproject needs a combined test job -- a CI-specific concern that local dry '
                              'runs can skip.')
    parser.add_argument('-o', '--output', default=DEFAULT_OUTPUT_FILE,
                         help=f'Path to write the plan as JSON (default: {DEFAULT_OUTPUT_FILE})')
    args = parser.parse_args(argv)

    plan = determine_base_plan(args)
    if args.matrix_candidates:
        plan = compute_matrix_plan(plan, json.loads(args.matrix_candidates))

    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(plan, f)
    print(render_markdown(plan), end='')


if __name__ == '__main__':
    main(sys.argv[1:])
