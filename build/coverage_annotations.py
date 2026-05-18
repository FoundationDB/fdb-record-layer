#!/usr/bin/env python3

#
# coverage_annotations.py
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
Parse a JaCoCo XML coverage report and emit GitHub Actions annotations
showing per-file coverage summaries for files changed by a pull request.

For each changed Java file, emits a single annotation with:
  - Overall file line coverage percentage
  - Line coverage percentage for the changed lines only

Usage:
    python build/coverage_annotations.py \
        --report .out/reports/jacoco/codeCoverageReport/codeCoverageReport.xml \
        --diff pr_diff.patch \
        [--source-prefix 'src/main/java/']

The script outputs GitHub Actions workflow commands (::notice) that
appear as inline annotations on the PR "Files changed" tab.
"""

import argparse
import os
import re
import sys
import xml.etree.ElementTree as ET

DEFAULT_SOURCE_PREFIXES = ['src/main/java/']
JAVA_EXTENSIONS = {'.java'}

# Matches a unified diff hunk header: @@ -old_start[,old_count] +new_start[,new_count] @@
HUNK_HEADER_RE = re.compile(r'^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@')


class LineCoverage:
    """Coverage data for a single source line."""

    __slots__ = ('line_number', 'missed_instructions', 'covered_instructions',
                 'missed_branches', 'covered_branches')

    def __init__(self, line_number, missed_instructions, covered_instructions,
                 missed_branches, covered_branches):
        self.line_number = line_number
        self.missed_instructions = missed_instructions
        self.covered_instructions = covered_instructions
        self.missed_branches = missed_branches
        self.covered_branches = covered_branches

    @property
    def is_covered(self):
        """Line has executable code and at least some was executed."""
        return self.covered_instructions > 0

    @property
    def is_executable(self):
        """Line has executable code (covered or not)."""
        return self.missed_instructions > 0 or self.covered_instructions > 0


def repo_path_to_jacoco_key(repo_path, prefixes=None):
    """
    Convert a repo-relative file path to a JaCoCo coverage key.

    Example:
        'fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/Foo.java'
        -> 'com/apple/foundationdb/record/Foo.java'

    Returns None if the path doesn't match any known source prefix.
    """
    if prefixes is None:
        prefixes = DEFAULT_SOURCE_PREFIXES
    for prefix in prefixes:
        idx = repo_path.find(prefix)
        if idx != -1:
            return repo_path[idx + len(prefix):]
    return None


def parse_jacoco_report(report_path):
    """
    Parse a JaCoCo XML report and return coverage data indexed by source file key.

    Returns:
        dict mapping 'package/path/FileName.java' -> list of LineCoverage
    """
    tree = ET.parse(report_path)
    root = tree.getroot()

    coverage_data = {}
    for package in root.findall('.//package'):
        package_name = package.get('name')
        for sourcefile in package.findall('sourcefile'):
            filename = sourcefile.get('name')
            file_key = f"{package_name}/{filename}"
            lines = []
            for line in sourcefile.findall('line'):
                lines.append(LineCoverage(
                    line_number=int(line.get('nr')),
                    missed_instructions=int(line.get('mi')),
                    covered_instructions=int(line.get('ci')),
                    missed_branches=int(line.get('mb')),
                    covered_branches=int(line.get('cb')),
                ))
            coverage_data[file_key] = lines
    return coverage_data


def parse_diff(diff_path):
    """
    Parse a unified diff and return the set of added line numbers per file.

    Returns:
        dict mapping repo-relative file path -> set of added/modified line numbers
    """
    changed_lines = {}
    current_file = None
    current_line = 0

    with open(diff_path) as f:
        for raw_line in f:
            line = raw_line.rstrip('\n')

            # Detect file header: +++ b/path/to/file.java
            if line.startswith('+++ b/'):
                current_file = line[6:]  # strip '+++ b/'
                if current_file not in changed_lines:
                    changed_lines[current_file] = set()
                continue

            # Detect hunk header
            hunk_match = HUNK_HEADER_RE.match(line)
            if hunk_match:
                current_line = int(hunk_match.group(1))
                continue

            if current_file is None:
                continue

            # Inside a hunk: track line numbers
            if line.startswith('+'):
                # This is an added/modified line
                changed_lines[current_file].add(current_line)
                current_line += 1
            elif line.startswith('-'):
                # Removed line - doesn't affect new file line numbering
                pass
            else:
                # Context line (or "\ No newline at end of file")
                if not line.startswith('\\'):
                    current_line += 1

    return changed_lines


def compute_file_coverage(lines):
    """
    Compute overall line coverage for a file.

    Returns:
        tuple of (covered_lines, total_executable_lines)
    """
    executable = [l for l in lines if l.is_executable]
    covered = [l for l in executable if l.is_covered]
    return len(covered), len(executable)


def compute_changed_lines_coverage(lines, changed_line_numbers):
    """
    Compute line coverage for only the changed lines in a file.

    Returns:
        tuple of (covered_changed, total_executable_changed)
    """
    line_map = {l.line_number: l for l in lines}
    executable_changed = 0
    covered_changed = 0

    for line_num in changed_line_numbers:
        line = line_map.get(line_num)
        if line is not None and line.is_executable:
            executable_changed += 1
            if line.is_covered:
                covered_changed += 1

    return covered_changed, executable_changed


def format_percentage(covered, total):
    """Format a coverage percentage string."""
    if total == 0:
        return 'N/A (no executable lines)'
    pct = (covered / total) * 100
    return f'{pct:.1f}% ({covered}/{total} lines)'


def format_file_annotation(repo_path, file_coverage, changed_coverage, first_changed_line):
    """
    Format a single GitHub Actions annotation for a file's coverage summary.

    The annotation is placed one line above the first changed line so it appears
    as a header above the changes in the PR diff view.

    Returns the annotation string, or None if there's nothing to report.
    """
    file_covered, file_total = file_coverage
    changed_covered, changed_total = changed_coverage

    file_pct_str = format_percentage(file_covered, file_total)
    changed_pct_str = format_percentage(changed_covered, changed_total)

    annotation_line = max(1, first_changed_line - 1)
    message = f'File coverage: {file_pct_str} | Changed lines: {changed_pct_str}'
    return f'::notice file={repo_path},line={annotation_line},title=Coverage::{message}'


def generate_annotations(coverage_data, diff_data, source_prefixes):
    """
    Generate one annotation per changed file with coverage summary.

    Returns:
        list of annotation strings
    """
    annotations = []

    for repo_path, changed_line_numbers in sorted(diff_data.items()):
        # Only process Java files
        if os.path.splitext(repo_path)[1] not in JAVA_EXTENSIONS:
            continue

        # Skip files with only deletions (no added/modified lines)
        if not changed_line_numbers:
            continue

        jacoco_key = repo_path_to_jacoco_key(repo_path, source_prefixes)
        if jacoco_key is None:
            continue

        lines = coverage_data.get(jacoco_key)
        if lines is None:
            continue

        file_coverage = compute_file_coverage(lines)
        changed_coverage = compute_changed_lines_coverage(lines, changed_line_numbers)
        first_changed_line = min(changed_line_numbers)

        annotation = format_file_annotation(
            repo_path, file_coverage, changed_coverage, first_changed_line)
        if annotation:
            annotations.append(annotation)

    return annotations


def main(argv):
    """Main entry point for the coverage annotations script."""
    parser = argparse.ArgumentParser(
        prog='coverage_annotations',
        description='Emit GitHub Actions annotations with per-file coverage summaries'
    )
    parser.add_argument('--report', required=True,
                        help='Path to JaCoCo codeCoverageReport XML file')
    parser.add_argument('--diff', required=True,
                        help='Path to unified diff file (e.g., from gh pr diff)')
    parser.add_argument('--source-prefix', action='append', default=None,
                        help='Source path prefixes to strip '
                             '(default: src/main/java/). '
                             'Can be specified multiple times.')
    args = parser.parse_args(argv)

    source_prefixes = args.source_prefix if args.source_prefix else DEFAULT_SOURCE_PREFIXES

    # Validate inputs
    if not os.path.isfile(args.report):
        print(f'::error ::Coverage report not found: {args.report}', file=sys.stderr)
        sys.exit(1)

    if not os.path.isfile(args.diff):
        print(f'::error ::Diff file not found: {args.diff}', file=sys.stderr)
        sys.exit(1)

    # Parse inputs
    coverage_data = parse_jacoco_report(args.report)
    diff_data = parse_diff(args.diff)

    if not diff_data:
        print('No changed files found in diff. Nothing to annotate.')
        return

    if not coverage_data:
        print('Coverage report contains no data. Nothing to annotate.')
        return

    # Generate and emit annotations
    annotations = generate_annotations(coverage_data, diff_data, source_prefixes)

    for annotation in annotations:
        print(annotation)

    # Summary
    print(f'\n{len(annotations)} file(s) annotated with coverage data.',
          file=sys.stderr)


if __name__ == '__main__':
    main(sys.argv[1:])
