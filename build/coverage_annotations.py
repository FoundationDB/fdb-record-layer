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
for uncovered and partially-covered lines in files changed by a pull request.

Usage:
    python build/coverage_annotations.py \
        --report .out/reports/jacoco/codeCoverageReport/codeCoverageReport.xml \
        --changed-files changed_files.txt \
        [--max-annotations 50] \
        [--source-prefix 'src/main/java/']

The script outputs GitHub Actions workflow commands (::warning, ::notice) that
appear as inline annotations on the PR "Files changed" tab.
"""

import argparse
import os
import sys
import xml.etree.ElementTree as ET

DEFAULT_SOURCE_PREFIXES = ['src/main/java/']
DEFAULT_MAX_ANNOTATIONS = 50
JAVA_EXTENSIONS = {'.java'}


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
    def is_uncovered(self):
        """Line has executable code but nothing was executed."""
        return self.covered_instructions == 0 and self.missed_instructions > 0

    @property
    def is_partial_branch(self):
        """Line was executed but some branches were not taken."""
        return (self.covered_instructions > 0
                and self.missed_branches > 0
                and self.covered_branches > 0)

    @property
    def status(self):
        if self.is_uncovered:
            return 'uncovered'
        elif self.is_partial_branch:
            return 'partial'
        else:
            return 'covered'


class AnnotationRange:
    """A range of consecutive lines with the same coverage status."""

    __slots__ = ('start_line', 'end_line', 'status', 'total_missed_instructions',
                 'covered_branches', 'total_branches')

    def __init__(self, start_line, end_line, status, total_missed_instructions=0,
                 covered_branches=0, total_branches=0):
        self.start_line = start_line
        self.end_line = end_line
        self.status = status
        self.total_missed_instructions = total_missed_instructions
        self.covered_branches = covered_branches
        self.total_branches = total_branches

    @property
    def message(self):
        if self.status == 'uncovered':
            return 'Not covered by tests'
        elif self.status == 'partial':
            return (f'Partial branch coverage '
                    f'({self.covered_branches}/{self.total_branches} branches covered)')
        return ''


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


def read_changed_files(changed_files_path):
    """Read a file containing one repo-relative path per line."""
    with open(changed_files_path) as f:
        return [line.strip() for line in f if line.strip()]


def filter_source_files(file_paths):
    """Filter to only Java source files."""
    return [p for p in file_paths if os.path.splitext(p)[1] in JAVA_EXTENSIONS]


def group_consecutive_lines(lines):
    """
    Group consecutive lines with the same coverage status into AnnotationRanges.

    Only groups 'uncovered' and 'partial' lines (covered lines are ignored).
    Consecutive means line numbers differ by exactly 1 and have the same status.
    """
    # Filter to only lines that need annotations
    notable_lines = [l for l in lines if l.status in ('uncovered', 'partial')]
    if not notable_lines:
        return []

    # Sort by line number
    notable_lines.sort(key=lambda l: l.line_number)

    ranges = []
    current_start = notable_lines[0]
    current_end = notable_lines[0]
    current_status = notable_lines[0].status
    missed_instr = notable_lines[0].missed_instructions
    cov_branches = notable_lines[0].covered_branches
    tot_branches = notable_lines[0].covered_branches + notable_lines[0].missed_branches

    for line in notable_lines[1:]:
        if (line.status == current_status
                and line.line_number == current_end.line_number + 1):
            # Extend the current range
            current_end = line
            missed_instr += line.missed_instructions
            cov_branches += line.covered_branches
            tot_branches += line.covered_branches + line.missed_branches
        else:
            # Emit the current range and start a new one
            ranges.append(AnnotationRange(
                start_line=current_start.line_number,
                end_line=current_end.line_number,
                status=current_status,
                total_missed_instructions=missed_instr,
                covered_branches=cov_branches,
                total_branches=tot_branches,
            ))
            current_start = line
            current_end = line
            current_status = line.status
            missed_instr = line.missed_instructions
            cov_branches = line.covered_branches
            tot_branches = line.covered_branches + line.missed_branches

    # Emit the last range
    ranges.append(AnnotationRange(
        start_line=current_start.line_number,
        end_line=current_end.line_number,
        status=current_status,
        total_missed_instructions=missed_instr,
        covered_branches=cov_branches,
        total_branches=tot_branches,
    ))
    return ranges


def format_annotation(repo_path, annotation_range):
    """Format a single GitHub Actions workflow command for an annotation."""
    level = 'warning' if annotation_range.status == 'uncovered' else 'notice'
    location = f'file={repo_path},line={annotation_range.start_line}'
    if annotation_range.end_line > annotation_range.start_line:
        location += f',endLine={annotation_range.end_line}'
    message = annotation_range.message
    return f'::{level} {location}::{message}'


def generate_annotations(coverage_data, changed_files, source_prefixes, max_annotations):
    """
    Generate annotation strings for changed files based on coverage data.

    Returns:
        tuple of (list of annotation strings, int of remaining annotations not emitted)
    """
    annotations = []
    total_skipped = 0

    for repo_path in changed_files:
        jacoco_key = repo_path_to_jacoco_key(repo_path, source_prefixes)
        if jacoco_key is None:
            continue

        lines = coverage_data.get(jacoco_key)
        if lines is None:
            continue

        ranges = group_consecutive_lines(lines)
        for r in ranges:
            if len(annotations) >= max_annotations:
                total_skipped += 1
            else:
                annotations.append(format_annotation(repo_path, r))

    return annotations, total_skipped


def main(argv):
    """Main entry point for the coverage annotations script."""
    parser = argparse.ArgumentParser(
        prog='coverage_annotations',
        description='Emit GitHub Actions annotations for uncovered lines in changed files'
    )
    parser.add_argument('--report', required=True,
                        help='Path to JaCoCo codeCoverageReport XML file')
    parser.add_argument('--changed-files', required=True,
                        help='Path to file listing changed files (one per line)')
    parser.add_argument('--max-annotations', type=int, default=DEFAULT_MAX_ANNOTATIONS,
                        help=f'Maximum number of annotations to emit (default: {DEFAULT_MAX_ANNOTATIONS})')
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

    if not os.path.isfile(args.changed_files):
        print(f'::error ::Changed files list not found: {args.changed_files}', file=sys.stderr)
        sys.exit(1)

    # Parse inputs
    coverage_data = parse_jacoco_report(args.report)
    changed_files = read_changed_files(args.changed_files)
    source_files = filter_source_files(changed_files)

    if not source_files:
        print('No Java source files in the changed files list. Nothing to annotate.')
        return

    if not coverage_data:
        print('Coverage report contains no data. Nothing to annotate.')
        return

    # Generate and emit annotations
    annotations, skipped = generate_annotations(
        coverage_data, source_files, source_prefixes, args.max_annotations
    )

    for annotation in annotations:
        print(annotation)

    if skipped > 0:
        print(f'::notice ::Coverage annotations truncated at {args.max_annotations}. '
              f'{skipped} more uncovered regions not shown.')

    # Summary
    matched_files = sum(
        1 for f in source_files
        if repo_path_to_jacoco_key(f, source_prefixes) in coverage_data
    )
    print(f'\nProcessed {len(source_files)} source file(s), '
          f'{matched_files} with coverage data, '
          f'{len(annotations)} annotation(s) emitted.',
          file=sys.stderr)


if __name__ == '__main__':
    main(sys.argv[1:])
