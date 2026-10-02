#!/usr/bin/env python3

#
# check_coverage_report.py
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
Sanity-check a JaCoCo XML coverage report before it is uploaded to Teamscale.

This guards against silently publishing an empty partition. The `codeCoverageReport` Gradle
task happily emits a well-formed report with no classes in it if it was handed an empty set
of class directories, and the Teamscale upload of such a report succeeds -- it just wipes the
partition. That is exactly what happened when `codeCoverageReport` regressed to deciding
which subproject jars to analyze at configuration time (see the task in gradle/root.gradle).

The check is deliberately "does the report contain any classes at all?" rather than "is
coverage above zero". A legitimately 0%-covered report is a real scenario: the
`createEmptyCoverageData` task seeds empty execution data so that a pull request which tested
nothing still publishes a full-codebase 0% report, and that must continue to upload.

Usage:
    python build/check_coverage_report.py \
        --report .out/reports/jacoco/codeCoverageReport/codeCoverageReport.xml

Exits non-zero, with a GitHub Actions ::error annotation, if the report is missing,
unparseable, or contains no classes.
"""

import argparse
import os
import sys
import xml.etree.ElementTree as ET


def count_report_elements(report_path):
    """
    Count the analyzed elements in a JaCoCo XML report.

    Returns:
        tuple of (packages, classes, sourcefiles)

    Raises:
        xml.etree.ElementTree.ParseError: if the report is not well-formed XML.
    """
    root = ET.parse(report_path).getroot()
    return (
        len(root.findall('.//package')),
        len(root.findall('.//class')),
        len(root.findall('.//sourcefile')),
    )


def main(argv):
    """Main entry point for the coverage report check."""
    parser = argparse.ArgumentParser(
        prog='check_coverage_report',
        description='Fail if a JaCoCo XML report contains no analyzed classes'
    )
    parser.add_argument('--report', required=True,
                        help='Path to JaCoCo codeCoverageReport XML file')
    args = parser.parse_args(argv)

    if not os.path.isfile(args.report):
        print(f'::error ::Coverage report not found: {args.report}', file=sys.stderr)
        return 1

    try:
        packages, classes, sourcefiles = count_report_elements(args.report)
    except ET.ParseError as e:
        print(f'::error ::Coverage report {args.report} is not valid XML: {e}',
              file=sys.stderr)
        return 1

    if classes == 0:
        print(f'::error ::Coverage report {args.report} contains no classes, so uploading '
              f'it would empty the Teamscale partition. This usually means '
              f'codeCoverageReport ran with empty classDirectories -- check that the '
              f'subproject jars exist when stageCoverageClasses runs.',
              file=sys.stderr)
        return 1

    print(f'Coverage report looks sane: {packages} packages, {classes} classes, '
          f'{sourcefiles} source files.')
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
