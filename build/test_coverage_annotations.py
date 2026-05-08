#!/usr/bin/env python3

#
# test_coverage_annotations.py
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

"""Unit tests for coverage_annotations.py"""

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
from coverage_annotations import (
    AnnotationRange,
    LineCoverage,
    filter_source_files,
    format_annotation,
    generate_annotations,
    group_consecutive_lines,
    parse_jacoco_report,
    repo_path_to_jacoco_key,
)

SAMPLE_JACOCO_XML = """\
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<!DOCTYPE report PUBLIC "-//JACOCO//DTD Report 1.1//EN" "report.dtd">
<report name="Code Coverage Report">
  <package name="com/apple/foundationdb/record">
    <sourcefile name="FDBRecordStore.java">
      <line nr="10" mi="0" ci="3" mb="0" cb="0"/>
      <line nr="11" mi="0" ci="2" mb="0" cb="0"/>
      <line nr="12" mi="4" ci="0" mb="0" cb="0"/>
      <line nr="13" mi="3" ci="0" mb="0" cb="0"/>
      <line nr="14" mi="2" ci="0" mb="0" cb="0"/>
      <line nr="15" mi="0" ci="5" mb="1" cb="3"/>
      <line nr="20" mi="0" ci="1" mb="0" cb="0"/>
      <line nr="21" mi="6" ci="0" mb="0" cb="0"/>
      <counter type="INSTRUCTION" missed="15" covered="11"/>
      <counter type="LINE" missed="4" covered="4"/>
      <counter type="BRANCH" missed="1" covered="3"/>
    </sourcefile>
    <sourcefile name="AllCovered.java">
      <line nr="5" mi="0" ci="2" mb="0" cb="0"/>
      <line nr="6" mi="0" ci="3" mb="0" cb="2"/>
      <counter type="INSTRUCTION" missed="0" covered="5"/>
      <counter type="LINE" missed="0" covered="2"/>
    </sourcefile>
  </package>
  <package name="com/apple/foundationdb/record/query">
    <sourcefile name="QueryPlanner.java">
      <line nr="100" mi="5" ci="0" mb="0" cb="0"/>
      <line nr="101" mi="0" ci="4" mb="2" cb="2"/>
      <line nr="102" mi="0" ci="3" mb="0" cb="0"/>
      <counter type="INSTRUCTION" missed="5" covered="7"/>
      <counter type="LINE" missed="1" covered="2"/>
      <counter type="BRANCH" missed="2" covered="2"/>
    </sourcefile>
  </package>
</report>
"""


class TestRepoPathToJacocoKey(unittest.TestCase):
    """Tests for repo_path_to_jacoco_key()"""

    def test_standard_java_path(self):
        path = 'fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/FDBRecordStore.java'
        result = repo_path_to_jacoco_key(path)
        self.assertEqual(result, 'com/apple/foundationdb/record/FDBRecordStore.java')

    def test_nested_module_path(self):
        path = 'fdb-relational-layer/fdb-relational-core/src/main/java/com/apple/foundationdb/relational/Foo.java'
        result = repo_path_to_jacoco_key(path)
        self.assertEqual(result, 'com/apple/foundationdb/relational/Foo.java')

    def test_no_match(self):
        path = 'fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/TestFoo.java'
        result = repo_path_to_jacoco_key(path, ['src/main/java/'])
        self.assertIsNone(result)

    def test_non_java_file(self):
        path = 'build.gradle'
        result = repo_path_to_jacoco_key(path)
        self.assertIsNone(result)

    def test_custom_prefix(self):
        path = 'module/src/main/java/com/example/Bar.java'
        result = repo_path_to_jacoco_key(path, ['src/main/java/'])
        self.assertEqual(result, 'com/example/Bar.java')

    def test_extensions_module(self):
        path = 'fdb-extensions/src/main/java/com/apple/foundationdb/async/AsyncUtil.java'
        result = repo_path_to_jacoco_key(path)
        self.assertEqual(result, 'com/apple/foundationdb/async/AsyncUtil.java')


class TestLineCoverage(unittest.TestCase):
    """Tests for LineCoverage classification."""

    def test_uncovered(self):
        line = LineCoverage(10, missed_instructions=4, covered_instructions=0,
                           missed_branches=0, covered_branches=0)
        self.assertTrue(line.is_uncovered)
        self.assertFalse(line.is_partial_branch)
        self.assertEqual(line.status, 'uncovered')

    def test_fully_covered(self):
        line = LineCoverage(10, missed_instructions=0, covered_instructions=3,
                           missed_branches=0, covered_branches=0)
        self.assertFalse(line.is_uncovered)
        self.assertFalse(line.is_partial_branch)
        self.assertEqual(line.status, 'covered')

    def test_fully_covered_with_branches(self):
        line = LineCoverage(10, missed_instructions=0, covered_instructions=3,
                           missed_branches=0, covered_branches=4)
        self.assertFalse(line.is_uncovered)
        self.assertFalse(line.is_partial_branch)
        self.assertEqual(line.status, 'covered')

    def test_partial_branch(self):
        line = LineCoverage(10, missed_instructions=0, covered_instructions=5,
                           missed_branches=2, covered_branches=2)
        self.assertFalse(line.is_uncovered)
        self.assertTrue(line.is_partial_branch)
        self.assertEqual(line.status, 'partial')

    def test_uncovered_with_branches(self):
        # Line not executed at all, so branches also not covered
        line = LineCoverage(10, missed_instructions=4, covered_instructions=0,
                           missed_branches=2, covered_branches=0)
        self.assertTrue(line.is_uncovered)
        self.assertFalse(line.is_partial_branch)
        self.assertEqual(line.status, 'uncovered')


class TestGroupConsecutiveLines(unittest.TestCase):
    """Tests for group_consecutive_lines()"""

    def test_empty_list(self):
        self.assertEqual(group_consecutive_lines([]), [])

    def test_all_covered(self):
        lines = [
            LineCoverage(1, 0, 3, 0, 0),
            LineCoverage(2, 0, 2, 0, 0),
        ]
        self.assertEqual(group_consecutive_lines(lines), [])

    def test_single_uncovered(self):
        lines = [LineCoverage(10, 4, 0, 0, 0)]
        ranges = group_consecutive_lines(lines)
        self.assertEqual(len(ranges), 1)
        self.assertEqual(ranges[0].start_line, 10)
        self.assertEqual(ranges[0].end_line, 10)
        self.assertEqual(ranges[0].status, 'uncovered')

    def test_consecutive_uncovered_grouped(self):
        lines = [
            LineCoverage(10, 4, 0, 0, 0),
            LineCoverage(11, 3, 0, 0, 0),
            LineCoverage(12, 2, 0, 0, 0),
        ]
        ranges = group_consecutive_lines(lines)
        self.assertEqual(len(ranges), 1)
        self.assertEqual(ranges[0].start_line, 10)
        self.assertEqual(ranges[0].end_line, 12)
        self.assertEqual(ranges[0].status, 'uncovered')

    def test_non_consecutive_separate_ranges(self):
        lines = [
            LineCoverage(10, 4, 0, 0, 0),
            LineCoverage(12, 3, 0, 0, 0),  # gap at line 11
        ]
        ranges = group_consecutive_lines(lines)
        self.assertEqual(len(ranges), 2)
        self.assertEqual(ranges[0].start_line, 10)
        self.assertEqual(ranges[0].end_line, 10)
        self.assertEqual(ranges[1].start_line, 12)
        self.assertEqual(ranges[1].end_line, 12)

    def test_mixed_status_splits(self):
        lines = [
            LineCoverage(10, 4, 0, 0, 0),   # uncovered
            LineCoverage(11, 0, 3, 2, 2),    # partial
            LineCoverage(12, 5, 0, 0, 0),    # uncovered
        ]
        ranges = group_consecutive_lines(lines)
        self.assertEqual(len(ranges), 3)
        self.assertEqual(ranges[0].status, 'uncovered')
        self.assertEqual(ranges[1].status, 'partial')
        self.assertEqual(ranges[2].status, 'uncovered')

    def test_covered_lines_interspersed(self):
        lines = [
            LineCoverage(10, 0, 3, 0, 0),   # covered - ignored
            LineCoverage(11, 4, 0, 0, 0),   # uncovered
            LineCoverage(12, 3, 0, 0, 0),   # uncovered
            LineCoverage(13, 0, 5, 0, 0),   # covered - ignored
            LineCoverage(14, 2, 0, 0, 0),   # uncovered
        ]
        ranges = group_consecutive_lines(lines)
        self.assertEqual(len(ranges), 2)
        self.assertEqual(ranges[0].start_line, 11)
        self.assertEqual(ranges[0].end_line, 12)
        self.assertEqual(ranges[1].start_line, 14)
        self.assertEqual(ranges[1].end_line, 14)

    def test_partial_branch_range(self):
        lines = [
            LineCoverage(5, 0, 3, 1, 3),    # partial
            LineCoverage(6, 0, 2, 2, 2),    # partial
        ]
        ranges = group_consecutive_lines(lines)
        self.assertEqual(len(ranges), 1)
        self.assertEqual(ranges[0].start_line, 5)
        self.assertEqual(ranges[0].end_line, 6)
        self.assertEqual(ranges[0].status, 'partial')
        self.assertEqual(ranges[0].covered_branches, 5)
        self.assertEqual(ranges[0].total_branches, 8)


class TestParseJacocoReport(unittest.TestCase):
    """Tests for parse_jacoco_report()"""

    def setUp(self):
        self.xml_file = tempfile.NamedTemporaryFile(
            mode='w', suffix='.xml', delete=False)
        self.xml_file.write(SAMPLE_JACOCO_XML)
        self.xml_file.close()

    def tearDown(self):
        os.unlink(self.xml_file.name)

    def test_parses_packages_and_files(self):
        data = parse_jacoco_report(self.xml_file.name)
        self.assertIn('com/apple/foundationdb/record/FDBRecordStore.java', data)
        self.assertIn('com/apple/foundationdb/record/AllCovered.java', data)
        self.assertIn('com/apple/foundationdb/record/query/QueryPlanner.java', data)

    def test_line_count(self):
        data = parse_jacoco_report(self.xml_file.name)
        self.assertEqual(len(data['com/apple/foundationdb/record/FDBRecordStore.java']), 8)
        self.assertEqual(len(data['com/apple/foundationdb/record/AllCovered.java']), 2)
        self.assertEqual(len(data['com/apple/foundationdb/record/query/QueryPlanner.java']), 3)

    def test_line_data_correct(self):
        data = parse_jacoco_report(self.xml_file.name)
        lines = data['com/apple/foundationdb/record/FDBRecordStore.java']
        # Line 10: covered (ci=3, mi=0)
        self.assertEqual(lines[0].line_number, 10)
        self.assertEqual(lines[0].status, 'covered')
        # Line 12: uncovered (ci=0, mi=4)
        self.assertEqual(lines[2].line_number, 12)
        self.assertEqual(lines[2].status, 'uncovered')
        # Line 15: partial branch (ci=5, mb=1, cb=3)
        self.assertEqual(lines[5].line_number, 15)
        self.assertEqual(lines[5].status, 'partial')


class TestFilterSourceFiles(unittest.TestCase):
    """Tests for filter_source_files()"""

    def test_filters_java_files(self):
        files = [
            'module/src/main/java/com/example/Foo.java',
            'module/src/main/java/com/example/Bar.java',
            'module/build.gradle',
            'README.md',
            'module/src/main/proto/record.proto',
        ]
        result = filter_source_files(files)
        self.assertEqual(result, [
            'module/src/main/java/com/example/Foo.java',
            'module/src/main/java/com/example/Bar.java',
        ])

    def test_empty_list(self):
        self.assertEqual(filter_source_files([]), [])


class TestFormatAnnotation(unittest.TestCase):
    """Tests for format_annotation()"""

    def test_single_line_warning(self):
        r = AnnotationRange(start_line=10, end_line=10, status='uncovered')
        result = format_annotation('module/src/main/java/com/example/Foo.java', r)
        self.assertEqual(
            result,
            '::warning file=module/src/main/java/com/example/Foo.java,line=10::Not covered by tests'
        )

    def test_multi_line_warning(self):
        r = AnnotationRange(start_line=10, end_line=14, status='uncovered')
        result = format_annotation('module/src/main/java/com/example/Foo.java', r)
        self.assertEqual(
            result,
            '::warning file=module/src/main/java/com/example/Foo.java,line=10,endLine=14::Not covered by tests'
        )

    def test_partial_branch_notice(self):
        r = AnnotationRange(start_line=15, end_line=15, status='partial',
                           covered_branches=3, total_branches=4)
        result = format_annotation('module/src/main/java/com/example/Foo.java', r)
        self.assertEqual(
            result,
            '::notice file=module/src/main/java/com/example/Foo.java,line=15::'
            'Partial branch coverage (3/4 branches covered)'
        )


class TestGenerateAnnotations(unittest.TestCase):
    """Tests for generate_annotations()"""

    def setUp(self):
        self.xml_file = tempfile.NamedTemporaryFile(
            mode='w', suffix='.xml', delete=False)
        self.xml_file.write(SAMPLE_JACOCO_XML)
        self.xml_file.close()
        self.coverage_data = parse_jacoco_report(self.xml_file.name)

    def tearDown(self):
        os.unlink(self.xml_file.name)

    def test_generates_annotations_for_changed_file(self):
        changed_files = [
            'fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/FDBRecordStore.java'
        ]
        annotations, skipped = generate_annotations(
            self.coverage_data, changed_files, None, 50)
        # Should have annotations for uncovered lines 12-14, partial line 15, uncovered line 21
        self.assertGreater(len(annotations), 0)
        self.assertEqual(skipped, 0)
        # Check that we have both warnings and notices
        warnings = [a for a in annotations if a.startswith('::warning')]
        notices = [a for a in annotations if a.startswith('::notice')]
        self.assertGreater(len(warnings), 0)
        self.assertGreater(len(notices), 0)

    def test_no_annotations_for_covered_file(self):
        changed_files = [
            'fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/AllCovered.java'
        ]
        annotations, skipped = generate_annotations(
            self.coverage_data, changed_files, None, 50)
        self.assertEqual(len(annotations), 0)
        self.assertEqual(skipped, 0)

    def test_no_annotations_for_unknown_file(self):
        changed_files = [
            'fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/Unknown.java'
        ]
        annotations, skipped = generate_annotations(
            self.coverage_data, changed_files, None, 50)
        self.assertEqual(len(annotations), 0)
        self.assertEqual(skipped, 0)

    def test_max_annotations_respected(self):
        changed_files = [
            'fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/FDBRecordStore.java',
            'fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/QueryPlanner.java',
        ]
        annotations, skipped = generate_annotations(
            self.coverage_data, changed_files, None, 2)
        self.assertEqual(len(annotations), 2)
        self.assertGreater(skipped, 0)

    def test_non_matching_path_skipped(self):
        changed_files = [
            'fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/TestFoo.java'
        ]
        annotations, skipped = generate_annotations(
            self.coverage_data, changed_files, None, 50)
        self.assertEqual(len(annotations), 0)


class TestEndToEnd(unittest.TestCase):
    """End-to-end tests using main() with captured output."""

    def setUp(self):
        self.xml_file = tempfile.NamedTemporaryFile(
            mode='w', suffix='.xml', delete=False)
        self.xml_file.write(SAMPLE_JACOCO_XML)
        self.xml_file.close()

        self.changed_file = tempfile.NamedTemporaryFile(
            mode='w', suffix='.txt', delete=False)
        self.changed_file.write(
            'fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/FDBRecordStore.java\n'
            'build.gradle\n'
            'README.md\n'
        )
        self.changed_file.close()

    def tearDown(self):
        os.unlink(self.xml_file.name)
        os.unlink(self.changed_file.name)

    def test_main_runs_without_error(self):
        from io import StringIO
        from unittest.mock import patch

        with patch('sys.stdout', new_callable=StringIO) as mock_out:
            from coverage_annotations import main
            main(['--report', self.xml_file.name,
                  '--changed-files', self.changed_file.name])

        output = mock_out.getvalue()
        self.assertIn('::warning', output)
        self.assertIn('FDBRecordStore.java', output)


if __name__ == '__main__':
    unittest.main()
