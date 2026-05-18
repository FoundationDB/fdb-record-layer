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

"""
Unit tests for coverage_annotations.py
Run these with:
    python3 -m pytest build/test_coverage_annotations.py
"""

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
from coverage_annotations import (
    LineCoverage,
    compute_changed_lines_coverage,
    compute_file_coverage,
    format_file_annotation,
    format_percentage,
    generate_annotations,
    parse_diff,
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

SAMPLE_DIFF = """\
diff --git a/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/FDBRecordStore.java b/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/FDBRecordStore.java
index abc1234..def5678 100644
--- a/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/FDBRecordStore.java
+++ b/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/FDBRecordStore.java
@@ -10,4 +10,6 @@ public class FDBRecordStore {
     existing line
+    added line on 11
+    added line on 12
     existing line
+    added line on 14
 }
@@ -19,3 +21,4 @@ public class FDBRecordStore {
     existing line
+    added line on 22
     existing line
"""

SAMPLE_DIFF_MULTIPLE_FILES = """\
diff --git a/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/FDBRecordStore.java b/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/FDBRecordStore.java
--- a/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/FDBRecordStore.java
+++ b/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/FDBRecordStore.java
@@ -11,2 +11,4 @@ public class FDBRecordStore {
     existing line
+    new line 12
+    new line 13
     existing line
diff --git a/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/QueryPlanner.java b/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/QueryPlanner.java
--- a/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/QueryPlanner.java
+++ b/fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/QueryPlanner.java
@@ -99,3 +99,5 @@ public class QueryPlanner {
     existing line
+    new line 100
+    new line 101
     existing line
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

    def test_no_match_test_source(self):
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

    def test_covered(self):
        line = LineCoverage(10, missed_instructions=0, covered_instructions=3,
                           missed_branches=0, covered_branches=0)
        self.assertTrue(line.is_covered)
        self.assertTrue(line.is_executable)

    def test_uncovered(self):
        line = LineCoverage(10, missed_instructions=4, covered_instructions=0,
                           missed_branches=0, covered_branches=0)
        self.assertFalse(line.is_covered)
        self.assertTrue(line.is_executable)

    def test_partial_branch_is_covered(self):
        line = LineCoverage(10, missed_instructions=0, covered_instructions=5,
                           missed_branches=2, covered_branches=2)
        self.assertTrue(line.is_covered)
        self.assertTrue(line.is_executable)

    def test_no_instructions(self):
        line = LineCoverage(10, missed_instructions=0, covered_instructions=0,
                           missed_branches=0, covered_branches=0)
        self.assertFalse(line.is_covered)
        self.assertFalse(line.is_executable)


class TestParseDiff(unittest.TestCase):
    """Tests for parse_diff()"""

    def _write_diff(self, content):
        f = tempfile.NamedTemporaryFile(mode='w', suffix='.patch', delete=False)
        f.write(content)
        f.close()
        return f.name

    def test_parses_added_lines(self):
        path = self._write_diff(SAMPLE_DIFF)
        try:
            result = parse_diff(path)
            file_path = 'fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/FDBRecordStore.java'
            self.assertIn(file_path, result)
            # Hunk 1: starts at line 10, context line (11), +line(12), +line(13), context(14), +line(15)
            # Wait, let me trace through:
            # @@ -10,4 +10,6 @@ -> new file starts at line 10
            #  "    existing line"  -> context, line 10, current_line becomes 11
            # "+    added line on 11" -> added, line 11, current_line becomes 12
            # "+    added line on 12" -> added, line 12, current_line becomes 13
            #  "    existing line"  -> context, line 13, current_line becomes 14
            # "+    added line on 14" -> added, line 14
            # Hunk 2: @@ -19,3 +21,4 @@ -> new file starts at line 21
            #  "    existing line"  -> context, line 21, current_line becomes 22
            # "+    added line on 22" -> added, line 22
            #  "    existing line"  -> context, line 23
            self.assertIn(11, result[file_path])
            self.assertIn(12, result[file_path])
            self.assertIn(14, result[file_path])
            self.assertIn(22, result[file_path])
            # Context lines should NOT be in the set
            self.assertNotIn(10, result[file_path])
            self.assertNotIn(13, result[file_path])
        finally:
            os.unlink(path)

    def test_parses_multiple_files(self):
        path = self._write_diff(SAMPLE_DIFF_MULTIPLE_FILES)
        try:
            result = parse_diff(path)
            self.assertEqual(len(result), 2)
            self.assertIn(
                'fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/FDBRecordStore.java',
                result)
            self.assertIn(
                'fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/QueryPlanner.java',
                result)
        finally:
            os.unlink(path)

    def test_empty_diff(self):
        path = self._write_diff('')
        try:
            result = parse_diff(path)
            self.assertEqual(result, {})
        finally:
            os.unlink(path)

    def test_non_java_files_included_in_parse(self):
        """parse_diff returns all files; filtering happens later."""
        diff = """\
diff --git a/build.gradle b/build.gradle
--- a/build.gradle
+++ b/build.gradle
@@ -1,2 +1,3 @@
 existing
+new line
 existing
"""
        path = self._write_diff(diff)
        try:
            result = parse_diff(path)
            self.assertIn('build.gradle', result)
        finally:
            os.unlink(path)


class TestComputeFileCoverage(unittest.TestCase):
    """Tests for compute_file_coverage()"""

    def test_mixed_coverage(self):
        lines = [
            LineCoverage(10, 0, 3, 0, 0),   # covered
            LineCoverage(11, 0, 2, 0, 0),   # covered
            LineCoverage(12, 4, 0, 0, 0),   # uncovered
            LineCoverage(13, 3, 0, 0, 0),   # uncovered
            LineCoverage(14, 2, 0, 0, 0),   # uncovered
            LineCoverage(15, 0, 5, 1, 3),   # covered (has instructions covered)
            LineCoverage(20, 0, 1, 0, 0),   # covered
            LineCoverage(21, 6, 0, 0, 0),   # uncovered
        ]
        covered, total = compute_file_coverage(lines)
        self.assertEqual(total, 8)
        self.assertEqual(covered, 4)

    def test_all_covered(self):
        lines = [
            LineCoverage(5, 0, 2, 0, 0),
            LineCoverage(6, 0, 3, 0, 2),
        ]
        covered, total = compute_file_coverage(lines)
        self.assertEqual(total, 2)
        self.assertEqual(covered, 2)

    def test_empty_file(self):
        covered, total = compute_file_coverage([])
        self.assertEqual(total, 0)
        self.assertEqual(covered, 0)


class TestComputeChangedLinesCoverage(unittest.TestCase):
    """Tests for compute_changed_lines_coverage()"""

    def test_some_changed_lines_covered(self):
        lines = [
            LineCoverage(10, 0, 3, 0, 0),   # covered
            LineCoverage(11, 0, 2, 0, 0),   # covered
            LineCoverage(12, 4, 0, 0, 0),   # uncovered
            LineCoverage(13, 3, 0, 0, 0),   # uncovered
        ]
        # Changed lines 10, 12, 13
        covered, total = compute_changed_lines_coverage(lines, {10, 12, 13})
        self.assertEqual(total, 3)   # all 3 are executable
        self.assertEqual(covered, 1)  # only line 10 is covered

    def test_changed_lines_not_executable(self):
        lines = [
            LineCoverage(10, 0, 3, 0, 0),
        ]
        # Line 5 is changed but not in coverage data (comment/blank)
        covered, total = compute_changed_lines_coverage(lines, {5})
        self.assertEqual(total, 0)
        self.assertEqual(covered, 0)

    def test_no_changed_lines(self):
        lines = [LineCoverage(10, 0, 3, 0, 0)]
        covered, total = compute_changed_lines_coverage(lines, set())
        self.assertEqual(total, 0)
        self.assertEqual(covered, 0)


class TestFormatPercentage(unittest.TestCase):
    """Tests for format_percentage()"""

    def test_normal_percentage(self):
        result = format_percentage(3, 4)
        self.assertEqual(result, '75.0% (3/4 lines)')

    def test_full_coverage(self):
        result = format_percentage(10, 10)
        self.assertEqual(result, '100.0% (10/10 lines)')

    def test_zero_coverage(self):
        result = format_percentage(0, 5)
        self.assertEqual(result, '0.0% (0/5 lines)')

    def test_no_executable_lines(self):
        result = format_percentage(0, 0)
        self.assertEqual(result, 'N/A (no executable lines)')


class TestFormatFileAnnotation(unittest.TestCase):
    """Tests for format_file_annotation()"""

    def test_basic_annotation(self):
        result = format_file_annotation(
            'module/src/main/java/com/example/Foo.java',
            (8, 10),
            (2, 3),
            first_changed_line=15,
        )
        self.assertEqual(
            result,
            '::notice file=module/src/main/java/com/example/Foo.java,line=14,title=Coverage::'
            'File coverage: 80.0% (8/10 lines) | Changed lines: 66.7% (2/3 lines)'
        )

    def test_annotation_line_does_not_go_below_1(self):
        result = format_file_annotation(
            'module/src/main/java/com/example/Foo.java',
            (8, 10),
            (2, 3),
            first_changed_line=1,
        )
        self.assertIn('line=1', result)

    def test_no_executable_changed_lines(self):
        result = format_file_annotation(
            'module/src/main/java/com/example/Foo.java',
            (8, 10),
            (0, 0),
            first_changed_line=5,
        )
        self.assertIn('N/A (no executable lines)', result)


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

    def test_generates_one_annotation_per_file(self):
        diff_data = {
            'fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/FDBRecordStore.java': {12, 13},
            'fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/query/QueryPlanner.java': {100},
        }
        annotations = generate_annotations(self.coverage_data, diff_data, None)
        self.assertEqual(len(annotations), 2)
        # Each should be a ::notice
        for a in annotations:
            self.assertTrue(a.startswith('::notice'))

    def test_skips_non_java_files(self):
        diff_data = {
            'build.gradle': {1, 2, 3},
            'README.md': {5},
        }
        annotations = generate_annotations(self.coverage_data, diff_data, None)
        self.assertEqual(len(annotations), 0)

    def test_skips_files_without_coverage(self):
        diff_data = {
            'fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/Unknown.java': {10},
        }
        annotations = generate_annotations(self.coverage_data, diff_data, None)
        self.assertEqual(len(annotations), 0)

    def test_skips_test_files(self):
        diff_data = {
            'fdb-record-layer-core/src/test/java/com/apple/foundationdb/record/FDBRecordStoreTest.java': {10},
        }
        annotations = generate_annotations(self.coverage_data, diff_data, None)
        self.assertEqual(len(annotations), 0)

    def test_skips_files_with_only_deletions(self):
        diff_data = {
            'fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/FDBRecordStore.java': set(),
        }
        annotations = generate_annotations(self.coverage_data, diff_data, None)
        self.assertEqual(len(annotations), 0)

    def test_annotation_contains_coverage_data(self):
        diff_data = {
            'fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/FDBRecordStore.java': {12, 13, 14},
        }
        annotations = generate_annotations(self.coverage_data, diff_data, None)
        self.assertEqual(len(annotations), 1)
        # File has 8 executable lines, 4 covered -> 50%
        self.assertIn('50.0%', annotations[0])
        # Changed lines 12, 13, 14 are all uncovered -> 0%
        self.assertIn('0.0%', annotations[0])


class TestEndToEnd(unittest.TestCase):
    """End-to-end tests using main() with captured output."""

    def setUp(self):
        self.xml_file = tempfile.NamedTemporaryFile(
            mode='w', suffix='.xml', delete=False)
        self.xml_file.write(SAMPLE_JACOCO_XML)
        self.xml_file.close()

        self.diff_file = tempfile.NamedTemporaryFile(
            mode='w', suffix='.patch', delete=False)
        self.diff_file.write(SAMPLE_DIFF)
        self.diff_file.close()

    def tearDown(self):
        os.unlink(self.xml_file.name)
        os.unlink(self.diff_file.name)

    def test_main_runs_without_error(self):
        from io import StringIO
        from unittest.mock import patch

        with patch('sys.stdout', new_callable=StringIO) as mock_out:
            from coverage_annotations import main
            main(['--report', self.xml_file.name,
                  '--diff', self.diff_file.name])

        output = mock_out.getvalue()
        self.assertIn('::notice', output)
        self.assertIn('FDBRecordStore.java', output)
        self.assertIn('File coverage:', output)
        self.assertIn('Changed lines:', output)


if __name__ == '__main__':
    unittest.main()
