#!/usr/bin/env python3

#
# test_check_coverage_report.py
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

"""Unit tests for check_coverage_report.py"""

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
from check_coverage_report import count_report_elements, main

# A report with real classes and real coverage.
POPULATED_XML = """\
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<report name="fdb-record-layer">
  <sessioninfo id="runnervmlun5p-bd006a00" start="1790147106663" dump="1790147110723"/>
  <package name="com/apple/foundationdb/relational/recordlayer/metadata">
    <class name="com/apple/foundationdb/relational/recordlayer/metadata/RecordLayerColumn" sourcefilename="RecordLayerColumn.java">
      <method name="&lt;init&gt;" desc="(Ljava/lang/String;Lcom/apple/foundationdb/relational/api/metadata/DataType;I)V" line="42">
        <counter type="INSTRUCTION" missed="0" covered="12"/>
        <counter type="LINE" missed="0" covered="5"/>
        <counter type="COMPLEXITY" missed="0" covered="1"/>
        <counter type="METHOD" missed="0" covered="1"/>
      </method>
    </class>
    <sourcefile name="RecordLayerUnnestedSyntheticTable.java">
      <line nr="74" mi="0" ci="4" mb="0" cb="0"/>
      <line nr="75" mi="0" ci="3" mb="0" cb="0"/>
      <line nr="76" mi="0" ci="3" mb="0" cb="0"/>
      <counter type="INSTRUCTION" missed="1" covered="343"/>
      <counter type="BRANCH" missed="6" covered="20"/>
      <counter type="LINE" missed="0" covered="71"/>
      <counter type="COMPLEXITY" missed="6" covered="30"/>
      <counter type="METHOD" missed="0" covered="23"/>
      <counter type="CLASS" missed="0" covered="3"/>
    </sourcefile>
  </package>
  <counter type="INSTRUCTION" missed="92273" covered="467036"/>
  <counter type="BRANCH" missed="12488" covered="33755"/>
  <counter type="LINE" missed="18661" covered="99806"/>
  <counter type="COMPLEXITY" missed="15811" covered="41165"/>
  <counter type="METHOD" missed="5993" covered="27406"/>
  <counter type="CLASS" missed="165" covered="3141"/>
</report>
"""

# The legitimate output of `jar createEmptyCoverageData codeCoverageReport`: the whole
# codebase is present but nothing is covered. This MUST be accepted.
ZERO_PERCENT_XML = """\
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<report name="fdb-record-layer">
  <sessioninfo id="runnervmlun5p-bd006a00" start="1790147106663" dump="1790147110723"/>
  <package name="com/apple/foundationdb/relational/recordlayer/metadata">
    <class name="com/apple/foundationdb/relational/recordlayer/metadata/RecordLayerColumn" sourcefilename="RecordLayerColumn.java">
      <method name="&lt;init&gt;" desc="(Ljava/lang/String;Lcom/apple/foundationdb/relational/api/metadata/DataType;I)V" line="42">
        <counter type="INSTRUCTION" missed="0" covered="0"/>
        <counter type="LINE" missed="0" covered="0"/>
        <counter type="COMPLEXITY" missed="0" covered="0"/>
        <counter type="METHOD" missed="0" covered="0"/>
      </method>
    </class>
    <sourcefile name="RecordLayerUnnestedSyntheticTable.java">
      <line nr="74" mi="2" ci="0" mb="0" cb="0"/>
      <line nr="75" mi="1" ci="0" mb="0" cb="0"/>
      <line nr="76" mi="3" ci="0" mb="0" cb="0"/>
      <counter type="INSTRUCTION" missed="1" covered="0"/>
      <counter type="BRANCH" missed="6" covered="0"/>
      <counter type="LINE" missed="0" covered="0"/>
      <counter type="COMPLEXITY" missed="6" covered="0"/>
      <counter type="METHOD" missed="0" covered="0"/>
      <counter type="CLASS" missed="0" covered="0"/>
    </sourcefile>
  </package>
  <counter type="INSTRUCTION" missed="92273" covered="0"/>
  <counter type="BRANCH" missed="12488" covered="0"/>
  <counter type="LINE" missed="18661" covered="0"/>
  <counter type="COMPLEXITY" missed="15811" covered="0"/>
  <counter type="METHOD" missed="5993" covered="0"/>
  <counter type="CLASS" missed="165" covered="0"/>
</report>
"""

# The regression signature: well-formed, but no classes were analyzed at all.
EMPTY_XML = """\
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<report name="codeCoverageReport">
  <counter type="INSTRUCTION" missed="0" covered="0"/>
</report>
"""

# An empty upload that we actually sent to teamscale
EMPTY_XML_2 = """\
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<!DOCTYPE report PUBLIC "-//JACOCO//DTD Report 1.1//EN" "report.dtd">
<report name="fdb-record-layer">
<sessioninfo id="runnervmlun5p-55d43c15" start="1790147126056" dump="1790147127816"/>
<sessioninfo id="runnervmlun5p-1aad5660" start="1790147128143" dump="1790147129103"/>
<sessioninfo id="runnervmlun5p-20cb41b1" start="1790147146845" dump="1790147150976"/>
</report>
"""

MALFORMED_XML = "<report name='codeCoverageReport'><package>"


class CheckCoverageReportTest(unittest.TestCase):

    def _write(self, content):
        handle = tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False)
        handle.write(content)
        handle.close()
        self.addCleanup(os.unlink, handle.name)
        return handle.name

    def test_counts_populated_report(self):
        packages, classes, sourcefiles = count_report_elements(self._write(POPULATED_XML))
        self.assertEqual((1, 1, 1), (packages, classes, sourcefiles))

    def test_counts_empty_report(self):
        packages, classes, sourcefiles = count_report_elements(self._write(EMPTY_XML))
        self.assertEqual((0, 0, 0), (packages, classes, sourcefiles))

    def test_counts_empty_report_2(self):
        packages, classes, sourcefiles = count_report_elements(self._write(EMPTY_XML_2))
        self.assertEqual((0, 0, 0), (packages, classes, sourcefiles))

    def test_accepts_populated_report(self):
        self.assertEqual(0, main(['--report', self._write(POPULATED_XML)]))

    def test_accepts_legitimate_zero_percent_report(self):
        # createEmptyCoverageData produces this for a PR that tested nothing; it has classes
        # but no coverage, and must still be uploadable.
        self.assertEqual(0, main(['--report', self._write(ZERO_PERCENT_XML)]))

    def test_rejects_report_with_no_classes(self):
        self.assertEqual(1, main(['--report', self._write(EMPTY_XML)]))

    def test_rejects_report_with_no_classes_2(self):
        self.assertEqual(1, main(['--report', self._write(EMPTY_XML_2)]))

    def test_rejects_missing_report(self):
        self.assertEqual(1, main(['--report', '/nonexistent/codeCoverageReport.xml']))

    def test_rejects_malformed_report(self):
        self.assertEqual(1, main(['--report', self._write(MALFORMED_XML)]))


if __name__ == '__main__':
    unittest.main()
