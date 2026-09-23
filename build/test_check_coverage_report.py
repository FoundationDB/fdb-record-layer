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
<report name="codeCoverageReport">
  <package name="com/apple/foundationdb/record">
    <class name="com/apple/foundationdb/record/Example" sourcefilename="Example.java">
      <method name="doThing" desc="()V" line="10">
        <counter type="INSTRUCTION" missed="0" covered="5"/>
      </method>
    </class>
    <sourcefile name="Example.java">
      <line nr="10" mi="0" ci="5" mb="0" cb="0"/>
    </sourcefile>
  </package>
</report>
"""

# The legitimate output of `jar createEmptyCoverageData codeCoverageReport`: the whole
# codebase is present but nothing is covered. This MUST be accepted.
ZERO_PERCENT_XML = """\
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<report name="codeCoverageReport">
  <package name="com/apple/foundationdb/record">
    <class name="com/apple/foundationdb/record/Example" sourcefilename="Example.java">
      <method name="doThing" desc="()V" line="10">
        <counter type="INSTRUCTION" missed="5" covered="0"/>
      </method>
    </class>
    <sourcefile name="Example.java">
      <line nr="10" mi="5" ci="0" mb="0" cb="0"/>
    </sourcefile>
  </package>
</report>
"""

# The regression signature: well-formed, but no classes were analyzed at all.
EMPTY_XML = """\
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<report name="codeCoverageReport">
  <counter type="INSTRUCTION" missed="0" covered="0"/>
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

    def test_accepts_populated_report(self):
        self.assertEqual(0, main(['--report', self._write(POPULATED_XML)]))

    def test_accepts_legitimate_zero_percent_report(self):
        # createEmptyCoverageData produces this for a PR that tested nothing; it has classes
        # but no coverage, and must still be uploadable.
        self.assertEqual(0, main(['--report', self._write(ZERO_PERCENT_XML)]))

    def test_rejects_report_with_no_classes(self):
        self.assertEqual(1, main(['--report', self._write(EMPTY_XML)]))

    def test_rejects_missing_report(self):
        self.assertEqual(1, main(['--report', '/nonexistent/codeCoverageReport.xml']))

    def test_rejects_malformed_report(self):
        self.assertEqual(1, main(['--report', self._write(MALFORMED_XML)]))


if __name__ == '__main__':
    unittest.main()
