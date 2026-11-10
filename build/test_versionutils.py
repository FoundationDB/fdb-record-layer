#!/usr/bin/env python3

#
# test_versionutils.py
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

"""Unit tests for versionutils.py"""

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
from versionutils import release_summary

PROPERTIES = """\
org.gradle.jvmargs=-Xmx4g
version=4.18.2.0
releaseBuild=false
"""


class ReleaseSummaryTest(unittest.TestCase):

    def _write(self, content):
        handle = tempfile.NamedTemporaryFile(mode='w', suffix='.properties', delete=False)
        handle.write(content)
        handle.close()
        self.addCleanup(os.unlink, handle.name)
        return handle.name

    def test_build_release_with_ref(self):
        self.assertEqual(
            '## Releasing version `4.18.3.0`\n'
            '\n'
            '- Update type: `BUILD`\n'
            '- Ref: `refs/heads/main`',
            release_summary(self._write(PROPERTIES), 'BUILD', 'refs/heads/main'))

    def test_patch_release_with_ref(self):
        self.assertEqual(
            '## Releasing version `4.18.2.1`\n'
            '\n'
            '- Update type: `PATCH`\n'
            '- Ref: `refs/heads/4.18.2`',
            release_summary(self._write(PROPERTIES), 'PATCH', 'refs/heads/4.18.2'))

    def test_omits_ref_when_not_set(self):
        self.assertEqual(
            '## Releasing version `4.18.3.0`\n'
            '\n'
            '- Update type: `BUILD`',
            release_summary(self._write(PROPERTIES), 'BUILD', None))

    def test_omits_ref_when_empty(self):
        self.assertNotIn('Ref', release_summary(self._write(PROPERTIES), 'BUILD', ''))

    def test_does_not_modify_file(self):
        filename = self._write(PROPERTIES)
        release_summary(filename, 'BUILD', 'refs/heads/main')
        with open(filename, 'r') as fin:
            self.assertEqual(PROPERTIES, fin.read())

    def test_rejects_file_without_version(self):
        with self.assertRaises(ValueError):
            release_summary(self._write('releaseBuild=false\n'), 'BUILD', None)


if __name__ == '__main__':
    unittest.main()
