#!/usr/bin/env python3

#
# test_affected_subprojects.py
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

"""Unit tests for affected_subprojects.py"""

import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(__file__))
from affected_subprojects import (
    compute_affected,
    compute_matrix_plan,
    is_ignored,
    is_build_affecting,
    map_changed_file_to_subproject,
    parse_subproject_deps,
    render_markdown,
)

# A trimmed-down but shape-accurate stand-in for the real printDependentSubprojects output:
# leaf subprojects, a core subproject most things depend on, and a couple of subprojects that
# only depend on core. Each value is the precomputed transitive impact set (subproject itself
# plus everything that depends on it) that the real Gradle task would compute.
SAMPLE_AFFECTED_MAP = {
    'fdb-java-annotations': [
        'fdb-extensions', 'fdb-java-annotations', 'fdb-record-layer-core',
        'fdb-record-layer-lucene', 'fdb-relational-api', 'fdb-relational-core', 'yaml-tests',
    ],
    'fdb-test-utils': [
        'fdb-extensions', 'fdb-record-layer-core', 'fdb-record-layer-lucene',
        'fdb-relational-api', 'fdb-relational-core', 'fdb-test-utils', 'yaml-tests',
    ],
    'fdb-extensions': [
        'fdb-extensions', 'fdb-record-layer-core', 'fdb-record-layer-lucene',
        'fdb-relational-api', 'fdb-relational-core', 'yaml-tests',
    ],
    'fdb-record-layer-core': [
        'fdb-record-layer-core', 'fdb-record-layer-lucene', 'fdb-relational-core', 'yaml-tests',
    ],
    'fdb-record-layer-lucene': ['fdb-record-layer-lucene'],
    'fdb-relational-api': ['fdb-relational-api', 'fdb-relational-core', 'yaml-tests'],
    'fdb-relational-core': ['fdb-relational-core', 'yaml-tests'],
    'yaml-tests': ['yaml-tests'],
}

SAMPLE_AFFECTED_MAP_TEXT = json.dumps(SAMPLE_AFFECTED_MAP)

ALL_SUBPROJECTS = set(SAMPLE_AFFECTED_MAP)


class TestParseSubprojectDeps(unittest.TestCase):
    """Tests for parse_subproject_deps()"""

    def test_parses_all_subprojects(self):
        result = parse_subproject_deps(SAMPLE_AFFECTED_MAP_TEXT)
        self.assertEqual(set(result), ALL_SUBPROJECTS)

    def test_preserves_affected_lists(self):
        result = parse_subproject_deps(SAMPLE_AFFECTED_MAP_TEXT)
        self.assertEqual(result['fdb-record-layer-lucene'], ['fdb-record-layer-lucene'])

    def test_not_an_object_raises(self):
        with self.assertRaises(ValueError):
            parse_subproject_deps('["not", "an", "object"]')

    def test_empty_object_raises(self):
        with self.assertRaises(ValueError):
            parse_subproject_deps('{}')

    def test_malformed_value_raises(self):
        with self.assertRaises(ValueError):
            parse_subproject_deps('{"fdb-test-utils": "not-a-list"}')

    def test_invalid_json_raises(self):
        with self.assertRaises(json.JSONDecodeError):
            parse_subproject_deps('not valid json')


class TestIsBuildAffecting(unittest.TestCase):
    """Tests for is_build_affecting()"""

    def test_root_build_gradle(self):
        self.assertTrue(is_build_affecting('build.gradle'))

    def test_settings_gradle(self):
        self.assertTrue(is_build_affecting('settings.gradle'))

    def test_project_gradle(self):
        # Individual project gradle files are not build affecting. They only affect their
        # subproject (and downstream dependencies), and so this doesn't need to result
        # in a full retest of everything
        self.assertFalse(is_build_affecting('fdb-record-layer-core/fdb-record-layer-core.gradle'))

    def test_gradle_directory(self):
        self.assertTrue(is_build_affecting('gradle/testing.gradle'))

    def test_gradle_wrapper(self):
        self.assertTrue(is_build_affecting('gradlew'))
        self.assertTrue(is_build_affecting('gradlew.bat'))

    def test_pull_request_workflow(self):
        self.assertTrue(is_build_affecting('.github/workflows/pull_request.yml'))

    def test_composite_actions(self):
        self.assertTrue(is_build_affecting('actions/run-gradle/action.yml'))

    def test_self_reference(self):
        self.assertTrue(is_build_affecting('build/affected_subprojects.py'))

    def test_subproject_source_is_not_build_affecting(self):
        path = 'fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/Foo.java'
        self.assertFalse(is_build_affecting(path))

    def test_unrelated_workflow_is_not_build_affecting(self):
        self.assertFalse(is_build_affecting('.github/workflows/nightly.yml'))


class TestIsIgnored(unittest.TestCase):
    """Tests for is_ignored()"""

    def test_build_gradle_not_ignored(self):
        self.assertFalse(is_ignored('build.gradle'))

    def test_subproject_gradle_not_ignored(self):
        self.assertFalse(is_ignored('fdb-record-layer-core/fdb-record-layer-core.gradle'))

    def test_java_not_ignored(self):
        self.assertFalse(is_ignored('fdb-record-layer-core/src/main/java/Foo.java'))

    def test_proto_not_ignored(self):
        self.assertFalse(is_ignored('fdb-record-layer-core/src/test/proto/test_records_1.proto'))

    def test_readme_ignored(self):
        self.assertTrue(is_ignored('README.md'))

    def test_subproject_readme_ignored(self):
        self.assertTrue(is_ignored('fdb-record-layer-core/README.md'))

    def test_idea_ignored(self):
        self.assertTrue(is_ignored('.idea/compiler.xml'))

    def test_gitignore_ignored(self):
        self.assertTrue(is_ignored('.gitignore'))

    def test_agitignore_not_ignored(self):
        self.assertFalse(is_ignored('agitignore'))

    def test_docs_content_ignored(self):
        self.assertTrue(is_ignored('docs/sphinx/source/ReleaseNotes.md'))

    def test_docs_script_ignored(self):
        self.assertTrue(is_ignored('docs/sphinx/source/generate_railroad_svg.py'))

    def test_license_ignored(self):
        self.assertTrue(is_ignored('LICENSE'))

    def test_acknowledgements_ignored(self):
        self.assertTrue(is_ignored('ACKNOWLEDGEMENTS'))

    def test_license_suffix_not_ignored(self):
        self.assertFalse(is_ignored('fdb-relational-server/LICENSE'))


class TestMapChangedFileToSubproject(unittest.TestCase):
    """Tests for map_changed_file_to_subproject()"""

    def test_maps_to_known_subproject(self):
        path = 'fdb-record-layer-core/src/main/java/com/apple/foundationdb/record/Foo.java'
        self.assertEqual(
            map_changed_file_to_subproject(path, ALL_SUBPROJECTS), 'fdb-record-layer-core')

    def test_unmapped_top_level_file(self):
        self.assertIsNone(map_changed_file_to_subproject('README.md', ALL_SUBPROJECTS))

    def test_unmapped_new_top_level_directory(self):
        self.assertIsNone(
            map_changed_file_to_subproject('some-new-subproject/build.gradle', ALL_SUBPROJECTS))


class TestComputeAffected(unittest.TestCase):
    """Tests for compute_affected()"""

    TO_BE_IGNORED = [
        'README.md',
        'CODE_OF_CONDUCT.md',
        'LICENSE',
        'ACKNOWLEDGEMENTS',
        '.gitignore',
        '.idea/misc.xml',
        'AGENTS.md',
        'docs/sphinx/source/ReleaseNotes.md',
        'docs/sphinx/generate_railroad_svg.py',
    ]

    def test_build_affecting_change_runs_all(self):
        result = compute_affected(['build.gradle'], SAMPLE_AFFECTED_MAP)
        self.assertTrue(result['run_all'])
        self.assertEqual(set(result['affected']), ALL_SUBPROJECTS)

    def test_ignored_file_change_runs_nothing(self):
        for path in TestComputeAffected.TO_BE_IGNORED:
            result = compute_affected([path], SAMPLE_AFFECTED_MAP)
            self.assertFalse(result['run_all'])
            self.assertEqual(result['affected'], [])

    def test_only_ignored_files_run_nothing(self):
        result = compute_affected(TestComputeAffected.TO_BE_IGNORED, SAMPLE_AFFECTED_MAP)
        self.assertFalse(result['run_all'])
        self.assertEqual(result['affected'], [])

    def test_change_confined_to_subproject_with_no_dependents(self):
        result = compute_affected(
            ['fdb-record-layer-lucene/src/main/java/Foo.java'], SAMPLE_AFFECTED_MAP)
        self.assertFalse(result['run_all'])
        self.assertEqual(result['affected'], ['fdb-record-layer-lucene'])

    def test_change_to_widely_depended_on_subproject(self):
        result = compute_affected(
            ['fdb-test-utils/src/main/java/Foo.java'], SAMPLE_AFFECTED_MAP)
        self.assertFalse(result['run_all'])
        self.assertEqual(result['affected'], SAMPLE_AFFECTED_MAP['fdb-test-utils'])

    def test_multiple_changed_files_union_affected_sets(self):
        result = compute_affected(
            ['fdb-record-layer-lucene/Foo.java', 'fdb-relational-api/Bar.java'],
            SAMPLE_AFFECTED_MAP)
        self.assertFalse(result['run_all'])
        self.assertEqual(
            set(result['affected']),
            set(SAMPLE_AFFECTED_MAP['fdb-record-layer-lucene'])
            | set(SAMPLE_AFFECTED_MAP['fdb-relational-api']))

    def test_project_gradle_affected_sets(self):
        result = compute_affected(['fdb-relational-core/fdb-relational-core.gradle'], SAMPLE_AFFECTED_MAP)
        self.assertFalse(result['run_all'])
        self.assertEqual(result['affected'], SAMPLE_AFFECTED_MAP['fdb-relational-core'])

    def test_no_changed_files(self):
        result = compute_affected([], SAMPLE_AFFECTED_MAP)
        self.assertFalse(result['run_all'])
        self.assertEqual(result['affected'], [])

    def test_mark_run_all_if_all_affected(self):
        result = compute_affected(['fdb-java-annotations/src/main/java/API.java', 'fdb-test-utils/src/test/java/Utils.java'],
              SAMPLE_AFFECTED_MAP)
        self.assertTrue(result['run_all'])
        self.assertEqual(result['affected'], sorted(SAMPLE_AFFECTED_MAP.keys()))

    def test_run_all_if_from_unknown(self):
        result = compute_affected(['new-subproject/src/main/java/Foo.java'],
              SAMPLE_AFFECTED_MAP)
        self.assertTrue(result['run_all'])
        self.assertEqual(result['affected'], sorted(SAMPLE_AFFECTED_MAP.keys()))


class TestComputeMatrixPlan(unittest.TestCase):
    """Tests for compute_matrix_plan()"""

    MATRIX_CANDIDATES = ['fdb-extensions', 'fdb-record-layer-core', 'fdb-record-layer-lucene', 'yaml-tests']

    def test_run_all_selects_every_candidate(self):
        plan = compute_matrix_plan({'run_all': True, 'affected': []}, self.MATRIX_CANDIDATES)
        self.assertEqual(plan['matrix'], sorted(self.MATRIX_CANDIDATES))
        self.assertTrue(plan['run_other_tests'])

    def test_affected_confined_to_candidates(self):
        plan = compute_matrix_plan(
            {'run_all': False, 'affected': ['fdb-record-layer-lucene']}, self.MATRIX_CANDIDATES)
        self.assertEqual(plan['matrix'], ['fdb-record-layer-lucene'])
        self.assertFalse(plan['run_other_tests'])

    def test_affected_outside_candidates_needs_other_tests(self):
        plan = compute_matrix_plan(
            {'run_all': False, 'affected': ['fdb-relational-api']}, self.MATRIX_CANDIDATES)
        self.assertEqual(plan['matrix'], [])
        self.assertTrue(plan['run_other_tests'])

    def test_no_affected_subprojects(self):
        plan = compute_matrix_plan({'run_all': False, 'affected': []}, self.MATRIX_CANDIDATES)
        self.assertEqual(plan['matrix'], [])
        self.assertFalse(plan['run_other_tests'])

    def test_preserves_base_plan_keys(self):
        plan = compute_matrix_plan({'run_all': False, 'affected': ['yaml-tests']}, self.MATRIX_CANDIDATES)
        self.assertFalse(plan['run_all'])
        self.assertEqual(plan['affected'], ['yaml-tests'])


class TestRenderMarkdown(unittest.TestCase):
    """Tests for render_markdown()"""

    def test_renders_base_plan_without_matrix_fields(self):
        text = render_markdown({'run_all': True, 'affected': ['fdb-record-layer-lucene']})
        self.assertIn('### CI Plan', text)
        self.assertIn('All tests need to be run: `true`', text)
        self.assertIn('Affected subprojects: `fdb-record-layer-lucene`', text)
        self.assertNotIn('individual jobs', text)

    def test_renders_matrix_fields_when_present(self):
        text = render_markdown({
            'run_all': False,
            'affected': ['fdb-record-layer-lucene'],
            'matrix': ['fdb-record-layer-lucene'],
            'run_other_tests': False,
        })
        self.assertIn('Subprojects to test in individual jobs: `fdb-record-layer-lucene`', text)
        self.assertIn('Remaining subprojects tested in a combined job: `false`', text)

    def test_renders_none_placeholder_for_empty_lists(self):
        text = render_markdown({'run_all': False, 'affected': []})
        self.assertIn('Affected subprojects: `(none)`', text)

    def test_output_ends_with_single_newline(self):
        text = render_markdown({'run_all': False, 'affected': []})
        self.assertTrue(text.endswith('\n'))
        self.assertFalse(text.endswith('\n\n'))


class TestMainEndToEnd(unittest.TestCase):
    """End-to-end tests using main() against fixture files, with captured output."""

    def setUp(self):
        self.deps_file = tempfile.NamedTemporaryFile(
            mode='w', suffix='.json', delete=False)
        self.deps_file.write(SAMPLE_AFFECTED_MAP_TEXT)
        self.deps_file.close()

        self.changed_file = tempfile.NamedTemporaryFile(
            mode='w', suffix='.txt', delete=False)
        self.changed_file.write('fdb-record-layer-lucene/src/main/java/Foo.java\n')
        self.changed_file.close()

        self.output_file = tempfile.NamedTemporaryFile(
            mode='w', suffix='.json', delete=False)
        self.output_file.close()

    def tearDown(self):
        os.unlink(self.deps_file.name)
        os.unlink(self.changed_file.name)
        os.unlink(self.output_file.name)

    def test_main_runs_without_error(self):
        from io import StringIO
        from unittest.mock import patch

        from affected_subprojects import main

        with patch('sys.stdout', new_callable=StringIO) as mock_out:
            main([self.deps_file.name, '--changed-files-file', self.changed_file.name,
                  '--output', self.output_file.name])

        self.assertIn('### CI Plan', mock_out.getvalue())
        with open(self.output_file.name, encoding='utf-8') as f:
            result = json.load(f)
        self.assertFalse(result['run_all'])
        self.assertEqual(result['affected'], ['fdb-record-layer-lucene'])

    def test_main_missing_deps_file_fails_safe(self):
        from io import StringIO
        from unittest.mock import patch

        from affected_subprojects import main

        with patch('sys.stdout', new_callable=StringIO):
            main(['nonexistent/path/subproject_deps.json',
                  '--changed-files-file', self.changed_file.name,
                  '--output', self.output_file.name])

        with open(self.output_file.name, encoding='utf-8') as f:
            result = json.load(f)
        self.assertTrue(result['run_all'])

    def test_main_with_matrix_candidates(self):
        from io import StringIO
        from unittest.mock import patch

        from affected_subprojects import main

        with patch('sys.stdout', new_callable=StringIO) as mock_out:
            main([self.deps_file.name, '--changed-files-file', self.changed_file.name,
                  '--matrix-candidates', '["fdb-record-layer-lucene", "yaml-tests"]',
                  '--output', self.output_file.name])

        self.assertIn('individual jobs', mock_out.getvalue())
        with open(self.output_file.name, encoding='utf-8') as f:
            result = json.load(f)
        self.assertEqual(result['matrix'], ['fdb-record-layer-lucene'])
        self.assertFalse(result['run_other_tests'])


if __name__ == '__main__':
    unittest.main()
