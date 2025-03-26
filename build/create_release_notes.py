#!/usr/bin/env python3

#
# create_release_notes.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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


import argparse
from collections import defaultdict
import json
import re
import subprocess
import sys

class Version:
    minor_header = re.compile(r'^## (\d+\.\d)$')  # match all version headers, including major or minor headers
    precise_header = re.compile(r'^### (\d+\.\d+\.\d+\.\d+)$')  # match precise version headers

    def __init__(self, version: str):
        self.version = version
        self.version_split = [int(part) for part in self.version.split('.')]

    def is_greater(self, other: str) -> bool:
        version = [int(part) for part in other.split('.')]
        for (s, h) in zip(self.version_split, version):
            if s > h:
                return True
            if s < h:
                return False
        return len(self.version_split) < len(version)

    def get_old_version(self, lines: list[str]) -> str:
        for line in lines:
            result = self.precise_header.match(line)
            if result:
                test_version = result[1]
                if self.is_greater(test_version):
                    return test_version
        raise Exception(f'Could not find previous version for {self.version}')

    def greater_than_precise_version_header(self, line: str) -> bool:
        result = self.precise_header.match(line)
        return result and self.is_greater(result[1])

    def greater_than_minor_version_header(self, line: str) -> bool:
        result = self.minor_header.match(line)
        if result:
            test_version = result[1]
            if self.is_greater(test_version):
                return True
        return False

    def minor_version_header(self) -> str:
        return '## ' + '.'.join([str(v) for v in self.version_split[:2]])

    def precise_version_header(self) -> str:
        return '### ' + self.version

    def precise_version(self) -> str:
        return self.version

def run(command: list[str]) -> str:
    ''' Run a command, returning its output
    If the command fails, the output will be dumped, and an exception thrown
    '''
    try:
        process = subprocess.run(command, check=True, capture_output=True, text=True)
        return process.stdout
    except subprocess.CalledProcessError as e:
        print("Failed: " + str(e.cmd))
        print(e.stdout)
        print(e.stderr)
        raise Exception(f"Failed to run command ({e.returncode}): {e.cmd}")

Commit = list[str] # Type hint

def get_commits(old_version: str, new_version: str, skip_commits: list[str]) -> list[Commit]:
    '''Return a list of tuples for each commit between the two versions
    The tuples will be (hash, commit subject)
    '''
    raw_log = run(['git', 'log', '--pretty=%H %s', old_version + ".." + new_version])
    commit_pairs = [commit.split(' ', maxsplit=1) for commit in raw_log.splitlines()]
    return [commit for commit in commit_pairs if not commit[0] in skip_commits]

def get_pr(commit_hash: str, pr_cache: str, repository: str) -> list[dict]:
    '''Get the PRs associated with the given commit hash.
    pr_cache: A path to a directory to cache PR results, or None.
    This will return the raw pr info from github associated with the commit,
    parsed into python objects.
    '''
    if (pr_cache is not None):
        try:
            with open(pr_cache + "/" + commit_hash + ".json", 'r') as fin:
                return json.load(fin)
        except:
            pass
    raw_info = run(['gh', 'api', f'/repos/{repository}/commits/{commit_hash}/pulls'])
    info = json.loads(raw_info)
    if (pr_cache is not None):
        with open(pr_cache + "/" + commit_hash + ".json", 'w') as fout:
            json.dump(info, fout)
    return info

def get_prs(commits: list[Commit], pr_cache: str, repository: str) -> list[tuple[list[dict], Commit]]:
    ''' Return all the prs for the given commits, optionally using the cache.
    pr_cache: A path to a directory to cache PR results or None.
    Returns a list of tuple pairs, the first being the PR info, and the second
    being the commit.
    '''
    return [(get_pr(commit[0], pr_cache, repository), commit) for commit in commits]

def dedup_prs(prs: list[tuple[list[dict], Commit]]) -> list[tuple[list[dict], Commit]]:
    ''' Remove any duplicate prs from the list based on urls.
    The prs should be the output of get_prs.
    Returns a list with the first pair that references a given
    pr.
    '''
    found = set()
    dedupped = []
    for pr, commit in prs:
        if pr is None or len(pr) == 0:
            print("No PR: " + str(commit))
        else:
            if len(pr) != 1:
                print("Too many PRs " + str(pr) + " " + str(commit))
            else:
                if not pr[0]['url'] in found:
                    found.add(pr[0]['url'])
                    dedupped.append((pr, commit))
    return dedupped

def get_category(pr: dict, label_config: dict, commit: Commit) -> str:
    ''' Get the appropriate category based on the labels in the given pr.'''
    main_label = None
    label_names = [label['name'] for label in pr['labels']]
    for category in label_config['categories']:
        for label in category['labels']:
            if label in label_names:
                return category['title']
    print(f"No label: {pr['html_url']} {str(label_names)} {str(commit)}")
    return label_config['catch_all']

def generate_note(prs: list[dict], commit: Commit, label_config: dict) -> tuple[str, str]:
    ''' Generate a release note for a single category, returning a pair of
    the category for the note, and the text as markdown '''
    if len(prs) == 0:
        return ("Direct Commit", commit[1])
    if len(prs) > 1:
        print("Too many PRs?")
    pr = prs[0]
    category = get_category(pr, label_config, commit)
    text = f'* {pr["title"]} - ' + \
        f'[PR #{pr["number"]}]({pr["html_url"]})'
    return (category, text)

def format_notes(notes: list[tuple[str, str]], label_config: dict,
                 old_version: str, new_version: str, repository: str, mixed_mode_results: str) -> str:
    ''' Format a list of notes for the changes between the two given versions '''
    grouping = defaultdict(list)
    for (category, line) in notes:
        grouping[category].append(line)
    text = ''
    categories = label_config['categories'] + [{"title": label_config['catch_all']}]
    for category in categories:
        title = category['title']
        if title in grouping:
            notes = grouping[title]
            put_in_summary = category.get('collapsed', False)
            # We use <h4> rather than #### because #### confuses sphinx
            if put_in_summary:
                text += f"\n<details>\n<summary>\n\n<h4> {title} (click to expand) </h4>\n\n</summary>\n\n"
            else:
                text += f'<h4> {title} </h4>\n\n'
            for note in notes:
                text += f"{note}\n"
            if put_in_summary:
                text += '\n</details>\n'
    text += f"\n\n**[Full Changelog ({old_version}...{new_version})](https://github.com/{repository}/compare/{old_version}...{new_version})**"
    if mixed_mode_results is not None:
        text += f"\n\n{mixed_mode_results}\n"
    return text

def replace_note(lines: list[str], version, note: str) -> list[str]:
    ''' Insert the given formatted release notes in to ReleaseNotes.md
    at the appropriate location.
    lines: The contents of ReleaseNotes.md split into lines
    note: The release notes for a given version
    Returns a new lits of lines, with the new notes inserted'''
    new_lines = []
    added = False
    for line in lines:
        if not added and version.greater_than_precise_version_header(line):
            new_lines.append(version.precise_version_header())
            new_lines.append('')
            new_lines.append(note)
            new_lines.append('')
            new_lines.append(line)
            added = True
        elif not added and version.greater_than_minor_version_header(line):
            new_lines.append(version.minor_version_header())
            new_lines.append('')
            new_lines.append(version.precise_version_header())
            new_lines.append('')
            new_lines.append(note)
            new_lines.append('')
            new_lines.append(line)
            added = True
        else:
            new_lines.append(line)
    if not added:
        raise Exception(f"Could not find spot for {version}")
    return new_lines

def read_notes(filename: str) -> list[str]:
    ''' Read the provided file, and split by line '''
    with open(filename, 'r') as fin:
        return fin.read().split('\n')

def replace_notes(lines: list[str], filename: str, version: Version , note):
    ''' Insert the given release notes into the given file '''
    lines = replace_note(lines, version, note)
    with open(filename, 'w') as fout:
        fout.write('\n'.join(lines))
    print(f'Updated {filename} with new release notes from {version.precise_version()}')

def commit_release_notes(filename: str, version: str):
    ''' Commit the updates to the release notes '''
    message = f"Updating release notes for {version}"
    subprocess.run(['git', 'commit', '-m', message, filename], check=True)

def main(argv: list[str]):
    '''Replace placeholder release notes with the final release notes for a version.'''
    parser = argparse.ArgumentParser()
    parser.add_argument('--pr-cache', help='dump associated prs to json, or read from them (used in testing)')
    parser.add_argument('--config', required=True, help="path to json configuration for release notes")
    parser.add_argument('--release-notes-md', required=True, help="path to ReleaseNotes.md to update")
    parser.add_argument('--commit', action='store_true', default=False, help="Commit the updates to the release notes")
    # The below option is necessary because during the release we will have changed the version, and committed
    # that, but not pushed it
    parser.add_argument('--skip-commit', action='append', default=[],
                        help="If provided, skip this commit (can be repeated)")
    parser.add_argument('--repository', default='FoundationDB/fdb-record-layer', help="Repository")
    parser.add_argument('--mixed-mode-results',
                        help="Path to markdown for mixed mode test results for this version; " +
                        "only available when there's 1 new_version")
    parser.add_argument('--version', required=True, help='New version to use when generating release notes.')
    args = parser.parse_args(argv)

    with open(args.config, 'r') as fin:
        label_config: dict = json.load(fin)

    old_release_notes = read_notes(args.release_notes_md)
    version = Version(args.version)
    old_version: str = version.get_old_version(old_release_notes)
    mixed_mode_results = None
    if args.mixed_mode_results is not None:
        with open(args.mixed_mode_results, 'r') as fin:
            mixed_mode_results = fin.read()
    print(f'Generating release notes from {old_version} to {version.precise_version()}')
    commits = get_commits(old_version, version.precise_version(), args.skip_commit)
    prs = get_prs(commits, args.pr_cache, args.repository)
    prs = dedup_prs(prs)
    new_notes: list[tuple[str, str]] = [generate_note(pr[0], pr[1], label_config) for pr in prs]
    note = format_notes(new_notes, label_config, old_version, version.precise_version(), args.repository, mixed_mode_results)
    replace_notes(old_release_notes, args.release_notes_md, version, note)
    if args.commit:
        commit_release_notes(args.release_notes_md, version.precise_version())

if __name__ == '__main__':
    main(sys.argv[1:])

# You can run the tests by doing the following, in this directory:
# python3 -m unittest create_release_notes.py
import unittest

class TestStringMethods(unittest.TestCase):

    def test_is_greater_header(self) -> None:
        for (less, greater) in [
                ("3.9.9.9", "4.0.0.0"),
                ("4.2.1.0", "4.2.1.1"),
                ("4.2.2.0", "4.2.3.0"),
                ("4.2.0.9", "4.2.1.0"),
                ("4.2.0.9", "4.2.0.10"),
                ("4.2.0.8", "4.2.0.9"),
                ("4.2.9.0", "4.2.10.0"),
                ("4.1.8.2", "4.2.0.0"),
                ("4.1.9.0", "4.1"),
                ("4.1.9.0", "4.2"),
                ("4.2", "4.3.0.0")]:
            with self.subTest(x=f'{greater} > {less}'):
                self.assertTrue(Version(greater).is_greater(less))
            with self.subTest(x=f'{less} > {greater}'):
                self.assertFalse(Version(less).is_greater(greater))

    def test_get_old_version(self) -> None:
        for (new, old, content) in [
                ("4.1.10.1", "4.1.10.0", "Some filler\n## 4.1\ncontent\n### 4.1.10.0\n<h4> Stuff </h4>\n### 4.1.9.0"),
                ("4.1.11.0", "4.1.10.0", "Some filler\n## 4.1\ncontent\n### 4.1.10.0\n<h4> Stuff </h4>\n### 4.1.9.0"),
                ("4.2.1.0", "4.1.10.0", "Some filler\n## 4.1\ncontent\n### 4.1.10.0\n<h4> Stuff </h4>\n### 4.1.9.0"),
                ("4.1.9.1", "4.1.9.0", "Some filler\n## 4.1\ncontent\n### 4.1.10.0\n<h4> Stuff </h4>\n### 4.1.9.0")
                ]:
            with self.subTest(new=new, old=old):
                self.assertEqual(old, Version(new).get_old_version(content.split('\n')))

    def test_replace_note(self) -> None:
        for (version, old, new) in [
                ("4.1.10.1", 
                 "Some filler\n## 4.1\ncontent\n### 4.1.10.0\n<h4> Stuff </h4>\n### 4.1.9.0",
                 "Some filler\n## 4.1\ncontent\n### 4.1.10.1\n\nbanana\n\n### 4.1.10.0\n<h4> Stuff </h4>\n### 4.1.9.0"),
                ("4.1.11.0",
                 "Some filler\n## 4.1\ncontent\n### 4.1.10.0\n<h4> Stuff </h4>\n### 4.1.9.0",
                 "Some filler\n## 4.1\ncontent\n### 4.1.11.0\n\nbanana\n\n### 4.1.10.0\n<h4> Stuff </h4>\n### 4.1.9.0"),
                ("4.2.1.0",
                 "Some filler\n## 4.1\ncontent\n### 4.1.10.0\n<h4> Stuff </h4>\n### 4.1.9.0",
                 "Some filler\n## 4.2\n\n### 4.2.1.0\n\nbanana\n\n## 4.1\ncontent\n### 4.1.10.0\n<h4> Stuff </h4>\n### 4.1.9.0"),
                ("4.1.9.1",
                 "Some filler\n## 4.2\n\ncontent\n\n### 4.2.10.0\n\n<h4> Stuff </h4>\n\n## 4.1\n\n### 4.1.9.0",
                 "Some filler\n## 4.2\n\ncontent\n\n### 4.2.10.0\n\n<h4> Stuff </h4>\n\n## 4.1\n\n### 4.1.9.1\n\nbanana\n\n### 4.1.9.0"),
                ("4.1.9.1",
                 "Some filler\n## 4.1\ncontent\n### 4.1.10.0\n<h4> Stuff </h4>\n### 4.1.9.0",
                 "Some filler\n## 4.1\ncontent\n### 4.1.10.0\n<h4> Stuff </h4>\n### 4.1.9.1\n\nbanana\n\n### 4.1.9.0")
                ]:
            with self.subTest(version=version, new=new, old=old):
                self.assertEqual(new, '\n'.join(replace_note(old.split('\n'), Version(version), 'banana')))

    def test_greater_than_precise_version_header(self) -> None:
        for (new_version, line) in [
                ("4.1.1.0", "### 4.0.10.0"),
                ("4.1.3.0", "### 4.1.0.0"),
                ("4.2.1.0", "### 4.1.18.0"),
                ("4.0.1.0", "### 3.8.9.0"),
                ("4.0.1.5", "### 4.0.1.4"),
                ]:
            with self.subTest(new=new_version, line=line):
                self.assertTrue(Version(new_version).greater_than_precise_version_header(line))
        for (old_version, line) in [
                ("3.9.20.0", "### 4.0.0.0"),
                ("4.1.0.0", "### 4.1.3.0"),
                ("4.0.1.0", "### 4.1.0.0"),
                ("4.0.1.0", "### 4.0.1.1"),
                ("4.0.1.0", "### 4.0.2.0")
                ]:
            with self.subTest(old=old_version, line=line):
                self.assertFalse(Version(old_version).greater_than_precise_version_header(line))
        for (old_version, line) in [
                ("3.9.20.0", "Here is some text"),
                ("4.1.3.0", "* boo"),
                ("4.0.1.0", "<h4> far </h4>"),
                ("4.0.1.0", "## 4.0"),
                ("4.0.1.0", "## 3.0"),
                ("4.0.1.0", "## 3.9")
                ]:
            with self.subTest(old=old_version, other_content=line):
                self.assertFalse(Version(old_version).greater_than_precise_version_header(line))

    def test_greater_than_minor_version_header(self) -> None:
        for (new_version, line) in [
                ("4.1.1.0", "## 4.0"),
                ("4.2.1.0", "## 4.1"),
                ("4.0.1.0", "## 3.8"),
                ]:
            with self.subTest(new=new_version, line=line):
                self.assertTrue(Version(new_version).greater_than_minor_version_header(line))
        for (old_version, line) in [
                ("3.9.20.0", "## 4.0"),
                ("4.1.3.0", "## 4.1"),
                ("4.0.1.0", "## 4.1")
                ]:
            with self.subTest(old=old_version, line=line):
                self.assertFalse(Version(old_version).greater_than_minor_version_header(line))
        for (old_version, line) in [
                ("3.9.20.0", "Here is some text"),
                ("4.1.3.0", "* boo"),
                ("4.0.1.0", "<h4> far </h4>")
                ]:
            with self.subTest(old=old_version, other_content=line):
                self.assertFalse(Version(old_version).greater_than_minor_version_header(line))
    def test_minor_version(self) -> None:
        for (minor, full) in [
                ("## 3.9", "3.9.10.0"),
                ("## 3.10", "3.10.8.0"),
                ("## 4.2", "4.2.88.0")
                ]:
            with self.subTest(minor=minor, full=full):
                self.assertEqual(minor, Version(full).minor_version_header())

    def simple_label_config(self) -> dict:
        return {'categories': [], 'catch_all': 'Other'}

    def test_format_empty_notes(self) -> None:
        self.assertEqual('\n\n**[Full Changelog (4.1.8.0...4.2.1.0)](https://github.com/FoundationDB/fdb-record-layer/compare/4.1.8.0...4.2.1.0)**\n\n' +
                         'Mixed Mode Results\n',
                         format_notes([], self.simple_label_config(), '4.1.8.0', '4.2.1.0',
                                      'FoundationDB/fdb-record-layer', 'Mixed Mode Results'))
