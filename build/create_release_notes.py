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

def run(command):
    try:
        process = subprocess.run(command, check=True, capture_output=True, text=True)
        return process.stdout
    except subprocess.CalledProcessError as e:
        print("Failed: " + str(e.cmd))
        print(e.stdout)
        print(e.stderr)
        exit(e.returncode)

def get_commits(old_version, new_version):
    raw_log = run(['git', 'log', '--pretty=%H %s', old_version + "..." + new_version])
    return [commit.split(' ', maxsplit=1) for commit in raw_log.splitlines()]

def get_pr(commit_hash, pr_cache):
    if (pr_cache is not None):
        try:
            with open(pr_cache + "/" + commit_hash + ".json", 'r') as fin:
                return json.load(fin)
        except:
            pass
    raw_info = run(['gh', 'api', f'/repos/FoundationDB/fdb-record-layer/commits/{commit_hash}/pulls'])
    info = json.loads(raw_info)
    if (pr_cache is not None):
        with open(pr_cache + "/" + commit_hash + ".json", 'w') as fout:
            json.dump(info, fout)
    return info

def get_prs(commits, pr_cache):
    return [(get_pr(commit[0], pr_cache), commit) for commit in commits]

def dedup_prs(prs):
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

def get_category(pr, label_config, commit):
    main_label = None
    label_names = [label['name'] for label in pr['labels']]
    for category in label_config['categories']:
        for label in category['labels']:
            if label in label_names:
                return category['title']
    print(f"No label: {pr['html_url']} {str(label_names)} {str(commit)}")
    return label_config['catch_all']

def generate_note(prs, commit, label_config):
    if len(prs) == 0:
        return ("Direct Commit", commit[1])
    if len(prs) > 1:
        print("Too many PRs?")
    pr = prs[0]
    category = get_category(pr, label_config, commit)
    text = '* ' + pr['title'] + " by @" + pr['user']['login'] + ' in ' + pr['html_url']
    return (category, text)

def format_notes(notes, label_config, old_version, new_version):
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
            header = f"<h4> {title} </h4>"
            if put_in_summary:
                text += f"\n<details>\n<summary>\n\n{header} (click to expand)\n\n</summary>\n\n"
            else:
                text += f'{header}\n\n'
            for note in notes:
                text += f"{note}\n"
            if put_in_summary:
                text += '\n</details>\n'
    text += f"\n\n**[Full Changelog](https://github.com/FoundationDB/fdb-record-layer/compare/{old_version}...{new_version})**"
    text += f"\n\n<!-- MIXED_MODE_RESULTS {new_version} PLACEHOLDER -->\n"
    return text

def replace_note(lines, note):
    print(f"Inserting note {note.old_version} -> {note.new_version}")
    new_lines = []
    added = False
    for line in lines:
        if not added and line == note.old_version_header():
            new_lines.append(note.new_version_header())
            new_lines.append('')
            new_lines.append(note.notes)
            new_lines.append('')
            new_lines.append(line)
            added = True
        elif not added and note.greater_than_minor_version(line):
            new_lines.append(note.new_minor_version_header())
            new_lines.append('')
            new_lines.append(note.new_version_header())
            new_lines.append('')
            new_lines.append(note.notes)
            new_lines.append('')
            new_lines.append(line)
            added = True
        else:
            new_lines.append(line)
    if not added:
        raise Exception(f"Could not find note for {note.old_version} -> {note.new_version}")
    return new_lines

def replace_notes(notes, filename):
    with open(filename, 'r') as fin:
        lines = fin.read().split('\n')
    for note in notes:
        lines = replace_note(lines, note)
    with open(filename, 'w') as fout:
        fout.write('\n'.join(lines))
    print(f'Updated {filename} with new release notes from {notes[0].old_version} to {notes[-1].new_version}')

def get_minor_version(version):
    return '.'.join(version.split('.')[:2])

version_header = re.compile('^#+ (\d+(?:\.\d+)+)$') # match only major or minor versions
class Note:
    def __init__(self, old_version, new_version, notes):
        self.old_version = old_version
        self.new_version = new_version
        self.new_version_split = [int(part) for part in self.new_version.split('.')]
        self.notes = notes
    def old_minor_version_header(self):
        return f'## {get_minor_version(self.old_version)}'
    def new_minor_version_header(self):
        return f'## {get_minor_version(self.new_version)}'
    def changes_minor_version(self):
        return get_minor_version(self.old_version) != get_minor_version(self.new_version)
    def old_version_header(self):
        return f'### {self.old_version}'
    def new_version_header(self):
        return f'### {self.new_version}'
    def greater_than_minor_version(self, header):
        result = version_header.match(header)
        if result:
            version = [int(part) for part in result[1].split('.')]
            for (s, h) in zip(self.new_version_split, version):
                if s > h:
                    print(f'"{header}" IS LESS THAN {self.new_version} ({self.old_version})')
                    return True
        return False

def main(argv):
    '''Replace placeholder release notes with the final release notes for a version.'''
    parser = argparse.ArgumentParser()
    parser.add_argument('--pr-cache', help='dump associated prs to json, or read from them')
    parser.add_argument('--config', required=True, help="path to json configuration for release notes")
    parser.add_argument('--release-notes-md', help="path to ReleaseNotes.md to update, will just print if not provided")
    parser.add_argument('old_version', help='Old version to use when generating release notes')
    parser.add_argument('new_version', nargs='+', 
                        help='New version to use when generating release notes.\n' + 
                        'If multiple values are provided, release notes will be generated for all versions')
    args = parser.parse_args(argv)

    with open(args.config, 'r') as fin:
        label_config = json.load(fin)

    old_version = args.old_version
    release_notes = []
    for new_version in args.new_version:
        print(f"Generating release notes for {new_version}")
        commits = get_commits(old_version, new_version)
        prs = get_prs(commits, args.pr_cache)
        prs = dedup_prs(prs)
        new_notes = [generate_note(pr[0], pr[1], label_config) for pr in prs]
        release_notes.append(Note(old_version, new_version, format_notes(new_notes, label_config, old_version, new_version)))
        old_version = new_version
    print("\n\n------------------------------\n\n")
    if args.release_notes_md is None:
        release_notes.reverse()
        for notes in release_notes:
            print(notes[1])
    else:
        replace_notes(release_notes, args.release_notes_md)

if __name__ == '__main__':
    main(sys.argv[1:])
