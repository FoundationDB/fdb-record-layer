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

def get_commits(old_version, new_version, skip_commits):
    '''Return a list of tuples for each commit between the two versions
    The tuples will be (hash, commit subject)
    '''
    raw_log = run(['git', 'log', '--pretty=%H %s', old_version + ".." + new_version])
    commit_pairs = [commit.split(' ', maxsplit=1) for commit in raw_log.splitlines()]
    return [commit for commit in commit_pairs if not commit[0] in skip_commits]

def get_pr(commit_hash, pr_cache, repository):
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

def get_prs(commits, pr_cache, repository):
    ''' Return all the prs for the given commits, optionally using the cache.
    pr_cache: A path to a directory to cache PR results or None.
    Returns a list of tuple pairs, the first being the PR info, and the second
    being the commit.
    '''
    return [(get_pr(commit[0], pr_cache, repository), commit) for commit in commits]

def dedup_prs(prs):
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

def get_category(pr, label_config, commit):
    ''' Get the appropriate category based on the labels in the given pr.'''
    main_label = None
    label_names = [label['name'] for label in pr['labels']]
    for category in label_config['categories']:
        for label in category['labels']:
            if label in label_names:
                return category['title']
    print(f"No label: {pr['html_url']} {str(label_names)} {str(commit)}")
    return label_config['catch_all']

def generate_note(prs, commit, label_config):
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

def format_notes(notes, label_config, old_version, new_version, repository, mixed_mode_results):
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
    text += f"\n\n{mixed_mode_results}\n"
    return text

def replace_note(lines, note):
    ''' Insert the given formatted release notes in to ReleaseNotes.md
    at the appropriate location.
    lines: The contents of ReleaseNotes.md split into lines
    note: The release notes for a given version
    Returns a new lits of lines, with the new notes inserted'''
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
    ''' Insert all the given notes into the given file '''
    with open(filename, 'r') as fin:
        lines = fin.read().split('\n')
    for note in notes:
        lines = replace_note(lines, note)
    with open(filename, 'w') as fout:
        fout.write('\n'.join(lines))
    print(f'Updated {filename} with new release notes from {notes[0].old_version} to {notes[-1].new_version}')

def commit_release_notes(filename, new_versions):
    ''' Commit the updates to the release notes '''
    message = f"Updating release notes for {new_versions[-1]}"
    if len(new_versions) > 1:
        message += f"\n\nAlso including versions {' '.join(new_versions[:-1])}"
    subprocess.run(['git', 'commit', '-m', message, filename], check=True)

def get_minor_version(version):
    return '.'.join(version.split('.')[:2])

version_header = re.compile(r'^#+ (\d+(?:\.\d+)+)$') # match all version headers, including major or minor headers
class Note:
    ''' The release notes for a single version bump '''
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
        ''' Check if the new version is greater than the version in the header.
        This will also return false if the header is not a header for a version '''
        result = version_header.match(header)
        if result:
            version = [int(part) for part in result[1].split('.')]
            for (s, h) in zip(self.new_version_split, version):
                if s > h:
                    print(f'"{header}" IS LESS THAN {self.new_version} ({self.old_version}) because {s} > {h}')
                    return True
                if s < h:
                    return False
        return False

def main(argv):
    '''Replace placeholder release notes with the final release notes for a version.'''
    parser = argparse.ArgumentParser()
    parser.add_argument('--pr-cache', help='dump associated prs to json, or read from them (used in testing)')
    parser.add_argument('--config', required=True, help="path to json configuration for release notes")
    parser.add_argument('--release-notes-md', 
                        help="path to ReleaseNotes.md to update, will just print if not provided " +
                        "(printing is only intended for testing)")
    parser.add_argument('--commit', action='store_true', default=False, help="Commit the updates to the release notes")
    # The below option is necessary because during the release we will have changed the version, and committed
    # that, but not pushed it
    parser.add_argument('--skip-commit', action='append', default=[],
                        help="If provided, skip this commit (can be repeated)")
    parser.add_argument('--repository', default='FoundationDB/fdb-record-layer', help="Repository")
    parser.add_argument('--mixed-mode-results',
                        help="Path to markdown for mixed mode test results for this version; " +
                        "only available when there's 1 new_version")
    parser.add_argument('old_version', help='Old version to use when generating release notes')
    parser.add_argument('new_version', nargs='+', 
                        help='New version to use when generating release notes.\n' + 
                        'If multiple values are provided, release notes will be generated for all versions')
    args = parser.parse_args(argv)

    with open(args.config, 'r') as fin:
        label_config = json.load(fin)

    old_version = args.old_version
    release_notes = []
    if len(args.new_version) == 0:
        print("At least one new version must be provided", file=sys.stderr)
        exit(1)
    elif len(args.new_version) > 1 and args.mixed_mode_results is not None:
        print("--mixed-mode-result cannot be provided with more than one new_version", file=sys.stderr)
        exit(1)
    mixed_mode_results = "<!-- MIXED_MODE_RESULTS {new_version} PLACEHOLDER -->"
    if args.mixed_mode_results is not None:
        with open(args.mixed_mode_results, 'r') as fin:
            mixed_mode_results = fin.read()
    for new_version in args.new_version:
        print(f"Generating release notes for {new_version}")
        commits = get_commits(old_version, new_version, args.skip_commit)
        prs = get_prs(commits, args.pr_cache, args.repository)
        prs = dedup_prs(prs)
        new_notes = [generate_note(pr[0], pr[1], label_config) for pr in prs]
        
        release_notes.append(Note(old_version, new_version, format_notes(new_notes, label_config, old_version, new_version, args.repository, mixed_mode_results)))
        old_version = new_version
    print("\n\n------------------------------\n\n")
    if args.release_notes_md is None:
        release_notes.reverse()
        for notes in release_notes:
            print(notes[1])
    else:
        replace_notes(release_notes, args.release_notes_md)
        if args.commit:
            commit_release_notes(args.release_notes_md, args.new_version)

if __name__ == '__main__':
    main(sys.argv[1:])
