#!/bin/python

#
# update_release_notes.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

#
# Utility script for updating the release notes as part of a release.
#
# The release notes for the Record Layer should generally be updated as
# features are added. However, as the build number is not known until the
# actual release is cut, the release notes generally include only a reference
# to the NEXT_RELEASE. This script should then be run as soon as the release
# version is known. It updates the release notes file and replaces the
# information for the "next release" with the actual release version and cleans
# up some cruft that is included in the (not yet updated) release section.
# It also adds a fresh version of the release notes template directly above
# where the new release notes have been added in preparation for the following
# release.
#

import argparse
import sys


template = '''### NEXT_RELEASE

* **Bug fix** Fix 1 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Bug fix** Fix 2 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Bug fix** Fix 3 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Bug fix** Fix 4 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Bug fix** Fix 5 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Performance** Improvement 1 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Performance** Improvement 2 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Performance** Improvement 3 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Performance** Improvement 4 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Performance** Improvement 5 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Feature** Feature 1 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Feature** Feature 2 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Feature** Feature 3 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Feature** Feature 4 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Feature** Feature 5 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Breaking change** Change 1 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Breaking change** Change 2 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Breaking change** Change 3 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Breaking change** Change 4 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
* **Breaking change** Change 5 [(Issue #NNN)](https://github.com/FoundationDB/fdb-record-layer/issues/NNN)
'''

def extract_endpoints(lines, name):
    '''Find the beginning and ending line numbers of the section with the given name'''
    start = 0
    template_lines = []
    i = 0
    while i < len(lines) and not lines[i].startswith('// begin ' + name):
        i += 1
    start = i + 1
    while i < len(lines) and not lines[i].startswith('// end ' + name):
        i += 1
    end = i
    return start, end


def get_next_release_endpoints(lines):
    '''Get the beginning and ending line numbers of the section corresponding to the next release.'''
    return extract_endpoints(lines, 'next release') 


def update_next_release_lines(next_release_lines, new_version):
    '''Update the contents of the next release by filling in the version number and removing any placeholder lines.'''
    updated_lines = []
    for line in next_release_lines:
        new_line = line.replace('NEXT_RELEASE', new_version)
        # Placeholder lines have #NNN to indicate where the issue number should go.
        # If they still have that line, then they weren't used.
        if '#NNN' not in new_line:
            updated_lines.append(new_line)
    return updated_lines


def get_new_contents(filename, new_version):
    '''Given the name of the release notes file and the new version, generate the new contents of the file.'''
    with open(filename, 'r') as fin:
        lines = fin.read().split('\n')
    next_release_start, next_release_end = get_next_release_endpoints(lines)
    updated_next_release_notes = update_next_release_lines(lines[next_release_start:next_release_end], new_version)

    return '\n'.join(lines[:next_release_start-2]
        + ['<!--', '// begin next release']
        + [template]
        + ['// end next release', '-->', '']
        + updated_next_release_notes
        + ['', '<!-- MIXED_MODE_RESULTS ' + new_version + ' PLACEHOLDER -->', '']
        + lines[next_release_end+3:])


def main(argv):
    '''Replace placeholder release notes with the final release notes for a version.'''
    parser = argparse.ArgumentParser()
    parser.add_argument('filename', help='Path to release notes document')
    parser.add_argument('new_version', help='New version to use when updating the document')
    parser.add_argument('--overwrite', action='store_true', default=False, help='Overwrite the existing file if set')
    args = parser.parse_args(argv)

    new_contents = get_new_contents(args.filename, args.new_version)
    if args.overwrite:
        with open(args.filename, 'w') as fout:
            fout.write(new_contents)
        print('Updated {} for version {}'.format(args.filename, args.new_version))
    else:
        print(new_contents)


if __name__ == '__main__':
    main(sys.argv[1:])
