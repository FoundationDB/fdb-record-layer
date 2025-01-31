#!/usr/bin/env python3

#
# commit_yamsql_updates.py
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

#
# Utility script for committing the updates to yamsql as part of release.
#
# As part of a release we should update the *.yamsql files, replacing !current_version
# with the version being released. This is done via a gradle task: updateYamsql
# This script will check that there are no extraneous changes staged, and commit
# the updates to the yamsql files.
#

import argparse
import subprocess
import sys


def main(argv):
    '''Replace !current_version in yamsql files with the provided version'''
    parser = argparse.ArgumentParser()
    parser.add_argument('new_version', help='New version to use when updating the yamsql files')
    args = parser.parse_args(argv)

    process = subprocess.run(['git', 'status', '--porcelain=v1', '--untracked=no', '--no-renames'],
                             check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    should_commit = False
    for line in process.stdout.splitlines():
        indexState = line[0]
        if indexState != ' ':
            if line.endswith('.yamsql'):
                should_commit = True
            else:
                print("Unexpected change: " + line)
                exit(1)

    if should_commit:
        print("Updating !current_version in yamsql files to " + args.new_version)
        subprocess.run(['git', 'commit', '-m', "Updating !current_version in yamsql files to " + args.new_version],
                       check=True)
    else:
        print("Nothing to commit")

if __name__ == '__main__':
    main(sys.argv[1:])
