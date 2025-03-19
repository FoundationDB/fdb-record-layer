#!/bin/python3
#
# versionutils.py
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
import re
import subprocess
import sys


VERSION_POSITIONS = ['MAJOR', 'MINOR', 'BUILD', 'PATCH']
VERSION_LINE = re.compile(r'version\s*\=\s*(\d+)\.(\d+)\.(\d+)\.(\d+)')


def incremented_version(version: tuple[int, int, int, int], update_type: str) -> tuple[int, int, int, int]:
    update_pos = VERSION_POSITIONS.index(update_type)
    new_version = []
    for i in range(len(version)):
        if i < update_pos:
            new_version.append(version[i])
        elif i == update_pos:
            new_version.append(version[i] + 1)
        else:
            new_version.append(0)
    return tuple(new_version)


def version_string(version: tuple[int, int, int, int]) -> str:
    return '.'.join(map(str, version))


def update_version(filename: str, update_type: str) -> tuple[int, int, int, int]:
    lines = []
    version = None
    new_version = None
    with open(filename, 'r') as fin:
        for l in fin:
            m = VERSION_LINE.match(l)
            if m:
                 if version is not None:
                     raise ValueError('File contains multiple version lines')
                 version = (int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4)))
                 new_version = incremented_version(version, update_type)
                 lines.append(f'version={version_string(new_version)}\n')
            else:
                 lines.append(l)

    if version is None:
        raise ValueError(f'Unable to find version in {filename}')

    with open(filename, 'w') as fout:
        for l in lines:
            fout.write(l)
    print(f'Updated version in {filename} from {version_string(version)} to {version_string(new_version)}')
    return new_version


def get_version(filename: str) -> tuple[int, int, int, int]:
    version = None
    with open(filename, 'r') as fin:
        for l in fin:
            m = VERSION_LINE.match(l)
            if m:
               if version is not None:
                     raise ValueError('File contains multiple version lines')
               version = (int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4)))
    if version is None:
        raise ValueError(f'Unable to find version in {filename}')
    return version


def main(argv: list[str]):
    parser = argparse.ArgumentParser(prog='versionutils',
                                     description='Utility to increment the project version stored in a version file')
    parser.add_argument('filename', type=str, help='File containing version to increment')
    parser.add_argument('--increment', action='store_true', default=False, help='Whether to increment the current version')
    parser.add_argument('-u', '--update-type', type=str, default='BUILD', choices=VERSION_POSITIONS,
                        help='Type of update. Determines which position within the build number is updated')
    parser.add_argument('-c', '--commit', action='store_true', default=False, help='Whether to commit the update or not')

    args = parser.parse_args(argv)
    if args.increment:
        new_version = update_version(args.filename, args.update_type)

        if args.commit:
            subprocess.check_output(['git', 'add', args.filename])
            subprocess.check_output(['git', 'commit', '-m', f'Updating version to {version_string(new_version)}'])
            print('Version update committed')

    else:
       print(version_string(get_version(args.filename)))


if __name__ == '__main__':
    main(sys.argv[1:])
