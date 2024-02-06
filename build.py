#!/usr/bin/python
#
# build.py
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

import collections, datetime, os, sys, time, traceback, shutil, subprocess

TEMP_ROOT = '.tmp'
PUBLISH_ROOT = '.dist'

dir_path = os.path.abspath(os.path.dirname(__file__))

def clear(path):
    print_with_date('Clearing {0}'.format(path))
    if os.path.exists(path):
        shutil.rmtree(path)

def mkdirp(path):
    if not os.path.exists(path):
        os.makedirs(path)

# Produce a human-readable date string that includes a UNIX timestamp.
def date_string():
    return '{0} ({1})'.format(datetime.datetime.now().strftime('%a %b %d %H:%M:%S %Y'), str(time.time()))

def print_with_date(text):
    print ('{0}   {1}'.format(date_string(), text))

# Constructs the gradle run path given the gradle args.
def run_gradle(*args):
    full_args = [os.path.join(dir_path, 'gradlew'), '--console=plain', '-b', os.path.join(dir_path, 'build.gradle'), ] + list(args)
    print_with_date('Running gradle build: {0}'.format(' '.join(full_args)))
    proc = subprocess.Popen(full_args)
    proc.communicate()

    if proc.returncode != 0:
        print_with_date('Could not successfully run gradle build!')
        return False
    else:
        return True

# This looks in the gradle.properties file to find the version (for local builds).
def parse_version():
    properties_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'gradle.properties'))
    print_with_date('Looking through properties file {0}...'.format(properties_path))

    if os.path.exists(properties_path):
        try:
            with open(properties_path, 'r') as fin:
                contents = fin.read()
            lines = filter(lambda x: len(x) > 0 and x[0] != '#', map(str.strip, contents.split('\n')))
            prop_dict = {}
            for line in lines:
                if '=' in line:
                    split = line.find('=')
                    prop_dict[line[:split]] = line[split+1:]

            if 'version' in prop_dict:
                print_with_date('Version found in properties file: {0}'.format(prop_dict['version']))
                return prop_dict['version']
            else:
                print_with_date('No version found in properties file.')
                return None
        except Exception as e:
            print_with_date('Error occurred while parsing file.')
            print (traceback.format_exc())
            return None
    else:
        print ('File not found.')
        return None

# Look for a version number and possibly add things.
def get_version(release=False):
    if 'CODE_VERSION' in os.environ:
        version_base = os.environ['CODE_VERSION']
    else:
        print_with_date('CODE_VERSION environment variable not set. Looking in properties file...')
        version_base = parse_version()

    if version_base is None:
        print_with_date('Could not determine version number!')
        return None

    if 'ARTIFACT_VERSION' in os.environ:
        version = str(os.environ['ARTIFACT_VERSION'])
    elif 'RECORD_LAYER_BUILD_NUMBER' in os.environ:
        version = version_base + '.' + str(os.environ['RECORD_LAYER_BUILD_NUMBER'])
    else:
        version = version_base

    if not release:
        version = version + '-SNAPSHOT'

    print_with_date('Building version: {0}'.format(version))

    return version

# Move all of the publishable files to a temporary directory.
def build(release=False, publish=False):
    print_with_date('Running build script within directory: {0}'.format(dir_path))

    print_with_date('Clearing temporary directory.')
    shutil.rmtree(os.path.join(dir_path, TEMP_ROOT), ignore_errors=True)

    # Get the correct version.
    version = get_version(release)
    if version is None:
        return False

    # Clear before.
    clear(os.path.join(dir_path, TEMP_ROOT))

    success = run_gradle('clean', 'build', 'destructiveTest',
                         '-PreleaseBuild={0}'.format('true' if release else 'false'))
    if not success:
        return False

    if publish:
        success = run_gradle('artifactoryPublish', '-PpublishBuild=true',
                             '-PreleaseBuild={0}'.format('true' if release else 'false'))
        if not success
            return False

    return True

usage = """
Usage: python build.py <release|snapshot> [--publish]
Builds the packages for this project. You must specify release or snapshot
The --proto2 flag is ignored (previously would use protobuf 2)
The --proto3 flag is ignored (previously would use protobuf 3)
The --publish flag indicates that the build should be published upon completion.
"""
def parse_args():
    ret = collections.namedtuple('Arguments', ['release', 'proto2', 'proto3', 'publish'])
    ret.proto2 = False
    ret.proto3 = False
    ret.publish = False

    if len(sys.argv) < 2:
        print usage
        quit(2)

    ret.release = (sys.argv[1] == 'release')

    for i in range(2, len(sys.argv)):
        arg = sys.argv[i]

        if arg == '--proto2':
            ret.proto2 = True
        elif arg == '--proto3':
            ret.proto3 = True
        elif arg == '--publish':
            ret.publish = True
        else:
            print ('Unrecognized argument: {0}'.format(arg))
            quit(3)

    return ret

if __name__ == "__main__":
    print_with_date('Starting build')
    success = True
    args = parse_args()
    success = success and build(release=args.release, publish=args.publish)

    if not success:
        print_with_date('Build failed!')
        quit(1)
    else:
        print_with_date('Build finished successfully.')
