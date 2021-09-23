#!/usr/bin/env python3
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
"""
build.py

wrapper for running gradle to build this project
"""

import collections
import datetime
import os
import shutil
import subprocess
import sys
import time

TEMP_ROOT = ".tmp"
PUBLISH_ROOT = ".dist"

dir_path = os.path.abspath(os.path.dirname(__file__))


def clear(path):
    """

    :param path:
    :return:
    """
    print_with_date("Clearing {0}".format(path))
    if os.path.exists(path):
        shutil.rmtree(path)


def mkdirp(path):
    """

    :param path:
    :return:
    """
    if not os.path.exists(path):
        os.makedirs(path)


def date_string():
    """
    Produce a human-readable date string that includes a UNIX timestamp.
    :return:
    """
    return "{0} ({1})".format(
        datetime.datetime.now().strftime("%a %b %d %H:%M:%S %Y"), str(time.time())
    )


def print_with_date(text):
    """

    :param text:
    :return:
    """
    print("{0}   {1}".format(date_string(), text))


def run_gradle(proto_version, *args):
    """
    Constructs the gradle run path given the gradle args.
    :param proto_version:
    :param args:
    :return:
    """
    return_value = None
    env = dict(os.environ)
    full_args = [
        os.path.join(dir_path, "gradlew"),
        "--console=plain",
        "-b",
        os.path.join(dir_path, "build.gradle"),
    ] + list(args)
    print_with_date(
        "Running gradle build: {0}; Proto version: {1}".format(
            " ".join(full_args), proto_version
        )
    )
    env["PROTO_VERSION"] = str(proto_version)
    proc = subprocess.Popen(full_args, env=env)
    proc.communicate()

    if proc.returncode != 0:
        print_with_date("Could not successfully run gradle build!")
        return_value = False
    else:
        return_value = True
    return return_value


def parse_version():
    """
    This looks in the gradle.properties file to find
    the version (for local builds).
    :return:
    """
    return_value = None
    properties_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "gradle.properties")
    )
    print_with_date("Looking through properties file {0}...".format(properties_path))

    if os.path.exists(properties_path):
        try:
            with open(properties_path, "r") as fin:
                contents = fin.read()
            lines = filter(
                lambda x: len(x) > 0 and x[0] != "#",
                map(str.strip, contents.split("\n")),
            )
            prop_dict = {}
            for line in lines:
                if "=" in line:
                    split = line.find("=")
                    prop_dict[line[:split]] = line[split + 1 :]

            if "version" in prop_dict:
                print_with_date(
                    "Version found in properties file: {0}".format(prop_dict["version"])
                )
                return_value = prop_dict["version"]
            else:
                print_with_date("No version found in properties file.")
                return_value = None
        except Exception as ex:
            print_with_date("Error occurred while parsing file.")
            print(ex)
            return_value = None
    else:
        print("File not found.")
        return_value = None

    return return_value


def get_version(release=False):
    """
    Look for a version number and possibly add things.
    :param release:
    :return:
    """
    if "CODE_VERSION" in os.environ:
        version_base = os.environ["CODE_VERSION"]
    else:
        print_with_date(
            "CODE_VERSION environment variable not set. Looking in properties file..."
        )
        version_base = parse_version()

    if version_base is None:
        print_with_date("Could not determine version number!")
        return None

    if "ARTIFACT_VERSION" in os.environ:
        version = str(os.environ["ARTIFACT_VERSION"])
    elif "RECORD_LAYER_BUILD_NUMBER" in os.environ:
        version = version_base + "." + str(os.environ["RECORD_LAYER_BUILD_NUMBER"])
    else:
        version = version_base

    if not release:
        version = version + "-SNAPSHOT"

    print_with_date("Building version: {0}".format(version))

    return version


def build(release=False, proto2=False, proto3=False, publish=False):
    """
    Move all of the publishable files to a temporary directory.

    :param release:
    :param proto2:
    :param proto3:
    :param publish:
    :return:
    """
    return_value = True
    print_with_date("Running build script within directory: {0}".format(dir_path))

    print_with_date("Clearing temporary directory.")
    shutil.rmtree(os.path.join(dir_path, TEMP_ROOT), ignore_errors=True)

    # Get the correct version.
    version = get_version(release)
    if version is None:
        return_value = False

    # Clear before.
    clear(os.path.join(dir_path, TEMP_ROOT))

    if proto2:
        # Make with protobuf 2.
        success = run_gradle(
            2,
            "clean",
            "build",
            "destructiveTest",
            "-PreleaseBuild={0}".format("true" if release else "false"),
        )
        if not success:
            return_value = False

        if publish:
            success = run_gradle(
                2,
                "artifactoryPublish",
                "-PpublishBuild=true",
                "-PreleaseBuild={0}".format("true" if release else "false"),
            )
            if not success:
                return_value = False

        success = run_gradle(2, "fdb-record-layer-core:clean")
        if not success:
            return_value = False

    if proto3:
        # Make with protobuf 3.
        success = run_gradle(
            3,
            "build",
            "destructiveTest",
            "-PcoreNotStrict",
            "-PreleaseBuild={0}".format("true" if release else "false"),
            "-PpublishBuild={0}".format("true" if publish else "false"),
        )
        if not success:
            return_value = False

        if publish:
            # These are enumerated rather than just using the full project
            # artifactoryPublish command to avoid uploading the fdb-extensions
            # subproject twice. (Note that as overwrite is not supported, doing
            # so would result in the build failing.)
            success = run_gradle(
                3,
                ":fdb-record-layer-core-pb3:artifactoryPublish",
                ":fdb-record-layer-core-pb3-shaded:artifactoryPublish",
                ":fdb-record-layer-icu-pb3:artifactoryPublish",
                ":fdb-record-layer-spatial-pb3:artifactoryPublish",
                ":fdb-record-layer-lucene-pb3:artifactoryPublish",
                "-PcoreNotStrict",
                "-PreleaseBuild={0}".format("true" if release else "false"),
                "-PpublishBuild=true",
            )
            if not success:
                return_value = False

    return return_value


USAGE = """
Usage: python build.py <release|snapshot> [--proto2] [--proto3] [--publish]
Builds the packages for this project. You must specify release or snapshot
The --proto2 flag indicates that the build should use protobuf 2.
The --proto3 flag indicates that the build should use protobuf 3.
The --publish flag indicates that the build should be published upon completion.
"""


def parse_args():
    """

    :return:
    """
    ret = collections.namedtuple(
        "Arguments", ["release", "proto2", "proto3", "publish"]
    )
    ret.proto2 = False
    ret.proto3 = False
    ret.publish = False

    if len(sys.argv) < 2:
        print(USAGE)
        sys.exit(2)

    ret.release = sys.argv[1] == "release"

    for i in range(2, len(sys.argv)):
        arg = sys.argv[i]

        if arg == "--proto2":
            ret.proto2 = True
        elif arg == "--proto3":
            ret.proto3 = True
        elif arg == "--publish":
            ret.publish = True
        else:
            print("Unrecognized argument: {0}".format(arg))
            sys.exit(3)

    # run both proto2 and proto3 by default
    if not ret.proto2 and not ret.proto3:
        ret.proto2 = True
        ret.proto3 = True

    return ret


if __name__ == "__main__":
    print_with_date("Starting build")
    SUCCESS = True
    Args = parse_args()
    SUCCESS = SUCCESS and build(
        release=Args.release,
        proto2=Args.proto2,
        proto3=Args.proto3,
        publish=Args.publish,
    )

    if not SUCCESS:
        print_with_date("Build failed!")
        sys.exit(1)
    else:
        print_with_date("Build finished successfully.")
