#!/usr/bin/python3
#
# genpackagelists.py
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
# Download and write the package-lists file for Javadoc generation. These
# files are used during the Javadoc creation process to produce external links
# to Java classes living outside the main project. Storing these files allows
# the project to have offline builds, though it also means that these files
# should be regenerated every so often in order to keep them from getting
# stale. This script automates that process.
#
# Usage:
#   python3 genpackagelists.py
#
# Requires: requests
#

import logging
import os
import requests
import sys


LOG_FORMAT = '%(asctime)s (%(created)f) - [%(levelname)s] %(message)s'

# URLs for the base Javadoc pages for each project
PROJECT_TO_URL_MAP = {
    'java': 'https://docs.oracle.com/javase/8/docs/api/',
    'fdb-java': 'https://apple.github.io/foundationdb/javadoc/',
    'protobuf': 'https://developers.google.com/protocol-buffers/docs/reference/java/',
    'fdb-extensions': 'https://javadoc.io/page/org.foundationdb/fdb-extensions/latest/',
    'fdb-record-layer-core': 'https://javadoc.io/page/org.foundationdb/fdb-record-layer-core/latest/',
    'fdb-test-utils': 'https://javadoc.io/page/org.foundationdb/fdb-test-utils/latest/',
}

# Packages to filter out from each project
EXCLUDED_PACKAGES = {
    'java': {'javax.annotations'},
    'fdb-extensions': {'com.apple.foundationdb', 'com.apple.foundationdb.tuple', 'com.apple.foundationdb.async'},
}


def download_package_list(project):
    '''
    Download the package list for the project of the given name. This will return the contents as a single mulit-line string.
    '''
    url = PROJECT_TO_URL_MAP[project] + 'package-list'
    logging.info('requesting package list %s from %s', project, url)
    r = requests.get(url, allow_redirects=True, timeout=5)

    if r.ok:
        logging.info('download successful for project %s', project)
        contents = r.text
        logging.debug('packages for project %s: %s:', project, contents)

        if project in EXCLUDED_PACKAGES:
            logging.info('removing certain packages from %s', project)
            exclude_set = EXCLUDED_PACKAGES[project]
            packages = filter(lambda package: package not in exclude_set, contents.split('\n'))
            contents = '\n'.join(packages)
        return contents

    else:
        logging.error('Unable to download package list for %s from %s', project, url)
        raise RuntimeException('Received non-OK code {} when retrieving package list from {}'.format(r.status_code, url))


def write_package_list(project, package_list, base_dir='.'):
    '''
    Write the package list file for the project of the given name. It assumes the contents are a single multi-line string.
    By default, it will write its contents to a subdirectory of the current working directory with a name matching
    the project name. The "base_dir" parameter can be used to choose another directory. 
    '''
    project_dir = os.path.abspath(os.path.join(base_dir, project))
    if not os.path.isdir(project_dir):
        os.makedirs(project_dir)
    file_name = os.path.join(project_dir, 'package-list')
    logging.info('writing package list to %s', file_name)
    with open(file_name, 'w') as fout:
        fout.write(package_list)
    logging.info('package list written for project %s', project)


def write_all_package_lists(base_dir='.'):
    '''
    Downloads and writes the package lists into the given directory. By default, it will write each
    project into the current directory. This requires making a network call to download this list
    from each of the projects' Javadoc sites.
    '''
    logging.info('downloading package-list files for all projects')
    for project in PROJECT_TO_URL_MAP.keys():
        package_list = download_package_list(project)
        write_package_list(project, package_list, base_dir=base_dir)
    logging.info('package lists written')


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout, format=LOG_FORMAT)
    write_all_package_lists()
