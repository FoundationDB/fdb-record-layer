#!/bin/bash -eu

#
# update_release_notes.bash
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
# The heavy lifting of this script is handled by the update_release_notes.py
# script. This wraps that script and also makes a few calls to git to make
# sure that its effects are made durable.
#

script_dir="$( dirname $0 )"
success=0

release_notes_file="${script_dir}/../docs/sphinx/source/ReleaseNotes.md"

if [[ -n "${GIT_BRANCH:-}" ]] ; then
    branch="${GIT_BRANCH#*/}"
    if ! git checkout "${branch}" ; then
        echo "Could not check out appropriate git branch."
        let success="${success} + 1"
    else
        echo "Branch updated to ${branch}."
    fi
fi

if [[ ! -f "${release_notes_file}" ]] ; then
    echo "Could not find release notes file at the following path: ${release_notes_file}"
    let success="${success} + 1"

elif [[ -z "${ARTIFACT_VERSION:-}" ]] ; then
    echo "The ARTIFACT_VERSION is unset. Set this variable to the current Record Layer release version."
    let success="${success} + 1"

elif ! python "${script_dir}/update_release_notes.py" "${release_notes_file}" "${ARTIFACT_VERSION}" --overwrite ; then
    echo "Failed to overwrite release notes!"
    let success="${success} + 1"

elif ! git add "${release_notes_file}" ; then
    echo "Failed to add updated release notes to git"
    let success="${success} + 1"

elif ! git commit -m "Release notes updated for version ${ARTIFACT_VERSION}" ; then
    echo "Failed to commit release notes changes"
    let success="${success} + 1"

else
    echo "Release notes updated for version ${ARTIFACT_VERSION}"
fi

exit "${success}"
