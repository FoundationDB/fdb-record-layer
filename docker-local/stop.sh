#! /bin/bash

#
# stop.sh
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2021 Apple Inc. and the FoundationDB project authors
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

set -eu

FDB_VERSION="${FDB_VERSION:-6.3.18}"
FDB_PORT="${FDB_PORT:-14550}"

cd $(dirname $0)
FDB_VERSION=$FDB_VERSION FDB_PORT=$FDB_PORT docker-compose down

echo "Docker-based FDB cluster is now down."
