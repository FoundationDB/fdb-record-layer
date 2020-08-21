#
# plannerRepl.sh
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

#!/bin/bash

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
DIR="$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )"

shopt -s nullglob
REPL_JAR_GLOB=$DIR/../fdb-record-layer-core/.out/libs/fdb-record-layer-core-*-SNAPSHOT-repl.jar

JARS=( $REPL_JAR_GLOB )
if (( ${#JARS[@]} )); then
    if [ ${#JARS[@]} -eq 1 ]; then
      echo "resolved REPL jar " ${JARS[0]}
      java -jar ${JARS[0]} $@
    else
      echo "Ambiguous number of REPL jars: " $JARS
      echo "Clean up .out!"
      exit 1
    fi
else
    echo "$REPL_JAR does not exist."
    echo "run ./gradlew gw :fdb-record-layer-core:replJar"
    exit 1
fi