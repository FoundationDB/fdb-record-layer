#! /bin/bash

#
# start.sh
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

ROOTDIR=$(cd $(dirname $0)/.. && pwd)

if [ $(uname -s) == 'Darwin' ]; then
    OS=macOS
    SO_SUFFIX=dylib
    LIBRARY_PATH=DYLD_LIBRARY_PATH
else
    OS=linux
    SO_SUFFIX=so
    LIBRARY_PATH=LD_LIBRARY_PATH
fi

RUNDIR=${ROOTDIR}/run
mkdir -p ${RUNDIR}

FDB_VERSION="${FDB_VERSION:-6.3.18}"
FDB_PORT="${FDB_PORT:-14550}"

FDB_CLUSTER_FILE=${RUNDIR}/docker.cluster
echo "docker:docker@127.0.0.1:$FDB_PORT" >$FDB_CLUSTER_FILE

FDB_CLI_FILE=${RUNDIR}/fdbcli_${FDB_VERSION}
FDB_LIBRARY_FILE=${RUNDIR}/libfdb_c_${FDB_VERSION}.${SO_SUFFIX}
if [ ! -x ${FDB_CLI_FILE} ]; then
    cd /tmp
    if [ $OS == 'macOS' ]; then
        curl -L -O https://www.foundationdb.com/downloads/${FDB_VERSION}/${OS}/installers/FoundationDB-${FDB_VERSION}.pkg
        pkgutil --expand-full FoundationDB-${FDB_VERSION}.pkg fdbpkg
        mv fdbpkg/FoundationDB-clients.pkg/Payload/usr/local/bin/fdbcli ${FDB_CLI_FILE}
        mv fdbpkg/FoundationDB-clients.pkg/Payload/usr/local/lib/libfdb_c.${SO_SUFFIX} ${FDB_LIBRARY_FILE}
        rm -rf fdbpkg FoundationDB-${FDB_VERSION}.pkg
    else
        curl -L -O https://www.foundationdb.com/downloads/${FDB_VERSION}/${OS}/fdb_${FDB_VERSION}.tar.gz
        tar fx fdb_${FDB_VERSION}.tar.gz
        mv fdb_binaries/fdbcli ${FDB_CLI_FILE}
        rm -rf fdb_binaries fdb_${FDB_VERSION}.tar.gz
    fi
fi

if [ ! -r ${FDB_LIBRARY_FILE} ]; then
    curl -L https://www.foundationdb.com/downloads/${FDB_VERSION}/${OS}/libfdb_c_${FDB_VERSION}.${SO_SUFFIX} -o ${FDB_LIBRARY_FILE}
fi

cd ${RUNDIR}
rm -f fdbcli libfdb_c.${SO_SUFFIX}
ln -s fdbcli_${FDB_VERSION} fdbcli
ln -s libfdb_c_${FDB_VERSION}.${SO_SUFFIX} libfdb_c.${SO_SUFFIX}

cd ../docker-local
FDB_VERSION=$FDB_VERSION FDB_PORT=$FDB_PORT docker-compose up --detach fdb

if ! ${RUNDIR}/fdbcli -C $FDB_CLUSTER_FILE --exec status --timeout 1 ; then
    if ! ${RUNDIR}/fdbcli -C $FDB_CLUSTER_FILE --exec "configure new single memory ; status" --timeout 10 ; then 
        echo "Unable to configure new FDB cluster."
        exit 1
    fi
fi

cat >${ROOTDIR}/fdb-environment.properties <<EOF
# docker-local
FDB_CLUSTER_FILE=${FDB_CLUSTER_FILE}
${LIBRARY_PATH}=${RUNDIR}
EOF

cat ${ROOTDIR}/fdb-environment.properties

echo "Docker-based FDB cluster is now up."
