#!/bin/bash

#
# fdb-build.sh
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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
# This script builds a local version of FDB to be used by record layer when in need of a pre-release build, or any other
# build that is not available through the artifact repository
#
# Environment variables:
# FDB_VERSION (Optional, defaults to 7.1.25): The FDB version that the code is on
# OKTETO_USERNAME (Optional, defaults to <username>-dev): The okteto development environment name (this would determine your custom image names)
# JAVA_HOME (Required): Java SDK home
# MVN_HOME (Optional, defaults to /opt/brew/Cellar/maven\@3.5/3.5.4_1/bin): location where maven is instaled
# FOUNDATIONDB_HOME (Required): Top level directory of the FDB that the script is building
# RECORD_LAYER_HOME (Optional, defaults to ..): The top level directory of record layer
# FDB_BUILD_HOME (Required): Local build artifact location
# CMAKE_PREFIX_PATH (Required for Intel Macs): CMAKE search path for libraries. Set to /opt/brew for Intel
#
# This script assumes that the FDB images are available through AWS ECR. The script constructs an expected image name
# and tries to use it. See fdb's build-images.sh for info on how to build and upload FDB images
#

set -e

# Set environment variables
ROOTDIR=$(cd $(dirname $0)/.. && pwd)
FDB_VERSION="${FDB_VERSION:-7.1.25}"
OKTETO_USERNAME="${OKTETO_USERNAME:-`whoami`-dev}"
RECORD_LAYER_HOME="${RECORD_LAYER_HOME:-$ROOTDIR}"
MVN_HOME="${MVN_HOME:-/opt/brew/Cellar/maven@3.5/3.5.4_1/bin}"

if [ -z "$JAVA_HOME" ]
then
  echo "JAVA_HOME environment variable not set"
  exit 1
fi
if [ -z "$FOUNDATIONDB_HOME" ]
then
  echo "FOUNDATIONDB_HOME environment variable not set"
  exit 1
fi
if [ -z "$FDB_BUILD_HOME" ]
then
  echo "FDB_BUILD_HOME environment variable not set"
  exit 1
fi

echo "Environment setup:"
echo "------------------"
echo "FDB_VERSION=$FDB_VERSION"
echo "OKTETO_USERNAME=$OKTETO_USERNAME"
echo "MVN_HOME=$MVN_HOME"
echo "JAVA_HOME=$JAVA_HOME"
echo "FOUNDATIONDB_HOME=$FOUNDATIONDB_HOME"
echo "RECORD_LAYER_HOME=$RECORD_LAYER_HOME"
echo "FDB_BUILD_HOME=$FDB_BUILD_HOME"
sleep 1

if [ "$1" == "clean" ]
then
  echo
  echo "CAUTION"
  echo "Removing everything under $FDB_BUILD_HOME"
  rm -rIf $FDB_BUILD_HOME/*
fi

echo "-----------------------------------------------------"
echo "Building local FDB from source. This may take a while"
cd ${FDB_BUILD_HOME}
cmake -D OPENSSL_ROOT_DIR=/opt/brew/opt/openssl -G Ninja ${FOUNDATIONDB_HOME}
ninja

echo "-----------------------------------------------------"
echo "Copying library artifacts"
cp ${FDB_BUILD_HOME}/packages/bin/fdbcli ${RECORD_LAYER_HOME}/run/fdbcli_${FDB_VERSION}-${OKTETO_USERNAME}
cp ${FDB_BUILD_HOME}/packages/lib/libfdb_c.dylib ${RECORD_LAYER_HOME}/run/libfdb_c_${FDB_VERSION}-${OKTETO_USERNAME}.dylib

echo "-----------------------------------------------------"
echo "Deploying java bindings to local maven repo"
$MVN_HOME/mvn install:install-file \
   -Dfile=${FDB_BUILD_HOME}/packages/fdb-java-${FDB_VERSION}-SNAPSHOT.jar \
   -DgroupId=org.foundationdb \
   -DartifactId=fdb-java \
   -Dversion=${FDB_VERSION}-SNAPSHOT \
   -Dpackaging=jar \
   -DgeneratePom=true

echo "-----------------------------------------------------"
echo "Downloading images from AWS"
aws_account_id=$(aws --output text sts get-caller-identity --query 'Account')
aws_region=us-west-2
registry="${aws_account_id}.dkr.ecr.${aws_region}.amazonaws.com"
aws ecr get-login-password | docker login --username AWS --password-stdin ${registry}
docker pull ${registry}/foundationdb/foundationdb:${FDB_VERSION}-${OKTETO_USERNAME}
docker tag ${registry}/foundationdb/foundationdb:${FDB_VERSION}-${OKTETO_USERNAME} foundationdb/foundationdb:${FDB_VERSION}-${OKTETO_USERNAME}

echo "-----------------------------------------------------"
echo "Done building. Start docker image by issuing the command: " FDB_VERSION=${FDB_VERSION}-${OKTETO_USERNAME} ./start.sh
