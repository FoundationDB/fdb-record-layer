#!/bin/bash

#
# fdb_docker_start.bash
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
# FoundationDB cluster file creation script
#
# This script will create and optionally validate the
# the cluster file for a FoundationDB server. This cluster
# file is designed to work with the clusters created within
# a docker compose network such as the one created by
# fdb_docker_start.bash.
#

# Defines
#
SCRIPTDIR=$( cd "${BASH_SOURCE[0]%\/*}" && pwd )
TZ="${TZ:-America/Los_Angeles}"
CWD=$(pwd)
OSNAME="$(uname -s)"
REQUIREDBINS=( 'which' 'getent' 'awk' 'fdbcli' )
FDBCLUSTER='/etc/foundationdb/fdb.cluster'
FDBCLUSTERPREFIX="foundationdb:foundationdb"
FDBHOSTNAME="${FDBHOSTNAME:-fdbserver}"
FDBPORT="${FDBPORT:-4500}"
VERSION='1.1'

# Display syntax
if [ "$#" -lt 1 ] || [ "${1}" -le 0 ]
then
	echo 'fdb_create_cluster.bash <Process 0|1|2> [FDB Server]'
	echo '   Process Options:'
	echo '      0       - Display this help'
	echo '      1       - Configure the cluster file from the environment'
	echo '      2       - Configure and verify the cluster file from the environment'
	echo ''
	echo '   Environment Variables:'
	echo '      FDBPORT     -  Port on which the server should run (4500 by default)'
	echo '      FDBHOSTNAME -  Hostname of FoundationDB server'
	echo ''
	echo "   version: ${VERSION}"
	exit 1
fi

# Read the original arguments
startopt="${1}"

# Define the FDB Server, if passed as argument or environment variable
if [ "$#" -gt 1 ]; then FDBHOSTNAME="${2}"; fi

function displayMessage
{
	local status=0

	if [ "$#" -lt 1 ]
	then
		echo 'displayMessage <message>'
		let status="${status} + 1"
	else
		# Increment the message counter
		let messagecount="${messagecount} + 1"

		# Display successful message, if previous message
		if [ "${messagecount}" -gt 1 ]
		then
			# Determine the amount of transpired time
			let timespent="${SECONDS}-${messagetime}"

			printf '... done in %3d seconds\n' "${timespent}"
		fi

		# Display message
		printf '%-16s      %-35s ' "$(TZ=${TZ} date '+%F %H-%M-%S')" "$1"

		# Update the variables
		messagetime="${SECONDS}"
	fi

	return "${status}"
}

# The following function will verify that the required binaries are present on the systems
checkRequiredBins()
{
	local status=0
	local errors=()

	# Ensure that the required binaries are present
	for binary in "${REQUIREDBINS[@]}"
	do
		# Ensure that the binary is in the path or is the full path
		if [ ! -f "${binary}" ] && ! which "${binary}" &> /dev/null
		then
			# Store the missing binary
			errors+=("${binary}")

			# Increment the error counter
			let status="${status} + 1"
		fi
	done

	# Report on the missing required binaries, if any
	if [ "${#errors[@]}" -gt 0 ]
	then
		printf 'Unable to build solution without %d required binaries' "${#errors[@]}"
		printf '\n    %s' "${errors[@]}"
		echo ''
	fi

	return "${status}"
}


# Initialize the variables
status=0
messagetime=0
messagecount=0
logdir="/tmp/fdbDocker/$$"
fdbserver=$(getent hosts "${FDBHOSTNAME}" | awk '{ print $1 }')

# Create the cluster connection string and write to the cluster file
fdbtext="${FDBCLUSTERPREFIX}@${fdbserver}:${FDBPORT}"
echo "${fdbtext}" > "${FDBCLUSTER}"

printf '%-16s  %-40s \n'		"$(TZ=${TZ} date '+%F %H-%M-%S')" "Creating FoundationDB Cluster"
printf '%-20s     FDB Host:     %-40s \n'	'' "${FDBHOSTNAME}"
printf '%-20s     FDB Port:     %-40s \n'	'' "${FDBPORT}"
printf '%-20s     FDB IP:       %-40s \n'	'' "${fdbserver}"
printf '%-20s     Cluster:      %-40s \n'	'' "${fdbtext}"
printf '%-20s     Cluster File: %-40s \n'	'' "${FDBCLUSTER}"
printf '%-20s     Option:       %-40s \n'	'' "${startopt}"
printf '%-20s     OS:           %-40s \n'	'' "${OSNAME}"
printf '%-20s     Version:      %-40s \n'	'' "${VERSION}"

echo ''

# Ensure that the required binaries are present
if ! checkRequiredBins
then
	echo "Missing required binaries."
	exit 1

# Ensure that the FDB Configuration file is present
elif [ ! -f "${FDBCLUSTER}" ]
then
	echo "Missing FoundationDB cluster file: ${FDBCLUSTER}"
	exit 1

elif [ "${startopt}" == 2 ] && ! displayMessage 'Create log directories'
then
	echo 'Failed to display user message'
	let status="${status} + 1"

# Ensure that the log directory is created
elif [ "${startopt}" == 2 ] && [ ! -d "${logdir}" ] && ! mkdir -p "${logdir}"
then
	let timespent="${SECONDS}-${messagetime}"
	echo "... failed in ${timespent} seconds to create log directory: ${logdir}"
	let status="${status} + 1"

elif [ "${startopt}" == 2 ] && ! displayMessage 'Validating Local FDB Access'
then
	echo 'Failed to display user message'
	let status="${status} + 1"

# Validate that one can connect to the FoundationDB cluster by trying to get status
elif [ "${startopt}" == 2 ] && ! fdbcli --exec 'status' &> "${logdir}/service-fdbstatus.log"
then
	let timespent="${SECONDS}-${messagetime}"
	echo "... failed in ${timespent} seconds to validate FDB database"
	let status="${status} + 1"

else
	let timespent="${SECONDS}-${messagetime}"
	printf '... done in %3d seconds\n' "${timespent}"
fi

if [ "${status}" -eq 0 ]
then
	echo ''
	printf '%-16s  Successfully created FoundationDB cluster file: %s in %d seconds.\n'	"$(TZ=${TZ} date '+%F %H-%M-%S')" "${FDBCLUSTER}" "${SECONDS}"

else
	echo ''
	printf '%-16s  Failed to create FoundationDB cluster file: %s.\nPlease check log directory: %s\n'	"$(TZ=${TZ} date '+%F %H-%M-%S')" "${FDBCLUSTER}" "${SECONDS}" "${logdir}"
fi

exit "${status}"
