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
# FoundationDB Docker Start script
#
# This script will start and configure a docker image
# to allow a single-instance FoundationDB cluster to be
# be accessible within the current host.
#

# Defines
#
SCRIPTDIR=$( cd "${BASH_SOURCE[0]%\/*}" && pwd )
TZ="${TZ:-America/Los_Angeles}"
CWD=$(pwd)
OSNAME="$(uname -s)"
REQUIREDBINS=( 'sed' 'hostname' 'getent' 'awk')
FDBCONF='/etc/foundationdb/foundationdb.conf'
FDBCLUSTER='/etc/foundationdb/fdb.cluster'
FDBDATADIR='/var/lib/foundationdb/data/'
FDBSTARTOPT="${FDBSTARTOPT:-2}"
FDBREPFACTOR="${FDBREPFACTOR:-single}"
CONFIG_DB="${CONFIG_DB:-1}"
RESET_DB="${RESET_DB:-1}"
HOST_IP="${HOST_IP:-127.0.0.1}"
PUBLICIP=$(hostname -I)
PUBLICIP="${PUBLICIP//[[:space:]]/}"

if [[ "${HOST_IP}" == "0.0.0.0" ]] ; then
	HOST_IP="${PUBLICIP}"
fi

FDBHOST="${FDBHOST:-$HOST_IP}"
FDBPORT="${FDBPORT:-4500}"

if [ -n "${FDBHOSTNAME}" ]; then
	FDBHOST=$(getent hosts "${FDBHOSTNAME}" | awk '{ print $1 }')
fi

FDBMONITOR='/usr/lib/foundationdb/fdbmonitor'
FDBCLUSTERPREFIX="foundationdb:foundationdb@${FDBHOST}"
VERSION='1.9'


# Display syntax
if [ "$#" -lt 1 ] || [ "${1}" -le 0 ]
then
	echo 'fdb_docker_start.bash <Process 0|1|2> [Server Port]'
	echo '   Process Options:'
	echo '      0        - Display this help'
	echo '      1        - Configure and start the docker image and exit'
	echo '      2        - Configure and start the docker image and wait'
	echo '      3        - Use the value of the FDBSTARTOPT environment variable'
	echo ''
	echo '   Default Cluster File: (Port may change):'
	echo "      ${FDBCLUSTERPREFIX}:${FDBPORT}"
	echo ''
	echo '   Environment Variables:'
	echo '      FDBPORT  -  Port on which the server should run (4500 by default)'
	echo '      HOST_IP  -  Public IP for the server (127.0.0.1 by default)'
	echo '                  Main interface IP for the server, if 0.0.0.0 is specified'
	echo '      FDBREPFACTOR -  Replication factor for the server (single, double, triple)'
	echo ''
	echo "   version: ${VERSION}"
	exit 1
fi

# Read the original arguments
startopt="${1}"
if [ "${startopt}" == "3" ]; then startopt="${FDBSTARTOPT}"; fi

# Define the revision, if passed as argument or environment variable
if [ "$#" -gt 1 ]; then FDBPORT="${2}"; fi

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


printf '%-16s  %-40s \n'		"$(TZ=${TZ} date '+%F %H-%M-%S')" "Starting FoundationDB Server"
printf '%-20s     Public IP:   %-40s \n'	'' "${PUBLICIP}"
printf '%-20s     Cluster IP:  %-40s \n'	'' "${HOST_IP}"
printf '%-20s     FDB Coord:   %-40s \n'	'' "${FDBHOST}"
printf '%-20s     FDB Port:    %-40s \n'	'' "${FDBPORT}"
printf '%-20s     Replication: %-40s \n'	'' "${FDBREPFACTOR}"
printf '%-20s     Config DB:   %-40s \n'	'' "${CONFIG_DB}"
printf '%-20s     Reset DB:    %-40s \n'	'' "${RESET_DB}"
printf '%-20s     Option:      %-40s \n'	'' "${startopt}"
printf '%-20s     OS:          %-40s \n'	'' "${OSNAME}"
printf '%-20s     Version:     %-40s \n'	'' "${VERSION}"

echo ''


# Ensure that the FDB Configuration file is present
if [ ! -f "${FDBCLUSTER}" ]
then
	echo "Missing FoundationDB cluster file: ${FDBCLUSTER}"
	exit 1

elif [ ! -f "${FDBCONF}" ]
then
echo "Missing FoundationDB configuration file: ${FDBCONF}"
	exit 1

# Ensure that the required binaries are present
elif ! checkRequiredBins
then
	echo "Missing required binaries."
	exit 1
fi

# Create the cluster connection string and store it within the cluster file
fdbclustertext="${FDBCLUSTERPREFIX}:${FDBPORT}"
echo "${fdbclustertext}" > "${FDBCLUSTER}"

if ! displayMessage 'Create work directories'
then
	echo 'Failed to display user message'
	let status="${status} + 1"

# Ensure that the log directory is created
elif [ ! -d "${logdir}" ] && ! mkdir -p "${logdir}"
then
	let timespent="${SECONDS}-${messagetime}"
	echo "... failed in ${timespent} seconds to create log directory: ${logdir}"
	let status="${status} + 1"

elif ! displayMessage 'Configuring Services'
then
	echo 'Failed to display user message'
	let status="${status} + 1"

# Remove any existing data from within the data directory 
elif ! rm -rf "${FDBDATADIR}"
then
	let timespent="${SECONDS}-${messagetime}"
	echo "... failed in ${timespent} seconds to remove FDB data directory: ${FDBDATADIR}"
	let status="${status} + 1"

elif [ "${CONFIG_DB}" -gt 0 ] && ! displayMessage 'Update FDB Configuration'
then
	echo 'Failed to display user message'
	let status="${status} + 1"

# Update public and listen addresses.
elif [ "${CONFIG_DB}" -gt 0 ] && ! sed -i -e "s/public_address = auto:\$ID/public_address = ${HOST_IP}:\$ID/g" -e "s/listen_address = public/listen_address = 0.0.0.0:\$ID/g" -e "s/fdbserver.4500/fdbserver.${FDBPORT}/g" "${FDBCONF}"
then
	let timespent="${SECONDS}-${messagetime}"
	echo "... failed in ${timespent} seconds to update FDB configuration"
	let status="${status} + 1"

elif [ "${RESET_DB}" -le 0 ] && ! displayMessage 'Starting FDB Service'
then
	echo 'Failed to display user message'
	let status="${status} + 1"

# Start fdbserver background process
elif ! "${FDBMONITOR}" "${FDBCONF}" --daemonize &> "${logdir}/fdbmonitor-start.log"
then
	let timespent="${SECONDS}-${messagetime}"
	echo "... failed in ${timespent} seconds to start FDB monitor with conf ${FDBCONF}"
	let status="${status} + 1"

elif [ "${RESET_DB}" -gt 0 ] && ! displayMessage "Configuring FDB Database as ${FDBREPFACTOR}"
then
	echo 'Failed to display user message'
	let status="${status} + 1"

# Configure the database
elif [ "${RESET_DB}" -gt 0 ] && ! fdbcli --exec "configure new ${FDBREPFACTOR} memory" &> "${logdir}/service-fdbreset.log"
then
	let timespent="${SECONDS}-${messagetime}"
	echo "... failed in ${timespent} seconds to reset FDB database"
	let status="${status} + 1"

elif [ "${RESET_DB}" -le 0 ] && ! displayMessage 'Wait 3 seconds for FDB Database'
then
	echo 'Failed to display user message'
	let status="${status} + 1"

elif [ "${RESET_DB}" -le 0 ] && ! sleep 3 &> "${logdir}/service-fdbsleep.log"
then
	let timespent="${SECONDS}-${messagetime}"
	echo "... failed in ${timespent} seconds to wait for FDB database"
	let status="${status} + 1"

elif ! displayMessage 'Validating FDB Database'
then
	echo 'Failed to display user message'
	let status="${status} + 1"

# Validate that the database is up by asking for status
elif ! fdbcli --exec 'status' &> "${logdir}/service-fdbvalidate.log"
then
	let timespent="${SECONDS}-${messagetime}"
	echo "... failed in ${timespent} seconds to reset FDB database"
	let status="${status} + 1"

elif ! displayMessage 'Validating Local FDB Access'
then
	echo 'Failed to display user message'
	let status="${status} + 1"

# Validating FoundationDB service
elif ! fdbcli --exec 'status' &> "${logdir}/service-fdbstatus.log"
then
	let timespent="${SECONDS}-${messagetime}"
	echo "... failed in ${timespent} seconds to reset FDB database"
	let status="${status} + 1"

elif [ "${startopt}" == 2 ] && ! displayMessage "Waiting forever"
then
	echo 'Failed to display user message'
	let status="${status} + 1"

elif [ "${startopt}" == 2 ] && (sleep inf || true)
then
	:

else
	let timespent="${SECONDS}-${messagetime}"
	printf '... done in %3d seconds\n' "${timespent}"
fi

if [ "${status}" -eq 0 ]
then
	echo ''
	printf '%-16s  Successfully started FoundationDB server on port: %d in %d seconds.\n'	"$(TZ=${TZ} date '+%F %H-%M-%S')" "${FDBPORT}" "${SECONDS}"

else
	echo ''
	printf '%-16s  Failed to start FoundationDB server using cluster: %s.\nPlease check log directory: %s\n'	"$(TZ=${TZ} date '+%F %H-%M-%S')" "${fdbclustertext}" "${logdir}"
fi

exit "${status}"
