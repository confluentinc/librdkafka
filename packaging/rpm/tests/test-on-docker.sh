#!/bin/bash
#
#
# Test librdkafka packages in <rpmdirectory> using docker.
# Must be executed from the librdkafka top-level directory.
#
# Usage:
#   packaging/rpm/test-on-docker.sh [<rpm-dir>]

set -e

if [[ ! -f configure.self ]]; then
    echo "Must be executed from the librdkafka top-level directory"
    exit 1
fi

_DOCKER_IMAGES="centos:6 centos:7 centos:8"
_RPMDIR=artifacts

if [[ -n $1 ]]; then
    _RPMDIR="$1"
    echo "Usage: $0 <path/to/rpmdirectory>"
    exit 1
fi

_RPMDIR=$(realpath $1)

if [[ ! -d $_RPMDIR ]]; then
    echo "$_RPMDIR does not exist"
    exit 1
fi


fails=""
for _IMG in $_DOCKER_IMAGES ; do
    if ! docker run \
         -t \
         -v $_RPMDIR:/rpms \
         -v $(realpath packaging/rpm/tests):/v \
         $_IMG \
         /v/run-test.sh $_IMG ; then
        echo "ERROR: $_IMG FAILED"
        fails="${fails}$_IMG "
    fi
done

if [[ -n $fails ]]; then
    echo "##################################################"
    echo "# Package verification failed for:"
    echo "# $fails"
    echo "# See previous errors"
    echo "##################################################"
    exit 1
fi

exit 0


