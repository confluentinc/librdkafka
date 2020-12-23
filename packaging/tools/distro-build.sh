#!/bin/bash
#
# Build librdkafka for different distros to produce distro-specific artifacts.
# Requires docker.
#

set -e

distro=$1
shift
config_args=$*

case $distro in
    centos)
        if [[ -n $config_args ]]; then
            echo "Warning: configure arguments ignored for centos RPM build"
        fi
        packaging/rpm/mock-on-docker.sh
        packaging/rpm/tests/test-on-docker.sh
        ;;
    debian)
        docker run -it -v "$PWD:/v" microsoft/dotnet:2-sdk /v/packaging/tools/build-debian.sh /v /v/artifacts/librdkafka-debian9.tgz $config_args
        ;;
    alpine)
        packaging/alpine/build-alpine.sh $config_args
        ;;
    alpine-static)
        packaging/alpine/build-alpine.sh --enable-static --source-deps-only $config_args
        ;;
    *)
        echo "Usage: $0 <centos|debian|alpine|alpine-static>"
        exit 1
        ;;
esac
