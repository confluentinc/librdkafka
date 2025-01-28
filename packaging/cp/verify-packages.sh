#!/bin/bash
#
# Verifies RPM and DEB packages from Confluent Platform
#
# Multiarch can be used to verify packages for different architectures
# docker run --rm --privileged multiarch/qemu-user-static --reset -p yes
#
set -e
base_url=$1
multiarch=no
[[ $2 = "yes" ]] && multiarch=yes

if [[ -z $base_url ]]; then
    echo "Usage: $0 <base-url> [ <multiarch> ]"
    echo ""
    echo " <base-url> is the release base bucket URL"
    exit 1
fi

thisdir="$( cd "$(dirname "$0")" ; pwd -P )"

verify_debian() {
    local arch=$2
    if [[ $arch != "" ]]; then
        arch="--platform $arch"
    fi
    docker run $arch -v $thisdir:/v $1 /v/verify-deb.sh $base_url
    deb_status=$?
    if [[ $deb_status == 0 ]]; then
        echo "SUCCESS: Debian based $1 $2 packages verified"
    else
        echo "ERROR: Debian based $1 $2 package verification failed"
        exit 1
    fi
}

verify_rpm() {
    local arch=$2
    if [[ $arch != "" ]]; then
        arch="--platform $arch"
    fi

    docker run $arch -v $thisdir:/v $1 /v/verify-rpm.sh $base_url
    rpm_status=$?
    if [[ $rpm_status == 0 ]]; then
        echo "SUCCESS: RPM $1 $2 packages verified"
    else
        echo "ERROR: RPM $1 $2 package verification failed"
        exit 1
    fi
}

multiarch_arg1=""
multiarch_arg1_description="current architecture"
if [[ $multiarch == "yes" ]]; then
    multiarch_arg1="linux/amd64"
    multiarch_arg1_description=$multiarch_arg1
fi

echo "#### Verifying RPM packages for $multiarch_arg1_description ####"
verify_rpm rockylinux:8 ${multiarch_arg1}
verify_rpm rockylinux:9 ${multiarch_arg1}

echo "#### Verifying Debian packages for $multiarch_arg1_description ####"
verify_debian debian:10 ${multiarch_arg1}
verify_debian debian:11 ${multiarch_arg1}
verify_debian debian:12 ${multiarch_arg1}
verify_debian ubuntu:20.04 ${multiarch_arg1}
verify_debian ubuntu:22.04 ${multiarch_arg1}
verify_debian ubuntu:24.04 ${multiarch_arg1}

if [[ $multiarch == "yes" ]]; then

multiarch_arg2="linux/arm64"

echo "#### Verifying RPM packages for linux/arm64 ####"
verify_rpm rockylinux:8 ${multiarch_arg2}
verify_rpm rockylinux:9 ${multiarch_arg2}

echo "#### Verifying Debian packages for linux/arm64 ####"
verify_debian debian:10 ${multiarch_arg2}
verify_debian debian:11 ${multiarch_arg2}
verify_debian debian:12 ${multiarch_arg2}
verify_debian ubuntu:20.04 ${multiarch_arg2}
verify_debian ubuntu:22.04 ${multiarch_arg2}
verify_debian ubuntu:24.04 ${multiarch_arg2}

fi
