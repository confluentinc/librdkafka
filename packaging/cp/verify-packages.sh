#!/bin/bash
#
# Verifies RPM and DEB packages from Confluent Platform
#

base_url=$1

if [[ -z $base_url ]]; then
    echo "Usage: $0 <base-url>"
    exit 1
fi

thisdir="$(dirname $0)"

echo "#### Verifying RPM packages ####"
docker run -v $thisdir:/v centos:7 /v/verify-rpm.sh $base_url
rpm_status=$?

echo "#### Verifying Debian packages ####"
docker run -v $thisdir:/v ubuntu:16.04 /v/verify-deb.sh $base_url
rpm_status=$?
