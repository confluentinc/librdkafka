# RPM packages for librdkafka

## Build with Mock on docker

From the librdkafka top-level directory:

    $ packaging/rpm/mock-on-docker.sh

Wait for packages to build, they will be copied to top-level dir artifacts/

Test the packages:

    $ packaging/rpm/tests/test-on-docker.sh

