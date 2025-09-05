#!/bin/bash
set -e


# Helper script to run all tests through the CI
# Converts the environment variables to the test arguments
# with the following defaults:
#
# - plaintext connection
# - no authentication
# - KRaft enabled
# - all tests
# - default parallelism
# - no assertions
# - following AK and CP versions

if [ "$1" = "coverage_report" ]; then
    artifact pull workflow coverage
    mkdir -p coverage/html
    gcovr --gcov-ignore-parse-errors --exclude src/nanopb/ \
        --exclude src/lz4.*\.c --exclude src/cJSON\.c \
        --exclude src/crc32c\.c --exclude src/snappy\.c \
        --json-add-tracefile "coverage/json/*.json" \
        --html-details coverage/html/coverage.html \
        --html-self-contained
    tar -czf coverage/coverage_report_html.tar.gz coverage/html
    artifact push workflow coverage/coverage_report_html.tar.gz \
        --destination "coverage/coverage_report_html.tar.gz"
    exit 0;
fi

export TEST_KAFKA_GIT_REF=${TEST_KAFKA_GIT_REF:-4.0.0}
export TEST_CP_VERSION=${TEST_CP_VERSION:-7.9.0}

TEST_SSL_ARG=""
TEST_SSL_INTERMEDIATE_CA_ARG=""
TEST_SASL_ARG=""
TEST_KRAFT_ARG="--kraft"
TEST_LOCAL_ARG=""
TEST_CONF_ARG=""
TEST_QUICK_ARG=""
TEST_ASSERT_ARG=""
TEST_PARALLEL_ARG=""

if [ "$TEST_COVERAGE" = "True" ]; then
    if [ "$1" = "aarch64_plaintext" ]; then
        export TEST_CONSUMER_GROUP_PROTOCOL="consumer"
        TEST_SASL="OAUTHBEARER"
        TEST_TRIVUP_PARAMETERS="$TEST_TRIVUP_PARAMETERS --oidc"
    elif [ "$1" = "aarch64_ssl" ]; then
        export TEST_CONSUMER_GROUP_PROTOCOL="consumer"
        TEST_SASL="PLAIN"
    fi
fi

if [ "$TEST_SSL" = "True" ]; then
    TEST_SSL_ARG="--ssl"
fi
if [ "$TEST_SSL" = "True" -a "$TEST_SSL_INTERMEDIATE_CA" = "True" ]; then
    TEST_SSL_ARG="$TEST_SSL_ARG --ssl-intermediate-ca"
fi
if [ ! -z $TEST_SASL ]; then
    TEST_SASL_ARG="--sasl $TEST_SASL"
fi
if [ "$TEST_KRAFT" = "False" ]; then
    TEST_KRAFT_ARG=""
fi
if [ "$TEST_LOCAL" = "Local" ]; then
    TEST_LOCAL_ARG="-l"
fi
if [ "$TEST_LOCAL" = "Non-local" ]; then
    TEST_LOCAL_ARG="-L"
fi
if [ "$TEST_QUICK" = "True" ]; then
    TEST_QUICK_ARG="-Q"
fi
if [ "$TEST_ASSERT" = "True" ]; then
    TEST_ASSERT_ARG="-a"
fi
if [ ! -z $TEST_PARALLEL ]; then
    TEST_PARALLEL_ARG="-p$TEST_PARALLEL"
fi
if [ ! -z $TEST_CONF ]; then
    TEST_CONF_ARG="--conf '$TEST_CONF'"
fi
if [ ! -z $TEST_ENV_VARIABLES ]; then
    IFS=',' read -ra TEST_ENV_VARIABLES_ARRAY <<< "$TEST_ENV_VARIABLES"
    for TEST_ENV_VARIABLE in "${TEST_ENV_VARIABLES_ARRAY[@]}"; do
        export "$TEST_ENV_VARIABLE"
    done
    unset TEST_ENV_VARIABLES_ARRAY
fi

TEST_ARGS="$TEST_PARALLEL_ARG $TEST_ASSERT_ARG $TEST_QUICK_ARG $TEST_LOCAL_ARG $TEST_RUNNER_PARAMETERS $TEST_MODE"
TEST_CONFIGURATION="$TEST_SSL_ARG $TEST_SASL_ARG $TEST_KRAFT_ARG $TEST_CONF_ARG $TEST_TRIVUP_PARAMETERS"

echo "Running tests with:"
echo "kafka branch: $TEST_KAFKA_GIT_REF"
echo "kafka version: $TEST_KAFKA_VERSION"
echo "CP version: $TEST_CP_VERSION"
echo "configuration: $TEST_CONFIGURATION"
echo "arguments: $TEST_ARGS"

# Install requirements for running the tests
wget -O rapidjson-dev.deb https://launchpad.net/ubuntu/+archive/primary/+files/rapidjson-dev_1.1.0+dfsg2-3_all.deb
sudo dpkg -i rapidjson-dev.deb
sudo apt install -y valgrind
python3 -m pip install -U pip
python3 -m pip -V
(cd tests && python3 -m pip install -r requirements.txt)
./configure --install-deps --enable-werror --enable-devel
if [ "$TEST_COVERAGE" = "True" ]; then
    make -j gcovr-build
else
    make -j
fi

# Disable exit on error
set +e
(cd tests && python3 -m trivup.clusters.KafkaCluster $TEST_CONFIGURATION \
--version "$TEST_KAFKA_GIT_REF" \
--cpversion "$TEST_CP_VERSION" \
--cmd "python run-test-batches.py $TEST_ARGS")
RESULT=$?

if [ ! -z "$1" -a "$TEST_COVERAGE" = "True" ]; then
    make gcovr-report
    artifact push workflow tests/coverage.json --destination "coverage/json/coverage_$1.json"
fi

if [ $RESULT -eq 0 ]; then
    echo "All tests passed"
    exit 0
else
    echo "Some tests failed"
    exit 1
fi
