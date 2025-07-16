#!/usr/bin/env python3
import os
import sys
import subprocess
import signal
import httpx
curr_dir = os.path.dirname(os.path.realpath(__file__))
os.chdir(curr_dir)

# This script is used to run all tests against Confluent Kafka.
#
# If `REST_ENDPOINT`, `CLIENT_KEY`, `CLIENT_SECRET`, `CLUSTER_LKC` are set,
# it will use the Confluent Kafka REST API to set `auto.create.topics.enable`
# to `true` (dedicated clusters only), otherwise it will skip that step.
#
# `num.partitions` cluster property for default partition number
# when auto-creating topics must be `4`.
#
# The script expects the `TEST_KAFKA_VERSION` environment variable to be set
# to the version of Apache Kafka that is compatible with the Confluent Kafka
# version being tested against.
#
# It will always skip local tests, it doesn't start a local clusters and
# requires a `test.conf` file to be present in the current directory instead.

args = sys.argv[1:]

if '-l' in args:
    print('Local tests need to be excluded when running against'
          ' Confluent Kafka', file=sys.stderr)
    sys.exit(1)

if 'TEST_KAFKA_VERSION' not in os.environ:
    print('TEST_KAFKA_VERSION environment variable is not set,'
          ' please set it to the AK compatible version used '
          'in Confluent Kafka',
          file=sys.stderr)
    sys.exit(1)

do_enable_auto_create_topics_enable = all([
    var in os.environ for var in
    ['REST_ENDPOINT', 'CLIENT_KEY', 'CLIENT_SECRET', 'CLUSTER_LKC']
])
if not do_enable_auto_create_topics_enable:
    print('WARNING: Not setting up auto.create.topics.enable for the cluster,'
          ' missing environment variables',
          file=sys.stderr)

if not os.path.exists('test.conf'):
    print('test.conf file does not exist',
          file=sys.stderr)
    sys.exit(1)

# FIXME: verify these skipped tests
TESTS_SKIP = '0054,0081,0113,0122,0129'


def enable_auto_create_topics_enable():
    REST_ENDPOINT = os.environ['REST_ENDPOINT']
    CLUSTER_LKC = os.environ['CLUSTER_LKC']
    CLIENT_KEY = os.environ['CLIENT_KEY']
    CLIENT_SECRET = os.environ['CLIENT_SECRET']

    r = httpx.put(f'{REST_ENDPOINT}/kafka/v3/clusters/{CLUSTER_LKC}'
                  '/broker-configs/auto.create.topics.enable',
                  auth=(CLIENT_KEY, CLIENT_SECRET), json={'value': 'true'})
    assert r.status_code == 204, ('Failed to enable auto.create.topics.enable'
                                  f': {r.status_code} {r.text}')


def run_tests():
    if do_enable_auto_create_topics_enable:
        enable_auto_create_topics_enable()

    interrupted = False
    p = subprocess.Popen(['./run-test-batches.py', '-L', '-p1'] + args,
                         env={'CI': 'true',
                              'TESTS_SKIP': TESTS_SKIP,
                              **os.environ},
                         start_new_session=True)
    try:
        p.communicate()
        return p.returncode
    except BaseException:
        interrupted = True
        return 1
    finally:
        if interrupted:
            print('Terminating process group...', file=sys.stderr)
            os.killpg(p.pid, signal.SIGINT)
            try:
                p.wait(10)
            except subprocess.TimeoutExpired:
                os.killpg(p.pid, signal.SIGKILL)
                p.wait(10)


error = run_tests()
print('End of run-tests-confluent-kafka', file=sys.stderr)
if error:
    sys.exit(error)
