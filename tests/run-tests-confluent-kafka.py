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

# When these are set we both enable auto.create.topics.enable and perform
# best-effort cleanup of rdkafka test topics at the end of the run.
_do_rest_ops = all([
    var in os.environ for var in
    ['REST_ENDPOINT', 'CLIENT_KEY', 'CLIENT_SECRET', 'CLUSTER_LKC']
])

if not _do_rest_ops:
    print('WARNING: Not setting up auto.create.topics.enable or REST cleanup, '
          'missing environment variables',
          file=sys.stderr)

if not os.path.exists('test.conf'):
    print('test.conf file does not exist',
          file=sys.stderr)
    sys.exit(1)

# FIXME: verify these skipped tests
TESTS_SKIP = '0054,0081,0113,0122,0129'


def _rest_client():
    """Return a cached httpx.Client configured for the Confluent REST API.

    Caller must ensure _do_rest_ops is True.
    """
    # Simple singleton on the module.
    global __rest_client
    try:
        return __rest_client
    except NameError:
        REST_ENDPOINT = os.environ['REST_ENDPOINT'].rstrip('/')
        CLIENT_KEY = os.environ['CLIENT_KEY']
        CLIENT_SECRET = os.environ['CLIENT_SECRET']
        __rest_client = httpx.Client(
            base_url=REST_ENDPOINT,
            auth=(CLIENT_KEY, CLIENT_SECRET),
            timeout=httpx.Timeout(10.0, read=30.0)
        )
        return __rest_client


def enable_auto_create_topics_enable():
    if not _do_rest_ops:
        return

    CLUSTER_LKC = os.environ['CLUSTER_LKC']

    c = _rest_client()
    r = c.put(f'/kafka/v3/clusters/{CLUSTER_LKC}'
              '/broker-configs/auto.create.topics.enable',
              json={'value': 'true'})
    assert r.status_code == 204, ('Failed to enable auto.create.topics.enable'
                                  f': {r.status_code} {r.text}')


def cleanup_test_topics():
    """Best-effort deletion of librdkafka test topics via Confluent REST.

    We currently delete all topics whose name starts with the prefix used by
    the C test helpers (see test_mk_topic_name in tests/testshared.h).
    This is intentionally conservative and non-fatal: failures here will be
    logged but will not fail the test run.
    """
    if not _do_rest_ops:
        return

    CLUSTER_LKC = os.environ['CLUSTER_LKC']
    # Prefix used by test_mk_topic_name() in the C test suite.
    topic_prefix = os.environ.get('TEST_TOPIC_PREFIX', 'rdkafkatest_')

    c = _rest_client()

    try:
        # List topics (v3 API). We assume number of topics is manageable for a
        # single request; pagination can be added if needed.
        r = c.get(f'/kafka/v3/clusters/{CLUSTER_LKC}/topics')
    except Exception as e:  # network/timeout/etc.
        print(f'WARNING: Failed to list topics for cleanup: {e}',
              file=sys.stderr)
        return

    if r.status_code != 200:
        print('WARNING: Failed to list topics for cleanup: '
              f'{r.status_code} {r.text}',
              file=sys.stderr)
        return

    data = r.json()
    topics = data.get('data') or []

    deleted = 0
    for t in topics:
        name = t.get('topic_name') or t.get('name')
        if not name or not name.startswith(topic_prefix):
            continue
        try:
            dr = c.delete(
                f'/kafka/v3/clusters/{CLUSTER_LKC}/topics/{name}')
            if dr.status_code not in (202, 204, 404):
                print('WARNING: Failed to delete topic '
                      f"{name}: {dr.status_code} {dr.text}",
                      file=sys.stderr)
            else:
                deleted += 1
        except Exception as e:
            print(f'WARNING: Exception while deleting topic {name}: {e}',
                  file=sys.stderr)

    print(f'INFO: Topic cleanup via REST deleted {deleted} topics with '
          f'prefix {topic_prefix}',
          file=sys.stderr)


def run_tests():
    if _do_rest_ops:
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

        # Always attempt cleanup at the very end, regardless of interruption.
        cleanup_test_topics()


error = run_tests()
print('End of run-tests-confluent-kafka', file=sys.stderr)
if error:
    sys.exit(error)
