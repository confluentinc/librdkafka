#!/usr/bin/env python
#
#
# Run librdkafka regression tests on different supported broker versions.
#
# Requires:
#  trivup python module
#  Kafka git clone (kafka_path below)
#  gradle in your PATH

from trivup.trivup import Cluster
from trivup.apps.ZookeeperApp import ZookeeperApp
from trivup.apps.KafkaBrokerApp import KafkaBrokerApp

import subprocess
import time
import tempfile
import os


# These versions needs to match git tags or branches
kafka_versions = ['0.8.2.2', '0.9.0.1', 'trunk']

# Path to Kafka git clone
kafka_path='/home/maglun/src/kafka'

# Make tests a bit quieter
test_level=1


def test_version (version):
    """
    @brief Create, deploy and start a Kafka cluster using Kafka \p version
    Then run librdkafka's regression tests.
    """
    
    cluster = Cluster('librdkafkaBrokerVersionTests', 'tmp')

    # One ZK (from Kafka repo)
    zk1 = ZookeeperApp(cluster, bin_path=kafka_path + '/bin/zookeeper-server-start.sh')
    zk_address = zk1.get('address')

    # Two brokers
    conf = {'replication_factor': 3, 'num_partitions': 4, 'version': version}
    broker1 = KafkaBrokerApp(cluster, conf, kafka_path=kafka_path)
    broker2 = KafkaBrokerApp(cluster, conf, kafka_path=kafka_path)
    broker3 = KafkaBrokerApp(cluster, conf, kafka_path=kafka_path)
    bootstrap_servers = ','.join(cluster.get_all('address','',KafkaBrokerApp))

    # Generate test config file
    fd, test_conf_file = tempfile.mkstemp(prefix='test_conf', text=True)
    os.write(fd, ('bootstrap.servers=%s\n' % bootstrap_servers).encode('ascii'))
    if version != 'trunk':
        os.write(fd, ('broker.version=%s\n' % version).encode('ascii'))
    os.close(fd)

    print('# Deploying cluster')
    cluster.deploy()

    print('# Starting cluster')
    cluster.start()

    print('# Waiting for brokers to come up')

    if not cluster.wait_operational(30):
        raise TimeoutError('Cluster did not go operational')

    print('# Connect to cluster with bootstrap.servers %s' % bootstrap_servers)

        
    print('\033[32mCluster started.. Executing librdkafka tests\033[0m')
    t_start = time.time()
    r = subprocess.call('TEST_LEVEL=%d RDKAFKA_TEST_CONF=%s ZK_ADDRESS=%s make' % (test_level, test_conf_file, zk_address), shell=True)
    if r == 0:
        print('\033[37;42mTests PASSED on broker version %s\033[0m' % version)
        ret = True
    else:
        print('\033[33;41mTests FAILED on broker version %s (ret %d)\033[0m' % (version, r))
        ret = False
    timing = time.time() - t_start

    os.remove(test_conf_file)

    cluster.stop(force=True)

    cluster.cleanup(keeptypes=['log'])
    return ret, timing


if __name__ == '__main__':

    results = dict()
    timing = dict()
    for version in kafka_versions:
        try:
            results[version], timing[version] = test_version(version)
        except Exception as e:
            print('EXCEPTION: ', str(e))
            results[version] = False
            timing[version] = -1

    print('\033[35mTEST RESULTS:\033[0m')
    for version in results:
        if results[version]:
            print('\033[37;42mBroker version %s PASSED in %ds\033[0m' % (version, timing[version]))
        else:
            print('\033[33;41mBroker version %s FAILED in %ds\033[0m' % (version, timing[version]))
            

