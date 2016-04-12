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
import sys

# Path to Kafka git clone
kafka_path='/home/maglun/src/kafka'



def test_version (version):
    """
    @brief Create, deploy and start a Kafka cluster using Kafka \p version
    Then run librdkafka's regression tests.
    """
    
    cluster = Cluster('librdkafkaInteractiveBrokerVersionTests', 'tmp')

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
    subprocess.call('RDKAFKA_TEST_CONF=%s ZK_ADDRESS=%s bash --rcfile <(cat ~/.bashrc; echo \'PS1="[TRIVUP:%s@%s] \\u@\\h:\w$ "\')' % (test_conf_file, zk_address, cluster.name, version), shell=True, executable='/bin/bash')

    os.remove(test_conf_file)

    cluster.stop(force=True)

    cluster.cleanup(keeptypes=['log'])
    return True

if __name__ == '__main__':

    test_version(sys.argv[1])
