#!/usr/bin/env python
#
#
# Cluster testing helper
#
# Requires:
#  trivup python module
#  Kafka git clone (kafka_path below)
#  gradle in your PATH

from trivup.trivup import Cluster, UuidAllocator
from trivup.apps.ZookeeperApp import ZookeeperApp
from trivup.apps.KafkaBrokerApp import KafkaBrokerApp


class LibrdkafkaTestCluster(Cluster):
    def __init__(self, version, conf={}, num_brokers=3, kafka_path=None):
        """
        @brief Create, deploy and start a Kafka cluster using Kafka \p version
        
        Supported \p conf keys:
         * security.protocol - PLAINTEXT, SASL_PLAINTEXT, SSL_SASL
    
        \p conf dict is passed to KafkaBrokerApp classes, etc.
        """

        super(LibrdkafkaTestCluster, self).__init__(self.__class__.__name__, 'tmp')

        self.brokers = list()

        # One ZK (from Kafka repo)
        ZookeeperApp(self, bin_path=kafka_path + '/bin/zookeeper-server-start.sh')

        # Two brokers
        defconf = {'replication_factor': min(num_brokers, 3), 'num_partitions': 4, 'version': version,
                   'security.protocol': 'PLAINTEXT'}
        defconf.update(conf)
        self.conf = defconf

        for n in range(0, num_brokers):
            self.brokers.append(KafkaBrokerApp(self, defconf, kafka_path=kafka_path))


    def bootstrap_servers (self):
        """ @return Kafka bootstrap servers based on security.protocol """
        all_listeners = (','.join(self.get_all('listeners', '', KafkaBrokerApp))).split(',')
        return ','.join([x for x in all_listeners if x.startswith(self.conf.get('security.protocol'))])


        
