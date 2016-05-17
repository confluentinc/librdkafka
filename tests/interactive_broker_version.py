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
from trivup.apps.KerberosKdcApp import KerberosKdcApp
from trivup.apps.SslApp import SslApp

import subprocess
import time
import tempfile
import os
import sys
import argparse
import json


# Path to Kafka git clone
kafka_path='/home/maglun/src/kafka'



def test_version (version, cmd=None, deploy=True, conf={}, debug=False, exec_cnt=1):
    """
    @brief Create, deploy and start a Kafka cluster using Kafka \p version
    Then run librdkafka's regression tests.
    """

    print('## Test version %s' % version)
    
    cluster = Cluster('librdkafkaInteractiveBrokerVersionTests', 'tmp', debug=debug)

    # Enable SSL if desired
    if 'SSL' in conf.get('security.protocol', ''):
        cluster.ssl = SslApp(cluster, conf)

    # One ZK (from Kafka repo)
    zk1 = ZookeeperApp(cluster, bin_path=kafka_path + '/bin/zookeeper-server-start.sh')
    zk_address = zk1.get('address')

    # Start Kerberos KDC if GSSAPI is configured
    if 'GSSAPI' in args.conf.get('sasl_mechanisms', []):
        KerberosKdcApp(cluster, 'MYREALM').start()

    # Three brokers
    defconf = {'replication_factor': 3, 'num_partitions': 4, 'version': version}
    defconf.update(conf)

    print('conf: ', defconf)

    broker1 = KafkaBrokerApp(cluster, defconf, kafka_path=kafka_path)
    broker2 = KafkaBrokerApp(cluster, defconf, kafka_path=kafka_path)
    broker3 = KafkaBrokerApp(cluster, defconf, kafka_path=kafka_path)

    # Generate test config file
    security_protocol='PLAINTEXT'
    fd, test_conf_file = tempfile.mkstemp(prefix='test_conf', text=True)
    if version != 'trunk':
        os.write(fd, ('broker.version.fallback=%s\n' % version).encode('ascii'))
    else:
        os.write(fd, 'api.version.request=true\n')
    # SASL (only one mechanism supported)
    mech = defconf.get('sasl_mechanisms', '').split(',')[0]
    if mech != '':
        os.write(fd, 'sasl.mechanisms=%s\n' % mech)
        if mech == 'PLAIN':
            print('# Writing SASL PLAIN client config to %s' % test_conf_file)
            security_protocol='SASL_PLAINTEXT'
            # Use first user as SASL user/pass
            for up in defconf.get('sasl_users', '').split(','):
                u,p = up.split('=')
                os.write(fd, 'sasl.username=%s\n' % u)
                os.write(fd, 'sasl.password=%s\n' % p)
                break
        else:
            print('# FIXME: SASL %s client config not written to %s' % (mech, test_conf_file))

    # SSL support
    ssl = getattr(cluster, 'ssl', None)
    if ssl is not None:
        if 'SASL' in security_protocol:
            security_protocol = 'SASL_SSL'
        else:
            security_protocol = 'SSL'

        key, req, pem = ssl.create_key('librdkafka')

        os.write(fd, 'ssl.ca.location=%s\n' % ssl.ca_cert)
        os.write(fd, 'ssl.certificate.location=%s\n' % pem)
        os.write(fd, 'ssl.key.location=%s\n' % key)
        os.write(fd, 'ssl.key.password=%s\n' % ssl.conf.get('ssl_key_pass'))


    # Define bootstrap brokers based on selected security protocol
    print('# Using client security.protocol=%s' % security_protocol)
    all_listeners = (','.join(cluster.get_all('listeners', '', KafkaBrokerApp))).split(',')
    bootstrap_servers = ','.join([x for x in all_listeners if x.startswith(security_protocol)])
    os.write(fd, ('bootstrap.servers=%s\n' % bootstrap_servers).encode('ascii'))
    os.write(fd, 'security.protocol=%s\n' % security_protocol)
    os.close(fd)

    if deploy:
        print('# Deploying cluster')
        cluster.deploy()
    else:
        print('# Not deploying')

    print('# Starting cluster')
    cluster.start()

    print('# Waiting for brokers to come up')

    if not cluster.wait_operational(30):
        cluster.stop(force=True)
        raise Exception('Cluster did not go operational, see logs in %s' % \
                        (cluster.root_path))

    print('# Connect to cluster with bootstrap.servers %s' % bootstrap_servers)

    cmd_env = 'export KAFKA_PATH=%s RDKAFKA_TEST_CONF=%s ZK_ADDRESS=%s BROKERS=%s TEST_KAFKA_VERSION=%s;' % \
              (broker1.conf.get('destdir'), test_conf_file, zk_address, bootstrap_servers, version)
    if not cmd:
        cmd = 'bash --rcfile <(cat ~/.bashrc; echo \'PS1="[TRIVUP:%s@%s] \\u@\\h:\w$ "\')' % (cluster.name, version)
    for i in range(0, exec_cnt):
        subprocess.call('%s %s' % (cmd_env, cmd), shell=True, executable='/bin/bash')

    os.remove(test_conf_file)

    cluster.stop(force=True)

    cluster.cleanup(keeptypes=['log'])
    return True

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Start a Kafka cluster and provide an interactive shell')

    parser.add_argument('versions', type=str, default=None, nargs='+',
                        help='Kafka version(s) to deploy')
    parser.add_argument('--no-deploy', action='store_false', dest='deploy', default=True,
                        help='Dont deploy applications, assume already deployed.')
    parser.add_argument('--conf', type=str, dest='conf', default=None,
                        help='JSON config object (not file)')
    parser.add_argument('-c', type=str, dest='cmd', default=None,
                        help='Command to execute instead of shell')
    parser.add_argument('-n', type=int, dest='exec_cnt', default=1,
                        help='Number of times to execute -c ..')
    parser.add_argument('--debug', action='store_true', dest='debug', default=False,
                        help='Enable trivup debugging')

    args = parser.parse_args()
    if args.conf is not None:
        args.conf = json.loads(args.conf)
    else:
        args.conf = {}

    for version in args.versions:
        test_version(version, cmd=args.cmd, deploy=args.deploy,
                     conf=args.conf, debug=args.debug, exec_cnt=args.exec_cnt)
