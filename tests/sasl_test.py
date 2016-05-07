#!/usr/bin/env python
#
#
# Run librdkafka regression tests on with different SASL parameters
# and broker verisons.
#
# Requires:
#  trivup python module
#  Kafka git clone (kafka_path below)
#  gradle in your PATH

from cluster_testing import LibrdkafkaTestCluster
from LibrdkafkaTestApp import LibrdkafkaTestApp


import subprocess
import time
import tempfile
import os
import sys
import argparse
import json
import tempfile

# Path to Kafka git clone
kafka_path='/home/maglun/src/kafka'



def test_it (version, deploy=True, conf={}, rdkconf={}, tests=None):
                  
    """
    @brief Create, deploy and start a Kafka cluster using Kafka \p version
    Then run librdkafka's regression tests.
    """
    
    cluster = LibrdkafkaTestCluster(version, conf, kafka_path=kafka_path)

    # librdkafka's regression tests, as an App.
    _rdkconf = conf.copy() # Base rdkconf on cluster conf + rdkconf
    _rdkconf.update(rdkconf)
    rdkafka = LibrdkafkaTestApp(cluster, version, _rdkconf, tests=tests)
    rdkafka.do_cleanup = False

    if deploy:
        cluster.deploy()

    cluster.start(wait_operational=30)

    print('# Connect to cluster with bootstrap.servers %s' % cluster.bootstrap_servers())
    rdkafka.start()
    print('# librdkafka regression tests started, logs in %s' % rdkafka.root_path())
    rdkafka.wait_stopped(timeout=60*10)
    print('wait stopped: %s, runtime %ds' % (rdkafka.state, rdkafka.runtime()))

    report = rdkafka.report()
    report['root_path'] = rdkafka.root_path()

    cluster.stop(force=True)

    cluster.cleanup()
    return report


def handle_report (report, version, suite):
    """ Parse test report and return tuple (Passed(bool), Reason(str)) """
    test_cnt = report.get('tests_run', 0)

    if test_cnt == 0:
        return (False, 'No tests run')

    passed = report.get('tests_passed', 0)
    failed = report.get('tests_failed', 0)
    if 'all' in suite.get('expect_fail', []) or version in suite.get('expect_fail', []):
        expect_fail = True
    else:
        expect_fail = False

    if expect_fail:
        if failed == test_cnt:
            return (True, 'All %d/%d tests failed as expected' % (failed, test_cnt))
        else:
            return (False, '%d/%d tests failed: expected all to fail' % (failed, test_cnt))
    else:
        if failed > 0:
            return (False, '%d/%d tests passed: expected all to pass' % (passed, test_cnt))
        else:
            return (True, 'All %d/%d tests passed as expected' % (passed, test_cnt))


def print_summary (fullreport):
    """ Print summary from a full report suite """
    print('#### Full test suite report')
    for suite in fullreport.get('suites', list()):
        for version,report in suite.get('version', {}).iteritems():
            passed =report.get('PASSED', False)
            if passed:
                resstr = '\033[42mPASSED\033[0m'
            else:
                resstr = '\033[41mFAILED\033[0m'

            print('# %6s: %-50s: %s' %
                  (resstr, '%s @ %s' % (suite.get('name','n/a'), version),
                   report.get('REASON', 'n/a')))
            if not passed:
                print('# %6s   --> %s/%s' %
                      ('', report.get('root_path', '.'), 'stderr.log'))
               
    print('#### %d suites \033[42mPASSED\033[0m, %d suites \033[41mFAILED\033[0m' % (fullreport.get('pass_cnt', -1), fullreport.get('fail_cnt', -1)))

        

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Run librdkafka test suit using SASL on a trivupped cluster')

    parser.add_argument('--conf', type=str, dest='conf', default=None,
                        help='trivup JSON config object (not file)')
    parser.add_argument('--rdkconf', type=str, dest='rdkconf', default=None,
                        help='trivup JSON config object (not file) for LibrdkafkaTestApp')
    parser.add_argument('--tests', type=str, dest='tests', default=None,
                        help='Test to run (e.g., "0002")')
    parser.add_argument('--report', type=str, dest='report', default=None,
                        help='Write test suites report to this filename')
    parser.add_argument('--read-report', type=str, dest='read_report', default=None,
                        help='Show summary from existing test suites report file')

    args = parser.parse_args()

    if args.read_report is not None:
        passed = False
        with open(args.read_report, 'r') as f:
            passed = print_summary(json.load(f))
        if passed:
            sys.exit(0)
        else:
            sys.exit(1)

    conf = dict()
    rdkconf = dict()

    if args.conf is not None:
        conf.update(json.loads(args.conf))
    if args.rdkconf is not None:
        rdkconf.update(json.loads(args.rdkconf))
    if args.tests is not None:
        tests = args.tests.split(',')
    else:
        tests = None

    # Test version + suite matrix
    versions = ['0.8.2.1', '0.9.0.1', 'trunk']
    sasl_plain_conf = {'sasl_mechanisms': 'PLAIN',
                       'sasl_users': 'myuser=mypassword'}
    suites = [{'name': 'SASL PLAIN',
               'conf': sasl_plain_conf,
               'expect_fail': ['0.8.2.1', '0.9.0.1']},
              {'name': 'PLAINTEXT (no SASL)'},
              {'name': 'SASL PLAIN with wrong username',
               'conf': sasl_plain_conf,
               'rdkconf': {'sasl_users': 'wrongjoe=mypassword'},
               'expect_fail': ['all']}]

    pass_cnt = 0
    fail_cnt = 0
    for version in versions:
        for suite in suites:
            _conf = conf.copy()
            _conf.update(suite.get('conf', {}))
            _rdkconf = rdkconf.copy()
            _rdkconf.update(suite.get('rdkconf', {}))

            if 'version' not in suite:
                suite['version'] = dict()

            if version == 'trunk':
                deploy = False
            else:
                deploy = True
                
            # Run tests
            print('#### Version %s, suite %s: STARTING' % (version, suite['name']))
            report = test_it(version, tests=tests, conf=_conf, rdkconf=_rdkconf,
                             deploy=deploy)

            # Handle test report
            report['version'] = version
            passed,reason = handle_report(report, version, suite)
            report['PASSED'] = passed
            report['REASON'] = reason
            
            if passed:
                print('\033[42m#### Version %s, suite %s: PASSED: %s\033[0m' %
                      (version, suite['name'], reason))
                pass_cnt += 1
            else:
                print('\033[41m#### Version %s, suite %s: FAILED: %s\033[0m' %
                      (version, suite['name'], reason))
                fail_cnt += 1
            print('#### Test output: %s/stderr.log' % (report['root_path']))

            suite['version'][version] = report

    # Write test suite report JSON file
    if args.report is not None:
        test_suite_report_file = args.report
        f = open(test_suite_report_file, 'w')
    else:
        fd, test_suite_report_file = tempfile.mkstemp(prefix='test_suite_',
                                                      suffix='.json',
                                                      dir='.')
        f = os.fdopen(fd, 'w')

    full_report = {'suites': suites, 'pass_cnt': pass_cnt,
                   'fail_cnt': fail_cnt, 'total_cnt': pass_cnt+fail_cnt}

    f.write(json.dumps(full_report).encode('ascii'))
    f.close()

    print('\n\n\n')
    print_summary(full_report)
    print('#### Full test suites report in: %s' % test_suite_report_file)

    if pass_cnt == 0 or fail_cnt > 0:
        sys.exit(1)
    else:
        sys.exit(0)
