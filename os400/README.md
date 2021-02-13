librdkafka - port to IBM i OS
==================================================

This document contains instructions for building, testing and using the **librdkafka** on the IBM i platform.

# **librdkafka** on IBM i
This project contains the source code and compiled binaries of the **librdkafka** library on the IBM i platform.

The supplied library RDKAFKA is a *SRVPGM object compiled on IBM i version V7R2M0, with storage model = *SNGLVL, data model = *P128.

The service program uses ASCII Runtime for IBM i (QADRT). Thus, all text parameters of the service program functions are accepted and returned in ASCII coding. Programs that use this *SRVPGM, must convert values from/to EBCDIC themselves. For this purpose it is convenient to use the standard QADRT functions like QadrtConvertA2E and QadrtConvertE2A. 

**Important!** The high performance of the librdkafka is achieved through the active use of the multi-threaded architecture. Library functions are available only in jobs with the ALWMLTTHD=*YES option enabled 

# Building **librdkafka** on IBM i

## Prerequisites
Required runtime object is included into OS400 - service program QSYS/QADRTTS. To be able to compile librdkafka on IBM i server, 
it is necessary to install [ASCII Runtime for IBM i](https://www.ibm.com/support/pages/node/6258183) on your IBM i server.

The preferred tool for building librdkafka on IBM i is gnu gmake, It was included in IBM Tools for Developers for IBM i (5799PTL).
If you do not have this product installed, you can build the library using a QShell script that executes standard commands CRTCMOD, CRTSRVPGM, CRTPGM (see below)

### Build steps using GNU utilites
BRIEF
```
crtlib rdkafka
crtlib rdkafkatst
addlible rdkafka
strqsh
cd <path to librdkafka forler>
gmake -C src -f ../os400/Makefile.src.os400 
gmake -C tests -f ../os400/Makefile.tests.os400 
```
TODO

### Build steps using QShell script
TODO

# Compiled binaries
TODO

# Testing **librdkafka** on IBM i
You will need access to the kafka cluster to run the tests. You can use an external cluster for this or start a cluster on your server. 
TODO

## How to run a test kafka cluster on your IBM i server
BRIEF
* Download Apache Kafka tar, extract to IFS
* Update server.config
* Start Zookeeper
```
ADDENVVAR ENVVAR(JAVA_HOME) VALUE('/qopensys/QIBM/ProdData/JavaVM/jdk80/64bit')
SBMJOB CMD(STRQSH CMD('cd <path to apache kafka folder> && bin/zookeeper-server-start.sh config/zookeeper.properties')) CPYENVVAR(*YES)
```
* Start brokers
```
SBMJOB CMD(STRQSH CMD('cd <path to apache kafka folder> && bin/kafka-server-start.sh config/server.properties')) CPYENVVAR(*YES)
SBMJOB CMD(STRQSH CMD('cd <path to apache kafka folder> && bin/kafka-server-start.sh config/server.properties --override broker.id=1 --override listeners=PLAINTEXT://:9093 --override port=9093 --override log.dirs=/tmp/kafka-logs-9093')) CPYENVVAR(*YES)
SBMJOB CMD(STRQSH CMD('cd <path to apache kafka folder> && bin/kafka-server-start.sh config/server.properties --override broker.id=2 --override listeners=PLAINTEXT://:9094 --override port=9094 --override log.dirs=/tmp/kafka-logs-9094')) CPYENVVAR(*YES) 
SBMJOB CMD(STRQSH CMD('cd <path to apache kafka folder> && bin/kafka-server-start.sh config/server.properties --override broker.id=2 --override listeners=PLAINTEXT://:9094 --override port=9094 --override log.dirs=/tmp/kafka-logs-9094')) CPYENVVAR(*YES) 
```
TODO

## How to run librdkafka tests
BRIEF
```
addlible rdkafka
strqsh
cd <path to librdkafka forler>
export QIBM_MULTI_THREADED=Y
export RDKAFKA_TEST_CONF=./os400/test.conf.os400
./tests/rdkafkatst/test
```
or
```
./tests/rdkafkatst/test -l -Q -p1
```
or
```
./tests/rdkafkatst/test "0001 0002"
```
TODO
