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

### Obtaining source codes
To compile librdkafka on IBM i, you have to copy source files to the IFS folder. As an option, it is possible to copy zipped sources from https://github.com/AlexeiBaranov/librdkafka/archive/port-os400.zip to IFS folder and run `jar xMvf port-os400.zip`

### Build steps using GNU utilites
BRIEF
```
crtlib rdkafka
crtlib rdkafkatst
addlible rdkafka
strqsh
cd <path to librdkafka folder>
gmake -C src -f ../os400/Makefile.src.os400 
gmake -C tests -f ../os400/Makefile.tests.os400 
```
TODO

### Build steps using QShell script
BRIEF
```
crtlib rdkafka
crtlib rdkafkatst
addlible rdkafka
strsql
cd <path to librdkafka folder>
export KAFKALIB=RDKAFKA
export KAFKATST=RDKAFKATST
./os400/crtrdkafka.qsh
```
TODO

# Compiled binaries
For those who are lazy - librdkafka_os400.zip contains a savf with compiled objects. For installation: 
* Copy librdkafka_os400.zip to IFS
* Extract librdkafka_os400.savf from zip:
```
strqsh
cd <path where librdkafka_os400.zip stored>
jar xMvf librdkafka_os400.zip
```
* Return to command line (press F12 in qshell)
* Restore LIBRDKAFKA library:
```
CRTSAVF QTEMP/LIBRDKAFKA
CPYFRMSTMF FROMSTMF('<path to librdkafka_os400.savf>') TOMBR('/QSYS.LIB/QTEMP.LIB/LIBRDKAFKA.FILE') MBROPT(*REPLACE)
RSTLIB SAVLIB(LIBRDKAFKA) DEV(*SAVF) SAVF(QTEMP/LIBRDKAFKA)
```
* This will create LIBRDKAFKA containing two objects - RDKAFKA.SRVPGM and TEST.PGM

# Testing **librdkafka** on IBM i
You will need access to the kafka cluster to run the tests. You can use an external cluster for this or start a cluster on your IBM i server. 

## How to run a test kafka cluster on your IBM i server
BRIEF
* Download Apache Kafka: https://www.apache.org/dyn/closer.cgi?path=/kafka/2.7.0/kafka_2.13-2.7.0.tgz, extract to IFS
* Update server.config (set following values in addition to default):
  * broker.id=0
  * listeners=PLAINTEXT://:9092
  * num.network.threads=3
  * num.partitions=4              
  * default.replication.factor=1  
  * auto.create.topics.enable=true
  * log.dirs=/tmp/kafka-logs
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
or (when librdkafka was compiled using qshell script or extracted from zipped binaries
```
liblist -a LIBRDKAFKA
/QSYS.LIB/LIBRDKAFKA.LIB/TEST.PGM "0001 0002"
```
etc
