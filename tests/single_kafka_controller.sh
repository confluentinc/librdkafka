#!/bin/bash

# a simple control for kafka standalone. for kudu duplicator itest

set -e

function usage() {
    echo "usage: =(install|start|stop|uninstall)"
}

if [[ -z $1 ]]; then
    echo "you should input an action"
    usage
    exit 1
fi

action=$1
port_offset=0
if [[ ! -z $2 ]]; then
    port_offset=$2
fi

kafka_package_name=kafka_2.12-3.2.3
kafka_package=$kafka_package_name.tgz

download_url="https://dlcdn.apache.org/kafka/3.2.3/$kafka_package"

topic_name=kudu_profile_record_stream
kafka_target=itest-kafka-$[2000+port_offset]

zookeeper_base_port=2181
kafka_base_port=9092
zookeeper_port=$[zookeeper_base_port+port_offset]
kafka_port=$[kafka_base_port+port_offset]
brokers_list=localhost:$kafka_port
zookeeper_list=localhost:$zookeeper_port

function download_kafka() {
    pushd /tmp
    if [ x"$kafka_package" != x ] && [ -f $kafka_package ]; then
        echo "/tmp/$kafka_package has exist, skip download"
    else
        wget -q --tries=5 --timeout=30 $download_url -O $kafka_package --no-check-certificate
    fi
    if [ x"$kafka_target" != x ] && [ -d $kafka_target ]; then
        uninstall_kafka
    fi

    mkdir -p tmp.$port_offset && cp -r $kafka_package tmp.$port_offset
    pushd tmp.$port_offset && tar xzf $kafka_package
    mv $kafka_package_name ../$kafka_target
    popd
    rm -rf tmp.$port_offset
    popd
}

function prepare_zookeeper() {
    download_kafka
    pushd /tmp/$kafka_target
    # Prepare the ZooKeeper service configure items
    # Note: Soon, ZooKeeper will no longer be required by Apache Kafka.
    set +e
    if grep "^clientPort=" config/zookeeper.properties &>/dev/null; then
        sed -i "s/^clientPort=.*/clientPort=$zookeeper_port/g" config/zookeeper.properties
    else
        echo "clientPort=$zookeeper_port" >> config/zookeeper.properties
    fi
    # maxSessionTimeout
    if grep "^maxSessionTimeout=" config/zookeeper.properties &>/dev/null; then
        sed -i "s/^maxSessionTimeout=.*/maxSessionTimeout=10000/g" config/zookeeper.properties
    else
        echo "maxSessionTimeout=10000" >> config/zookeeper.properties
    fi
    mkdir -p /tmp/$kafka_target/zookeeper_data
    if grep "^dataDir=" config/zookeeper.properties &>/dev/null; then
        sed -i "s#^dataDir=.*#dataDir=/tmp/$kafka_target/zookeeper_data#g" config/zookeeper.properties
    else
        echo "dataDir=/tmp/$kafka_target/zookeeper_data" >> config/zookeeper.properties
    fi
    popd
}

function prepare_kafka() {
    pushd /tmp/$kafka_target
    # Prepare the Kafka service configure items
    if grep "^listeners=PLAINTEXT://:" config/server.properties &>/dev/null; then
        sed -i "s/^listeners=PLAINTEXT://:.*/listeners=PLAINTEXT://:$kafka_port/g" config/server.properties
    else
        echo listeners=PLAINTEXT://:$kafka_port >> config/server.properties
    fi
    if grep "^zookeeper.connect=" config/server.properties &>/dev/null; then
        sed -i "s/^zookeeper.connect=.*/zookeeper.connect=$zookeeper_list/g" config/server.properties
    else
        echo zookeeper.connect=$zookeeper_list >> config/server.properties
    fi
    mkdir -p /tmp/$kafka_target/kafka_data
    if grep "^log.dirs=" config/server.properties &>/dev/null; then
        sed -i "s#^log.dirs=.*#log.dirs=/tmp/$kafka_target/kafka_data#g" config/server.properties
    else
        echo "log.dirs=/tmp/$kafka_target/kafka_data" >> config/server.properties
    fi
    set -e
    popd
}

function start_zookeeper() {
    pushd /tmp/$kafka_target
    set -e
    # Start the Zookeeper service
    bin/zookeeper-server-start.sh config/zookeeper.properties \
        &> /tmp/$kafka_target/zookeeper.out </dev/null &
    sleep 2
    popd
}

function start_kafka() {
    pushd /tmp/$kafka_target
    set -e

    # Start the Kafka broker service
    bin/kafka-server-start.sh config/server.properties \
        &> /tmp/$kafka_target/kafka-server.out </dev/null &

    sleep 3
    popd
}

function create_topic() {
    pushd /tmp/$kafka_target
    bin/kafka-topics.sh --create --topic $topic_name --bootstrap-server $brokers_list
    sleep 1
    bin/kafka-topics.sh --describe --topic $topic_name --bootstrap-server $brokers_list
    echo "create_topic success"
    popd
}

function delete_topic() {
    if [ ! -d /tmp/$kafka_target ]; then
        return
    fi
    pushd /tmp/$kafka_target
    set +e
    bin/kafka-topics.sh --delete --topic $topic_name --bootstrap-server $brokers_list
    sleep 1
    bin/kafka-topics.sh --describe --topic $topic_name --bootstrap-server $brokers_list
    ret=$?
    set -e
    if [ $ret != 0 ]; then
        echo "delete_topic success"
    fi
    popd
}

function test_write_and_read() {
    # Test kafka service ok? mannual confirm service

    pushd /tmp/$kafka_target
    # producer
    bin/kafka-console-producer.sh --topic $topic_name --bootstrap-server $brokers_list

    # consumer
    bin/kafka-console-consumer.sh --topic $topic_name --from-beginning --bootstrap-server $brokers_list
    popd
}

function stop_zookeeper() {
    ## TODO(duyuqi) should fix it.
    # stop server.
    ps auxwf | grep $kafka_target | grep org.apache.zookeeper.server.quorum.QuorumPeerMain \
        | grep -v grep | awk '{print $2}' | xargs -i kill -9 {}
}

function stop_kafka() {
    ## TODO(duyuqi) should fix it.
    # stop server.
    ps auxwf | grep $kafka_target | grep kafka.Kafka | grep -v grep \
        | awk '{print $2}' | xargs -i kill -9 {}

}

function uninstall_kafka() {
    stop_kafka
    clean_env
}

function clean_env() {
    rm -rf /tmp/$kafka_target /tmp/$kafka_target/zookeeper_data /tmp/kafka-logs
}

case $action in
    "install")
        download_kafka
        exit 0
        ;;
    "start")
        prepare_zookeeper
        prepare_kafka
        start_zookeeper
        start_kafka
        create_topic
        exit 0
        ;;
    "stop")
        delete_topic
        stop_kafka
        stop_zookeeper
        clean_env
        exit 0
        ;;
    "start_only")
        start_kafka
        exit 0
        ;;
    "stop_only")
        stop_kafka
        exit 0
        ;;
    "uninstall")
        uninstall_kafka
        exit 0
        ;;
esac
