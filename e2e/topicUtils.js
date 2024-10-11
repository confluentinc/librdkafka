/*
 * confluent-kafka-javascript - Node.js wrapper  for RdKafka C/C++ library
 * Copyright (c) 2024 Confluent, Inc.
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */

module.exports = { createTopics, deleteTopics };

var Kafka = require('../');

// Create topics and wait for them to be created in the metadata.
function createTopics(topics, brokerList, cb) {
    const client = Kafka.AdminClient.create({
        'client.id': 'kafka-test-admin-client',
        'metadata.broker.list': brokerList,
    });
    let promises = [];
    for (const topic of topics) {
        client.createTopic(topic, (err) => {
            promises.push(new Promise((resolve, reject) => {
                if (err && err.code !== Kafka.CODES.ERR_TOPIC_ALREADY_EXISTS) {
                    reject(err);
                }
                resolve();
            }));
        });
    }

    Promise.all(promises).then(() => {
        let interval = setInterval(() => {
            client.listTopics((err, topicList) => {
                if (err) {
                    client.disconnect();
                    clearInterval(interval);
                    cb(err);
                    return;
                }
                for (const topic of topics) {
                    if (!topicList.includes(topic.topic)) {
                        return;
                    }
                }
                client.disconnect();
                clearInterval(interval);
                cb();
            });
        }, 100);
    }).catch((err) => {
        client.disconnect();
        cb(err);
    });
}

// Delete topics.
function deleteTopics(topics, brokerList, cb) {
    const client = Kafka.AdminClient.create({
        'client.id': 'kafka-test-admin-client',
        'metadata.broker.list': brokerList,
    });
    let promises = [];
    for (const topic of topics) {
        client.deleteTopic(topic, (err) => {
            promises.push(new Promise((resolve, reject) => {
                if (err && err.code !== Kafka.CODES.ERR_UNKNOWN_TOPIC_OR_PART) {
                    reject(err);
                }
                resolve();
            }));
        });
    }

    Promise.all(promises).then(() => {
        client.disconnect();
        cb();
    }).catch((err) => {
        client.disconnect();
        cb(err);
    });
}
