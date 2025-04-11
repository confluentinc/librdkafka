#!/usr/bin/env python
import json
import sys
import re

# This script updates the Apache Kafka RPCs max versions.
# It reads the input from stdin, which should be a table
# looking like the first three columns of the table in `INTRODUCTION.md`.
# Should look like this (without the first space after the comment):
# | 0       | Produce                       | 12         |
# | 1       | Fetch                         | 17         |
# | 2       | ListOffsets                   | 10         |
# | 3       | Metadata                      | 13         |
# | 8       | OffsetCommit                  | 9          |
# | 9       | OffsetFetch                   | 9          |
# | 10      | FindCoordinator               | 6          |
# | 11      | JoinGroup                     | 9          |
# | 12      | Heartbeat                     | 4          |
# | 13      | LeaveGroup                    | 5          |
# | 14      | SyncGroup                     | 5          |
# | 15      | DescribeGroups                | 6          |
# | 16      | ListGroups                    | 5          |
# | 17      | SaslHandshake                 | 1          |
# | 18      | ApiVersions                   | 4          |
# | 19      | CreateTopics                  | 7          |
# | 20      | DeleteTopics                  | 6          |
# | 21      | DeleteRecords                 | 2          |
# | 22      | InitProducerId                | 5          |
# | 23      | OffsetForLeaderEpoch          | 4          |
# | 24      | AddPartitionsToTxn            | 5          |
# | 25      | AddOffsetsToTxn               | 4          |
# | 26      | EndTxn                        | 5          |
# | 28      | TxnOffsetCommit               | 5          |
# | 29      | DescribeAcls                  | 3          |
# | 30      | CreateAcls                    | 3          |
# | 31      | DeleteAcls                    | 3          |
# | 32      | DescribeConfigs               | 4          |
# | 33      | AlterConfigs                  | 2          |
# | 36      | SaslAuthenticate              | 2          |
# | 37      | CreatePartitions              | 3          |
# | 42      | DeleteGroups                  | 2          |
# | 43      | ElectLeaders                  | 2          |
# | 44      | IncrementalAlterConfigs       | 1          |
# | 47      | OffsetDelete                  | 0          |
# | 50      | DescribeUserScramCredentials  | 0          |
# | 51      | AlterUserScramCredentials     | 0          |
# | 68      | ConsumerGroupHeartbeat        | 1          |
# | 71      | GetTelemetrySubscriptions     | 0          |
# | 72      | PushTelemetry                 | 0          |
#
# Output will be the same with max versions updated
# Should pass Apache Kafka root folder as first argument
ak_folder = sys.argv[1]

if len(sys.argv) != 2:
    print("Usage: python3 update_rpcs_max_versions.py <kafka_folder>")
    sys.exit(1)

lines = sys.stdin.readlines()
max_first_column = 0
max_second_column = 0
max_third_column = 0
apis = []
for line in lines:
    line = re.sub('^\\s*\\|\\s*', '', line)
    pipe_char = line.find('|')
    max_first_column = max(max_first_column, pipe_char)
    api_num = int(line[0:pipe_char])
    line = line[pipe_char + 1:]
    line = re.sub('^\\s*', '', line)
    pipe_char = line.find('|')
    max_second_column = max(max_second_column, pipe_char)
    api = line[0:pipe_char].strip()
    apis.append((api_num, api))
    line = line[pipe_char + 1:].lstrip()
    pipe_char = line.find('|')
    max_third_column = max(max_third_column, pipe_char)

for api_num, api in apis:
    with open(f'{ak_folder}/clients/src/main/resources/common/message/'
              f'{api}Request.json',
              'r') as f:
        text = f.readlines()
        text = "".join([line for line in text
                        if '#' not in line and '//' not in line])
        json_object = json.loads(text)
        max_version = json_object["validVersions"].split("-")[-1]
        print('| ', end='')
        print(str(api_num).ljust(max_first_column), end='')
        print('| ', end='')
        print(api.ljust(max_second_column), end='')
        print('| ', end='')
        print(str(max_version).ljust(max_third_column) + '|')
