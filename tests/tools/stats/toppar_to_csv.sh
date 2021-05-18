#!/bin/bash
#
#
# Emit CSV to values for arbitrary partition fields from librdkafka JSON.
#
# Example:
# To create a CSV list with the rxmsgs,txmsgs field for each partition
# based on the last stats (well, second last because the last are typically
# truncated):
#
#  $ tail -2 x_toppar.csv | head -1 | ./toppar_to_csv.sh rxmsgs txmsgs
#

set -e

fields=$*

if [[ -z $fields ]]; then
    echo "Usage: $0 field1 field2 .. < some_stats.json" 1>&2
    exit 1
fi

# Print field header

echo "partition $fields" | sed -E 's/ +/,/g'

jqfields=$(echo "$fields" | sed -E 's/ +/,./g' | sed -E 's/,\.$//')

jq -r ".topics[].partitions[] | [ .partition, .$jqfields ] | @csv"

