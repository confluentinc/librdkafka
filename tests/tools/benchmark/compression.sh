#!/bin/bash
#

set -e

outdir=$1
brokers=$2
topic=$3

msgcnt=5000000
msgsize=100
modes=(batch streaming)
codecs=(none gzip snappy lz4 zstd)
#codecs=(none lz4)

function show_summary() {
    echo -e "Benchmark results for $outdir:\n"
    cat $outdir/info

    local csv="$outdir/summary.csv"

    echo ""
    printf "%-10s  %-7s  %6s  %10s  %6s  %7s  %5s %5s %5s %8s %8s %5s\n" \
           "Mode" "Codec" "Ratio" "Msgs/Batch" "MB/s" "Msgs/s" \
           "Elaps" "Sys" "User" "ICtx" "VCtx" "RSS"
    echo "==================================================================================================="

    echo "Mode&Codec" "Ratio" "Msgs/Batch" "MB/s" "Msgs/s" \
           "Elaps" "Sys" "User" "ICtx" "VCtx" "RSS" "Extra" | tr ' ' ',' > $csv


    local -A res
    local codec=
    for codec in ${codecs[@]}; do
        local mode=
        for mode in ${modes[@]}; do
            local f=$outdir/stats_${mode}_${codec}.json
            local bn=$(basename $f)
            bn=${bn%.json}
            local name=${bn#stats_}
            local extra=""

            local ratio=$(jq .ratio $f | tail -1)
            local batchcnt=$(jq .batchcnt $f | tail -1)
            local mbrate=$(egrep -io '[0-9.]+ MB/s' $outdir/out_${name} | tail -1 | awk '{print $1}')
            local msgrate=$(egrep -io '[0-9.]+ msgs/s' $outdir/out_${name} | tail -1 | awk '{print $1}')

            local -A times
            local kv=
            for kv in $(cat $outdir/time_${mode}_${codec}); do
                local k=${kv%=*}
                local v=${kv#*=}
                times[$k]=$v
            done

            res[$name]=$mbrate
            if [[ $mode == streaming ]]; then
                local prev=${res[batch_${codec}]}
                #echo "compare $prev to $mbrate"
                local diff=$(echo "scale=2; ($mbrate / $prev * 100.0) - 100.0" | bc -l)
                if [[ $diff != -* ]]; then
                    diff=+$diff
                fi
                extra="$diff% MB/s"
            fi

            printf "%-10s  %-6s  %6sx  %10s  %6s  %7s  %5s %5s %5s %8s %8s %5s  %s\n" \
                   $mode $codec $ratio $batchcnt $mbrate $msgrate \
                   ${times[e]} ${times[S]} ${times[U]} ${times[c]} \
                   ${times[w]} "${times[M]}" \
                   "$extra"

            echo $mode $codec, $ratio, $batchcnt, $mbrate, $msgrate, \
                 ${times[e]}, ${times[S]}, ${times[U]}, ${times[c]}, \
                 ${times[w]}, ${times[M]}, \
                 "$extra" >> $csv
done
    done
}

if [[ -z $outdir ]]; then
    echo "Usage: $0 <report-dir> [<brokers> <topic>]"
    exit 1
fi

if [[ -f $outdir/info ]]; then
    if [[ -n $brokers ]]; then
        echo "$0: report directory $outdir already exists"
        exit 1
    fi
    show_summary
    exit 0
fi

if [[ -z $topic ]]; then
    echo "Usage: $0 <report-dir> [<brokers> <topic>]"
    exit 1
fi

if [[ ! -x examples/rdkafka_performance ]]; then
    echo "$0: this tool needs to be run from the librdkafka top level directory"
    exit 1
fi

mkdir -p "$outdir"

set -u


version=$(examples/rdkafka_performance -h 2>&1 | grep librdkafka.version)

cat >$outdir/info <<EOF
version=$version
msgcnt=$msgcnt
msgsize=$msgsize
EOF

echo "$version"

# Prime topic with a single message to avoid auto-creation costs in benchmarks.
echo "Priming topic $topic"
examples/rdkafka_performance -b $brokers -P -t "$topic" -p 0 -c 1 -q

function run_benchmark() {
    local mode=
    for mode in ${modes[@]}; do
        if [[ $mode == streaming ]]; then
            streamconf=true
        else
            streamconf=false
        fi

        local codec=
        for codec in ${codecs[@]}; do
            echo "Start: $mode $codec"

            statsfile="$outdir/stats_${mode}_${codec}.json"

            $(which time) \
                -o $outdir/time_${mode}_${codec} \
                -f 'F=%F S=%S U=%U c=%c e=%e r=%r s=%s w=%w t=%t M=%M' \
                examples/rdkafka_performance \
                -b $brokers \
                -X test.mock.num.brokers=3 \
                -X test.mock.broker.rtt=200 \
                -P \
                -a 1 \
                -t "$topic" \
                -p 0 \
                -c $msgcnt \
                -s $msgsize \
                -S 1231241245 \
                -T 1000 \
                -Y "tee $outdir/all_stats.json | jq '.topics[] | {batchcnt: .batchcnt.p99, ratio: (.batchcompratio.p99 / 100.0)'} > $statsfile" \
                -X linger.ms=1000 \
                -X batch.num.messages=1000000 \
                -X enable.streaming.compression=$streamconf \
                -z $codec > "$outdir/out_${mode}_${codec}"
        done

    done
}

run_benchmark

show_summary
