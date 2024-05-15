#!/bin/sh
if [ ! -z $1 ]; then
    rm -f ./out.db
    ./logs_to_sqlite.py "$1" ./out.db
fi

if [ ! -f ./out.db ]; then
    echo "DB not built"
    exit 1
fi

chmod go+w ./data/grafana.db
docker build . -t librdkafka_stats_dashboard
docker run -p 3000:3000 -v $PWD:/mnt -v ./data/grafana.db:/var/lib/grafana/grafana.db \
 -v ./data/grafana.ini:/etc/grafana/grafana.ini librdkafka_stats_dashboard &

while ! nc -z localhost 3000; do
    echo "Waiting for Grafana to start"
    sleep 1
done
echo "Grafana started"
./open_dashboard.py ./out.db
wait
