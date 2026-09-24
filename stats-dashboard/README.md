# Grafana dashboard for librdkafka stats

This Grafana dashboard allows to show metrics created from a series of librdkafka json stats, collected from a text file.

Every line that looks like a librdkafka stats JSON is parsed and the corresponding metrics are put into a SQLite DB.

From there the metrics are ingested into VictoriaMetrics to be queried with PromQL.

Then a Grafana instance is started with a dashboard that queries those metrics, as soon as it's started the corresponding dashboard url
is opened.

> **_NOTE:_**  When mixing stats from different processes the instance name can be the same. To differentiate them, one can put in the same line a process id enclosed by the `####` separator e.g.: `####process 1####`.

Default password is `librdkafka:librdkafka`

## Requirements

* Python 3
* Docker

## How to run

1. In a virtual environment install the dependencies with `pip install -r requirements.txt`

2. Generate the DB and open the dashboard

```
./run.py <text_file_to_parse.log>
```

2. Later checks

```
./run.py
```

## Preview

<img src="img/stats-dashboard.png" width="1200">
