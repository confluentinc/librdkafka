#!/usr/bin/env python
import sys
import os
import sqlite3
import webbrowser


def open_dashboard(db):
    if not os.path.isfile(db):
        raise Exception("db doesn't exist")
    with sqlite3.connect(db) as con:
        cur = con.cursor()
        cur.execute(
            '''SELECT min(timestamp) as min,
            max(timestamp) as max from stats''')
        data = cur.fetchall()
        min_and_max = data[0]
        min_and_max = map(lambda x: x / 1000, min_and_max)
        min_ts, max_ts = map(int, min_and_max)

        url = f'http://localhost:3000/d/f4a833ec-9c79-432e-893e-afcf0b627a47/librdkafka-dashboard?orgId=1&from={min_ts}&to={max_ts}'  # noqa: E501
        print(f'Opening {url}')
        webbrowser.open(url)


if __name__ == "__main__":
    open_dashboard(*sys.argv[1:3])
