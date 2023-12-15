#!/usr/bin/env python
import sys
import os
import json
import sqlite3

max_cols = 6
params = (',?' * 8)[1:]
sep = '####'


def extract_tuples(json_dict, name, time):
    ret = []
    for k, v in json_dict.items():
        if isinstance(v, dict):
            for p in extract_tuples(v, name, time):
                ret.append((k,) + p)
        else:
            ret.append((str(k), str(v), name, time))
    return ret


def extract_logs(f, db):
    if os.path.isfile(db):
        raise Exception("db exists")
    with sqlite3.connect(db) as con:
        cur = con.cursor()
        cur.execute('''CREATE TABLE stats
                    (f1 text, f2 text, f3 text, f4 text, f5 test, f6 text,
                    value text, name text, timestamp INTEGER)''')

        with open(f) as f:
            for line in f.readlines():
                i, j = line.find('{'), line.rfind('}') + 1
                json_string = line[i:j]
                try:
                    start_process_id = line.find(sep)
                    end_process_id = line.find(
                        sep, start_process_id + len(sep))

                    json_dict = json.loads(json_string)
                    if 'name' not in json_dict or 'time' not in json_dict:
                        raise ValueError(
                            'doesn\'t seem to be a librdkafka stats JSON')

                    time = json_dict['time']
                    ts = json_dict['ts'] % 1000000
                    time = time * 1000000 + ts
                    name = json_dict['name']
                    if start_process_id >= 0 and \
                       end_process_id >= 0:
                        name = f'{line[start_process_id+len(sep):end_process_id]},{name}'  # noqa: E501

                    tuples = extract_tuples(json_dict, name, time)
                    rows = []
                    for tuple1 in tuples:
                        empty_cols = (None,) * (max_cols - len(tuple1) + 3)
                        row = tuple(tuple1[0:-3]) + empty_cols + tuple1[-3:]
                        rows.append(row)
                    cur.executemany(
                        'INSERT INTO stats VALUES (?,?,?,?,?,?,?,?,?)', rows)
                    con.commit()
                except json.decoder.JSONDecodeError:
                    print(
                        f'Doesn\'t seem to be a JSON: {json_string}',
                        file=sys.stderr)
                except ValueError:
                    print(
                        f'Doesn\'t seem to be a librdkafka stats JSON: {json_string}',  # noqa: E501
                        file=sys.stderr)
                except Exception as e:
                    print(
                        f'Unexpected error: {e} JSON: {json_string}',
                        file=sys.stderr)


if __name__ == "__main__":
    extract_logs(*sys.argv[1:3])
