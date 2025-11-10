#!/usr/bin/env python3
"""
librdkafka Stats Dashboard runner

This script starts the Grafana dashboard with VictoriaMetrics for librdkafka statistics,
handles first-time setup, and imports data from logs files and dashboards.
"""

import os
import sys
import time
import json
import socket
import subprocess
import urllib.request
import urllib.error
import base64
import sqlite3
import webbrowser
from prometheus_remote_writer import RemoteWriter
from typing import Optional, List

max_cols = 6
params = (',?' * 8)[1:]
sep = '####'

class Grafana:
    def __init__(self):
        self.grafana_url = "http://localhost:3000"
        self.username = "librdkafka"
        self.password = "librdkafka"
        # Create base64 encoded auth header
        credentials = f"{self.username}:{self.password}"
        self.auth_header = base64.b64encode(credentials.encode()).decode()
        self.librdkafka_dashboard_url = None
    
    def make_request(self, url: str, method: str = "GET", data: Optional[dict] = None, headers: Optional[dict] = None) -> tuple:
        """Make HTTP request using urllib."""
        try:
            req_headers = {"Authorization": f"Basic {self.auth_header}"}
            if headers:
                req_headers.update(headers)
            
            if data:
                json_data = json.dumps(data).encode('utf-8')
                req_headers["Content-Type"] = "application/json"
            else:
                json_data = None
            
            request = urllib.request.Request(url, data=json_data, headers=req_headers, method=method)
            
            with urllib.request.urlopen(request, timeout=10) as response:
                response_data = response.read().decode('utf-8')
                return response.status, response_data
                
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode('utf-8')
        except Exception as e:
            return 0, str(e)
        
    def check_port_open(self, host: str, port: int) -> bool:
        """Check if a port is open on the given host."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(1)
                result = sock.connect_ex((host, port))
                return result == 0
        except Exception:
            return False
    
    def wait_for_grafana(self, max_wait: int = 60) -> bool:
        """Wait for Grafana to start up."""
        print("Waiting for Grafana to start...")
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            if self.check_port_open("localhost", 3000):
                print("Grafana started")
                return True
            print("Waiting for Grafana to start")
            time.sleep(1)
        
        print(f"Grafana failed to start within {max_wait} seconds")
        return False
    
    def wait_for_grafana_api(self, max_wait: int = 60) -> Optional[int]:
        """Wait for Grafana API to be ready and return datasource count."""
        print("Waiting for Grafana API to be ready...")
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            try:
                status, response_data = self.make_request(f"{self.grafana_url}/api/datasources")
                if status == 200:
                    datasources = json.loads(response_data)
                    count = len(datasources)
                    print(f"Grafana API ready, found {count} datasources")
                    return count
            except Exception as e:
                print(f"Waiting for Grafana API to be ready... ({e})")
            
            time.sleep(2)
        
        print(f"Grafana API failed to be ready within {max_wait} seconds")
        return None


class VictoriaMetrics:
    def __init__(self, victoriametrics_url: str = "http://localhost:8428"):
        self.victoriametrics_url = victoriametrics_url
        self.remote_writer = RemoteWriter(f"{victoriametrics_url}/api/v1/write")
    
    def wait_for_victoriametrics(self, max_wait: int = 60) -> bool:
        """Wait for VictoriaMetrics to start up and be healthy."""
        print("Waiting for VictoriaMetrics to be ready...")
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            if self.check_victoriametrics_health():
                print("VictoriaMetrics is ready")
                return True
            print("Waiting for VictoriaMetrics to start")
            time.sleep(2)
        
        print(f"VictoriaMetrics failed to start within {max_wait} seconds")
        return False
    
    def check_victoriametrics_health(self) -> bool:
        """Check if VictoriaMetrics is running and healthy."""
        try:
            health_url = f"{self.victoriametrics_url}/health"
            request = urllib.request.Request(health_url, method='GET')
            
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status == 200
                    
        except (urllib.error.URLError, Exception):
            return False

    def backfill_metrics(self, db_path: str) -> bool:
        """Backfill metrics from SQLite database to VictoriaMetrics."""
        try:
            if not os.path.exists(db_path):
                print(f"Database {db_path} not found")
                return False
            
            # Connect to SQLite database
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT f1, f2, f3, f4, f5, f6, name, value, timestamp 
                    FROM stats 
                    ORDER BY timestamp ASC
                """)
                
                rows_batch_size = 10000
                rows = cursor.fetchmany(rows_batch_size)
                while len(rows) > 0:
                    # Convert SQLite data to VictoriaMetrics format and push
                    data = []
                    batched_metrics = {}
                    for row in rows:
                        f1, f2, f3, f4, f5, f6, instance, value, timestamp_us = row
                        # Convert microsecond timestamp to milliseconds
                        timestamp_ms = timestamp_us // 1000
                        tags = {
                            'instance': instance
                        }

                        if f1 == 'brokers':
                            tags['broker'] = f2
                            f2, f3, f4, f5, f6 = f3, f4, f5, f6, None
                            if f2 == "toppars":
                                topic, partition = f3.rsplit('-',1)
                                tags['topic'] = topic
                                tags['partition'] = partition
                                f3, f4, f5, f6 = f4, f5, f6, None
                        if f1 == 'topics':
                            tags['topic'] = f2
                            f2, f3, f4, f5, f6 = f3, f4, f5, f6, None
                            if f2 == "partitions":
                                tags['partition'] = f3
                                f3, f4, f5, f6 = f4, f5, f6, None
                        try:
                            metric_value = float(value)
                        except (ValueError, TypeError):
                            metric_value = 1
                            tags['value'] = value

                        metric_parts = [part for part in [f1, f2, f3, f4, f5, f6] if part is not None]
                        metric_name = "_".join(metric_parts)
                        # Create metric name (sanitized for VictoriaMetrics)
                        metric_name = f"librdkafka_{metric_name}".replace("-", "_").replace(".", "_").replace("?", "").lower()

                        tags_key = f"{tags['instance']}"
                        if 'broker' in tags:
                            tags_key += f"_{tags['broker']}"
                        if 'topic' in tags:
                            tags_key += f"_{tags['topic']}"
                        if 'partition' in tags:
                            tags_key += f"_{tags['partition']}"
                        if 'value' in tags:
                            tags_key += f"_{tags['value']}"

                        if not metric_name in batched_metrics:
                            batched_metrics[metric_name] = {}
                        if not tags_key in batched_metrics[metric_name]:
                            batched_metrics[metric_name][tags_key] = {
                                'tags': tags,
                                'values': [],
                                'timestamps': []
                            }
                        batched_metrics[metric_name][tags_key]['values'].append(metric_value)
                        batched_metrics[metric_name][tags_key]['timestamps'].append(timestamp_ms)

                    for metric_name, tags in batched_metrics.items():
                        for tags_key, metric_data in tags.items():
                            # Prepare data in the format expected by RemoteWriter
                            data.append({
                                'metric': {
                                    '__name__': metric_name,
                                    **metric_data['tags']
                                },
                                'values': metric_data['values'],
                                'timestamps': metric_data['timestamps']
                            })

                    self.remote_writer.send(data)
                    print(f"Pushed {len(rows)} metrics")
                    rows = cursor.fetchmany(rows_batch_size)
                return True
                
        except Exception as e:
            print(f"Error backfilling metrics: {repr(e)}")
            return False


def run_command(cmd: List[str], check: bool = True) -> subprocess.CompletedProcess:
    """Run a shell command."""
    try:
        result = subprocess.run(cmd, check=check, capture_output=True, text=True)
        return result
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {' '.join(cmd)}")
        print(f"Error: {e.stderr}")
        if check:
            raise
        return e

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
                    (f1 text, f2 text, f3 text, f4 text, f5 text, f6 text,
                    value text, name text, timestamp INTEGER)''')

        with open(f) as f:
            stats = 0
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
                    stats+=1
                except json.decoder.JSONDecodeError:
                    # There will always be extraneous log lines
                    pass
                except ValueError:
                    print(
                        f'Doesn\'t seem to be a librdkafka stats JSON: {json_string}',  # noqa: E501
                        file=sys.stderr)
                except Exception as e:
                    print(
                        f'Unexpected error: {e} JSON: {json_string}',
                        file=sys.stderr)
            print(f'{stats} stats written')
        cur.execute('''CREATE INDEX idx_stats ON stats(
            name,f1,f2,f3,f4,f5,timestamp)''')

def read_min_interval(db):
    with sqlite3.connect(db) as con:
        cur = con.cursor()
        cur.execute("SELECT interval_ms FROM (SELECT name, ((max(timestamp) - min(timestamp)) / (count(*) - 1) / 1000) as interval_ms FROM stats WHERE f1 = 'name' GROUP BY name ORDER BY interval_ms ASC) LIMIT 1")  # noqa: E501
        data = cur.fetchall()
        if data and len(data) > 0:
            return data[0][0]
    return None

def update_victoriametrics_interval(min_interval):
    prometheus_config_path = os.path.join(
        os.path.dirname(__file__),
        'victoriametrics',
        'prometheus.yml')

    with open(prometheus_config_path, 'r') as f:
        config = f.read()
    
    # Update scrape_interval
    new_config = ""
    for line in config.splitlines():
        stripped_line = line.lstrip()
        spacing = line[:len(line) - len(stripped_line)]
        if stripped_line.startswith("scrape_interval:"):
            new_config += f"{spacing}scrape_interval: {min_interval}ms\n"
        else:
            new_config += line + "\n"
    
    with open(prometheus_config_path, 'w') as f:
        f.write(new_config)

    print(f"Updated VictoriaMetrics scrape_interval to {min_interval}ms")


def open_dashboard(db, min_interval_ms):
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
        min_ts, max_ts = min_ts - 1000, max_ts + min_interval_ms

        url = f'http://localhost:3000/d/b94091f3-25ca-4e71-a632-e94fda3a173e/librdkafka-dashboard-copy?orgId=1&from={min_ts}&to={max_ts}'  # noqa: E501
        print(f'Opening {url}')
        webbrowser.open(url)

def main():
    # Parse command line arguments
    log_file = sys.argv[1] if len(sys.argv) > 1 else None

    # Process log file if provided
    if log_file:
        print(f"Processing log file: {log_file}")
        # Remove existing database
        if os.path.exists("./out.db"):
            os.remove("./out.db")
        
        # Convert logs to SQLite
        extract_logs(log_file, "./out.db")

        min_interval = read_min_interval("./out.db")
        if min_interval is None:
            print("Failed to read minimum interval from database")
            sys.exit(1)
        update_victoriametrics_interval(min_interval)

    # Start docker compose
    print("Starting librdkafka stats dashboard with docker compose...")
    try:
        subprocess.run(["docker", "compose", "up", "-d"], check=True)
    except subprocess.CalledProcessError:
        print("Failed to start docker compose")
        sys.exit(1)
    
    # Initialize Grafana
    grafana = Grafana()
    
    # Wait for Grafana to start
    if not grafana.wait_for_grafana():
        sys.exit(1)
    
    # Wait for Grafana API and get datasource count
    datasource_count = grafana.wait_for_grafana_api()
    if datasource_count is None:
        sys.exit(1)

    # Initialize VictoriaMetrics and backfill data
    print("\nInitializing VictoriaMetrics...")
    victoriametrics = VictoriaMetrics()
    
    # Wait for VictoriaMetrics to be ready
    if not victoriametrics.wait_for_victoriametrics():
        sys.exit(1)
    

    if log_file:
        # Backfill metrics from database
        print("Backfilling metrics from database...")
        if victoriametrics.backfill_metrics("./out.db"):
            print("Metrics backfilled successfully")
        else:
            print("Failed to backfill metrics")

    # Check if database exists
    if not os.path.exists("./out.db"):
        print("DB not built")
        sys.exit(1)

    min_interval = read_min_interval("./out.db")
    if min_interval is None:
        print("Failed to read minimum interval from database")
        sys.exit(1)

    # Open dashboard
    open_dashboard("./out.db", min_interval)

    # Print connection info
    print()
    print("Dashboard is running at: http://localhost:3000")
    print("Username: librdkafka")
    print("Password: librdkafka")
    print()
    print("To stop the dashboard, run: docker compose down")


if __name__ == "__main__":
    main()
