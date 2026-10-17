#!/usr/bin/env python3
"""Linux-only consumer-close regression using private in-process mock brokers."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--library', type=Path)
parser.add_argument('--output', required=True, type=Path)
args = parser.parse_args()
if sys.platform != 'linux':
    parser.error('sendmsg fault injection requires Linux LD_PRELOAD')
here = Path(__file__).resolve().parent
root = here.parents[2]
library = (args.library or root / 'src/librdkafka.so.1').resolve()
out = args.output.resolve()
out.mkdir(parents=True, exist_ok=True)
(out / 'result.json').write_text('{"passed":false,"status":"running"}\n')
cc = os.environ.get('CC', 'cc')
subprocess.run([cc, '-std=gnu11', '-O1', '-g', '-Wall', '-Wextra', '-Werror',
                '-shared', '-fPIC', str(here / 'unsent_send.c'), '-ldl', '-pthread',
                '-o', str(out / 'unsent-send.so')], check=True, timeout=60)
subprocess.run([cc, '-std=gnu11', '-O1', '-g', '-Wall', '-Wextra', '-Werror',
                str(here / 'probe.c'), '-isystem', str(root / 'src'), str(library),
                '-Wl,-rpath,' + str(library.parent), '-ldl', '-pthread',
                '-o', str(out / 'probe')], check=True, timeout=60)
env = dict(os.environ, LD_PRELOAD=str(out / 'unsent-send.so'),
           LD_LIBRARY_PATH=str(library.parent))
results = []
for mode in ['normal', 'fatal-immediate', 'fatal-late', 'held-send', 'destroy-pending']:
    try:
        result = subprocess.run([str(out / 'probe'), mode], env=env, text=True,
                                capture_output=True, timeout=60)
        (out / (mode + '.log')).write_text(result.stdout + result.stderr)
        evidence = json.loads(result.stdout.strip()) if result.returncode == 0 else None
        passed = result.returncode == 0 and evidence.get('mode') == mode
        results.append(dict(mode=mode, passed=passed, evidence=evidence))
    except subprocess.TimeoutExpired as error:
        (out / (mode + '.log')).write_bytes((error.stdout or b'') + (error.stderr or b''))
        results.append(dict(mode=mode, passed=False, error='parent deadline exceeded'))
    print(json.dumps(results[-1]), flush=True)
    (out / 'result.json').write_text(json.dumps(dict(passed=all(x['passed'] for x in results),
                                                    results=results), indent=2) + '\n')
if not all(x['passed'] for x in results):
    raise SystemExit(1)
