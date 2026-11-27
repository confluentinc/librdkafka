#!/usr/bin/env python3
"""Optional real-module policy controls, not deployment qualification."""
import argparse
import os
from pathlib import Path
import subprocess
import tempfile
from acceptance import certificates

p = argparse.ArgumentParser()
p.add_argument("--policy", type=Path, required=True)
p.add_argument("--config", type=Path, required=True)
p.add_argument("--selftest-failure-library", type=Path, required=True)
a = p.parse_args()
with tempfile.TemporaryDirectory(prefix="rdkafka-fips-check-") as temp:
    root = Path(temp)
    root.chmod(0o700)
    certificates(root)
    env = dict(os.environ, RDKAFKA_TEST_FIPS_CONFIG=str(a.config.resolve()))
    command = [str(a.policy.resolve()), str(root / "ca.crt")]
    subprocess.run(command + ["available", str(root / "client.key")], env=env, check=True, timeout=45)
    # The explicitly queried provider self-test fails in this process only.
    # A real module must have loaded first, otherwise this is not the intended control.
    failed = dict(env, LD_PRELOAD=str(a.selftest_failure_library.resolve()))
    subprocess.run(command + ["unavailable", str(root / "client.key")], env=failed, check=True, timeout=45)
    content = a.config.read_text()
    old = "tls1-prf-ems-check = 1"
    if old not in content:
        raise SystemExit("Use the branch config template: safety-check injection point missing")
    disabled = root / "disabled-check.cnf"
    disabled.write_text(content.replace(old, "tls1-prf-ems-check = 0"))
    subprocess.run(command + ["unavailable", str(root / "client.key")],
                   env=dict(env, RDKAFKA_TEST_FIPS_CONFIG=str(disabled)), check=True, timeout=45)
print("PASS: positive module runtime path, self-test failure and disabled security check refusal")
