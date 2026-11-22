#!/usr/bin/env python3
"""Configuration and certificate tests, deliberately independent of Kafka."""
import argparse
import os
from pathlib import Path
import subprocess
import tempfile
from acceptance import certificates
from cryptography import x509
from cryptography.hazmat.primitives import serialization

p = argparse.ArgumentParser()
p.add_argument("--policy", type=Path, action="append", default=[])
p.add_argument("--cert-buffers", type=Path, action="append", default=[])
p.add_argument("--transport", type=Path, help="Deterministic real-rustls socketpair test")
p.add_argument("--positive-policy", type=Path, help="Requires a working configured FIPS module")
a = p.parse_args()
with tempfile.TemporaryDirectory(prefix="rdkafka-tls-unit-") as temp:
    root = Path(temp)
    root.chmod(0o700)
    certificates(root)
    for name in ("ca", "client"):
        cert = x509.load_pem_x509_certificate((root / f"{name}.crt").read_bytes())
        (root / f"{name}.der").write_bytes(cert.public_bytes(serialization.Encoding.DER))
    key = serialization.load_pem_private_key((root / "client.key").read_bytes(), None)
    (root / "key.der").write_bytes(key.private_bytes(serialization.Encoding.DER,
                  serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    if a.transport:
        subprocess.run([str(a.transport.resolve()), str(root / "ca.crt"),
                        str(root / "server.crt"), str(root / "server.key")], check=True, timeout=45)
    for binary in a.cert_buffers:
        subprocess.run([str(binary.resolve())] + [str(root / n) for n in
                       ("ca.crt", "client.crt", "client.key", "ca.der", "client.der", "key.der")],
                       check=True, timeout=30)
    modules = root / "empty-modules"
    modules.mkdir()
    empty_config = root / "empty.cnf"
    empty_config.write_text("")
    env = dict(os.environ, OPENSSL_CONF=str(empty_config), OPENSSL_MODULES=str(modules))
    env.pop("RDKAFKA_TEST_FIPS_CONFIG", None)
    for binary in a.policy:
        subprocess.run([str(binary.resolve()), str(root / "ca.crt"), "unavailable", str(root / "client.key")],
                       check=True, timeout=45, env=env)
    if a.positive_policy:
        subprocess.run([str(a.positive_policy.resolve()), str(root / "ca.crt"), "available", str(root / "client.key")],
                       check=True, timeout=45)
