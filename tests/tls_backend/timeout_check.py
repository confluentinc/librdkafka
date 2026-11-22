#!/usr/bin/env python3
"""A peer that accepts TCP but never replies; does not emulate Kafka."""
import argparse
import contextlib
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
from acceptance import certificates

p = argparse.ArgumentParser()
p.add_argument("--client", type=Path, action="append", required=True)
a = p.parse_args()
with tempfile.TemporaryDirectory(prefix="rdkafka-stalled-tls-") as temp:
    root = Path(temp)
    root.chmod(0o700)
    certificates(root)
    stop = threading.Event()
    peers = []
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    listener.settimeout(.1)
    def accept():
        while not stop.is_set():
            try:
                peer, _ = listener.accept()
                peers.append(peer)
            except (OSError, socket.timeout):
                pass
    worker = threading.Thread(target=accept, daemon=True)
    worker.start()
    try:
        endpoint = f"localhost:{listener.getsockname()[1]}"
        for binary in a.client:
            subprocess.run([str(binary.resolve()), endpoint, str(root / "ca.crt")],
                           check=True, timeout=10)
    finally:
        stop.set()
        listener.close()
        worker.join(2)
        for peer in peers:
            with contextlib.suppress(OSError):
                peer.close()
