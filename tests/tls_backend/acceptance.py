#!/usr/bin/env python3
"""Real Kafka TLS/mTLS acceptance; exits nonzero when runtime is unavailable."""
import argparse
import contextlib
import datetime as dt
import io
import os
from pathlib import Path
import socket
import struct
import subprocess
import tarfile
import tempfile
import threading
import time
import uuid

# A Podman Docker-compatible socket is usable without changing host policy.
if not os.environ.get("DOCKER_HOST") and not Path("/var/run/docker.sock").exists():
    candidate = Path(f"/run/user/{os.getuid()}/podman/podman.sock")
    if candidate.exists():
        os.environ["DOCKER_HOST"] = f"unix://{candidate}"
# The fixture owns teardown, including failed starts, without a privileged sidecar.
os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")
import docker
from testcontainers.core.container import DockerContainer
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID, ExtendedKeyUsageOID

IMAGE = "apache/kafka:4.1.1"


def certificates(root):
    now = dt.datetime.now(dt.timezone.utc)
    def issue(name, ca=None, expired=False, server=False):
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
        issuer, signer = (ca[0].subject, ca[1]) if ca else (subject, key)
        b = (x509.CertificateBuilder().subject_name(subject).issuer_name(issuer)
             .public_key(key.public_key()).serial_number(x509.random_serial_number())
             .not_valid_before(now - dt.timedelta(days=3))
             .not_valid_after(now + dt.timedelta(days=-1 if expired else 2))
             .add_extension(x509.BasicConstraints(ca=ca is None, path_length=None), critical=True))
        if ca:
            b = b.add_extension(x509.ExtendedKeyUsage([
                ExtendedKeyUsageOID.SERVER_AUTH if server else ExtendedKeyUsageOID.CLIENT_AUTH]), False)
            if server:
                b = b.add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), False)
        cert = b.sign(signer, hashes.SHA256())
        (root / f"{name}.crt").write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        p = root / f"{name}.key"
        p.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                       serialization.NoEncryption()))
        p.chmod(0o600)
        return cert, key
    ca = issue("ca")
    other = issue("unknown-ca")
    issue("server", ca, server=True)
    issue("expired-server", ca, expired=True, server=True)
    issue("client", ca)
    issue("invalid-client", other)
    issue("expired-client", ca, expired=True)


class FaultProxy:
    """Bounded streaming proxy: no application buffering beyond one 4 KiB block."""
    def __init__(self):
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen()
        self.listener.settimeout(.2)
        self.port = self.listener.getsockname()[1]
        self.target = None
        self.stop = threading.Event()
        self.sockets = set()
        self.lock = threading.Lock()
        self.threads = []
        self.fragment = False
        self.drop_after = None
        self.dropped = False
        self.bytes_forwarded = 0
        self.thread = threading.Thread(target=self.accept, daemon=True)
        self.thread.start()

    def accept(self):
        while not self.stop.is_set():
            try:
                incoming, _ = self.listener.accept()
            except (socket.timeout, OSError):
                continue
            try:
                outgoing = socket.create_connection(self.target, timeout=5)
            except OSError:
                incoming.close()
                continue
            with self.lock:
                self.sockets.update((incoming, outgoing))
            for src, dst in ((incoming, outgoing), (outgoing, incoming)):
                t = threading.Thread(target=self.pipe, args=(src, dst), daemon=True)
                self.threads.append(t)
                t.start()

    def pipe(self, src, dst):
        try:
            src.settimeout(.5)
            while not self.stop.is_set():
                try:
                    block = src.recv(4096)
                except socket.timeout:
                    continue
                if not block:
                    break
                if self.fragment:
                    # Uneven fragments split TLS headers and Kafka response bodies.
                    for at in range(0, len(block), 137):
                        dst.sendall(block[at:at + 137])
                    time.sleep(.0005)  # Intentional bounded backpressure fault.
                else:
                    dst.sendall(block)
                with self.lock:
                    self.bytes_forwarded += len(block)
                    drop = (self.drop_after is not None and not self.dropped and
                            self.bytes_forwarded >= self.drop_after)
                    if drop:
                        self.dropped = True
                if drop:
                    self.disconnect()
        except OSError:
            pass
        finally:
            for s in (src, dst):
                with contextlib.suppress(OSError):
                    s.shutdown(socket.SHUT_RDWR)
                s.close()
                with self.lock:
                    self.sockets.discard(s)

    def disconnect(self):
        with self.lock:
            current = list(self.sockets)
        for s in current:
            with contextlib.suppress(OSError):
                s.shutdown(socket.SHUT_RDWR)

    def close(self):
        self.stop.set()
        self.listener.close()
        self.disconnect()
        self.thread.join(3)
        for t in self.threads:
            t.join(2)


def upload(container, files):
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w") as archive:
        for name, content in files.items():
            entry = tarfile.TarInfo(name)
            entry.size = len(content)
            entry.mode = 0o600
            archive.addfile(entry, io.BytesIO(content))
    if not container.get_wrapped_container().put_archive("/tmp", data.getvalue()):
        raise RuntimeError("could not upload temporary broker configuration")


def run_client(binary, op, conf, topic=None, timeout=85):
    args = [str(binary), op, str(conf)] + ([topic] if topic else [])
    return subprocess.run(args, capture_output=True, text=True, timeout=timeout)


def suite(binary, root, expired_server=False, fips=False):
    proxy = FaultProxy()
    # Start an idle container first, then resolve its dynamically assigned port and
    # upload config. No bound-port guessing or fixed host ports are required.
    script = ("set -e; for i in $(seq 1 600); do test ! -f /tmp/ready && sleep 0.1 || break; done; "
              "test -f /tmp/ready; /opt/kafka/bin/kafka-storage.sh format -t "
              "MkU3OEVBNTcwNTJENDM2Qk -c /tmp/server.properties; "
              "exec /opt/kafka/bin/kafka-server-start.sh /tmp/server.properties")
    container = (DockerContainer(IMAGE).with_exposed_ports(9093)
                 .with_kwargs(entrypoint=["/bin/bash", "-c"], user="0")
                 .with_command([script]).with_env("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m"))
    if os.environ.get("RDKAFKA_TEST_NETWORK"):
        container.with_kwargs(entrypoint=["/bin/bash", "-c"], user="0",
                              network_mode=os.environ["RDKAFKA_TEST_NETWORK"])
    def config(name, **updates):
        props = {"bootstrap.servers": f"localhost:{proxy.port}", "security.protocol": "ssl",
                 "broker.address.family": "v4",
                 "ssl.ca.location": str(root / "ca.crt"),
                 "ssl.certificate.location": str(root / "client.crt"),
                 "ssl.key.location": str(root / "client.key")}
        if fips:
            props["ssl.fips.mode"] = "required"
            if os.environ.get("RDKAFKA_TEST_FIPS_CONFIG"):
                props["ssl.fips.config"] = os.environ["RDKAFKA_TEST_FIPS_CONFIG"]
        props.update(updates)
        dest = root / f"{name}.conf"
        dest.write_text("".join(f"{k}={v}\n" for k, v in props.items() if v is not None))
        return dest
    try:
        container.start()
        proxy.target = (container.get_container_host_ip(), int(container.get_exposed_port(9093)))
        server = "expired-server" if expired_server else "server"
        pem = (root / f"{server}.key").read_bytes() + (root / f"{server}.crt").read_bytes()
        properties = f"""process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9094
controller.listener.names=CONTROLLER
listeners=INTERNAL://:9092,EXTERNAL://:9093,CONTROLLER://:9094
advertised.listeners=INTERNAL://localhost:9092,EXTERNAL://localhost:{proxy.port}
listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:SSL,CONTROLLER:PLAINTEXT
inter.broker.listener.name=INTERNAL
log.dirs=/tmp/kafka-data
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
ssl.keystore.type=PEM
ssl.keystore.location=/tmp/server.pem
ssl.truststore.type=PEM
ssl.truststore.location=/tmp/ca.crt
ssl.client.auth=required
"""
        upload(container, {"server.pem": pem, "ca.crt": (root / "ca.crt").read_bytes(),
                           "server.properties": properties.encode(), "ready": b"yes"})
        good = config("good")
        # Readiness is an authenticated Kafka metadata exchange through the tested
        # client. An expired broker instead uses its private, unexposed listener.
        deadline = time.monotonic() + 100
        while time.monotonic() < deadline:
            if expired_server:
                ready = container.exec(["timeout", "8", "/opt/kafka/bin/kafka-broker-api-versions.sh",
                                        "--bootstrap-server", "localhost:9092"])[0] == 0
            else:
                ready = run_client(binary, "probe", good, timeout=12).returncode == 0
            if ready:
                break
            if container.get_wrapped_container().status == "exited":
                raise RuntimeError("Kafka exited during startup")
        else:
            raise RuntimeError("authenticated Kafka readiness timed out")
        if expired_server:
            result = run_client(binary, "probe", good)
            assert result.returncode == 3, result.stdout + result.stderr
            print(f"PASS {binary.name}: expired server rejected", flush=True)
            return
        cases = [
            ("wrong-hostname", {"bootstrap.servers": f"127.0.0.1:{proxy.port}"}),
            ("unknown-ca", {"ssl.ca.location": str(root / "unknown-ca.crt")}),
            ("missing-client", {"ssl.certificate.location": None, "ssl.key.location": None}),
            ("invalid-client", {"ssl.certificate.location": str(root / "invalid-client.crt"),
                                "ssl.key.location": str(root / "invalid-client.key")}),
            ("expired-client", {"ssl.certificate.location": str(root / "expired-client.crt"),
                                "ssl.key.location": str(root / "expired-client.key")}),
        ]
        for name, changes in cases:
            result = run_client(binary, "probe", config(name, **changes))
            assert result.returncode == 3, f"{name}: {result.returncode}\n{result.stdout}{result.stderr}"
            print(f"PASS {binary.name}: {name} rejected", flush=True)
        for op, faults in (("roundtrip", False), ("roundtrip", True), ("acks0", True)):
            proxy.fragment = faults
            proxy.disconnect()
            proxy.bytes_forwarded = 0
            proxy.dropped = False
            proxy.drop_after = 300000 if faults and op != "acks0" else None
            topic = "tls-" + uuid.uuid4().hex
            result = run_client(binary, op, good, topic)
            assert result.returncode == 0, result.stdout + result.stderr
            if proxy.drop_after:
                assert proxy.dropped, "disconnect injection was not exercised"
            print(f"PASS {binary.name}: {op} faults={faults}: {result.stdout.strip()}", flush=True)
    except BaseException:
        with contextlib.suppress(Exception):
            out, err = container.get_logs()
            # Broker logs contain paths/handshake errors, never uploaded PEM values.
            print((out + err).decode(errors="replace")[-18000:], flush=True)
        raise
    finally:
        proxy.close()
        with contextlib.suppress(Exception):
            container.stop(force=True, delete_volume=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--client", type=Path, action="append", required=True,
                        help="C acceptance binary linked to one backend; repeat for both")
    parser.add_argument("--fips", action="store_true", help="Require FIPS mode for every tested client")
    args = parser.parse_args()
    try:
        runtime = docker.from_env(timeout=10)
        runtime.ping()
        print("Container runtime:", runtime.version().get("Version"), flush=True)
    except Exception as exc:
        raise SystemExit(f"UNMET acceptance criterion: Docker-compatible runtime unavailable: {exc}")
    with tempfile.TemporaryDirectory(prefix="rdkafka-tls-") as temp:
        root = Path(temp)
        root.chmod(0o700)
        certificates(root)
        for binary in args.client:
            suite(binary.resolve(), root, fips=args.fips)
            suite(binary.resolve(), root, expired_server=True, fips=args.fips)
    print("PASS: real Kafka TLS/mTLS acceptance completed for all supplied clients", flush=True)


if __name__ == "__main__":
    main()
