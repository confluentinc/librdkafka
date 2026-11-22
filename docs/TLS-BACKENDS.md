# Experimental broker TLS backends and FIPS policy

This branch implements one private TLS backend per build: OpenSSL (the compatible
ordinary default), rustls-ffi 0.15.7, or none. It does not add a public plugin ABI,
change the public C/C++ ABI, or switch a live client's backend. `ssl.providers`
continues to mean OpenSSL provider modules. It never selects a TLS backend.

The strict profile is `ssl.fips.mode=required`, verified broker TLS/mTLS only.
It refuses plaintext/SASL, verification overrides, custom algorithm expressions,
engines, PKCS#12 and unimplemented cryptographic paths. Built-in OIDC/curl HTTPS
is outside this profile. SCRAM authentication and Admin credential upsertion are
unsupported under it; no assumption about PBKDF applicability is made. Ordinary
OpenSSL SCRAM, OAuth and HTTPS capabilities keep their existing implementation.

A passing runtime FIPS check is **not a FIPS 140-3 deployment qualification**.
See [qualification evidence](TLS-QUALIFICATION.md) and
[executed results](TLS-BACKENDS-RESULTS.md) for the actual scope.

## Build dependencies and commands

Use a C/C++ compiler, CMake, pkg-config, Python 3.11 or newer with venv/pip, and a
running Docker-compatible container engine. The Linux development dependency
helper downloads and verifies a pinned ordinary x86_64 rustls release. It retains
the release's license files outside the source tree. These binaries are not
FIPS artifacts. Other platforms must supply the exact matching 0.15.7 header,
library and `rustls.pc` themselves.

From the branch root, the single acceptance command is:

```sh
tests/tls_backend/run.sh
```

This creates an external build/venv directory (`/tmp/librdkafka-tls-tests-UID` by
default), builds OpenSSL/rustls/no-TLS C and C++ libraries, runs independent
policy/certificate checks, and launches the same real Kafka suite against both
TLS libraries. Set `RDKAFKA_TLS_TEST_ARTIFACTS` to retain it elsewhere.
`--unit-only` runs the broker-independent part and explicitly omits acceptance.
Missing runtime or broker readiness produces failure, never a passing skip.
No separate Kafka cluster, host trust-store change or account credential is used.

For an already installed pinned dependency:

```sh
export RUSTLS_PREFIX=/path/to/rustls-ffi-0.15.7
export PKG_CONFIG_PATH="$RUSTLS_PREFIX/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
cmake -S . -B /tmp/rdk-rustls -DRDKAFKA_TLS_BACKEND=rustls \
  -DWITH_CURL=OFF -DWITH_SASL=OFF -DRDKAFKA_BUILD_TESTS=OFF
cmake --build /tmp/rdk-rustls -j4
cmake --install /tmp/rdk-rustls --prefix /tmp/rdk-rustls-install
```

Use `-DRDKAFKA_TLS_BACKEND=openssl` or `none` for the other configurations.
Omitting the selector preserves the existing `WITH_SSL` switch. Rustls is a
broker-only build: OpenSSL, SCRAM/OAuth, curl and external SASL are disabled,
preventing those dependencies from silently bringing a second crypto stack.
`WITH_TLS` describes broker TLS internally; `WITH_SSL` remains OpenSSL support.
`builtin.features` retains `ssl` for both backends and separately gates
OpenSSL-dependent authentication capabilities. The build summary, generated
configuration and TLS diagnostics identify the selected backend.

Configure/mklove supports the same selector:

```sh
# Use a separate source checkout/copy: configure generates source config.h.
./configure --with-tls-backend=rustls --disable-curl --disable-sasl
make -j4
```

Do not share configure-generated `config.h` with a CMake build of another
backend. It shadows the generated CMake header. `run.sh` detects this condition.
`pkg-config --libs --static rdkafka` and the installed CMake package carry rustls
requirements. If distributing a static artifact, include the Rust/native
transitive dependencies and their notices, not only this project's license.

## How the Kafka fixture works

The fixture uses Testcontainers Python `DockerContainer` because the broker
requires custom PEM configuration. It pins `apache/kafka:4.1.1`. It generates a
private temporary test CA and separate server/client RSA certificates. The
server SAN is `localhost`; using `127.0.0.1` is intentionally a negative test.
The fixture explicitly selects IPv4 to match its loopback proxy; native IPv6
coverage remains pending.
The broker's externally mapped listener requires client authentication and
trusts only the fixture CA. Unexposed broker/controller listeners are local to
the disposable container. They are not host-published acceptance endpoints.

The container initially waits for a configuration upload. Testcontainers assigns
its host port, then the fixture inserts a local proxy's dynamically allocated
port into `advertised.listeners`, uploads the private PEM and configuration via
the Docker API, and starts Kafka. Every external bootstrap and advertised
connection passes through the same proxy. No fixed host ports or guessed port
reservation are used. Kafka metadata over verified mutual TLS establishes
readiness; the expired-server control uses a bounded internal Kafka API check
before asserting that the external TLS client fails.

Each roundtrip uses a unique topic and checks 96 distinct keys and all bytes of
96 64-KiB values. Runs cover `acks=all`, forced connection cuts with retries,
and `acks=0`, plus purge/failure-callback accounting under backpressure. The proxy fragments records into uneven 137-byte pieces, applies
bounded backpressure, and closes active sockets during acknowledged production.
The C adapter test separately exercises deterministic partial I/O invariants.
A stalled TLS peer test verifies the broker-owned handshake deadline on both
backends without emulating Kafka.

Startup, client operations and shutdown have deadlines. A `finally` block stops
and removes the container (including anonymous volumes), closes proxy sockets,
and the temporary directory removes credentials on success or failure. The
fixture creates no persistent network or host volume. Cleanup errors fail the suite and identify the container for manual removal;
proxy shutdown has one global deadline. Broker logs and client
errors are captured on failure; private PEM contents are never logged. Ryuk is
disabled by default to avoid requiring a sidecar socket mount; teardown is owned
by this process. An uncatchable process kill can bypass teardown, so inspect
Testcontainers labels before manually removing only this run's resources.

## Runtime prerequisites and manual maintenance

For Docker, `docker info` and `docker run --rm hello-world` should work under the
same user. For rootless Podman, enable its user API socket if needed, then set
`DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock`. Run `podman info` to
check the configured network helper. `RDKAFKA_TEST_NETWORK=slirp4netns` selects
that network mode for fixture containers where installed. This is a per-test
choice; the suite does not edit host-wide network/security configuration.
The results guide records the isolated API service used on the implementation
host because its default pasta helper could not sandbox in this environment.

To run already-built clients directly and preserve diagnostics in a file:

```sh
/path/to/venv/bin/python tests/tls_backend/acceptance.py \
  --client /path/to/client-openssl --client /path/to/client-rustls \
  > /tmp/rdk-tls-acceptance.log 2>&1
```

Expected output contains each certificate rejection, three successful roundtrips
and expired-server rejection for each backend, ending with an all-clients PASS.
A nonzero exit means the suite did not pass. To inspect a running fixture, use
`docker ps --filter label=org.testcontainers`, then `docker logs CONTAINER` and
`docker port CONTAINER`. The uploaded `/tmp/server.properties` identifies the
listener mapping, and PEM files stay inside the container/temp directory. To
clean up after an interrupted run, remove only that identified container using
`docker rm -fv CONTAINER`, then remove its `rdkafka-tls-*` temporary directory
when no fixture process uses it. Remove the external artifacts directory only
when you no longer need the builds and logs.

## Transport ownership and completion

The backend owns immutable client configuration and an opaque session. Existing
transport/broker code owns sockets, broker transitions, deadlines, request
ordering, in-flight accounting and delivery callbacks. The private ops include
connect/handshake/read/write, pending plaintext, poll interests, output service,
shutdown and partially initialized cleanup.

Rustls accepts at most a 16-KiB plaintext chunk at a time and limits its buffered
output to 64 KiB. An accepted prefix is recorded against the request buffer and
absolute slice offset. The caller's slice advances exactly once, after that
ciphertext has reached the socket's send operation. Retrying while blocked
flushes the existing ciphertext; it never resubmits the plaintext prefix. A
changed pending request fails instead of committing bytes to another request.
Timeout, purge and termination classify an accepted pending prefix as a started
request even at slice offset zero; the connection is discarded when cancellation
requires it. TLS close does not flush canceled application ciphertext. The
whitebox test checks this shared classification predicate and close behavior;
it does not execute every broker timeout callback. This preserves Kafka ordering and prevents `acks=0` delivery completion merely
because plaintext was copied into rustls. Socket acceptance does not establish
broker receipt; normal `acks=0` delivery semantics still apply.

Rustls output requests POLLOUT even with an empty Kafka send queue. Buffered
plaintext creates a synthetic readable event without waiting for the socket.
Handshake completion waits for the final ciphertext flush. Platform would-block
and interrupted I/O are retried; EOF, alerts and hard failures return through
normal broker failure accounting. Reconnect creates a new session and drops
pending state belonging to the failed connection.

## Compatibility and platform scope

```text
Build / feature                  Linux native    Windows          macOS
OpenSSL ordinary                 tested          definitions      not executed
No TLS                           tested          definitions      not executed
rustls ordinary broker TLS/mTLS   tested          CMake path only  CMake path only
OpenSSL required runtime policy   see results     not executed     not executed
ordinary FFI required-mode       refusal tested  not executed     not executed
exact AWS-LC required-mode        runtime tested  not executed     not executed
Qualified FIPS deployment         none claimed    none claimed     none claimed
```

The existing OpenSSL verification callback, engine/provider meanings, encrypted
keys and PKCS#12 behavior remain ordinary-OpenSSL features. Rustls rejects these
explicit options. Rustls accepts explicit PEM trust roots, PEM client chains and
unencrypted provider-supported keys. Ordinary memory setters support PEM and DER (private
DER keys must be PKCS#8 on rustls), validate before replacing, own input bytes,
and retain material across config duplication/clearing. Required OpenSSL mode
rejects memory private keys because their earlier parsing can execute crypto
outside its isolated context; use ssl.key.location or ssl.key.pem. Required
rustls mode rejects keys imported earlier through its ordinary provider. Unsupported encodings fail early.
Rustls uses WebPKI verification; platform trust probing and OpenSSL callback/error
semantics are not approximated. Optional client-auth issuer-selection parity,
CRLs and directory trust are not implemented. The profile is tested with required
mutual authentication; do not infer parity for optional-auth plus SASL.

Windows Visual Studio defaults remain OpenSSL and include the private dispatcher.
Existing vcpkg/NuGet/release packaging does not publish rustls variants. A new
Windows/macOS rustls package needs exact FFI linkage/deployment rules and native
installed-consumer tests before being advertised. Cross-platform source paths or
configured build files are not passed native tests.

## Optional positive OpenSSL required-mode checks

Install the module using its vendor's procedure first. Produce a local config
from the template, preserving the vendor's integrity/installation data:

```sh
sed 's|@FIPS_MODULE_CONFIG@|/path/to/vendor/fipsmodule.cnf|' \
  tests/tls_backend/openssl-required.cnf.in > /tmp/rdk-required.cnf
RDKAFKA_TEST_FIPS_CONFIG=/tmp/rdk-required.cnf tests/tls_backend/run.sh
```

This also runs real-module startup/isolation/Admin controls, a test-only self-test
failure injection, a disabled-safety-check control and required-mode real Kafka
TLS/mTLS acceptance. Inspect every result; an unavailable module is a failure of
this optional positive run. The preload helper is compiled separately and never
linked into the production library. It checks refusal control flow, not the
module's self-test implementation. On non-Linux platforms supply an equivalent
safe native test mechanism; the checked-in preload test is Linux only.

To run only policy controls against existing artifacts:

```sh
/path/to/venv/bin/python tests/tls_backend/fips_checks.py \
  --policy /path/to/policy-openssl --config /tmp/rdk-required.cnf \
  --selftest-failure-library /path/to/selftest-fail.so
```

The exact-policy AWS-LC/rustls source-build experiment and its separate positive
runtime commands are in [EXACT-FIPS-MODULE.md](../packaging/rustls/EXACT-FIPS-MODULE.md).
These positive paths remain runtime engineering tests on this host, without a
qualified operating-environment or released-artifact claim.
