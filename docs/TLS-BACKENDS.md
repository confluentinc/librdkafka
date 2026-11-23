# Experimental broker TLS backends and FIPS policy

This branch implements one private TLS backend per build: OpenSSL (the compatible
ordinary default), rustls through rustls-ffi 0.15.7, or none. It does not add a
public plugin ABI, change the public C/C++ ABI, or switch a live client's backend.
`ssl.providers` continues to mean OpenSSL provider modules. It never selects a TLS
backend.

The strict profile is `ssl.fips.mode=required`, verified broker TLS/mTLS only.
It refuses plaintext/SASL, verification overrides, custom algorithm expressions,
engines, PKCS#12 and unimplemented cryptographic paths. Built-in OIDC/curl HTTPS
is outside this profile. SCRAM authentication and Admin credential upsertion are
unsupported under it; no assumption about PBKDF applicability is made. Ordinary
OpenSSL SCRAM, OAuth and HTTPS capabilities keep their existing implementation.

A passing runtime FIPS check is **not a FIPS 140-3 deployment qualification**.
See [qualification evidence](TLS-QUALIFICATION.md) and
[executed results](TLS-BACKENDS-RESULTS.md) for the actual scope.

## The rustls backend uses the application's provider

The rustls backend is for a Rust application that links librdkafka, for example
through the `rdkafka` crate. librdkafka contains no rustls-ffi, rustls or crypto
code. It uses only the rustls-ffi 0.15.7 header. The application must:

1. Depend on `rustls-ffi = "=0.15.7"` as a Rust library. Use
   `default-features = false` and the `no_log_capture` feature, so that
   rustls-ffi does not install a global `log` logger. Reference the crate (for
   example `use rustls_ffi as _;`), because rustc does not link an unused crate.
2. Link static librdkafka into the same binary. Its `rustls_*` references then
   resolve to the application's rustls-ffi.
3. Call `rustls::crypto::CryptoProvider::install_default()` before it creates a
   TLS client or imports a private key.

librdkafka gets the provider from `rustls_crypto_provider_default()` for the
client configuration and for every private-key import. If no provider is
installed, client creation and key import fail with "no process-default rustls
crypto provider is installed". There is no fallback provider. librdkafka also
refuses a linked rustls-ffi whose `rustls_version()` is not 0.15.7.

The provider state is global to one rustls copy. A rustls-ffi library linked
into librdkafka (a static archive, a shared library or a DLL) has its own state
and does not see the application's provider. For this reason librdkafka never
links rustls-ffi. A shared librdkafka could only use the application's
rustls-ffi through unresolved symbols, and a Windows DLL cannot import symbols
from the executable. CMake rejects a shared rustls build, and configure builds
only static libraries.

`ssl.fips.mode=required` with rustls is decided at runtime by the installed
provider. Client creation requires `rustls_crypto_provider_fips()`, then
`rustls_client_config_fips()` (rustls 0.23.46 also requires extended master
secret here), and each connection requires `rustls_connection_fips()`. A
provider that reports FIPS does not prove a validated module; that is the
application's deployment evidence. On Windows, `rustls-cng-crypto` 0.1.2 reports
FIPS from `BCryptGetFipsAlgorithmMode`, which follows the legacy Windows FIPS
policy registry value (measured on an Azure Windows Server 2022 VM, 2026-11-23).
Required mode with that provider therefore needs the legacy policy enabled; see
[Windows checks](../packaging/rustls/WINDOWS-CHECKS.md).

## Build dependencies and commands

Use a C/C++ compiler, CMake, pkg-config, Python 3.11 or newer with venv/pip, a
running Docker-compatible container engine, and for rustls a Rust toolchain
(Rust 1.85 or newer, as rustls-ffi 0.15.7 requires). The rustls test host builds
aws-lc-rs as a stand-in provider, which needs CMake and a C compiler; its FIPS
stand-in also needs Go.

From the branch root, the single acceptance command is:

```sh
tests/tls_backend/run.sh            # add --rustls-fips for the FIPS stand-in
```

This creates an external build/venv directory (`/tmp/librdkafka-tls-tests-UID` by
default), builds OpenSSL and no-TLS C and C++ libraries, runs independent
policy/certificate checks and the real Kafka suite for OpenSSL, and then runs
`tests/tls_backend/rustls.sh`. That script builds static rustls librdkafka and
the Rust test host in `tests/tls_backend/rustls_host`, and runs the rustls
checks through the host. Set `RDKAFKA_TLS_TEST_ARTIFACTS` to retain the
artifacts elsewhere. `--unit-only` runs the broker-independent part and
explicitly omits acceptance. Missing runtime or broker readiness produces
failure, never a passing skip. No separate Kafka cluster, host trust-store
change or account credential is used.

To build static rustls librdkafka by hand, give CMake the directory that holds
the rustls-ffi 0.15.7 `rustls.h`. A build script that depends on rustls-ffi
receives it as `DEP_RUSTLS_FFI_INCLUDE`; the crate source also ships it as
`src/rustls.h`. Without `RUSTLS_INCLUDE_DIR`, CMake asks pkg-config for
`rustls = 0.15.7`, for the include directory only.

```sh
cmake -S . -B /tmp/rdk-rustls -DRDKAFKA_TLS_BACKEND=rustls \
  -DRUSTLS_INCLUDE_DIR=/path/to/rustls-ffi-0.15.7/src \
  -DRDKAFKA_BUILD_STATIC=ON -DWITH_CURL=OFF -DWITH_SASL=OFF
cmake --build /tmp/rdk-rustls -j4
cmake --install /tmp/rdk-rustls --prefix /tmp/rdk-rustls-install
```

CMake checks the header version with a compile-only test. The rustls build
turns off examples and tests, because they have no rustls-ffi. The installed
`rdkafka-static.pc` and the CMake package do not name rustls-ffi as a
dependency; their description and `RdKafka_RUSTLS_REQUIRES_APPLICATION_RUSTLS_FFI`
say that the application must supply it.

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
./configure --with-tls-backend=rustls --with-rustls-include-dir=/path/to/rustls-ffi-0.15.7/src \
  --disable-curl --disable-sasl
make -j4
```

With rustls, configure sets `MKL_NO_SHARED_LIB`: make builds and installs only
the static C and C++ libraries and their `-static.pc` files, and skips the
examples and the `CONFIGURATION.md` regeneration that needs them.
Do not share configure-generated `config.h` with a CMake build of another
backend. It shadows the generated CMake header. `run.sh` detects this condition.
If distributing a static artifact, include the Rust/native transitive
dependencies and their notices, not only this project's license.

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
  --client /path/to/client-openssl --client "$ARTIFACTS/rustls/client-ordinary" \
  > /tmp/rdk-tls-acceptance.log 2>&1
```

The rustls clients are small scripts that `rustls.sh` writes; each runs the
Rust test host with one C program. Expected output contains each certificate rejection, three successful roundtrips
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
Build / feature                        Linux native    Windows          macOS arm64
OpenSSL ordinary                       see results     tested (x64)     tested
No TLS                                 see results     tested (x64)     tested
rustls, application aws-lc-rs stand-in not executed    not executed     tested
rustls, application FIPS stand-in      not executed    not executed     tested
rustls, application rustls-cng-crypto  n/a             not executed     n/a
OpenSSL required runtime policy        see results     refusal tested   not executed
Qualified FIPS deployment              none claimed    none claimed     none claimed
```

Results for the removed AWS-LC rustls mode (librdkafka linked its own
rustls-ffi) are kept as dated history in the results files. They are not
evidence for the current rustls backend.

The existing OpenSSL verification callback, engine/provider meanings, encrypted
keys and PKCS#12 behavior remain ordinary-OpenSSL features. Rustls rejects these
explicit options. Rustls accepts explicit PEM trust roots, PEM client chains and
unencrypted provider-supported keys. Ordinary memory setters support PEM and DER (private
DER keys must be PKCS#8 on rustls), validate before replacing, own input bytes,
and retain material across config duplication/clearing. Required OpenSSL mode
rejects memory private keys because their earlier parsing can execute crypto
outside its isolated context; use ssl.key.location or ssl.key.pem. Required
rustls mode rejects keys imported earlier without the required policy, even
when the same provider is installed. Unsupported encodings fail early.
Rustls uses WebPKI verification; platform trust probing and OpenSSL callback/error
semantics are not approximated. Optional client-auth issuer-selection parity,
CRLs and directory trust are not implemented. The profile is tested with required
mutual authentication; do not infer parity for optional-auth plus SASL.

Windows Visual Studio defaults remain OpenSSL and include the private dispatcher.
Existing vcpkg/NuGet/release packaging does not publish rustls variants. On
Windows, the rustls backend is a static `rdkafka.lib` in the application's Rust
binary; the planned provider is `rustls-cng-crypto`, which uses the Windows CNG
module. That route has not run yet; see [Windows checks](../packaging/rustls/WINDOWS-CHECKS.md)
for the exact test. A rustls package still needs deployment rules and native
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
