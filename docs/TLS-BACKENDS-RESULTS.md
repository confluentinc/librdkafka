# Executed checks and remaining acceptance criteria

This is runtime and build evidence, with no qualified deployment claim.

## Current results: rustls with the application's provider, 2026-11-23

Host: macOS 26 (Darwin 25.6.0) arm64, Apple clang, CMake, Rust/Cargo 1.97.1,
Python 3.14.6, Docker 27.4 with `apache/kafka:4.1.1`, Homebrew OpenSSL 3.
Branch head for code: `5e15fecd`. The rustls backend links no rustls-ffi; the
Rust test host links rustls-ffi 0.15.7 (rustls 0.23.46) and installs a stand-in
provider: aws-lc-rs 1.19.0 (aws-lc-sys 0.47.0) or the rustls FIPS provider
(aws-lc-fips-sys 0.14.4).

Command, from the branch root:

```sh
caffeinate -ims tests/tls_backend/run.sh --rustls-fips
```

The run used a scratch copy of `run.sh` that skips the venv and pip steps and
uses an existing venv. The local package proxy did not offer
`certifi==2026.11.20` from `requirements.txt`, so that venv has
`certifi==2026.10.23`; all other pins match. The laptop slept during earlier
runs: two Kafka runs failed with a consumer `FetchRequest` timed out after 15 to
18 minutes, which matches the sleep records in `pmset -g log`. Those runs are
not counted. The counted run used `caffeinate` on AC power.

```text
Check                                   OpenSSL  No TLS  rustls aws-lc-rs  rustls FIPS stand-in
Required-policy refusal (unavailable)   PASS     PASS    PASS              n/a
Positive required policy (available)    n/a      n/a     n/a               PASS
No installed provider refuses           n/a      n/a     PASS              n/a
Certificate buffers, PEM/DER            PASS     n/a     PASS              n/a
Strict key import (fips_cert_buffers)   n/a      n/a     n/a               PASS
Transport whitebox (socketpair)         n/a      n/a     PASS              n/a
Stalled handshake deadline              PASS     n/a     PASS              n/a
Kafka acceptance, ordinary policy       PASS     n/a     PASS              n/a
Kafka acceptance, --fips                n/a      n/a     n/a               PASS
```

`rustls.sh` also checks linkage before the tests: `nm -g librdkafka.a` has no
defined `rustls_*` or AWS-LC symbols, and each host binary has exactly one
`rustls_version` definition and no rustls dynamic library. The FIPS host loads
`libaws_lc_fips_0_14_4_crypto.dylib`, because aws-lc-fips-sys builds the module
as a shared library on macOS. Configure/mklove static-only rustls builds and
the CMake error cases are in [build checks](../packaging/rustls/CHECKS.md).

A separate experiment confirmed the linking model: a C function that called
`rustls_crypto_provider_default()` saw the provider that a Rust `main`
installed when both used the rustls-ffi rlib, and saw NULL when the C function
used `librustls.dylib` from the rustls-ffi v0.15.7 macOS release.

Later on 2026-11-23 the first Windows run with `rustls-cng-crypto` found two
defects: librdkafka rejected the "unknown" certificate/key match result of CNG
keys (7601), and an empty provider gave a vague error (7112). Both are fixed
([Windows checks](../packaging/rustls/WINDOWS-CHECKS.md)). The test host
reproduces them on macOS with `--opaque-keys` and `--empty-provider`. Before
the fix, `key_match --opaque-keys` failed with 7601 and the empty-provider
check failed with 7112. After the fix, `rustls.sh --fips` passed again,
including `key_match` (match and 7600 mismatch), the opaque-key match, the
empty-provider refusal, and Kafka acceptance with the opaque-key client.

The Windows rerun at `257b1cf9` on Windows Server 2022 build 20348.5870
passed every check with `rustls-cng-crypto`: with `cng` and the legacy FIPS
policy off (no-provider, policy refusal, certificate buffers, opaque key match,
empty-provider refusal, handshake deadline, Kafka acceptance), and with
`cng-fips` and the policy on (`fips=true`; positive policy, strict key import,
opaque key match, handshake deadline, Kafka acceptance with `--fips`). Details
are in [Windows checks](../packaging/rustls/WINDOWS-CHECKS.md).

Not run: Linux for the current rustls backend, the transport whitebox test,
OpenSSL, no TLS and MSBuild on Windows at this head, the OpenSSL positive required-mode checks on macOS (no OpenSSL FIPS provider
installed), and the existing `tests/` regression runner.

## History: 2026-11-22 results before the provider change

These results are for commit `83d4be55` and earlier. The OpenSSL and no-TLS
results still describe those backends. Every rustls result in this section is
for the removed AWS-LC mode, in which librdkafka linked its own rustls-ffi
(ordinary aws-lc-rs, aws-lc-fips-sys 0.14.3 or an exact AWS-LC 4.0.0 build).
They are not evidence for the current rustls backend. `EXACT-FIPS-MODULE.md`
and `install-dev-dependency.sh` were removed with that mode; recover them from
commit `83d4be55` if needed.

Implementation host: Fedora 43 x86_64, kernel 7.3.3, GCC 15.3.1, CMake 3.31.11,
Rust/Cargo 1.90.0, Go 1.26.10, Python 3.14.10, Podman 5.8.9 Docker-compatible API.
Branch began at `ad4391a0e433a275f4c07a26f98ad70142b3e3a0`; origin/master and
upstream/master were identical. The original checkout remained separate.
This is runtime/build evidence, with no qualified deployment claim.

### Build and consumer results

Passed native CMake C/C++ builds: ordinary OpenSSL, no TLS, ordinary rustls shared
and static, and rustls shared with the exact-policy AWS-LC FIPS 4.0.0 source.
Passed configure/mklove builds: OpenSSL, none, ordinary rustls and exact-module
rustls FIPS. Installed ordinary rustls CMake/pkg-config shared/static consumers
linked and ran. The embedded ordinary static consumer's full link map and
DT_NEEDED contained no OpenSSL. The exact-module shared wrapper hides native
crypto archive symbols and passed mixed OpenSSL 3 process tests in both link
orders. Unsafe FIPS static packaging is rejected; configure installation omits
those archives and static metadata. See [build checks](../packaging/rustls/CHECKS.md)
and the exact module experiment (removed file `EXACT-FIPS-MODULE.md`) for commands,
artifact hashes, source comparison, features and notices.

Public C/C++ headers were not changed. C++ libraries and installed consumers
build. This is not a complete downstream ABI/bindings certification.

### Kafka and transport results

The same Testcontainers fixture passed against four client variants:

```text
Variant                          Ordinary policy     Required policy
OpenSSL 3.5.9                    PASS                PASS (vendor config)
rustls-ffi 0.15.7 ordinary        PASS                refusal PASS
rustls-ffi + exact AWS-LC 4.0.0   separate artifact   PASS (runtime only)
```

Each positive variant uses a real `apache/kafka:4.1.1` broker, verified server TLS
and required mutual TLS. Results cover authenticated metadata; three complete
96-record roundtrips checking keys, counts and every byte of 64-KiB values;
wrong hostname, unknown CA, missing/invalid/expired client credentials and expired
server rejection; uneven fragmentation/backpressure; forced disconnect/reconnect;
and `acks=0` delivery. A further real-broker `acks=0` purge under proxy-induced
backpressure checks that every callback is accounted for and unsent records
report failure. In the completed pause-triggered runs, all 96 records reported
failure and none reported success. Containers and temporary credentials are removed in finally
blocks. These tests use IPv4 explicitly; IPv6 and trailing-dot cases are pending.

Deterministic tests use the actual rustls adapter and a real TLS client/server
socketpair, not emulated Kafka. They pass with thousands of forced short writes
and fragmented reads, forced EAGAIN, exactly 131071 payload bytes, deferred slice
commitment, changed-request retry rejection, bounded pending prefix, output
flush without a Kafka send queue, buffered plaintext without a read callback,
close-notify, handshake EOF and partial initialization cleanup. They also verify
that a pending prefix counts as started at slice offset zero with
`socket.max.fails=0`, and cancellation/close performs no write or app-data delivery
after unblocking. This tests the predicate used by broker timeout/purge paths;
it does not execute every broker cancellation callback in isolation.

A stalled TLS peer test passed on both backends: broker-owned connection setup
timeouts close a peer that accepts TCP but never completes TLS. This peer does
not emulate Kafka. The real Kafka suite remains an independent requirement.

### Policy and regression results

Passed focused configuration and certificate tests on OpenSSL, ordinary rustls
and no-TLS builds: required-mode refusal without capability; disabled certificate
or hostname verification; unsupported SASL/algorithm options; immediate invalid
certificate replacement; owned input buffers; PEM/DER; duplication and clearing;
and private-key import provenance. The positive exact-module rustls test also
passes strict provider key-buffer import and lifetime checks.

OpenSSL positive policy tests include the installed vendor module data via the
branch template, enforce enabled advertised safety controls, and pass concurrent
ordinary/strict client isolation and actual Admin SCRAM upsert refusal. Removing
the module fails startup without fallback. A separate preload-only helper makes
the explicit provider self-test call fail; a disabled safety-control configuration
also fails. These controls verify enforcement, not the module's own validation.

Existing brokerless regression runner: `0000_unittests`, `0004_conf`,
`0080_admin_ut`, `0133_ssl_keys` and `0134_ssl_provider` passed. `0124` was skipped
because this build does not support its engine property. Broker-dependent `0097`,
`0081`, `0126` and `0135` were not claimed as executed; the new real TLS fixture
covers the implemented broker scope, while broader authentication remains out
of strict scope. The full upstream test suite was not run.

### Commands and local logs

From the branch root the complete entry point is `tests/tls_backend/run.sh`.
The exact implementation-host invocation was:

```sh
DOCKER_HOST=unix:///home/j/Dev/librdkafka-worktrees/deps/test-podman.sock \
RDKAFKA_TEST_NETWORK=slirp4netns \
RDKAFKA_TEST_FIPS_CONFIG=/home/j/Dev/librdkafka-worktrees/deps/required-openssl.cnf \
RUSTLS_PREFIX=/home/j/Dev/librdkafka-worktrees/deps/rustls-ffi-0.15.7 \
RDKAFKA_TLS_TEST_ARTIFACTS=/home/j/Dev/librdkafka-worktrees/acceptance-final \
CMAKE_BUILD_PARALLEL_LEVEL=8 tests/tls_backend/run.sh
```

This runs all three ordinary build configurations, broker-independent checks,
ordinary OpenSSL/rustls real Kafka and the optional required OpenSSL controls and
Kafka run. Its final log is `/tmp/rdk-acceptance-final.log`. The separate exact
module runtime Kafka command was:

```sh
DOCKER_HOST=unix:///home/j/Dev/librdkafka-worktrees/deps/test-podman.sock \
RDKAFKA_TEST_NETWORK=slirp4netns \
/home/j/Dev/librdkafka-worktrees/test-venv/bin/python \
  tests/tls_backend/acceptance.py --fips \
  --client /home/j/Dev/librdkafka-worktrees/deps/client-fips-rustls
```

Its final log is `/tmp/rdk-acceptance-fips-rustls-final.log`. The dependency source,
module and FFI build logs are retained outside git under
`/home/j/Dev/librdkafka-worktrees/deps/`, as listed by the module experiment.

Existing regression commands (from `tests/`, after a CMake build with tests,
SASL and curl enabled) were:

```sh
TESTS=0000,0004,0080,0124,0133,0134 \
  /home/j/Dev/librdkafka-worktrees/build-regression/tests/test-runner -p3 -l
```

Results: `/tmp/rdk-regression-tests.log`; full-feature build:
`/tmp/rdk-build-regression.log`. Focused controls are also available independently
through `tests/tls_backend/focused.py`, `fips_checks.py` and `timeout_check.py`.

### Failed attempts and corrections

- Default rootless pasta startup failed because this execution environment denied
  its sandbox tmpfs mount. No mocked/external broker was substituted. A local
  verified slirp4netns helper and isolated Podman API service provided mapped-port
  networking; no host-wide security/trust setting was edited.
- The ordinary dependency does not export the FIPS provider symbol, as expected.
  The first exact-system module attempt failed for missing pregenerated bindings;
  separately generated exact-header bindings resolved it without source changes.
- A first exact-module Kafka run failed because localhost could select IPv6 while
  the proxy listened on IPv4. The fixture now explicitly selects IPv4, and the
  rerun passed. IPv6 coverage is not inferred.
- One orchestration invocation passed both Kafka suites but then failed because
  the coordinator edited its shell script while it was running. The script was
  rerun from stable source; that earlier invocation is not counted as a full
  command pass.
- Early mixed CMake/configure work exposed source config.h shadowing; configure
  checks moved to copied sources. Correct include/import declarations, shared
  certificate ownership, native symbol isolation and install metadata were fixed.
- A preliminary FIPS static mixed-stack experiment exposed symbol redirection;
  final supported packaging rejects that case, rather than treating archive
  visibility as sufficient. Ordinary static linkage remains tested.
- The first exact-module purge check used a fixed 400ms scheduling delay and
  failed to reach injected backpressure. The test now waits for the measured
  proxy pause before instructing the client to purge, with bounded deadlines.
- Independent fixture review found suppressed teardown failures, unbounded total
  worker joins, optimization-sensitive assertions and incomplete trailing-count
  checks. Teardown failures now fail acceptance; shutdown has one deadline;
  explicit checks survive Python optimization; consumers drain a quiet tail and
  require the broker high watermark to equal the exact record count.
- ASan/UBSan execution was unavailable because this host's GCC sanitizer library
  symlinks reference missing runtime files. Sanitizer execution remains pending.

### Native Windows x64 results

On 2026-11-22 a disposable Windows Server 2022 x64 VM built and ran this
branch with MSVC 14.44. The `win32` MSBuild solution and the CMake `openssl`,
`none` and `rustls` builds passed. A rustls build with rustls-ffi 0.15.7 from
source (`fips,aws-lc-rs`, bundled aws-lc-fips-sys 0.14.3) detected
`WITH_RUSTLS_FIPS=1`. The policy, certificate-buffer, handshake-deadline and
real Kafka acceptance tests passed for OpenSSL, ordinary rustls and rustls
FIPS. The rustls FIPS client also passed acceptance with `--fips`. Kafka ran in
containers on a separate Linux Docker host, and the clients and fault proxy ran
on Windows. On Windows the FIPS module is a separate DLL with prefixed names,
and `rdkafka.dll` exports no crypto names. Commands, linkage facts and open
items are in [Windows checks](../packaging/rustls/WINDOWS-CHECKS.md).

### Unmet criteria and limitations

Native macOS execution and installed-package qualification remain unmet; no
native macOS host was available. Windows x64 has build and runtime evidence
only (see above). Cross compilation and CI configuration are not
reported as passed tests. Neither the Fedora OpenSSL provider nor this Fedora
AWS-LC build is established as a policy-covered deployment tuple. Required-mode
runtime behavior passed; module identity, operating environment, linkage/service
review, release signatures/SBOM and qualification remain open.

Rustls Windows/macOS FIPS distribution, macOS FIPS linkage, platform trust, CRLs, optional
client-auth issuer parity, broader authentication/HTTPS, complete downstream
bindings coverage, IPv6/trailing-dot acceptance, sanitizer runs and sustained
throughput/tail-latency/RSS comparisons remain pending or explicitly unsupported.
The branch is buildable/testable, not a released or assessed FIPS product.
No PR, upstream modification, merge or release is authorized or performed.

### Local runtime workaround and cleanup

This host already had an active rootless Podman socket, but its default pasta
helper could not create its sandbox. The local alternative was a verified
upstream slirp4netns 1.3.7 x86_64 helper (SHA256
`1c177694fe6bf299debfd8ff601a2ee3c4f7554d9983acdf1f854a6b34d53e70`), retained
at `/home/j/Dev/librdkafka-worktrees/deps/bin/slirp4netns`. No host-wide install
or configuration edit was made. To repeat on this same host, start the isolated
API service in one terminal:

```sh
PATH=/home/j/Dev/librdkafka-worktrees/deps/bin:$PATH \
  podman system service --time=0 \
  unix:///home/j/Dev/librdkafka-worktrees/deps/test-podman.sock
```

Run the recorded acceptance command in another terminal. Verify its final PASS
and exit status zero, then check for leftover containers using
`podman ps -a --filter label=org.testcontainers`. Stop this foreground test API
service with Ctrl-C when finished. Remove only containers identified as belonging
to an interrupted run, never all host containers. Build outputs, downloaded
sources and logs remain in the external worktrees directory for review/reuse;
they can be deleted after no test uses them. The final branch contains no private
certificates, keys, dependency binaries or runtime secrets.

Final cleanup verification found zero Testcontainers-labeled containers. The
original `/home/j/Dev/librdkafka` checkout was clean. Both final acceptance
commands exited zero, including strict teardown, exact watermark checks and
pause-triggered purge failure accounting. Final implementation reviews reported
no remaining actionable finding in the exercised Linux paths; the limitations
above remain open.

### Delivery

The tested branch was pushed to
[origin/feat/tls-backends-fips](https://github.com/j7nw4r/librdkafka/tree/feat/tls-backends-fips).
Remote SHA verification matched the local implementation/documentation commit
`99bde1a334d080a96b16e35aeeddcf284d99c1e8`; the final delivery-checklist commit
only updates this record. No merge, release, upstream write or PR was performed.
No native Windows/macOS execution host was configured in this session; a later
Windows x64 run is recorded above. The
isolated test API service was stopped after acceptance; dependency/build evidence
remains outside git for manual reuse.
