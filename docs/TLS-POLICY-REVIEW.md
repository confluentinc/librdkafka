# TLS policy and implementation review

The implementation was reviewed separately across the OpenSSL policy and
rustls transport owners. These checks concern runtime behavior. They do not
establish that the shipped artifact or operating environment is covered by a
CMVP certificate.

## Findings fixed

- Strict OpenSSL clients now use an isolated `OSSL_LIB_CTX`, explicit `fips=yes`
  fetches, a required `fips` provider, module self-test/status checks and the
  provider's exposed safety checks. Ordinary clients retain their existing
  provider semantics and process-default context.
- TLS 1.2 must negotiate the extended master secret. The fixed profile allows
  TLS 1.2/1.3, AES-GCM, P-256/P-384/P-521 and RSA-PSS/ECDSA signatures, with
  OpenSSL security level 2. Custom lists and verification callbacks are refused.
- Certificate setters can precede policy selection. Encrypted PEM and PKCS#12
  buffers are rejected before parsing in required mode, and their import
  provenance prevents enabling required mode later. All OpenSSL memory private
  keys are refused by the strict profile: even unencrypted EC decoding can
  derive a public key using the ordinary context. Use `ssl.key.location` or
  `ssl.key.pem`, which are decoded inside the selected strict client context.
  Rustls key setters use the explicitly selected provider; ordinary-provider
  imports cannot subsequently become strict clients.
- Public certificate/CA setter ownership, immediate parsing, invalid replacement,
  clearing and duplication remain available in ordinary builds. OpenSSL client
  initialization now acquires references to shared CA stores and certificate
  chains instead of stealing and mutating configuration-owned objects.
- Strict Admin SCRAM upsertion is rejected at its API operation entry point,
  before broker lookup or password derivation. SCRAM's PBKDF/service-boundary
  qualification remains unresolved. The client-free upsertion object constructor
  has no policy and is outside a client policy claim; rejected operations never
  use its password/salt to derive credentials.
- Immediate OpenSSL handshake completion now receives the same verification
  checks as asynchronous completion. A verification failure already destroys the
  transport, so that connect path returns without a second use of the pointer.
- Rustls write review confirmed that an accepted plaintext prefix remains
  associated with the original request and offset, is submitted only once, and
  advances the caller slice only after ciphertext drains to the socket. This
  preserves the existing request-sent and `acks=0` delivery eligibility point.
- Accepted-but-uncommitted rustls prefixes now count as partially started
  requests for timeout, purge, partition purge and termination. Teardown drops
  buffered application/handshake output rather than flushing canceled bytes.
  Deterministic cancellation tests assert zero write callbacks and zero app
  bytes after unblocking immediately before close.
- Exact-system AWS-LC archive symbols are hidden inside the shared wrapper;
  unsafe FIPS static-consumer packaging is rejected. Mixed OpenSSL/shared-rustls
  process tests verify both library orders and the real OpenSSL version.
- Rustls alternative credential/trust sources are mutually exclusive, avoiding
  silent precedence differences from OpenSSL. Temporary private-key and Base64
  buffers are wiped. Authentication feature reporting is gated by implemented
  crypto capabilities independently of broker TLS availability.

## Checks performed

The ordinary OpenSSL CMake library build passed on native Linux x86-64 against
OpenSSL 3.5.9. `tests/tls_policy.c` passed with a generated temporary CA and,
for provenance tests, its private key. The completed cases were:

- missing FIPS provider in an empty module directory and empty process config;
- strict positive initialization using the branch configuration template with
  `/etc/pki/tls/fips_local.cnf` included;
- disabled provider safety-check refusal;
- a test-only `LD_PRELOAD` override causing the explicit provider self-test call
  to fail; this verifies refusal control flow, not a genuine module failure;
- ordinary TLS clients before/after failed strict initialization and concurrently
  with a strict client;
- insecure verification, hostname bypass, unsupported SASL and algorithm lists;
- local Admin SCRAM upsertion rejection without a broker;
- setter input lifetime, failed replacement, three duplicated configurations,
  clearing, strict private-key setter refusal and pre-policy private-key
  provenance refusal.

The installed runtime module identifies itself as
`Red Hat Enterprise Linux OpenSSL FIPS Provider`, version
`3.5.9-da9af220b1c05159`, from Fedora 43 `openssl-libs`. This is not evidence
that it is the OpenSSL Corporation artifact covered by certificate #5586.
Several safety-check parameters default to zero on this host; required mode
refuses that configuration. The branch template enables the relevant checks in
an isolated client configuration. Its successful runtime test establishes only
the observed approved-mode enforcement path.

See the main guide for integrated Kafka acceptance and build-matrix results.
A positive rustls FIPS runtime artifact was subsequently built using the exact
AWS-LC 4.0.0 policy source; its results and restrictions are recorded in
`packaging/rustls/EXACT-FIPS-MODULE.md`. Native Windows/macOS execution, applicable
operating-environment evidence and deployment qualification remain separate
requirements. Linux socketpair and real Kafka tests do not establish
Windows edge-triggered socket behavior or a performance qualification.

## Manual policy verification

Build the policy executable against the library artifact being examined:

```sh
cc tests/tls_policy.c -L"$build/src" -Wl,-rpath,"$build/src" \
  -lrdkafka -o /tmp/tls-policy
cc -shared -fPIC tests/tls_fips_selftest_fail.c -o /tmp/fips-selftest-fail.so
```

Use the branch Testcontainers/focused scripts to generate temporary credentials
and run the executable; the main guide contains the exact artifact paths and
commands. For a positive OpenSSL runtime check, substitute the vendor-installed
module configuration path in `tests/tls_backend/openssl-required.cnf.in`, then
set `RDKAFKA_TEST_FIPS_CONFIG` to the resulting file when running the policy
checks. This environment variable is a test-runner input; the library itself
uses `ssl.fips.config`. Inspect the provider identity, required installation
integrity data and security policy independently before making a deployment
claim. All fault-injection libraries belong only in isolated test processes.
