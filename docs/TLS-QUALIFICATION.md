# Qualification boundary and investigation updates

Reviewed 2026-11-22 against starting checkout
`ad4391a0e433a275f4c07a26f98ad70142b3e3a0`, pinned rustls-ffi 0.15.7, primary
vendor sources and current CMVP records. This is an experimental engineering
branch. No Linux, Windows or macOS distribution tuple is declared qualified.

## Scope and checked policy

`ssl.fips.mode=required` supports only broker `security.protocol=ssl` with
certificate and hostname verification, including required mutual TLS. It
rejects SASL/plaintext, verification callbacks, engines, caller-selected
algorithm expressions, PKCS#12, CRLs and trust probing. Admin SCRAM *upsertion*
is rejected at its client entry point before its request-side cryptographic
work; describe/delete remain distinct operations. The clientless upsertion
object constructor retains its existing ordinary behavior, including generating
an optional salt before a client policy can be supplied. Applications must not
interpret constructing such an object as an approved service. Ordinary SCRAM
remains unchanged; its authentication/PBKDF/module-boundary questions are open.

OpenSSL required mode uses a dedicated `OSSL_LIB_CTX`, loads the installed
configuration, base and fips providers, runs the provider self-test and checks
status, advertised safety controls, digest provider and private DRBG origin.
It uses `SSL_CTX_new_ex` with `fips=yes`, TLS 1.2/1.3, AES-GCM, NIST P-curves,
RSA-PSS/ECDSA signatures and authenticated peers. TLS 1.2 must negotiate extended
master secret. Reconnect reuses that immutable strict configuration and checks
provider status; failures never select an ordinary context. Thread-associated
private-context state is released before context destruction.

Certificate/CA objects are decoded again in the isolated context. File and
`ssl.key.pem` private keys are decoded/validated using that context and provider.
Encrypted PEM and PKCS#12 memory parsing can execute crypto before client
construction; even plaintext EC key decoding may derive a public key. Therefore
required OpenSSL mode rejects memory PRIVATE_KEY setters in either ordering,
and refuses keys previously imported outside its strict context. File and
PEM-string mTLS remain supported. Ordinary OpenSSL setter ownership, immediate
validation, replacement/clear and config duplication are preserved. Rustls
strict setters select/check their FIPS provider first; keys imported through an
ordinary provider before selecting required mode are rejected by provenance.

Rustls required mode explicitly selects the feature-gated FIPS provider and
checks provider, client-config and completed-connection FIPS status. It also
checks its provider RNG and imports keys/verifies certificates with that same
provider. A pinned ordinary FFI lacks the required symbol and fails required
initialization with a named error. No private Rust symbols or a libcrypto
compatibility facade are used. This branch does not qualify AWS-LC services by
inference from a Cargo feature or boolean.

Built-in OIDC is rejected by the SSL-only profile. Curl HTTPS, JWT signing,
external Kerberos and application token acquisition need separate crypto/trust
assessment before expanding it. Base64 codec and generic authentication crypto
were not abstracted because the delivered broker-only rustls profile disables
those unrelated services.

## Investigation verified and corrected

- The initial source/remotes match the report; origin/master and upstream/master
  were equal at branch creation. Original checkout was preserved.
- The pinned manifest requires Rust 1.85 and rustls 0.23.46. The tagged lockfile
  selects aws-lc-rs 1.18.2, ordinary aws-lc-sys 0.46.0 and optional FIPS sys 0.14.3.
  [Manifest](https://github.com/rustls/rustls-ffi/blob/v0.15.7/librustls/Cargo.toml),
  [lockfile](https://github.com/rustls/rustls-ffi/blob/v0.15.7/Cargo.lock).
- CMVP [#5586](https://csrc.nist.gov/projects/cryptographic-module-validation-program/certificate/5586)
  identifies OpenSSL FIPS Provider; the host actually reports a **Red Hat
  Enterprise Linux OpenSSL FIPS Provider**, version `3.5.9-da9af220b1c05159` from
  Fedora 43's openssl-libs. Matching version text does not connect this binary
  to the generic certificate. It supplies runtime test evidence only.
- The host default provider configuration exposes disabled safety controls;
  required initialization now refuses that condition. The tested positive
  configuration comes from the branch template, includes vendor installation
  data and enables applicable controls. It is loaded per client and does not
  alter global OpenSSL policy. Older provider versions may not expose newer
  parameters; the core security check and every applicable advertised control
  are enforced. The exact module policy still needs an external review.
- AWS-LC FIPS sys 0.14.3 bundles a 4.2.0 snapshot, while CMVP
  [#5596 static](https://csrc.nist.gov/projects/cryptographic-module-validation-program/certificate/5596)
  and [#5595 dynamic](https://csrc.nist.gov/projects/cryptographic-module-validation-program/certificate/5595)
  name module 4.0.0. No claim transfers from those policies to the snapshot.
  [Vendor release](https://github.com/aws/aws-lc-rs/releases/tag/v1.18.2).
- The static policy names source
  `1276fc7d9cd0f509000294e9657914c3a23e65d2` with archive SHA256
  `33b5443d2213bfca45071b300daeb45decf13e3b46f726890b48d6267968f049`,
  and specific Amazon Linux 2023/EC2 environments. Its unmodified source was
  downloaded, digest checked and built separately for compatibility investigation
  on this Fedora host. This does not satisfy that operating-environment condition.
  See `packaging/rustls/EXACT-FIPS-MODULE.md` for the successful exact-source linking experiment, generated-binding
  requirements and symbol-isolation limits. No validation coverage is inferred.
- Runtime and static dependency inspections are distinct. The ordinary FFI
  contains version-namespaced AWS-LC objects; an absence of dynamic OpenSSL
  imports alone does not establish absence of embedded crypto. Complete
  artifact-specific notices/SBOM and module discovery evidence remain release work.

## Deployment checklist (all needed for a claim)

- [ ] Identify exact module, version, CMVP certificate, security-policy revision,
      approved services and vendor-supported artifact provenance.
- [ ] Match OS release/CPU/hardware/linkage to supported operating environments;
      obtain applicable vendor/assessor evidence for other environments.
- [ ] Preserve policy source digest, exact compiler/build options, complete
      dependency lock/feature graph, signed artifacts, hashes and license notices.
- [ ] Execute the policy's installation, integrity and startup self-test procedure;
      retain module/config paths, permissions and discovery rules.
- [ ] Inspect final static link maps plus ELF/Mach-O/PE imports and runtime-loaded
      modules; verify no second unqualified crypto path is enabled in the product.
- [ ] Establish key/certificate provenance, import and RNG behavior, enabled TLS
      parameters and any required per-service approval indicators.
- [ ] Run positive real-broker TLS/mTLS and negative removal/self-test/configuration
      controls against the final installed package, without fallback.
- [ ] Repeat on native Linux, Windows and macOS target tuples and downstream
      bindings/packages; a configured CI job or cross build is not native execution.
      Windows x64 engineering runs are in `packaging/rustls/WINDOWS-CHECKS.md`;
      they are not qualification evidence.
- [ ] Review SCRAM/PBKDF, OIDC/HTTPS, platform trust, optional auth and all future
      capabilities before admitting them into the strict scope.
- [ ] Associate results with the exact released artifact, qualification owner and
      applicable customer deployment; implement servicing/requalification rules.

For manual validation, begin with the selected vendor policy, not a library
version string. Use its prescribed installation verification, then the branch
policy tests and real Kafka command with that module's configuration. Compare
`readelf -d`, `nm`, the final link map and loaded-module inventory with the
manifest. Preserve failures as well as positive results. Runtime checks here
are necessary enforcement mechanisms; this checklist supplies the missing
artifact/environment evidence they cannot establish.
