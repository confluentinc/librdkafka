# Experimental TLS backends implementation

Starting point: `ad4391a0e433a275f4c07a26f98ad70142b3e3a0`, fetched personal-fork
`origin/master` on 2026-11-22. Upstream/master has zero commits of divergence in
either direction. Worktree: `/home/j/Dev/librdkafka-worktrees/tls-backends-fips`.
Branch: `feat/tls-backends-fips`. Preserve the original checkout and public ABI.

## Plan

1. Define a private compile-time broker TLS boundary; retain socket lifecycle,
   Kafka ordering, timeout and delivery accounting in the existing machinery.
2. Preserve the OpenSSL adapter and implement pinned rustls-ffi 0.15.7 verified
   TLS/mTLS with bounded nonblocking buffering and explicit unsupported options.
3. Add an opt-in `ssl.fips.mode=required` broker-only policy with isolated OpenSSL
   contexts and explicit rustls provider checks. Reject unimplemented crypto paths.
4. Integrate CMake/configure, no-TLS and broker-only builds, installed consumers,
   feature reporting and packaging constraints.
5. Exercise both backends against a disposable real Kafka Testcontainers fixture,
   certificate failures and a transport fault proxy; run configuration regressions.
6. Review implementation and security, fix findings, record actual evidence and
   remaining qualification requirements; commit and push only the personal branch.

## Acceptance checklist (results in TLS-BACKENDS-RESULTS.md)

- [x] Working OpenSSL and rustls broker TLS through a private operations interface.
- [x] Ordinary OpenSSL compatibility, no-TLS and OpenSSL-free rustls builds.
- [x] Both CMake and configure paths; installed static/shared consumer checks.
- [x] Metadata, acknowledged production and consumption with keys/counts verified.
- [x] Verified TLS and required mTLS; wrong name/CA/expiry/client credential failures.
- [x] Reconnect/disconnect, short writes, fragmentation, backpressure and buffered I/O.
- [x] FIPS requirement refusal, insecure/unsupported options, isolation/no fallback.
- [x] Positive required-mode runtime tests with OpenSSL and, for rustls, an application-installed FIPS provider (macOS stand-in). The earlier AWS-LC 4.0.0 runs were for the removed AWS-LC mode.
- [x] Rustls uses the application's process-default provider; librdkafka links no rustls-ffi (2026-11-23).
- [x] Windows x64 run with `rustls-cng-crypto`, ordinary and FIPS policy ([results](../packaging/rustls/WINDOWS-CHECKS.md)).
- [ ] Policy-covered operating environment, release artifact/service evidence and qualification.
- [x] Native Windows x64 build and execution for OpenSSL, no TLS and the removed AWS-LC rustls mode ([Windows checks](../packaging/rustls/WINDOWS-CHECKS.md)).
- [x] Native macOS arm64 execution for OpenSSL, no TLS and rustls with stand-in providers.
- [x] Dependency/linkage inventory and qualification checklist, independent review.
- [x] Coherent commits pushed to origin; no merge, release, upstream write or PR.

A runtime FIPS check is a policy mechanism, not a deployment validation claim.
