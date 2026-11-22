# Build integration checks, Linux x86_64, 2026-11-22

These checks ran on the implementation worktree based on
`ad4391a0e433a275f4c07a26f98ad70142b3e3a0`. Native Windows/macOS and cross builds
were not run. This is engineering evidence, not module qualification.

Passed:

- CMake `RDKAFKA_TLS_BACKEND=none`, curl off: C and C++ shared libraries build.
- CMake `RDKAFKA_TLS_BACKEND=rustls`: C and C++ shared libraries build.
  Generated config reports TLS=1, RUSTLS=1, SSL=0, CURL=0 and all optional
  crypto/SASL capability macros=0. Ordinary FFI's FIPS symbol probe failed as
  expected; required-policy runtime checks are covered by the policy tests.
- CMake rustls `RDKAFKA_BUILD_STATIC=ON`: C and C++ archives build and install.
- Clean external installed consumer projects using `find_package(RdKafka)`:
  both shared and static artifacts compile, link and run (`2.16.0`).
- Installed shared and static pkg-config C consumers compile, link and run.
  This exposed and fixed existing hardcoded `lib` versus GNUInstallDirs `lib64`
  metadata and missing static dependency requirements.
- Configure/mklove `--with-tls-backend=rustls`, `none` and `openssl` followed by
  `make -j6` in copied sources: C/C++ shared and static libraries plus examples
  built, and built-in symbol visibility/integrity checks passed.
  Rustls runtime commands use the dependency prefix's `LD_LIBRARY_PATH`.
- Public certificate setter test on rustls: PEM and DER import, immediate
  rejection of invalid replacement, config duplication and clearing passed.
- A consumer explicitly linked against both `librdkafka.a` and `librustls.a`
  ran. Its ELF DT_NEEDED contains libc/libm/libgcc_s/zlib/zstd and no libssl,
  libcrypto or librustls. Its full link map has no libssl/libcrypto inputs.
  The FFI archive contains version-namespaced ordinary AWS-LC symbols.

The initial configure rustls run built all artifacts but its documentation
regeneration could not load the external FFI shared object. Repeating with the
correct `LD_LIBRARY_PATH` passed. The initial static pkg-config consumer failed
because of the preexisting install libdir metadata issue above; the corrected
consumer passed. Neither failed attempt is treated as a successful check.

External local logs are under `/home/j/Dev/librdkafka-worktrees/`:
`cmake-none.log`, `cmake-rustls.log`, `cmake-rustls-static.log`,
`configure-{openssl,none,rustls}.log`, `make-{openssl,none,rustls}.log`,
`consumer-rustls.log`, and `consumer-rustls-static.log`.
The consumer link maps are `consumer-rustls/static.map` and
`consumer-rustls/embedded.map`. These local files are not release artifacts.

Kafka acceptance, strict policy, fault injection and final integrated results
are tracked separately in the branch's main implementation guide.
