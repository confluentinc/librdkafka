# Build integration checks

## Current rustls backend, macOS arm64, 2026-11-23

These checks ran on macOS 26 (Darwin 25.6.0) arm64 with Apple clang, CMake,
Rust 1.97.1 and Docker 27.4. The rustls backend links no rustls-ffi; see
[DEPENDENCIES.md](DEPENDENCIES.md).

Passed:

- CMake `RDKAFKA_TLS_BACKEND=rustls -DRDKAFKA_BUILD_STATIC=ON` with
  `RUSTLS_INCLUDE_DIR` set to the rustls-ffi 0.15.7 crate's `src`: C and C++
  archives build and install. The header check passes without a link.
  Examples and tests are turned off. The installed `rdkafka-static.pc` has no
  rustls requirement and says that the application supplies rustls-ffi.
- `nm -g librdkafka.a` shows no defined `rustls_*`, `aws_lc` or `awslc`
  symbols. The `rustls_*` references are undefined, and no AWS-LC constructor
  is referenced.
- The Rust test host (`tests/tls_backend/rustls_host`) links that archive and
  the rustls-ffi rlib. Each host binary has one `rustls_version` definition and
  no rustls dynamic library dependency. The FIPS stand-in host depends on
  `libaws_lc_fips_0_14_4_crypto.dylib`, which aws-lc-fips-sys builds as a
  shared library on macOS; librdkafka itself still has no AWS-LC code.
- CMake configure errors were checked: a shared rustls build, no `rustls.h`
  found, and a `rustls.h` whose `RUSTLS_VERSION_NUMBER` is not `0x000f0700`
  each stop with a FATAL_ERROR that names the cause.
- Configure/mklove `--with-tls-backend=rustls --with-rustls-include-dir=DIR`
  in a copied source, then `make -j8` and `make install`: only static C and
  C++ archives and `-static.pc` files are built and installed. The archive
  defines no rustls or AWS-LC symbols. The `TAGS` step printed a `ctags -e`
  error from the macOS ctags, which is not specific to this branch.

## History: Linux x86_64 checks, 2026-11-22

The rustls items below describe the removed AWS-LC mode, in which librdkafka
linked a rustls-ffi release archive. They are not evidence for the current
rustls backend.

These checks ran on the implementation worktree based on
`ad4391a0e433a275f4c07a26f98ad70142b3e3a0`. Native macOS and cross builds
were not run. Native Windows x64 checks are in [WINDOWS-CHECKS.md](WINDOWS-CHECKS.md). This is engineering evidence, not module qualification.

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
