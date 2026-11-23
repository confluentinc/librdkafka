# Experimental rustls dependency contract

The rustls backend uses only the public C ABI of rustls-ffi **0.15.7**, and it
links no rustls-ffi library. librdkafka needs the 0.15.7 `rustls.h` at build
time. The application supplies the library and the crypto provider at link and
run time. See [the main guide](../../docs/TLS-BACKENDS.md#the-rustls-backend-uses-the-applications-provider).

## Build-time contract

- CMake: `-DRDKAFKA_TLS_BACKEND=rustls -DRUSTLS_INCLUDE_DIR=DIR
  -DRDKAFKA_BUILD_STATIC=ON`. Without `RUSTLS_INCLUDE_DIR`, CMake asks
  pkg-config for `rustls = 0.15.7` for the include directory only. A shared
  rustls build is a configure error.
- Configure: `--with-tls-backend=rustls --with-rustls-include-dir=DIR`, or
  pkg-config as above. Configure builds static libraries only.
- Both build systems compile a test that requires `RUSTLS_VERSION_NUMBER ==
  0x000f0700`. Neither links it.
- The static archive defines no `rustls_*` or AWS-LC symbols. Its undefined
  `rustls_*` references must resolve to the application's rustls-ffi.

The header source: rustls-ffi 0.15.7 has `links = "rustls_ffi"`, and its build
script copies `src/rustls.h` to `$OUT_DIR/include` and prints
`cargo:include=...` (crate source `rustls-ffi-0.15.7/build.rs`). A build script
of a crate that depends directly on rustls-ffi therefore receives the directory
as `DEP_RUSTLS_FFI_INCLUDE`.

## Application contract

- Depend on `rustls-ffi = "=0.15.7"` with `default-features = false`. The
  crate's default features are `aws-lc-rs` and `prefer-post-quantum`; neither
  is needed when the application installs its own provider. Add
  `no_log_capture`, because otherwise rustls-ffi calls `log::set_logger` when
  it creates a connection (crate source `src/log.rs`, `src/connection.rs`).
- Reference the crate in Rust code (for example `use rustls_ffi as _;`), so
  rustc links it. Emit librdkafka with `cargo:rustc-link-lib=static=rdkafka`
  from a build script, so that it comes before the rlib on linkers that resolve
  archives in order.
- rustls-ffi 0.15.7 pins `rustls = "=0.23.46"`. A provider crate must accept
  that rustls version so that Cargo builds one rustls copy.
- Install the provider with `CryptoProvider::install_default()` before any TLS
  client or private-key import. `rustls_crypto_provider_default()` returns
  `CryptoProvider::get_default()` and does not install one from crate features
  (crate source `src/crypto_provider.rs`; rustls 0.23.46 `src/crypto/mod.rs`).
- At run time librdkafka refuses a rustls-ffi whose `rustls_version()` does not
  start with `rustls-ffi/0.15.7/`.

The crate type of rustls-ffi 0.15.7 is `["lib", "staticlib"]`. Only the `lib`
(rlib) form shares the application's provider state. The release archives
(`librustls.a`, `librustls.dylib`, `rustls.dll`) each hold a separate rustls
copy. A local experiment confirmed this: a C function that called
`rustls_crypto_provider_default()` saw the provider that a Rust `main` installed
when both used the rlib, and saw NULL when the C function used
`librustls.dylib`.

## Providers

librdkafka does not select or ship a provider. The provider decides the crypto
module, the algorithms and the result of `fips()`. Examples:

- `rustls::crypto::aws_lc_rs::default_provider()` (AWS-LC, non-FIPS build).
- `rustls::crypto::default_fips_provider()` with the rustls `fips` feature
  (aws-lc-fips-sys). On macOS aws-lc-fips-sys builds the module as a shared
  library, `libaws_lc_fips_0_14_4_crypto.dylib` in the tested lock file.
- `rustls_cng_crypto::fips_provider()` or `default_provider()` from
  `rustls-cng-crypto` 0.1.2 on Windows (Windows CNG). It depends on
  `rustls = "0.23.20"` with default features off, so it unifies with 0.23.46.

The test host uses the first two as stand-ins and has optional features for the
third. That is test scaffolding, not librdkafka support for a provider.

## Licenses

The rustls-ffi license is `Apache-2.0 OR ISC OR MIT`. A distributor of an
application binary must retain the notices of rustls-ffi, rustls, the provider
and their transitive dependencies, and generate an artifact-specific SBOM. No
redistributable SBOM is claimed for this experimental branch.

## Removed AWS-LC mode

Before 2026-11-23 this branch linked a rustls-ffi release or source build
(aws-lc-rs, or aws-lc-fips-sys for FIPS) into librdkafka, hid its archive
symbols on ELF, rejected static FIPS builds and downloaded a pinned Linux
archive with `install-dev-dependency.sh`. That mode, its helper and its exact
AWS-LC 4.0.0 module experiment are removed. Results recorded for it are history
and do not describe the current backend.
