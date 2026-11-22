# Experimental rustls dependency contract

The broker adapter uses only the public C ABI of rustls-ffi **0.15.7**. Both build
systems reject other pkg-config versions. The matching header is also checked
by configure. CMake detects the feature-gated `rustls_default_fips_provider`
symbol at link time. Its absence leaves ordinary TLS available and makes the
runtime required policy fail; it does not trigger a provider substitution.

The ordinary Linux x86_64 development artifact is pinned by
`install-dev-dependency.sh`: SHA-256
`855ce5017c7f37ea505e343dd4bfc84c6591e635eb35bbfd931f14bbb61b426e`.
The helper keeps the archive, headers, libraries and upstream license notices at
the supplied external prefix, and relocates its pkg-config staging prefix.
It does not modify host trust or module configuration. The release shared object
embeds AWS-LC with version-prefixed symbols (`aws_lc_0_46_0_*`), and directly
imports libc, libgcc_s and libm, not OpenSSL. `librustls.a` embeds the Rust and
AWS-LC implementation; inspecting DT_NEEDED alone is insufficient.

Primary inputs checked on 2026-11-22:

- [Tagged manifest](https://github.com/rustls/rustls-ffi/blob/v0.15.7/librustls/Cargo.toml):
  Rust minimum 1.85, rustls exactly 0.23.46, ordinary defaults `aws-lc-rs` and
  `prefer-post-quantum`; FIPS is a separate feature.
- [Tagged lockfile](https://github.com/rustls/rustls-ffi/blob/v0.15.7/Cargo.lock):
  aws-lc-rs 1.18.2, ordinary aws-lc-sys 0.46.0, optional aws-lc-fips-sys 0.14.3.
- [Upstream build and linkage guidance](https://github.com/rustls/rustls-ffi/blob/v0.15.7/README.md):
  C ABI version pinning remains required. Windows/macOS FIPS has additional
  experimental dynamic-link requirements and is not qualified by this branch.

The FFI license is `Apache-2.0 OR ISC OR MIT`; the development archive retains all
three notices. A distributor must additionally retain the applicable Rust,
AWS-LC and transitive dependency notices and generate an artifact-specific SBOM.
No complete redistributable SBOM is claimed for this experimental branch.

The pinned FIPS crate is not the validated AWS-LC 4.0.0 module merely because its
feature contains the name FIPS. Its bundled 4.2.0 snapshot differs from the
module version identified by CMVP #5595/#5596. A policy-built system module route has now been built and runtime-tested against
the exact 4.0.0 policy source; see [the experiment](EXACT-FIPS-MODULE.md). It still
requires applicable operating-environment and deployment evidence; no positive
qualification is inferred here.

## Build and inspect manually

```sh
packaging/rustls/install-dev-dependency.sh /absolute/external/rustls-0.15.7
export PKG_CONFIG_PATH=/absolute/external/rustls-0.15.7/lib/pkgconfig
export LD_LIBRARY_PATH=/absolute/external/rustls-0.15.7/lib
cmake -S . -B /absolute/external/build-rustls \
  -DRDKAFKA_TLS_BACKEND=rustls -DRDKAFKA_BUILD_TESTS=OFF
cmake --build /absolute/external/build-rustls -j4
readelf -d /absolute/external/build-rustls/src/librdkafka.so | grep NEEDED
nm -g --defined-only /absolute/external/rustls-0.15.7/lib/librustls.a \
  | grep aws_lc_0_46_0_
```

`RDKAFKA_TLS_BACKEND=openssl` retains the existing crypto and authentication
capabilities; `none` removes broker TLS. An empty selector preserves the existing
`WITH_SSL` behavior. `rustls` selects a broker-only artifact and disables curl,
SCRAM, OAuth and external SASL crypto. Ordinary SASL PLAIN is compiled as before
but is outside the strict broker SSL profile.

Configure uses `./configure --with-tls-backend=rustls` and the same pkg-config
prefix. Run it in a copied checkout because mklove generates source-local
`config.h` and `Makefile.config`; those files shadow CMake's generated header.
`make -j4` then builds C and C++ libraries. Use `--with-tls-backend=none` or
`openssl` for the other configurations. The checked-in Visual Studio projects
retain OpenSSL and now include the common TLS dispatcher; rustls Windows builds
must use CMake and matching target dependency metadata. Windows x64 builds and
runtime tests passed, including a FIPS FFI build that needs Perl and ships
`aws_lc_fips_0_14_3_crypto.dll`; see [WINDOWS-CHECKS.md](WINDOWS-CHECKS.md).
Native macOS execution and distribution packaging remain pending.
