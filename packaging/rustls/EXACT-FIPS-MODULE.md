# Exact AWS-LC 4.0.0 system-module experiment

On 2026-11-22 the pinned rustls-ffi 0.15.7 successfully built against the exact,
unmodified AWS-LC source identified by static CMVP #5596, instead of the bundled
4.2.0 snapshot in aws-lc-fips-sys 0.14.3. This establishes a tested engineering
linking route. **This Fedora 43 x86_64 run is not deployment qualification.**
The policy's named Amazon Linux 2023/processor combinations, installation rules,
service evidence and release artifact review still need to be satisfied.

Primary evidence:

- [CMVP #5596](https://csrc.nist.gov/projects/cryptographic-module-validation-program/certificate/5596)
  and [security policy section 11.1](https://csrc.nist.gov/CSRC/media/projects/cryptographic-module-validation-program/documents/security-policies/140sp5596.pdf).
- [Exact source](https://github.com/aws/aws-lc/tree/1276fc7d9cd0f509000294e9657914c3a23e65d2):
  revision `1276fc7d9cd0f509000294e9657914c3a23e65d2`, archive SHA-256
  `33b5443d2213bfca45071b300daeb45decf13e3b46f726890b48d6267968f049`.
- [Pinned system-module builder](https://github.com/aws/aws-lc-rs/blob/v1.18.2/builder/system_library.rs):
  FIPS version floor 4; explicit SYSTEM_DIR does not silently choose the bundled
  module when the supplied system installation fails.

## Results and restrictions

The full policy-source CMake FIPS build and install passed. `bssl isfips` returned
`1`; a separately statically linked program printed `AWS-LC FIPS 4.0.0` from
`awslc_version_string`, with its uppercase `T` symbol in `nm`. Comparing every
extracted file and symlink against the verified zip after building found zero
content changes. No source files were added to or changed within that source tree.

The first meaningful rustls system-module attempt failed because this exact
source predates the installed `share/rust/aws_lc_bindings.rs` facility. Generating
bindings separately from its installed headers and supplying the documented
`AWS_LC_FIPS_SYS_SYSTEM_BINDINGS` plus `AWS_LC_FIPS_SYS_NO_PREFIX=1` resolved that
blocker. No version-check bypass or dependency/source patch was used.

Cargo features were exactly `--no-default-features --features fips,aws-lc-rs`;
`--locked` retained rustls 0.23.46, aws-lc-rs 1.18.2, aws-lc-fips-sys 0.14.3.
`aws-lc-rs` exposes the FFI's explicit provider entry point, including for
non-strict clients; the actual crypto instance is the supplied FIPS module.
The provider reported FIPS true. CMake detected WITH_RUSTLS_FIPS=1. Strict client
configuration, policy isolation, actual Admin upsert refusal, strict plaintext
key-buffer import and duplicate lifetime, and ordinary-import provenance refusal
passed. Kafka connection results are recorded in the main implementation guide.

This module has unprefixed OpenSSL-compatible symbols. ELF builds now require a
linker capability probe and hide `librustls.a`/`libcrypto.a` internals with
`--exclude-libs`; the shared librdkafka dynamic symbol table exposes no native
EVP/OPENSSL/BORING/AWS-LC/RSA/AES/BN entry points. Real OpenSSL 3 default-context
SSL/RNG operations coexisted with a strict rustls configuration with either
shared-library load order. Tests verify the application's version function still
resolves to OpenSSL 3 and its global FIPS policy remains unchanged.

**Static librdkafka FIPS consumers are rejected by CMake.** Archive visibility
cannot separate an application's OpenSSL calls from unprefixed AWS-LC calls
inside the same final executable. A negative mixed-link experiment demonstrated
that redirection. Configure retains internal archives for examples, but omits
FIPS static archives and static pkg-config metadata from installation. Use a
clean install prefix; do not reuse old installed archives. Ordinary non-FIPS
rustls static consumers remain supported. Raw direct linking of internal FIPS
archives is unsupported. The positive supported artifact here is a shared
librdkafka wrapper with an internally embedded, hidden static crypto module.
Its static module identity comes from its link map and pre-hiding module checks;
the wrapper deliberately does not publicly export `awslc_version_string`.

## Reproduce manually

The commands below reproduce the experiment on a development host; running them
on an arbitrary host does not confer policy coverage. The installed tools used
here were GCC 15.3.1, CMake 3.31.11, Go 1.26.10 and Rust 1.90.0.
Choose an external directory and preserve its source/archive, logs and notices.

```sh
export experiment=/absolute/external/aws-lc-experiment
mkdir -p "$experiment"
curl -fL https://github.com/aws/aws-lc/archive/1276fc7d9cd0f509000294e9657914c3a23e65d2.zip \
  -o "$experiment/aws-lc.zip"
echo "33b5443d2213bfca45071b300daeb45decf13e3b46f726890b48d6267968f049  $experiment/aws-lc.zip" | sha256sum -c -
unzip -q "$experiment/aws-lc.zip" -d "$experiment"
cmake -S "$experiment/aws-lc-1276fc7d9cd0f509000294e9657914c3a23e65d2" \
  -B "$experiment/module-build" -DFIPS=1 -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_INSTALL_PREFIX="$experiment/module-install"
cmake --build "$experiment/module-build" -j6
cmake --install "$experiment/module-build"
"$experiment/module-build/tool/bssl" isfips

git clone --depth 1 --branch v0.15.7 https://github.com/rustls/rustls-ffi.git "$experiment/ffi-source"
cargo fetch --locked --manifest-path "$experiment/ffi-source/Cargo.toml"
cargo install bindgen-cli --version 0.73.2 --locked --root "$experiment/bindgen"
aws_sys_manifest=$(rg --files "$HOME/.cargo/registry/src" | rg '/aws-lc-fips-sys-0.14.3/Cargo.toml$' | head -1)
aws_sys_source=$(dirname "$aws_sys_manifest")
"$experiment/bindgen/bin/bindgen" "$aws_sys_source/include/rust_wrapper.h" \
  --allowlist-file '.*openssl/.*\.h' --allowlist-file '.*rust_wrapper\.h' \
  --rustified-enum point_conversion_form_t --default-macro-constant-type signed \
  --with-derive-default --with-derive-partialeq --with-derive-eq \
  --generate functions,types,vars,methods,constructors,destructors \
  --rust-target 1.71 --output "$experiment/bindings.rs" -- \
  -I"$experiment/module-install/include" -isystem "$(cc -print-file-name=include)"
AWS_LC_FIPS_SYS_SYSTEM_DIR="$experiment/module-install" \
AWS_LC_FIPS_SYS_SYSTEM_BINDINGS="$experiment/bindings.rs" \
AWS_LC_FIPS_SYS_NO_PREFIX=1 CARGO_TARGET_DIR="$experiment/ffi-target" \
  cargo build --manifest-path "$experiment/ffi-source/librustls/Cargo.toml" \
    --locked --release --no-default-features --features fips,aws-lc-rs
```

The bindgen command uses the GCC system-header location because this host's
libclang installation lacked its builtin header search path. All wrapper-included
OpenSSL headers were verified present in the exact module installation, so host
OpenSSL headers were not substituted. The generated bindings stay outside the
module source. Preserve their hash and review their ABI for each target.

Install the resulting `ffi-target/release/librustls_ffi.a` externally as
`ffi-install/lib/librustls.a`, copy the pinned `librustls/src/rustls.h` to
`ffi-install/include/`, and provide `ffi-install/lib/pkgconfig/rustls.pc`:

```text
prefix=/absolute/external/aws-lc-experiment/ffi-install
libdir=${prefix}/lib
includedir=${prefix}/include
Name: rustls
Description: experimental exact-policy AWS-LC 4.0.0 static FFI
Version: 0.15.7
Libs: -L${libdir} -lrustls -lm -ldl -lpthread -lrt -lutil -lgcc_s
Cflags: -I${includedir}
```

Then select this prefix through PKG_CONFIG_PATH and build shared librdkafka with
`-DRDKAFKA_TLS_BACKEND=rustls`. The ordinary setup helper downloads a different,
non-FIPS artifact; it must not overwrite this installation. Compile
`tests/tls_policy.c` against the resulting library and run
`policy CA.pem available`. Compile `tests/tls_backend/fips_cert_buffers.c` and run
`fips_cert_buffers CA.pem client.pem client.key`. For mixed shared-library
verification compile `tests/tls_backend/crypto_isolation.c` with `-Isrc`, the
library's `-L`/rpath, and both orders of `-lrdkafka -lssl -lcrypto -ldl`.
The latter test targets Linux ELF/OpenSSL 3 and must not be applied to a static
librdkafka FIPS link as an expected-success case.

## Local evidence and remaining qualification

This run's prefix is `/home/j/Dev/librdkafka-worktrees/deps/`. Exact module
installation: `aws-lc-policy-install`; FFI metadata: `rustls-policy-ffi`;
CMake wrapper: `/home/j/Dev/librdkafka-worktrees/build-rustls-policy`;
Kafka binary: `deps/client-fips-rustls` (rpath embedded).
The locked FFI checkout is `rustls-ffi-source-0.15.7` at
`8f0eadf96fa21ade96d867c7ea06f8f5fddda856`; its git status remained clean.

Recorded SHA-256 values for this host build:

```text
bcm.o          b6124da6249727d70d384044617057ff8f1480be56cdd08540a61dec347e7521
libcrypto.a    447df55da7a1de8cb44b56a9595fbb78005cc50e7cc51e96d36583114f76fdb1
librustls.a    684ab989d026cc8249f40a87cc2d67fdbfdf372d4bc8f5a8acc8871f3c7b03be
bindings.rs   a6b55eada85dee3ac85917c32b62a4e91b541022e4a44ed3f8ccc4b99bc288f7
```

Logs include `aws-lc-policy-{configure,build,install}.log`,
`rustls-policy-system-{missing-bindings,bindings}.log`,
`cmake-rustls-policy.log`, `configure-rustls-policy.log` and
`make-rustls-policy.log`. `rustls-policy-features.txt` and
`rustls-policy-licenses.txt` record the actual Cargo graph and declared licenses.
Rustls FFI is Apache-2.0 OR ISC OR MIT; aws-lc-rs is ISC AND (Apache-2.0 OR ISC),
and aws-lc-fips-sys additionally carries OpenSSL notices. The verified complete
AWS-LC source and Cargo registry sources retain their notices. A distributor
still needs a complete artifact SBOM and license bundle.

Pending qualification: repeat the exact source, toolchain and operating-environment
procedure on a policy-covered Amazon Linux 2023/CPU tuple; review generated
bindings and unchanged module boundary; preserve installed binary/module hashes
and full final link maps; collect required service and self-test evidence;
qualify TLS/mTLS certificates, algorithms and keys; review the hidden shared
wrapper's module-boundary treatment with the deployment owner. Windows/macOS
require separate module/environment evidence and linkage work. No module,
operating environment or version was substituted as validation evidence here.
