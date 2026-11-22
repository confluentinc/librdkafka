# Native Windows x64 checks, 2026-11-22

These checks ran on branch commit `966b43d0f23d1328ae160eca464238ff4c74617b`
on a disposable Azure VM. The VM was deleted after the run. This is engineering
evidence for build, linkage and runtime behavior. It is not FIPS qualification,
and it does not establish the CMVP status of any module on Windows.

## Environment

- Windows Server 2022 Datacenter Azure Edition, `Standard_D8s_v5`, x86_64.
- Visual Studio 2022 Build Tools, MSVC 14.44.35207 (`v143`), CMake 4.5, Ninja.
- vcpkg tag `2026.07.29` in manifest mode: OpenSSL 3.6.3, zstd 1.5.7, zlib, curl.
- Python 3.12.10 with `tests/tls_backend/requirements.txt`.
- Rust 1.90.0, Go 1.27.4, NASM 3.02, LLVM/libclang 23.1.3, Strawberry Perl.
- A second VM (Ubuntu 24.04, Docker 29.1.3) in the same private network ran
  the `apache/kafka:4.1.1` containers. The Windows test process used
  `DOCKER_HOST=tcp://<private IP>:2375`. The fault proxy and all librdkafka
  clients ran on Windows.

`tests/tls_backend/run.sh` is a bash script, so the same steps ran manually.
`transport_io.c` (Linux/macOS socketpair) and `crypto_isolation.c` (Linux ELF
and OpenSSL 3) were not run. `fips_checks.py` was not run because no OpenSSL
FIPS provider was installed.

## Builds

| Build | Result |
|---|---|
| `win32/librdkafka.sln`, MSBuild Release x64, `v143` | Pass |
| CMake `RDKAFKA_TLS_BACKEND=openssl` | Pass, `WITH_TLS=1`, `WITH_SSL=1` |
| CMake `RDKAFKA_TLS_BACKEND=none` | Pass, `WITH_TLS=0`, `WITH_SSL=0` |
| CMake `RDKAFKA_TLS_BACKEND=rustls`, release zip | Pass, `WITH_RUSTLS=1`, `WITH_RUSTLS_FIPS=0` |
| CMake `RDKAFKA_TLS_BACKEND=rustls`, FIPS FFI from source | Pass, `WITH_RUSTLS=1`, `WITH_RUSTLS_FIPS=1` |

All CMake builds used `-DWITH_CURL=OFF -DWITH_SASL=OFF` and the shared library.
The MSBuild solution needed `VcpkgInstalledDir` set explicitly when MSBuild ran
as the SYSTEM account; `vcpkg integrate install` alone did not apply.

The ordinary rustls build used the official
`rustls-ffi-x86_64-windows.zip` v0.15.7, SHA-256
`b96b9695440b259e413e0d970215bea6c9179cb805acdeb226b2fda23042e37b`. CMake
resolved `-lrustls` to the import library, so `rdkafka.dll` imports
`rustls.dll`. Ship `rustls.dll` next to `rdkafka.dll`. The OpenSSL DLLs are not
in the import table of this build.

The FIPS build used rustls-ffi v0.15.7 from source:

```bat
cargo build --locked --release -p rustls-ffi --no-default-features --features fips,aws-lc-rs
```

The locked graph is rustls 0.23.46, aws-lc-rs 1.18.2 and aws-lc-fips-sys
0.14.3 with its bundled AWS-LC source. The graph also compiles the ordinary
aws-lc-sys 0.46.0, as on Linux. The final `native-static-libs` list names only
the FIPS crypto library:

```text
aws_lc_fips_0_14_3_crypto.lib legacy_stdio_definitions.lib kernel32.lib
ntdll.lib userenv.lib ws2_32.lib dbghelp.lib /defaultlib:msvcrt
```

## Linkage facts for the FIPS build

- aws-lc-fips-sys builds the module as `aws_lc_fips_0_14_3_crypto.dll`. The
  Rust static library `rustls_ffi.lib` links to its import library. Ship this
  DLL next to `rdkafka.dll`.
- The module DLL exports 3002 names, and all start with `aws_lc_fips_0_14_3_`.
  The unprefixed-symbol collision that `--exclude-libs` prevents on ELF does not
  occur with this module.
- `rdkafka.dll` exports 552 `rd_kafka*` names and no `EVP_`, `OPENSSL_`,
  `aws_lc`, `rustls_`, `BN_` or `RSA_` names.
- The AWS-LC FIPS CMake step requires Perl on Windows. Without Perl, it stops
  with `Could NOT find Perl (missing: PERL_EXECUTABLE)`.
- aws-lc-fips-sys generated its bindings with internal bindgen and libclang
  (`LIBCLANG_PATH`). Go, NASM and Ninja were on `PATH`, and
  `CMAKE_GENERATOR=Ninja` was set.

rustls-ffi publishes no FIPS archive for Windows, so the pkg-config file was
written by hand. CMake reads `Libs`, not `Libs.private`, so all link inputs go
in `Libs`:

```text
prefix=C:/deps/rustls-fips
libdir=${prefix}/lib
includedir=${prefix}/include

Name: rustls
Description: rustls-ffi 0.15.7 fips,aws-lc-rs (aws-lc-fips-sys 0.14.3)
Version: 0.15.7
Libs: -L${libdir} -lrustls -laws_lc_fips_0_14_3_crypto -llegacy_stdio_definitions -lkernel32 -lntdll -luserenv -lws2_32 -ldbghelp
Cflags: -I${includedir}
```

`lib/rustls.lib` is a copy of `rustls_ffi.lib`. `lib/` also holds the module
import library, and `include/rustls.h` comes from `librustls/src/`. If this
file is incomplete, the `WITH_RUSTLS_FIPS_AVAILABLE` link probe fails and CMake
builds a non-FIPS library without an error. Required mode then refuses every
client at runtime. Read `WITH_RUSTLS_FIPS` in the generated `config.h` after
each configure.

## Configure and test commands

Each build ran inside `vcvars64.bat`:

```bat
cmake -S C:\src\librdkafka -B C:\val\build\<name> -G Ninja -DCMAKE_BUILD_TYPE=Release ^
  -DCMAKE_TOOLCHAIN_FILE=C:/src/vcpkg/scripts/buildsystems/vcpkg.cmake ^
  -DVCPKG_TARGET_TRIPLET=x64-windows -DVCPKG_MANIFEST_INSTALL=OFF ^
  -DVCPKG_INSTALLED_DIR=C:/src/librdkafka/vcpkg_installed ^
  -DPKG_CONFIG_EXECUTABLE=C:/ProgramData/chocolatey/bin/pkg-config.exe ^
  -DRDKAFKA_TLS_BACKEND=<backend> -DWITH_CURL=OFF -DWITH_SASL=OFF ^
  -DRDKAFKA_BUILD_TESTS=OFF -DRDKAFKA_BUILD_EXAMPLES=OFF
cl /nologo /W3 /O2 /IC:\src\librdkafka\src /D_CRT_SECURE_NO_WARNINGS ^
  tests\tls_backend\client.c C:\val\build\<name>\src\rdkafka.lib
```

`PKG_CONFIG_PATH` selected the ordinary or the FIPS prefix. Each variant's
programs and DLLs went into a separate directory. Tests ran from
`tests/tls_backend`:

```bat
python focused.py --policy <openssl>\policy.exe --policy <none>\policy.exe --policy <rustls>\policy.exe ^
  --cert-buffers <openssl>\cert.exe --cert-buffers <rustls>\cert.exe
python focused.py --cert-buffers <fips>\cert.exe --positive-policy <fips>\policy.exe
<fips>\fips_cert.exe ca.crt client.crt client.key
python timeout_check.py --client <openssl>\timeout.exe --client <rustls>\timeout.exe --client <fips>\timeout.exe
python acceptance.py --client <openssl>\client.exe --client <rustls>\client.exe --client <fips>\client.exe
python acceptance.py --fips --client <fips>\client.exe
```

## Results

| Check | OpenSSL | No TLS | rustls | rustls FIPS |
|---|---|---|---|---|
| Required-policy refusal (`unavailable`) | Pass | Pass | Pass | n/a |
| Positive required policy (`available`) | n/a | n/a | n/a | Pass |
| Certificate buffers, PEM/DER | Pass | n/a | Pass | Pass |
| Strict key import (`fips_cert_buffers`) | n/a | n/a | n/a | Pass |
| Stalled handshake deadline | Pass | n/a | Pass | Pass |
| Kafka acceptance, ordinary policy | Pass | n/a | Pass | Pass |
| Kafka acceptance, `--fips` | n/a | n/a | n/a | Pass |

Each Kafka pass covered rejection of a wrong hostname, an unknown CA, a missing,
invalid or expired client credential, and an expired server. It also covered
three 96-record roundtrips with exact keys, counts and 64-KiB values, injected
fragmentation and disconnects, `acks=0`, and the `acks=0` purge. The purge
reported 0 success and 96 failed, with all 96 callbacks accounted for.

The MSBuild `tests.exe -l` run of `0000_unittests`, `0004_conf` and
`0133_ssl_keys` passed with the OpenSSL backend.

The first test run failed for the ordinary rustls variant with
`STATUS_DLL_NOT_FOUND` (`0xC0000135`). The test setup had not copied
`rustls.dll`. After the copy, all rustls checks passed. This failure is not
counted as a pass.

## Open items

- The OpenSSL required-policy refusal logged a module path with each backslash
  and the next character removed, for example
  `C:\Windows\TEMPdkafka-tls-unit-...mpty-modulesips.dll`. The refusal test
  still passed. The cause was not investigated. Investigate it before you rely
  on OpenSSL required mode on Windows.
- CMake rejects `RDKAFKA_BUILD_STATIC` with `WITH_RUSTLS_FIPS` on every
  platform. On Windows, the module is a separate DLL with prefixed names, so the
  ELF reason for the rejection does not apply. The `rdkafka-sys` crate's
  `cmake-build` feature always sets `RDKAFKA_BUILD_STATIC=1`. Rust applications
  must use its `dynamic-linking` feature against this shared build, or this rule
  must change for Windows.
- The exact AWS-LC 4.0.0 policy source was not built on Windows. The tested
  module is the AWS-LC snapshot bundled in aws-lc-fips-sys 0.14.3.
- Windows installed-package consumers, the configure path (MinGW), x86 and
  ARM64, platform trust and distribution packaging were not tested.
