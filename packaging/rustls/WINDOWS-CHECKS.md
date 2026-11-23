# Native Windows x64 checks

## Status after the provider change, 2026-11-23

The rustls backend now links no rustls-ffi. A Rust application links static
librdkafka and rustls-ffi 0.15.7 and installs the crypto provider. The planned
Windows provider is `rustls-cng-crypto` 0.1.2, which uses the Windows CNG
module. The first Windows run (below) found two librdkafka defects, which are
now fixed; the steps further down are the test to repeat.

### First run with `rustls-cng-crypto`, 2026-11-23

Windows Server 2022 (build 20348) on Azure, MSVC, branch commit `097affaa`,
static `rdkafka.lib`, host built with `--no-default-features` and the `cng` or
`cng-fips` feature. Kafka ran in Docker on a Linux host in the same network.

Passed:

- Build and linkage: `rdkafka.lib` defines no `rustls_*` and no AWS-LC
  symbols and has 49 undefined `rustls_*` references. The host executable
  imports `bcrypt.dll` and `bcryptprimitives.dll` and no rustls or AWS-LC DLL.
  `cargo tree` has no `aws-lc` or `ring` crate.
- Legacy FIPS policy off: the no-provider refusal, the `policy unavailable`
  refusal of strict clients, the certificate buffers and the handshake deadline
  with `cng`.
- Legacy FIPS policy on: the handshake deadline with `cng-fips`. With the
  ordinary `cng` provider the host reports `fips=false`, and required mode
  refuses with "does not report FIPS mode".

Failed, because of librdkafka defects fixed after `097affaa`:

- Every client certificate failed at `rd_kafka_new` with "rustls client
  certificate/private key match failed: keys may not be consistent: Unknown
  (7601)". `rustls-cng-crypto` signing keys do not expose a public key, so
  rustls reports `InconsistentKeys::Unknown`. rustls itself accepts that result
  ([rustls 0.23.46 `signer.rs`](https://github.com/rustls/rustls/blob/v/0.23.46/rustls/src/crypto/signer.rs),
  "Don't treat unknown consistency as an error"), but librdkafka treated it as
  fatal. This failed Kafka acceptance with `cng` (the readiness probe uses
  mTLS), Kafka acceptance with `cng-fips --fips`, and `fips_cert_buffers` with
  `cng-fips`. librdkafka now accepts the unknown result and still rejects
  `RUSTLS_RESULT_INCONSISTENT_KEYS_KEYS_MISMATCH` (7600) and every other error.
- With the legacy policy off, `rustls_cng_crypto::fips_provider()` has no
  cipher suites, and even ordinary client creation failed with the vague
  "rustls client configuration failed: general error (7112)". librdkafka now
  refuses such a provider with "the process-default rustls crypto provider has
  no cipher suites".
- `pkg-config --static --libs rdkafka-static` named `librdkafka.a`, but MSVC
  builds `rdkafka.lib`. The generated static `.pc` files now use the platform's
  static library prefix and suffix.

The macOS test host now reproduces both librdkafka defects with stand-ins:
`--opaque-keys` wraps each loaded key so that `public_key()` returns `None`,
and `--empty-provider` removes all suites and groups. Without the fixes they
fail with the same 7601 and 7112 errors; with the fixes they pass.

The 2026-11-22 run further down is history. Its OpenSSL and no-TLS results
still describe those backends. Its rustls and rustls FIPS rows describe the
removed AWS-LC mode, in which `rdkafka.dll` imported `rustls.dll` or an
aws-lc-fips-sys DLL. They are not evidence for the current rustls backend.

### Facts to know before the test

- Microsoft validates the CNG modules, not applications: "Microsoft validates
  the cryptographic modules used in Windows and other products, not individual
  Windows services or applications." The user-mode module is the Cryptographic
  Primitives Library (`bcryptprimitives.dll`). Its CMVP certificate is #5410 for
  Windows 11 22H2 and for Windows Server 2022 build 10.0.20348.1668, and #4825
  for Windows Server 2022 build 10.0.20348
  ([overview](https://learn.microsoft.com/en-us/windows/security/security-foundations/certification/fips-140-validation),
  [Windows 11](https://learn.microsoft.com/en-us/windows/security/security-foundations/certification/validations/fips-140-windows11),
  [Server 2022](https://learn.microsoft.com/en-us/windows/security/security-foundations/certification/validations/fips-140-windows-server-2022)).
- The same overview says that since the April 2024 quality updates (KB5036893,
  KB5036909) the modules run their self-tests automatically, and that the
  "legacy FIPS policy" ("System cryptography: Use FIPS-compliant algorithms for
  encryption, hashing, and signing") is no longer needed except for a BitLocker
  case.
- `rustls-cng-crypto` 0.1.2 reports `fips()` for its random source and key
  provider from `BCryptGetFipsAlgorithmMode` (crate source `src/fips.rs`,
  `src/lib.rs`). `fips_provider()` keeps only FIPS-approved suites and groups;
  `default_provider()` includes others, so its `fips()` is false.
  `fips.rs` calls `.unwrap()` on the `BCryptGetFipsAlgorithmMode` result.
- `BCryptGetFipsAlgorithmMode` follows the legacy policy registry value
  `HKLM\System\CurrentControlSet\Control\Lsa\FipsAlgorithmPolicy\Enabled`,
  also after the April 2024 updates. With the value 0 it returned 0, and with
  the value 1 it returned 1, before and after a reboot. A new process saw a
  change without a reboot. (Measured on an Azure Windows Server 2022 VM,
  2026-11-23, build 20348.5870.)
- Result: a provider that reports `fips()` from this flag, such as
  `rustls-cng-crypto`, passes `ssl.fips.mode=required` only when that legacy
  policy is enabled. Microsoft calls the policy unnecessary for the module
  itself, but librdkafka's required mode needs it with this provider.

### The remaining Windows test

Run in a "x64 Native Tools" prompt (MSVC), with Rust stable, CMake, Ninja,
Python 3.11 or newer and `tests/tls_backend/requirements.txt` in a venv. Kafka
runs in Docker on a Linux host; set `DOCKER_HOST=tcp://<linux-host>:2375`.

1. Get the header from the locked rustls-ffi package:

   ```bat
   cd tests\tls_backend\rustls_host
   cargo fetch --locked
   cargo metadata --locked --format-version 1 > %TEMP%\meta.json
   ```

   `RUSTLS_INCLUDE_DIR` is the directory of the `rustls-ffi` package
   `manifest_path` plus `\src` (it holds `rustls.h`).

2. Build static librdkafka:

   ```bat
   cmake -S C:\src\librdkafka -B C:\val\rustls -G Ninja -DCMAKE_BUILD_TYPE=Release ^
     -DRDKAFKA_TLS_BACKEND=rustls -DRUSTLS_INCLUDE_DIR=<dir> -DRDKAFKA_BUILD_STATIC=ON ^
     -DWITH_CURL=OFF -DWITH_SASL=OFF -DWITH_ZLIB=OFF -DWITH_ZSTD=OFF -DENABLE_LZ4_EXT=OFF ^
     -DCMAKE_INSTALL_PREFIX=C:\val\rustls-install
   cmake --build C:\val\rustls
   cmake --install C:\val\rustls
   ```

3. Build the host twice, once per CNG provider. `RDKAFKA_STATIC_LIBS` lists the
   static library and the Windows system libraries that `rdkafka.lib` needs:

   ```bat
   rem Or use: pkg-config --static --libs rdkafka-static (fixed after 097affaa).
   set RDKAFKA_STATIC_LIBS=C:\val\rustls-install\lib\rdkafka.lib -lws2_32 -lsecur32
   set CARGO_TARGET_DIR=C:\val\host-cng
   cargo build --locked --release --no-default-features --features cng
   set CARGO_TARGET_DIR=C:\val\host-cng-fips
   cargo build --locked --release --no-default-features --features cng-fips
   ```

   The transport whitebox test is Unix only, so `RDKAFKA_CONFIG_DIR` is not
   needed on Windows.

4. Inspect the binaries. `dumpbin /dependents` must name no rustls or
   aws-lc DLL. `dumpbin /symbols` on `rdkafka.lib` must show `rustls_*` only as
   `UNDEF`. `cargo tree --locked -e normal --target x86_64-pc-windows-msvc
   --no-default-features --features cng-fips -i rustls` must show one rustls
   0.23.46, and the graph must contain no aws-lc crate.

5. Write one `.cmd` wrapper for each program, for example `client-cng.cmd`
   containing `@C:\val\host-cng\release\rdkafka-rustls-host.exe client %*`.
   Make wrappers for `policy`, `cert_buffers`, `fips_cert_buffers`,
   `handshake_timeout`, `no_provider` and `key_match` the same way, for both
   hosts.

6. Run from `tests\tls_backend`:

   ```bat
   python focused.py --no-provider no_provider-cng.cmd --policy policy-cng.cmd ^
     --cert-buffers cert_buffers-cng.cmd --key-match-opaque key_match-cng.cmd
   python timeout_check.py --client handshake_timeout-cng.cmd
   python acceptance.py --client client-cng.cmd
   ```

   Expected: every check passes. With `default_provider()` the provider reports
   FIPS false, so the `unavailable` policy run must refuse strict clients. CNG
   keys hide their public key, so use `--key-match-opaque` (a matching pair
   must work; a mismatch cannot be detected before the handshake).

7. Repeat with the FIPS provider, once with the legacy FIPS policy disabled and
   once with it enabled (`HKLM\SYSTEM\CurrentControlSet\Control\Lsa\FipsAlgorithmPolicy`,
   `Enabled` = 1):

   ```bat
   python focused.py --positive-policy policy-cng-fips.cmd ^
     --fips-cert-buffers fips_cert_buffers-cng-fips.cmd
   python acceptance.py --fips --client client-cng-fips.cmd
   ```

   With the policy disabled, also make `no_provider-installed-cng-fips.cmd`
   containing `@C:\val\host-cng-fips\release\rdkafka-rustls-host.exe
   --install-provider no_provider %*` and run
   `python focused.py --empty-provider no_provider-installed-cng-fips.cmd`.
   The real, empty FIPS provider must be refused with "has no cipher suites".

   Record the `fips=` value that the host prints for each policy state.
   Expected from the measured flag behavior: `fips=false` with the policy
   disabled, so the positive runs must fail with "does not report FIPS mode"
   (record it; it is not a pass), and `fips=true` with the policy enabled, so
   they must pass. A new process sees a policy change, so a reboot is not
   needed for the flag itself.

`acceptance.py` and `focused.py` start the wrappers with `subprocess`. If
Python does not run `.cmd` files directly on the VM, build one small `.exe`
launcher or run each program through `cmd /c`; record which one you used.

## History: 2026-11-22 run (OpenSSL, no TLS and the removed AWS-LC rustls mode)

These checks ran on branch commit `966b43d0f23d1328ae160eca464238ff4c74617b`
on a disposable Azure VM. The VM was deleted after the run. This is engineering
evidence for build, linkage and runtime behavior. It is not FIPS qualification,
and it does not establish the CMVP status of any module on Windows.

### Environment

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

### Builds

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

### Linkage facts for the FIPS build

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

### Configure and test commands

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

### Results

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

### Open items

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
