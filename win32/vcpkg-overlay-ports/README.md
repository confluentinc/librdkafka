# vcpkg overlay ports

The vcpkg registry only carries one OpenSSL release line, currently the non-LTS
3.6 line, and does not add releases to older lines such as 3.5 LTS
(microsoft/vcpkg#54187). The Windows (vcpkg) build therefore takes OpenSSL from
the overlay port in `openssl/`, so it bundles the same OpenSSL 3.5 LTS release
as source/autoconf builds (`mklove/modules/configure.libssl`).

`vcpkg-configuration.json` in the repository root registers this directory.
vcpkg then always uses `openssl/`, regardless of the `builtin-baseline` or any
version constraint in `vcpkg.json`.


## Origin

`openssl/` is a copy of `ports/openssl` from microsoft/vcpkg at tag
`2026.07.29`, the tag that `win32/setup-vcpkg.ps1` checks out (registry version
3.6.3, git tree `b4a2360e8425d8d5e2a24436804b592bef26980b`). Only three lines
differ from it:

 * `"version"` in `openssl/vcpkg.json`.
 * `SHA512` in `openssl/portfile.cmake`.
 * `BUILD_TARGET` in `openssl/unix/portfile.cmake`: `build_sw`, as OpenSSL 3.5
   has no `build_inst_sw` target (added in 3.6). This only affects non-Windows
   vcpkg builds.

All other make and `./Configure` targets used by the port exist in OpenSSL
3.5. The one exception is `mingwarm64`, so arm64 MinGW triplets are not
supported.

These files are distributed under vcpkg's MIT license, see `LICENSE.vcpkg`.


## Upgrading to a new OpenSSL 3.5.x release

 1. Set `"version"` in `openssl/vcpkg.json`.
 2. Set `SHA512` in `openssl/portfile.cmake` to the SHA-512 of the GitHub
    source archive that `vcpkg_from_github` downloads (not the release
    tarball):

        curl -sSL https://github.com/openssl/openssl/archive/openssl-<version>.tar.gz | sha512sum

 3. Bump `ver` and `checksum` in `mklove/modules/configure.libssl` so that all
    platforms bundle the same release.

The version string in the built DLL shows which OpenSSL was bundled:

    Select-String -Path libcrypto-3-x64.dll -Pattern 'OpenSSL \d+\.\d+\.\d+' | ForEach-Object { $_.Matches.Value } | Select-Object -Unique


## Updating the vcpkg tag

When `win32/setup-vcpkg.ps1` moves to a newer vcpkg tag, copy `ports/openssl`
from that tag again and reapply the three changes above. Check that the port's
patches still apply to the 3.5 sources, and that the make and `./Configure`
targets it uses exist in OpenSSL 3.5.


## Removing the overlay

Add a `"version>="` constraint for openssl back to `vcpkg.json` at the same
time. The `builtin-baseline` there dates from 2022, so without a constraint
vcpkg would build OpenSSL 3.0.7.
