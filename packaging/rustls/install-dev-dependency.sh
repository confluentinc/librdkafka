#!/bin/sh
# Development-only ordinary AWS-LC rustls-ffi binary. This is not a FIPS module.
set -eu
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 INSTALL_PREFIX (outside the source checkout)" >&2
    exit 2
fi
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) ;;
  *) echo 'This helper pins only Linux x86_64. Build rustls-ffi v0.15.7 from source or obtain the exact target artifact.' >&2; exit 2 ;;
esac
mkdir -p "$1"
prefix=$(cd "$1" && pwd)
archive="$prefix/rustls-ffi-x86_64-linux-gnu.zip"
curl --fail --location --proto '=https' --tlsv1.2 \
  https://github.com/rustls/rustls-ffi/releases/download/v0.15.7/rustls-ffi-x86_64-linux-gnu.zip \
  --output "$archive"
echo "855ce5017c7f37ea505e343dd4bfc84c6591e635eb35bbfd931f14bbb61b426e  $archive" | sha256sum --check -
unzip -o "$archive" -d "$prefix"
# cargo-c's release package has a relative staging prefix; relocate its metadata.
python3 - "$prefix" <<'PY'
import pathlib
import sys
prefix = pathlib.Path(sys.argv[1])
p = prefix / 'lib/pkgconfig/rustls.pc'
p.write_text(p.read_text().replace('prefix=dist', 'prefix=' + str(prefix)))
PY
printf 'Set PKG_CONFIG_PATH=%s/lib/pkgconfig and LD_LIBRARY_PATH=%s/lib for development.\n' "$prefix" "$prefix"
