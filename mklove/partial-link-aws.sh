#!/bin/bash
#
# partial-link-aws.sh - Partial-link AWS SDK static archives into a single
# relocatable object to eliminate internal cross-TU symbols before they are
# merged into librdkafka-static.a.
#
# This reduces the AWS SDK contribution to librdkafka-static.a significantly
# by allowing the linker to discard unreferenced internal symbols across
# translation unit boundaries.
#
# Usage:
#   mklove/partial-link-aws.sh <cxx-compiler> <libaws-1.a> [<libaws-2.a> ...]
#
# All input archives must reside in the same directory.
# On success, produces <dir>/libaws-merged.a and removes the input archives.
#
# The CXX compiler driver is used (rather than ld directly) so that the
# correct target triple and platform flags are picked up automatically.
#

set -e

CXX="$1"
shift

if [[ $# -eq 0 ]]; then
    echo "$0: no input archives" >&2
    exit 1
fi

# All archives live in the same directory (destdir/usr/lib).
AWS_DIR="$(dirname "$1")"
MERGED_OBJ="${AWS_DIR}/libaws-merged.o"
MERGED_AR="${AWS_DIR}/libaws-merged.a"

echo "### Partial-linking $(echo "$@" | wc -w | tr -d ' ') AWS archives -> ${MERGED_AR}"

OS="$(uname -s)"

if [[ "$OS" == "Darwin" ]]; then
    # On macOS, the compiler driver does not auto-extract .a members for -r.
    # Extract all .o files from every archive into a temp directory first.
    TMP_DIR="$(mktemp -d)"
    trap 'rm -rf "$TMP_DIR"' EXIT

    for _a in "$@"; do
        # Extract into a per-archive subdirectory to avoid name collisions
        # between object files with the same name in different archives.
        _sub="${TMP_DIR}/$(basename "${_a%.a}")"
        mkdir -p "$_sub"
        (cd "$_sub" && ar -x "$_a")
    done

    # Determine the macOS deployment target for -platform_version.
    # Falls back to 13.0 if not set (matches the Semaphore global env var).
    _min="${MACOSX_DEPLOYMENT_TARGET:-13.0}"

    # Use ld -r directly with -platform_version to avoid the Apple linker's
    # "Missing -platform_version option" error when called outside a full link.
    # find + xargs avoids mapfile (bash 4+) and handles large file lists safely.
    find "$TMP_DIR" -name '*.o' -print0 | \
        xargs -0 ld -r \
            -platform_version macos "${_min}" "${_min}" \
            -o "$MERGED_OBJ"
else
    # On Linux, the compiler driver handles .a member extraction for -r automatically.
    "$CXX" -r -nostdlib -o "$MERGED_OBJ" "$@"
fi

# Repack the single relocatable object into a static archive.
ar rcs "$MERGED_AR" "$MERGED_OBJ"
rm -f "$MERGED_OBJ"

# Strip local symbol table entries (-x) and debug info (-S).
# The object code is untouched; only non-global C++ mangled names are removed.
strip -Sx "$MERGED_AR"

# Remove the original per-library archives — the merged archive replaces them.
rm -f "$@"

echo "### AWS partial-link complete: $(du -sh "$MERGED_AR" | cut -f1) ${MERGED_AR}"
