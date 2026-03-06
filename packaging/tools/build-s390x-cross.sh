#!/bin/bash
#
# Build librdkafka for s390x using cross-compilation
#
# This script uses Docker BuildKit with cross-compilation to build
# s390x binaries on AMD64 hosts, avoiding QEMU emulation issues.
#
# Usage:
# packaging/tools/build-s390x-cross.sh <output-tarball-path.tgz>
#
# Requirements:
# - Docker with BuildKit support
# - QEMU binfmt support (for verification only)
#

set -e

if [ $# -ne 1 ]; then
    echo "Usage: $0 <output-path.tgz>"
    echo ""
    echo "Example:"
    echo "  $0 artifacts/librdkafka-s390x.tgz"
    exit 1
fi

output="$1"
output_dir=$(dirname "$output")
output_file=$(basename "$output")

# Ensure output directory exists
mkdir -p "$output_dir"

# Get absolute path for output
output_abs=$(cd "$output_dir" && pwd)/$output_file

echo "Building librdkafka for s390x using cross-compilation..."
echo "Output will be: $output_abs"

# Enable Docker BuildKit
export DOCKER_BUILDKIT=1

# Build using cross-compilation Dockerfile
docker buildx build \
    --platform linux/s390x \
    --file packaging/tools/Dockerfile.s390x \
    --target artifacts \
    --output type=local,dest=/tmp/librdkafka-s390x-build \
    .

# Package the artifacts
echo "Packaging artifacts..."
cd /tmp/librdkafka-s390x-build
tar czf "$output_abs" .
cd -

# Clean up
rm -rf /tmp/librdkafka-s390x-build

# Emit SHA256 for verification
echo ""
echo "Build complete!"
echo "SHA256 $output:"
sha256sum "$output"
