#!/bin/sh
# This script is used to build the project within a docker image.
# The docker image is assumed to be an alpine docker image, for glibc based builds, we use
# the semaphhore agent directly.

apk add -U ca-certificates openssl ncurses coreutils python3 make gcc g++ libgcc linux-headers grep util-linux binutils findutils perl patch musl-dev bash
# /v is the volume mount point for the project root
cd /v
npm install
npx node-pre-gyp package
