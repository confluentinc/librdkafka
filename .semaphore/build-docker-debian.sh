#!/bin/sh

export DEBIAN_FRONTEND=noninteractive
export NEEDRESTART_MODE=a
apt update
apt install -y gcc make build-essential

# /v is the volume mount point for the project root
cd /v
npm --userconfig /.npmrc ci
npx node-pre-gyp package
