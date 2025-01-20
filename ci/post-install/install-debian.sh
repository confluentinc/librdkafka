#!/bin/sh
set -e

export DEBIAN_FRONTEND=noninteractive
export NEEDRESTART_MODE=a

apt update
apt install -y bash curl

. /v/ci/post-install/_install_nvm.sh
. /v/ci/post-install/_install_ckjs.sh
