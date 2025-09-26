#!/bin/bash

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <docker-image> [<args>...]"
    exit 1
fi

IMAGE=$1
SCRIPT_DIR=$(dirname "$0")
ENTRYPOINT=${2}
REST=${@:3}

if [ $(which cache) ]; then
    cache restore ${IMAGE}.tar
fi
if [ ! -f ./${IMAGE}.tar ]; then
    docker build -f $SCRIPT_DIR/Dockerfile -t $IMAGE --build-arg UID=$UID .
    docker save $IMAGE -o ./${IMAGE}.tar

    if [ $(which cache) ]; then
        cache store ${IMAGE}.tar ./${IMAGE}.tar
    fi
else
    docker load -i ./${IMAGE}.tar
fi

docker run --rm --entrypoint $ENTRYPOINT \
    -v .:/librdkafka -w /librdkafka -e CI -u $UID:$UID ${IMAGE} ${REST}
