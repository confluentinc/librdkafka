#!/bin/bash

COMPOSE_VERSION=$(docker-compose --version)
DOCKER_VERSION=$(docker --version)
JEST=${JEST:-../node_modules/.bin/jest}
INTEG_DIR=../e2e/schemaregistry

# Start the docker compose file
echo "Running docker compose up. Docker version $DOCKER_VERSION. Compose version $COMPOSE_VERSION. "

docker-compose -f docker-compose.schemaregistry.yml up -d

if [ "$?" == "1" ]; then
  echo "Failed to start docker images."
  exit 1
fi

echo "Running schema registry e2e tests"

$JEST $INTEG_DIR
