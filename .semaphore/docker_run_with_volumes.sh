#!/bin/sh
docker run -v "$HOME/.npmrc:/.npmrc" -v "$(pwd):/v" "$@"