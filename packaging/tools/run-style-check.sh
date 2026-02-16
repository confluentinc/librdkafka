#!/bin/bash
set -e
source /home/user/venv/bin/activate
clang-format --version
make style-check
