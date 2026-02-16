#!/bin/bash
set -e
source /home/user/venv/bin/activate
clang-format-18 --version
make style-check
