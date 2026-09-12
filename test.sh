#!/usr/bin/env sh

set -eu

exec python3 -m unittest discover -s tests -v
