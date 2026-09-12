#!/usr/bin/env sh

set -eu

PYTHONPATH=src${PYTHONPATH:+:$PYTHONPATH} exec python3 -m unittest discover -s tests -v
