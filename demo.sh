#!/bin/sh
set -eu
if [ "$#" -eq 0 ]; then
  echo "用法: ./demo.sh xxx.txt（读取 examples/xxx.txt）" >&2
  exit 1
fi
if [ "$#" -gt 0 ]; then
  case "$1" in
    /*) script="$1" ;;
    */*) script="$PWD/$1" ;;
    *) script="examples/$1" ;;
  esac
fi
cd "$(dirname "$0")"
export PYTHONPATH="src${PYTHONPATH:+:$PYTHONPATH}"
export PYTHONDONTWRITEBYTECODE=1
shift
exec python3 -m tinyzj.dashboard --script "$script" "$@"
