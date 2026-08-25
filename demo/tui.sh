#!/bin/sh
set -eu

cd "$(dirname "$0")/.."

if ! command -v camel >/dev/null 2>&1; then
  echo "Camel CLI is not available on PATH." >&2
  echo 'Open a new terminal or run: export PATH="$HOME/.jbang/bin:$PATH"' >&2
  exit 1
fi

exec camel tui --theme=dark
