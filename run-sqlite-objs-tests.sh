#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
SQLITE_OBJS_DIR="$ROOT_DIR/sqlite-objs"

if [[ ! -d "$SQLITE_OBJS_DIR" ]]; then
  echo "sqlite-objs crate not found at $SQLITE_OBJS_DIR" >&2
  exit 1
fi

cd "$SQLITE_OBJS_DIR"

# The sqlite-objs crate loads configuration from sqlite-objs/.env.
# Run the crate's full nextest suite from the main repo with one command.
exec cargo nextest run "$@"