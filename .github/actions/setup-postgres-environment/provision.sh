#!/usr/bin/env bash
# Provision PostgreSQL client/server binaries for disposable harness tests.
#
# 1. runner-image: GitHub's Ubuntu runner images ship PostgreSQL (service
#    disabled). If pg_config resolves a bindir that contains executable
#    initdb, pg_ctl and psql, and initdb runs, those binaries are used and no
#    package index or mirror is contacted.
# 2. apt (fallback, unchanged behaviour): restore cached archives,
#    apt-get update, install postgresql + postgresql-client, save archives.
# 3. If neither path yields validated binaries, exit non-zero.
#
# Only binaries are provisioned. No cluster or service is started here; each
# test creates its own disposable cluster (initdb) with its own data directory
# and port.
#
# Environment:
#   CACHE_DIR       apt archive cache directory (required for the apt path)
#   GITHUB_OUTPUT   receives postgres-source=<runner-image|apt> and bindir=<dir>
#   GITHUB_PATH     receives the bindir
# Test hooks (never set in workflows):
#   PG_PROVISION_SUDO     command prefix used for privileged steps (default: sudo)
#   PG_PROVISION_APT_GET  apt-get command (default: apt-get)
set -euo pipefail

SUDO="${PG_PROVISION_SUDO-sudo}"
APT_GET="${PG_PROVISION_APT_GET:-apt-get}"
GITHUB_OUTPUT="${GITHUB_OUTPUT:-/dev/null}"
GITHUB_PATH="${GITHUB_PATH:-/dev/null}"

# Prints the validated bindir, or returns non-zero with a reason on stderr.
validated_bindir() {
  if ! command -v pg_config >/dev/null 2>&1; then
    echo "pg_config not found on PATH" >&2
    return 1
  fi
  local bindir
  if ! bindir="$(pg_config --bindir 2>/dev/null)" || [ -z "$bindir" ] || [ ! -d "$bindir" ]; then
    echo "pg_config --bindir did not return a directory" >&2
    return 1
  fi
  local name
  for name in initdb pg_ctl psql; do
    if [ ! -x "$bindir/$name" ]; then
      echo "$bindir/$name is missing or not executable" >&2
      return 1
    fi
  done
  if ! "$bindir/initdb" --version >/dev/null 2>&1; then
    echo "$bindir/initdb --version failed" >&2
    return 1
  fi
  printf '%s\n' "$bindir"
}

source_kind=""
if bindir="$(validated_bindir)"; then
  source_kind="runner-image"
  echo "Using runner-image PostgreSQL binaries (no apt access)"
else
  echo "Runner-image PostgreSQL binaries unavailable; falling back to apt"
  : "${CACHE_DIR:?CACHE_DIR is required for the apt fallback}"
  $SUDO mkdir -p "$CACHE_DIR"
  if [ -n "$(ls -A "$CACHE_DIR" 2>/dev/null || true)" ]; then
    echo "Restoring cached apt archives"
    $SUDO cp -a "$CACHE_DIR/." /var/cache/apt/archives/ 2>/dev/null || true
  fi
  export DEBIAN_FRONTEND=noninteractive
  $SUDO $APT_GET update -qq
  $SUDO $APT_GET install -y -qq --no-install-recommends postgresql postgresql-client
  $SUDO mkdir -p "$CACHE_DIR"
  $SUDO cp -a /var/cache/apt/archives/. "$CACHE_DIR/" 2>/dev/null || true
  if ! bindir="$(validated_bindir)"; then
    echo "::error::PostgreSQL binaries are still unavailable after the apt fallback" >&2
    exit 1
  fi
  source_kind="apt"
fi

echo "$bindir" >> "$GITHUB_PATH"
echo "postgres-source=$source_kind" >> "$GITHUB_OUTPUT"
echo "bindir=$bindir" >> "$GITHUB_OUTPUT"
echo "PostgreSQL source: $source_kind"
echo "PostgreSQL bindir: $bindir"
"$bindir/initdb" --version
"$bindir/pg_ctl" --version
"$bindir/psql" --version
