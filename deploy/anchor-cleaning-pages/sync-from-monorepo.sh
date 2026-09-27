#!/usr/bin/env bash
# Sync sites/anchor-cleaning → pulseforgeatmns-ops/anchor-cleaning (GitHub Pages).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
TARGET="${1:-}"
if [[ -z "$TARGET" || ! -d "$TARGET/.git" ]]; then
  echo "Usage: $0 /path/to/anchor-cleaning-clone"
  exit 1
fi
SRC="$ROOT/sites/anchor-cleaning"
cp "$SRC/index.html" "$TARGET/index.html"
mkdir -p "$TARGET/assets/service-assurance" "$TARGET/framer"
cp -r "$SRC/assets/service-assurance/." "$TARGET/assets/service-assurance/"
cp -r "$SRC/framer/." "$TARGET/framer/"
cp "$SRC/assets/"*.jpg "$TARGET/assets/" 2>/dev/null || true
echo "Synced. Commit and push main on anchor-cleaning to publish goanchorcleaning.com."
