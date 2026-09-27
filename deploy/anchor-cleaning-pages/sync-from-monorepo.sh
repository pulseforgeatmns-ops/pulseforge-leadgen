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
mkdir -p "$TARGET/assets/service-assurance" "$TARGET/assets/brand" "$TARGET/framer"
# Publish the brand files referenced by the homepage and web manifest as well.
# A partial asset sync previously left the production header logo returning 404.
for asset in anchor-logo-canonical.png apple-touch-icon-v20260916.png \
  favicon-v20260916.ico favicon-v20260916.svg \
  favicon-16x16-v20260916.png favicon-32x32-v20260916.png \
  icon-192-v20260916.png icon-512-v20260916.png \
  site.webmanifest social-preview-v20260916.jpg; do
  cp "$SRC/assets/brand/$asset" "$TARGET/assets/brand/$asset"
done
cp -r "$SRC/assets/service-assurance/." "$TARGET/assets/service-assurance/"
cp -r "$SRC/framer/." "$TARGET/framer/"
cp "$SRC/assets/"*.jpg "$TARGET/assets/" 2>/dev/null || true
echo "Synced. Commit and push main on anchor-cleaning to publish goanchorcleaning.com."
