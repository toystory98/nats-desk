#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."
SPARKLE_VERSION=2.9.6
SPARKLE_SHA256=52bf9e88cdd972fc0c81501377a880e90d47031bd8ca5462488f843e2609e192
ARCHIVE="$PWD/dist/build/Sparkle-$SPARKLE_VERSION.tar.xz"
mkdir -p dist/build
if [ ! -f "$ARCHIVE" ]; then
  curl --fail --location --retry 3 "https://github.com/sparkle-project/Sparkle/releases/download/$SPARKLE_VERSION/Sparkle-$SPARKLE_VERSION.tar.xz" -o "$ARCHIVE.tmp"
  mv "$ARCHIVE.tmp" "$ARCHIVE"
fi
printf '%s  %s\n' "$SPARKLE_SHA256" "$ARCHIVE" | shasum -a 256 -c -
# Always extract from the verified archive, rather than trusting a cached framework.
rm -rf dist/build/sparkle
mkdir -p dist/build/sparkle
tar -xJf "$ARCHIVE" -C dist/build/sparkle
