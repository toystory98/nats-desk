#!/bin/bash
# Build/package first. Sign only the current version and generate a signed feed.
set -euo pipefail
cd "$(dirname "$0")/.."
VERSION=$(cat VERSION)
APP="$PWD/dist/NATS Desk.app"
ACTUAL=$(/usr/libexec/PlistBuddy -c 'Print :CFBundleVersion' "$APP/Contents/Info.plist")
[ "$ACTUAL" = "$VERSION" ] || { echo 'Rebuild before releasing.' >&2; exit 1; }
DMG="NATS-Desk-$VERSION-universal.dmg"
# Recreate the DMG from the verified current bundle; do not sign a stale package.
codesign --verify --deep --strict "$APP"
./desktop/package.sh
rm -rf dist/release
mkdir -p dist/release
cp "dist/$DMG" "dist/$DMG.sha256" dist/release/
KEY_ARGS=(--account toystory98.nats-desk)
if [ -n "${SPARKLE_KEY_FILE:-}" ]; then KEY_ARGS=(--ed-key-file "$SPARKLE_KEY_FILE"); fi
./dist/build/sparkle/bin/generate_appcast "${KEY_ARGS[@]}" \
  --download-url-prefix "https://github.com/toystory98/nats-desk/releases/download/v$VERSION/" \
  --link 'https://github.com/toystory98/nats-desk/releases' \
  --maximum-deltas 0 "$PWD/dist/release"
# SURequireSignedFeed causes generate_appcast to sign the feed as well.
./dist/build/sparkle/bin/sign_update "${KEY_ARGS[@]}" --verify dist/release/appcast.xml
python3 desktop/verify-release.py
