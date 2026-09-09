#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."
GO_BIN="${GO_BIN:-go}"
VERSION=$(cat VERSION)
if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then echo "VERSION must be major.minor.patch" >&2; exit 1; fi
./desktop/fetch-sparkle.sh
BUILD="$PWD/dist/build"
APP="$PWD/dist/NATS Desk.app"
rm -rf "$APP"
mkdir -p "$BUILD" "$APP/Contents/MacOS" "$APP/Contents/Resources"
cp desktop/Info.plist "$APP/Contents/Info.plist"
/usr/libexec/PlistBuddy -c "Set :CFBundleShortVersionString $VERSION" "$APP/Contents/Info.plist"
/usr/libexec/PlistBuddy -c "Set :CFBundleVersion $VERSION" "$APP/Contents/Info.plist"
mkdir -p "$APP/Contents/Frameworks"
ditto "$BUILD/sparkle/Sparkle.framework" "$APP/Contents/Frameworks/Sparkle.framework"
cp "$BUILD/sparkle/LICENSE" "$APP/Contents/Resources/Sparkle-LICENSE.txt"
for ARCH in arm64 amd64; do
  GOOS=darwin GOARCH="$ARCH" CGO_ENABLED=0 "$GO_BIN" build -trimpath -ldflags='-s -w' -o "$BUILD/server-$ARCH" .
done
lipo -create "$BUILD/server-arm64" "$BUILD/server-amd64" -output "$APP/Contents/MacOS/nats-desk-server"
for ARCH in arm64 x86_64; do
  xcrun swiftc -parse-as-library -O -target "$ARCH-apple-macos13.0" -module-cache-path "$BUILD/module-cache" desktop/App.swift -F "$BUILD/sparkle" -framework Sparkle -Xlinker -rpath -Xlinker @executable_path/../Frameworks -o "$BUILD/app-$ARCH"
done
lipo -create "$BUILD/app-arm64" "$BUILD/app-x86_64" -output "$APP/Contents/MacOS/NATSDesk"
xcrun swiftc -module-cache-path "$BUILD/module-cache" desktop/Icon.swift -o "$BUILD/make-icon"
"$BUILD/make-icon" "$BUILD/icon.png"
mkdir -p "$BUILD/AppIcon.iconset"
for SIZE in 16 32 128 256 512; do
  sips -z "$SIZE" "$SIZE" "$BUILD/icon.png" --out "$BUILD/AppIcon.iconset/icon_${SIZE}x${SIZE}.png" >/dev/null
  DOUBLE=$((SIZE * 2))
  sips -z "$DOUBLE" "$DOUBLE" "$BUILD/icon.png" --out "$BUILD/AppIcon.iconset/icon_${SIZE}x${SIZE}@2x.png" >/dev/null
done
iconutil -c icns "$BUILD/AppIcon.iconset" -o "$APP/Contents/Resources/AppIcon.icns"
SIGN_ARGS=(--force --sign "${SIGN_IDENTITY:--}")
if [ "${SIGN_IDENTITY:--}" != "-" ]; then SIGN_ARGS+=(--options runtime --timestamp); fi
# Sign nested code from the inside out (also supports a future Developer ID identity).
FRAMEWORK="$APP/Contents/Frameworks/Sparkle.framework/Versions/B"
for COMPONENT in "$FRAMEWORK/XPCServices/Downloader.xpc" "$FRAMEWORK/XPCServices/Installer.xpc" "$FRAMEWORK/Autoupdate" "$FRAMEWORK/Updater.app" "$APP/Contents/Frameworks/Sparkle.framework"; do
  codesign "${SIGN_ARGS[@]}" "$COMPONENT"
done
codesign "${SIGN_ARGS[@]}" "$APP/Contents/MacOS/nats-desk-server"
codesign "${SIGN_ARGS[@]}" "$APP"
codesign --verify --deep --strict "$APP"
echo "Built: $APP"
