#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."
GO_BIN="${GO_BIN:-go}"
VERSION=$(cat VERSION)
APP_DISPLAY_NAME="${APP_DISPLAY_NAME:-NATS Desk}"
APP_BUNDLE_ID="${APP_BUNDLE_ID:-local.natsdesk.desktop}"
APP_DATA_DIRECTORY="${APP_DATA_DIRECTORY:-NATS Desk}"
APP_PORT="${APP_PORT:-48127}"
APP_UPDATES_ENABLED="${APP_UPDATES_ENABLED:-true}"
if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then echo "VERSION must be major.minor.patch" >&2; exit 1; fi
if ! [[ "$APP_PORT" =~ ^[0-9]+$ ]] || (( APP_PORT < 1024 || APP_PORT > 65535 )); then echo "APP_PORT must be from 1024 to 65535" >&2; exit 1; fi
if ! [[ "$APP_UPDATES_ENABLED" =~ ^(true|false)$ ]]; then echo "APP_UPDATES_ENABLED must be true or false" >&2; exit 1; fi
./desktop/fetch-sparkle.sh
BUILD="$PWD/dist/build"
APP="$PWD/dist/$APP_DISPLAY_NAME.app"
rm -rf "$APP"
mkdir -p "$BUILD" "$APP/Contents/MacOS" "$APP/Contents/Resources"
cp desktop/Info.plist "$APP/Contents/Info.plist"
/usr/libexec/PlistBuddy -c "Set :CFBundleShortVersionString $VERSION" "$APP/Contents/Info.plist"
/usr/libexec/PlistBuddy -c "Set :CFBundleVersion $VERSION" "$APP/Contents/Info.plist"
/usr/libexec/PlistBuddy -c "Set :CFBundleDisplayName $APP_DISPLAY_NAME" "$APP/Contents/Info.plist"
/usr/libexec/PlistBuddy -c "Set :CFBundleName $APP_DISPLAY_NAME" "$APP/Contents/Info.plist"
/usr/libexec/PlistBuddy -c "Set :CFBundleIdentifier $APP_BUNDLE_ID" "$APP/Contents/Info.plist"
/usr/libexec/PlistBuddy -c "Add :NATSDeskDataDirectory string $APP_DATA_DIRECTORY" "$APP/Contents/Info.plist"
/usr/libexec/PlistBuddy -c "Add :NATSDeskPort integer $APP_PORT" "$APP/Contents/Info.plist"
/usr/libexec/PlistBuddy -c "Add :NATSDeskUpdatesEnabled bool $APP_UPDATES_ENABLED" "$APP/Contents/Info.plist"
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
cp desktop/AppIcon.icns "$APP/Contents/Resources/AppIcon.icns"
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
