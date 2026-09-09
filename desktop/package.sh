#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."
APP="$PWD/dist/NATS Desk.app"
VERSION=$(/usr/libexec/PlistBuddy -c 'Print :CFBundleShortVersionString' "$APP/Contents/Info.plist")
if [ "$VERSION" != "$(cat VERSION)" ]; then echo 'Rebuild the app after changing VERSION.' >&2; exit 1; fi
STAGE="$PWD/dist/dmg-stage"
rm -rf "$STAGE"
mkdir -p "$STAGE"
ditto "$APP" "$STAGE/NATS Desk.app"
ln -s /Applications "$STAGE/Applications"
cat > "$STAGE/Read Me.txt" <<'TXT'
NATS Desk
macOS 13 or later • Apple Silicon and Intel

Drag NATS Desk.app to Applications, eject this disk, and launch the app.
No Go, Node.js, or separate dashboard server installation is required.
Connect to your existing NATS server; this app does not install a NATS broker.

This release is ad-hoc signed, not Apple Developer ID signed or notarized.
If macOS blocks the first launch, go to System Settings > Privacy & Security
and use Open Anyway for NATS Desk after attempting to open it.

Use NATS Desk > Check for Updates… to install future releases.
You can enable automatic update checks in the same menu.
Updates are verified with the publisher's Sparkle signing key.

Use the host button to connect to localhost or add another host.
Local endpoints have management access. Remote hosts permit viewing and
publishing; stream management, purge, and message deletion are blocked.

Settings: ~/Library/Application Support/NATS Desk/
Credentials entered in the host switcher are not saved to disk.
TXT
DMG="NATS-Desk-$VERSION-universal.dmg"
hdiutil create -ov -volname "NATS Desk" -srcfolder "$STAGE" -format UDZO "$PWD/dist/$DMG"
(cd dist && shasum -a 256 "$DMG" > "$DMG.sha256")
echo "Packaged: dist/$DMG"
