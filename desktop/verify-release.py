"""Check release metadata and Ed25519 signatures against the bundled public key."""
import base64
import hashlib
from pathlib import Path
import plistlib
import subprocess
import tempfile
import xml.etree.ElementTree as ET

root = Path(__file__).resolve().parent.parent
version = (root / 'VERSION').read_text().strip()
info = plistlib.loads((root / 'dist/NATS Desk.app/Contents/Info.plist').read_bytes())
assert info['CFBundleVersion'] == version
assert info['CFBundleShortVersionString'] == version
feed = ET.parse(root / 'dist/release/appcast.xml')
ns = {'s': 'http://www.andymatuschak.org/xml-namespaces/sparkle'}
items = feed.findall('./channel/item')
assert len(items) == 1, 'Expected only the current release'
item = items[0]
assert item.findtext('s:version', namespaces=ns) == version
asset = root / f'dist/release/NATS-Desk-{version}-universal.dmg'
enclosure = item.find('enclosure')
assert enclosure.get('url') == f'https://github.com/toystory98/nats-desk/releases/download/v{version}/{asset.name}'
assert int(enclosure.get('length')) == asset.stat().st_size
signature = enclosure.get(f'{{{ns["s"]}}}edSignature')
assert len(base64.b64decode(signature, validate=True)) == 64
assert item.findtext('s:minimumSystemVersion', namespaces=ns) in ('13.0', '13.0.0')
checksum = asset.with_suffix('.dmg.sha256').read_text().split()[0]
assert hashlib.sha256(asset.read_bytes()).hexdigest() == checksum
# CryptoKit is available with the same Swift toolchain used to build the app.
# Verify with the PUBLIC key embedded in the app, not just the local private key.
with tempfile.TemporaryDirectory() as tmp:
    src = Path(tmp) / 'verify.swift'
    src.write_text('''import Foundation
import CryptoKit
let args = CommandLine.arguments
let key = try Curve25519.Signing.PublicKey(rawRepresentation: Data(base64Encoded: args[1])!)
let signature = Data(base64Encoded: args[2])!
let data = try Data(contentsOf: URL(fileURLWithPath: args[3]))
guard key.isValidSignature(signature, for: data) else { exit(1) }
''')
    subprocess.run(['xcrun', 'swift', '-module-cache-path', str(root / 'dist/build/module-cache'), str(src), info['SUPublicEDKey'], signature, str(asset)], check=True)
print(f'Validated {version}: URLs, version, checksum, and archive signature match the app.')
