# NATS Desk

A local NATS and JetStream dashboard. One Go executable embeds all browser assets; no Node.js, database, CDN, or frontend build step.

## Run

Start a local NATS container, then launch a built executable:

```sh
docker start nats
./nats-desk
```

Open http://127.0.0.1:8080. Build the executable with `go build -o nats-desk .`.

Defaults: NATS `nats://127.0.0.1:4222`, monitoring `http://127.0.0.1:8222`. Enable JetStream and HTTP monitoring on your broker (for example, `-js -m 8222`).

Optional flags:

```sh
./nats-desk -listen 127.0.0.1:8081
./nats-desk -config settings.example.json
```

Copy `settings.example.json` to `settings.json` for personal settings. Explicit flags override the JSON file. The HTTP listener accepts loopback IPs only. Stop with Ctrl+C.

## Use

- **Overview:** three-second snapshots, message rate history for this browser session, connections and subscription patterns (first 100 clients), JetStream storage and consumer totals.
- **Streams:** create, edit, inspect, purge or delete streams. Configure subject patterns, description, age/count/byte limits, and initial storage/retention. Existing storage/retention are read-only in the editor. Other existing options are preserved; a configuration fingerprint detects edits made since the dialog opened (not a server-side atomic lock). Full configuration and consumer pending/ack/redelivery counts are in Details.
- **Messages → Retained:** select a stream, optionally filter by NATS subject pattern, browse in ascending sequence pages of 50. Deleted sequence gaps are skipped. Reads do not create a consumer or acknowledge messages. No automatic browse refresh while inspecting; use Browse to reload.
- **Publish:** send text or JSON with optional string-valued JSON headers. Selecting a stream requires a JetStream publish acknowledgment; Core NATS publishing does not confirm persistence. Avoid blindly retrying a timed-out publish: the server may have accepted it. `Nats-Msg-Id` can be supplied for JetStream deduplication.
- **Messages → Live:** start an ordinary subscription for a subject pattern. Latest 500 messages remain in browser memory. Stop unsubscribes. Clear only clears the local list. Dropped count covers bounded server queues; old rows evicted from the browser buffer are not transport drops. Navigating pages keeps capture active.
- Message payload previews are bounded to 64 KiB, with explicit truncation labels; binary payloads display as base64. Publishing is text/UTF-8. Server-side capture queues are bounded, and slow browser connections are closed.
- Destructive operations require typing the exact target: stream name for purge/delete or `STREAM/sequence` for message deletion. Purging retains the stream/consumers. Deleting a stream removes its messages and consumers. Delivered message copies cannot be recalled.

Subjects are routing names. Subject patterns are edited on their stream; changing a pattern affects future capture, not subjects on retained messages. Interest retention may discard publications when there are no consumers; work-queue retention removes messages after acknowledgment.

## Build and verify

Requires Go 1.25 or later:

```sh
go build -o nats-desk .
go test ./...
go vet ./...
NATS_DESK_INTEGRATION=1 go test -race -v ./...
```

Integration tests connect to local NATS, create a uniquely named `NATSDESK_TEST_*` stream and a unique live test subject, and clean up the test stream. They exercise settings preservation, stale-edit rejection, publish/view/delete/purge, confirmation validation, sequence gaps, and live delivery. Existing streams are not modified.

The monitoring service must be enabled and reachable. NATS disconnects are retried automatically. Requests time out instead of leaving operations pending indefinitely. This tool is intended for a single local user and is not a remote authenticated admin portal.

## Bulk create streams from YAML

Open **Streams → Import YAML**, select a `.yaml` / `.yml` file or paste YAML, then choose **Preview YAML**. Review the normalized settings and click **Create N streams**. An example is available to download in the dialog and at `web/streams.example.yaml`.

```yaml
streams:
  - name: ORDERS
    subjects: [orders.>]
    storage: file
    retention: limits
    max_age: 24h
    max_messages: 100000
    max_bytes: 104857600
  - name: NOTIFICATIONS
    subjects: [notifications.>]
    max_age: 1h
```

The import format accepts the fields shown above plus `description`. `name` and a nonempty `subjects` list are required. Defaults are `file`, `limits`, `0s` (unlimited age), `-1` for maximum messages/bytes (unlimited), and one replica. Durations use Go syntax, e.g. `24h` or `168h`, not `1d`. Byte limits are integers. This is the dashboard's import format, not a full NATS server configuration or Kubernetes manifest.

- Maximum 100 streams and 256 KiB per file; one YAML document with a `streams` list.
- Unknown fields, invalid YAML, and duplicate keys reject the document. Invalid settings and duplicate stream names within a file are shown per row.
- Preview is read-only. Existing names are skipped and rechecked before creation. No import operation updates existing streams.
- NATS validates subject overlaps and other server restrictions at creation time. Rows can fail independently; successful creations remain. There is no batch rollback.
- After any interrupted or uncertain request, preview again. Existing names are skipped on retry. Imports in this dashboard process are serialized; the existence check is not an atomic create-only lock against other NATS administrators.
- Editing YAML invalidates the preview and requires another preview before creation.

## Host switcher and remote permissions

Use the host button in the header to choose **Local NATS** or **Add another host**. Profiles persist in `hosts.json` (override with `-profiles /path/hosts.json`). The selected profile is scoped to the browser tab. Switching reloads the tab and clears captured messages, previews, and open forms. In-flight requests retain their original host; changing one profile's client connection closes that profile's previous connection.

Supply the externally reachable NATS client URL (`nats://`, `tls://`, `ws://`, or `wss://`) to enable retained messages, live capture, consumer details, and publishing. Monitoring URLs and client URLs are separate. Failed client connections leave the currently selected profile in place. Fresh installs include only localhost; existing saved profiles are preserved.

Optional username/password or token can be entered when connecting; credentials remain in process memory and are not saved. URLs containing embedded credentials are rejected. Reconnect authenticated profiles after restarting the dashboard. TLS verification remains enabled; custom CA/client-certificate files and NATS `.creds` files are not supported by this form.

Permissions are enforced on the backend, determined by the configured endpoints, not by a client-supplied role:

| Operation | Both endpoints on loopback | Any non-loopback endpoint |
| --- | --- | --- |
| View monitoring, streams, messages | Yes | Yes (messages require client connection) |
| Publish application messages | Yes | Yes (requires client connection) |
| Create/edit/import/purge/delete streams | Yes | Blocked |
| Delete retained messages | Yes | Blocked |
| Publish to `$…` / `_…` administration or reply subjects | Yes | Blocked |
| Publish with `Nats-Rollup` | Yes | Blocked |

These are **dashboard restrictions**, not NATS account permissions. Other clients using the same credentials are governed by the server's NATS authorization policy. Loopback classification is based on literal localhost/loopback endpoints; if you forward a remote server through localhost, use a non-loopback profile endpoint to retain remote restrictions. Host profiles themselves may be added/connected without modifying NATS streams. Remote operation tests use local fixtures only and do not publish to the real remote server.

## macOS desktop app and DMG

Download the universal DMG from [GitHub Releases](https://github.com/toystory98/nats-desk/releases/latest). It includes Apple Silicon and Intel support and requires macOS 13 or later. Drag **NATS Desk.app** to **Applications**, eject the DMG, and open the app. No Go or Node.js installation is needed; a reachable NATS broker is still required.

This app is ad-hoc signed and **not Apple-notarized**. If macOS blocks first launch, use **System Settings → Privacy & Security → Open Anyway** after attempting to open it. See [Apple's instructions](https://support.apple.com/en-us/102445). Do not disable Gatekeeper globally.

Use **NATS Desk → Check for Updates…** to check for a release, download it, and install/relaunch. Enable **Automatically Check for Updates** in the same menu for background checks. Updates and their feed are signed with Sparkle's Ed25519 key. This verifies the publisher of updates; it does not provide Apple notarization. Builds from before 1.0.1 require one manual installation of a newer DMG to get the updater.

The Cocoa/WebKit app runs its bundled Go dashboard at `127.0.0.1:48127`, independently of development port 8080, and stops its server when quitting. Profiles and logs are stored in `~/Library/Application Support/NATS Desk/`; updates preserve that directory. Theme preferences live in WebKit storage. No saved workspace profiles, passwords, or tokens are included in the DMG.

The app supports native YAML file selection and example downloads. The View menu can reload the dashboard or open its data folder.

Build requirements: Go 1.25+, Python 3, Apple Command Line Tools with Swift, a macOS SDK, `lipo`, `sips`, `iconutil`, `codesign`, and `hdiutil`. The build downloads a checksum-pinned Sparkle framework.

```sh
./desktop/build.sh
./desktop/package.sh
```

Set `GO_BIN` if Go is not on PATH. `VERSION` controls the app and DMG version. See [the release guide](docs/RELEASING.md) for publishing and signing-key setup.
