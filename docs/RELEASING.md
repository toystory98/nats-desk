# Publishing NATS Desk

Repository: https://github.com/toystory98/nats-desk

Download page: https://github.com/toystory98/nats-desk/releases/latest

Update feed: https://github.com/toystory98/nats-desk/releases/latest/download/appcast.xml

## One-time setup

1. Sign into GitHub CLI with `gh auth login --hostname github.com --git-protocol https --web`. Do not put a PAT in source files or share it in chat. Creating the repository and pushing the workflow needs repository/workflow access.
2. Create the public repository from this folder with `gh repo create toystory98/nats-desk --public --source=. --remote=origin --push` after committing the reviewed source. Personal `hosts.json`, settings, logs, build outputs, and keys are ignored.
3. The Sparkle update-signing key has been created in the publisher's macOS login Keychain under account `toystory98.nats-desk`. Its public key is in `desktop/Info.plist`. Keep the private key backed up securely; without Developer ID signing, losing this key can require users to reinstall manually. Never generate a replacement key for routine releases.
4. Transfer the existing signing key to the GitHub Actions repository secret without printing it:

```sh
./desktop/fetch-sparkle.sh
umask 077
KEY_FILE=$(mktemp)
trap 'rm -f "$KEY_FILE"' EXIT
./dist/build/sparkle/bin/generate_keys --account toystory98.nats-desk -x "$KEY_FILE"
gh secret set SPARKLE_PRIVATE_KEY --repo toystory98/nats-desk < "$KEY_FILE"
rm -f "$KEY_FILE"
trap - EXIT
```

The secret is used only by the tag-triggered release job. Forks must use their own feed URL and signing key. Do not put a GitHub token in the app: public downloads need no user credentials.

## Publish a version

Change `VERSION` to a new, increasing `major.minor.patch` number, update code, then:

```sh
go test ./...
go vet ./...
git add VERSION desktop web '*.go' docs README.md .github .gitignore go.mod go.sum settings.example.json
git commit -m "Release NATS Desk VERSION"
git push origin main
git tag "v$(cat VERSION)"
git push origin "v$(cat VERSION)"
```

Replace the commit message's VERSION with the actual number. The first updater release is `1.0.1`.

The workflow tests, builds a universal app, creates the DMG, signs the archive and appcast, validates the archive signature against the app's public key, uploads all three assets to a draft release, then publishes it as latest. It refuses existing release tags and older versions. It never publishes for an ordinary code push.

If upload fails after creating a draft, inspect the draft/assets before retrying. The workflow intentionally refuses to overwrite existing releases; delete an incomplete draft explicitly or release a new version. Do not replace a published DMG: its signature and checksum would change. To roll back code, publish the old code under a higher version.

## Local release verification

```sh
./desktop/build.sh
./desktop/release.sh
```

`dist/release/` contains the current DMG, SHA-256 checksum, and signed `appcast.xml`. The signing tools read the private key from Keychain. CI supplies a temporary `SPARKLE_KEY_FILE`, which it deletes after use.

For an end-to-end check, install a genuine older updater-enabled app in Applications, publish a higher version, select **Check for Updates…**, then verify the version, relaunch, and saved profiles. A local build and signature check alone do not prove that GitHub hosting and installation work. Versions before 1.0.1 need one manual DMG installation first.

## Apple signing

Current releases use ad-hoc code signatures; no paid Apple membership is configured. Sparkle signatures verify updates but do not remove Gatekeeper first-launch checks. Users may need Privacy & Security → Open Anyway. Do not tell users to disable Gatekeeper globally.

If Developer ID becomes available later, configure signing with `SIGN_IDENTITY`, add notarization and stapling before update signing, and update the installer text. Preserve the app bundle ID (`local.natsdesk.desktop`), update key, and profile storage path so existing installations can migrate.

## Dependencies

Sparkle 2.9.6 is downloaded from the official release and verified against its pinned SHA-256 in `desktop/fetch-sparkle.sh`. Its license is included in the app's Resources. Update this pin deliberately, with the matching official release digest, and retest the update flow.
