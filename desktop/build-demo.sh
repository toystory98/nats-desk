#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")/.."

APP_DISPLAY_NAME="NATS Desk demo" \
APP_BUNDLE_ID="local.natsdesk.desktop.demo" \
APP_DATA_DIRECTORY="NATS Desk demo" \
APP_PORT="48128" \
APP_UPDATES_ENABLED="false" \
./desktop/build.sh
