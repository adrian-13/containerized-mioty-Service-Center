#!/bin/sh
# ──────────────────────────────────────────────────────────
# Wrapper for docker compose — ensures all bind-mounted files
# exist BEFORE Docker tries to mount them (otherwise Docker
# creates directories and the container fails to start).
# ──────────────────────────────────────────────────────────
set -e
cd "$(dirname "$0")"

seed() {
    target="$1"
    default="$2"

    # Fix: Docker may have created a directory instead of a file
    [ -d "$target" ] && rm -rf "$target"

    if [ ! -f "$target" ]; then
        if [ -f "$default" ]; then
            cp "$default" "$target"
            echo "  Created $target from $default"
        else
            touch "$target"
            echo "  Created empty $target"
        fi
    fi
}

echo "Checking config files..."
seed "endpoints.json"          "endpoints.default.json"
seed "users.json"              "users.default.json"
seed "coverage_positions.json" "coverage_positions.default.json"
seed "coverage_floorplan.txt"  ""
seed ".env"                    ".env.example"

mkdir -p data
seed "data/base_stations.json" "base_stations.default.json"

mkdir -p certs logs

echo "Starting docker compose..."
exec docker compose "$@"
