#!/usr/bin/env bash
# Fetches the six game-engine files this project depends on but does not
# vendor, from the competition organizer's own public repo. Not vendored
# because that repo carries no LICENSE file -- "public on GitHub" means
# viewable/forkable there, not redistributable elsewhere without one -- so
# this script downloads them fresh from upstream instead of shipping a copy
# in this repo's history.
set -euo pipefail

UPSTREAM_RAW="https://raw.githubusercontent.com/vishwasmiddha/quantstorm-ps/main"
FILES=(engine.py game_config.py sandbox.py policy.py limits.py bot_loader.py)
DEST="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Fetching engine files from vishwasmiddha/quantstorm-ps into $DEST ..."
for f in "${FILES[@]}"; do
    curl -fsSL "$UPSTREAM_RAW/$f" -o "$DEST/$f"
    echo "  + $f"
done
echo "Done. Run: python matchup.py --strategies /path/to/your/strategies"
