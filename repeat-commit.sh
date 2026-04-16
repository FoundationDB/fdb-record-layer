#!/bin/bash
set -euo pipefail

if [ $# -ne 1 ]; then
    echo "Usage: $0 <file>"
    exit 1
fi

FILE="$1"
ITERATION=0

while true; do
    ITERATION=$((ITERATION + 1))
    echo "=== Iteration $ITERATION at $(date) ==="

    echo "FIXME $(date '+%Y-%m-%d %H:%M:%S') (iteration $ITERATION)" >> "$FILE"

    git add "$FILE"
    git commit -m "FIXME"
    git push

    echo "Push succeeded. Sleeping 30 minutes..."
    sleep 1200
done
