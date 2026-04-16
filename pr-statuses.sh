#!/bin/bash
set -euo pipefail

if [ $# -ne 1 ]; then
    echo "Usage: $0 <pr-number>"
    exit 1
fi

PR="$1"
REPO=$(gh repo view --json nameWithOwner -q '.nameWithOwner')
REPO="FoundationDB/fdb-record-layer"

echo "Looking at $REPO PR #$PR"

# Fetch all commits for the PR (paginate up to 250)
COMMITS=$(gh api "repos/${REPO}/pulls/${PR}/commits" --paginate \
    -q '.[] | "\(.sha)\t\(.commit.message | split("\n")[0])"')

while IFS=$'\t' read -r SHA MSG; do
    SHORT_SHA="${SHA:0:7}"
    echo "Commit ${SHORT_SHA} - \"${MSG}\""

    # Fetch check runs for this commit (paginate)
    CHECKS=$(gh api "repos/${REPO}/commits/${SHA}/check-runs" --paginate \
        -q '.check_runs[] | "\(.conclusion // .status)\t\(.name)"')

    if [ -z "$CHECKS" ]; then
        echo "  (no checks)"
    else
        while IFS=$'\t' read -r CONCLUSION NAME; do
            case "$CONCLUSION" in
                success)   ICON="✓" ;;
                failure)   ICON="✗" ;;
                cancelled) ICON="⊘" ;;
                skipped)   ICON="⊘" ;;
                in_progress|queued|pending)
                           ICON="⧖" ;;
                *)         ICON="?" ;;
            esac
            printf "  %s %-50s %s\n" "$ICON" "$NAME" "$CONCLUSION"
        done <<< "$CHECKS"
    fi
    echo
done <<< "$COMMITS"
