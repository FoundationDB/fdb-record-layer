#!/usr/bin/env bash
#
# run.sh
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Runs one headless iteration of the /auto-qa skill (see .claude/skills/auto-qa/SKILL.md).
# Meant to be called from cron, launchd, or CI.
#
# Usage: auto-qa/run.sh [FOCUS] [batch=<n>] [push=no]
#
# Environment:
#   AUTO_QA_DIR       dedicated checkout (default ~/.auto-qa/fdb-record-layer); cloned on first run
#   AUTO_QA_LOG_DIR   log folder (default ~/.auto-qa/logs); logs older than 30 days are deleted
#   AUTO_QA_TIMEOUT   seconds before the run is killed (default 14400 = 4 hours)
#   AUTO_QA_MAX_TURNS cap on agent turns (default 400)
#   AUTO_QA_FDB_ENV   fdb-environment.yaml to copy into the checkout
#                     (default: the one next to this script's repo, if any)

set -uo pipefail

AUTO_QA_DIR="${AUTO_QA_DIR:-$HOME/.auto-qa/fdb-record-layer}"
AUTO_QA_LOG_DIR="${AUTO_QA_LOG_DIR:-$HOME/.auto-qa/logs}"
AUTO_QA_TIMEOUT="${AUTO_QA_TIMEOUT:-14400}"
AUTO_QA_MAX_TURNS="${AUTO_QA_MAX_TURNS:-400}"
UPSTREAM_URL="https://github.com/FoundationDB/fdb-record-layer.git"

SCRIPT_REPO="$(cd "$(dirname "$0")/.." && pwd)"
AUTO_QA_FDB_ENV="${AUTO_QA_FDB_ENV:-$SCRIPT_REPO/fdb-environment.yaml}"

mkdir -p "$AUTO_QA_LOG_DIR"
LOG="$AUTO_QA_LOG_DIR/$(date +%Y-%m-%d_%H-%M-%S).log"
find "$AUTO_QA_LOG_DIR" -name '*.log' -mtime +30 -delete 2>/dev/null

log() {
    echo "[run.sh $(date +%H:%M:%S)] $*" | tee -a "$LOG"
}

# 1. Dedicated checkout, never the one you work in.
if [[ ! -e "$AUTO_QA_DIR/.git" ]]; then
    log "Cloning $UPSTREAM_URL into $AUTO_QA_DIR"
    mkdir -p "$(dirname "$AUTO_QA_DIR")"
    if ! git clone -o upstream "$UPSTREAM_URL" "$AUTO_QA_DIR" >>"$LOG" 2>&1; then
        log "ERROR: clone failed"
        exit 1
    fi
fi
cd "$AUTO_QA_DIR" || exit 1

if [[ ! -f fdb-environment.yaml && -f "$AUTO_QA_FDB_ENV" ]]; then
    cp "$AUTO_QA_FDB_ENV" fdb-environment.yaml
fi
if [[ ! -f fdb-environment.yaml ]]; then
    log "ERROR: no fdb-environment.yaml in $AUTO_QA_DIR (set AUTO_QA_FDB_ENV)"
    exit 1
fi

# Start from the latest upstream main, so the session uses the current skill and agents.
UPSTREAM_REMOTE="$(git remote -v | awk '/FoundationDB\/fdb-record-layer/ {print $1; exit}')"
if [[ -z "$UPSTREAM_REMOTE" ]]; then
    log "ERROR: no remote pointing to FoundationDB/fdb-record-layer in $AUTO_QA_DIR"
    exit 1
fi
if [[ -n "$(git status --porcelain)" ]]; then
    log "ERROR: $AUTO_QA_DIR has uncommitted changes; a previous run may have crashed. Clean it up by hand."
    exit 1
fi
git fetch -q "$UPSTREAM_REMOTE" && git checkout -q --detach "$UPSTREAM_REMOTE/main" || {
    log "ERROR: could not update to $UPSTREAM_REMOTE/main"
    exit 1
}
if [[ ! -f .claude/skills/auto-qa/SKILL.md ]]; then
    log "ERROR: $UPSTREAM_REMOTE/main has no auto-qa skill (not merged yet?)"
    exit 1
fi

# 2. Run the skill headless, with an explicit allow-list (no blanket permission mode).
ALLOWED_TOOLS=(
    "Read" "Grep" "Glob" "Agent" "Task" "Skill"
    "Edit(/**/src/test/**)" "Write(/**/src/test/**)"
    "Edit(/auto-qa/**)" "Write(/auto-qa/**)"
    "Bash(./gradlew:*)"
    "Bash(git:*)"
    "Bash(gh pr list:*)" "Bash(gh pr create:*)" "Bash(gh pr view:*)"
    "Bash(ls:*)" "Bash(cat:*)" "Bash(head:*)" "Bash(tail:*)" "Bash(grep:*)" "Bash(find:*)"
    "Bash(wc:*)" "Bash(sort:*)" "Bash(date:*)" "Bash(hostname)" "Bash(stat:*)"
    "Bash(rm:*)" "Bash(mkdir:*)" "Bash(echo:*)" "Bash(test:*)"
)

log "Starting /auto-qa $* (timeout ${AUTO_QA_TIMEOUT}s, max turns $AUTO_QA_MAX_TURNS)"
claude -p "/auto-qa $*" \
    --max-turns "$AUTO_QA_MAX_TURNS" \
    --allowedTools "${ALLOWED_TOOLS[@]}" \
    >>"$LOG" 2>&1 &
CLAUDE_PID=$!

# 3. Kill the run after the timeout (macOS has no `timeout` command).
(
    sleep "$AUTO_QA_TIMEOUT"
    if kill -0 "$CLAUDE_PID" 2>/dev/null; then
        echo "[run.sh] TIMEOUT after ${AUTO_QA_TIMEOUT}s, killing the run" >>"$LOG"
        kill "$CLAUDE_PID" 2>/dev/null
        sleep 30
        kill -9 "$CLAUDE_PID" 2>/dev/null
    fi
) &
WATCHDOG_PID=$!

wait "$CLAUDE_PID"
STATUS=$?
kill "$WATCHDOG_PID" 2>/dev/null
wait "$WATCHDOG_PID" 2>/dev/null

# 5. Exit non-zero if the iteration failed.
if [[ $STATUS -ne 0 ]]; then
    log "FAILED: claude exited with status $STATUS. Log: $LOG"
    exit "$STATUS"
fi
if ! grep -q "Auto QA report" "$LOG"; then
    log "FAILED: no report in the output. Log: $LOG"
    exit 1
fi
log "Done. Log: $LOG"
exit 0
