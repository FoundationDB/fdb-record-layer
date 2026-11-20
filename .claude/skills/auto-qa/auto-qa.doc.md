# auto-qa: requirements and design

This document records what the `/auto-qa` skill is for and how it is designed. It is reference
documentation only: Claude Code does not load it when the skill runs. For a one-page usage guide,
see [auto-qa.doc.short.md](auto-qa.doc.short.md). **`SKILL.md` is the source
of truth** for the exact steps. If the two disagree, `SKILL.md` wins, and this document should be
updated.

## Objective

Catch bugs by writing new unit tests for existing test files and running them. The tool only finds
and reports bugs. It never fixes production code.

## Components

| Component | File | Role |
|---|---|---|
| Orchestrator skill | `.claude/skills/auto-qa/SKILL.md` | Runs one iteration: git, batching, workers, verifiers, commits, pushes, report. Runs in the main session, because subagents cannot start other subagents. |
| Worker agent | `.claude/agents/auto-qa-worker.md` | Gets one test file in a fresh context. Writes 1-5 tests in a companion file and reports candidate bugs. |
| Verifier agent | `.claude/agents/auto-qa-verifier.md` | Gets one candidate bug, without the worker's reasoning. Tries to prove the test wrong and returns CONFIRMED or REJECTED. |
| Wrapper script | `auto-qa/run.sh` | Runs a headless iteration (cron, launchd, CI) in a dedicated checkout, with a tool allow-list, a timeout and a log. |
| Fixer skill | `.claude/skills/auto-qa-fix/SKILL.md` | `/auto-qa-fix AQA-<n>`: fixes one confirmed bug interactively on branch `fix-AQA-<n>` (see "Fixing bugs"). The only part of the tooling that edits production code. |
| Checkstyle rule | `gradle/codequality/checkstyle.xml` | Allows the `QA` abbreviation, so companions can be named `*AutoQATest`. |

## Usage

```
/auto-qa help                      usage guide; does nothing else
/auto-qa [FOCUS] [batch=<n>] [no-push]
                                   one bug-hunting iteration (default batch: 5 test files)
/auto-qa check-fixed [no-push]     only rebase and re-check known bugs (cheap, no hunting)
/auto-qa reject AQA-<n> [reason]   mark confirmed bugs "won't fix" (asks for the reason if missing)
/auto-qa unreject AQA-<n> [reason] undo a rejection: restore the test on both branches, reopen the bug
```

- **FOCUS** (one word) limits which test files are candidates. It is tried in this order: `yaml`
  (`.yamsql` files), a Gradle module, a package or path, a keyword (file name or tested class
  name). The report says which interpretation was used.
- `help`, `--help`, `-h`, `check-fixed`, `reject` and `unreject` are reserved words and are never
  treated as a FOCUS. Any other word that contains `=` or starts with `-` (such as the old
  `push=no`) is an error, so a typo can never start a run that pushes.
- `no-push` commits locally but does not push and does not touch the PR. Use it for trial runs.

## Branches and state

Both branches live on upstream (`FoundationDB/fdb-record-layer`), as `CLAUDE.md` requires. They are
**deliberately shared** by the whole team, so coverage and the bug list add up across people. The
owner name `jjezra` is a placeholder and may change later, for example to per-user branches or a
team-owned name.

| Branch | Contents |
|---|---|
| `apple/jjezra/auto-qa-search` | Every auto-QA test written so far, plus the state files. |
| `apple/jjezra/auto-qa-caught` | Only the tests of open bugs, plus `auto-qa/rejected.md`. It has a permanent draft PR (label `testing improvement`) whose CI is expected to fail, one failure per known bug. |

State files:

- `auto-qa/progress.md` (search): one line per processed test file, with the visit date, the
  upstream `main` commit and the test count.
- `auto-qa/notes/<test file path>.md` (search): what is known about a test file across visits:
  Visits, Production code, Explored, Rejected ideas (with reasons), Untried ideas. Each iteration
  builds on these notes. Workers try Untried ideas first, skip Rejected ones unless the code
  changed, and look at production-code diffs since the last visit first.
- `auto-qa/bugs.md` (search): one entry per confirmed bug (`AQA-<n>`), with its status, test, spec
  reference and details.
- `auto-qa/rejected.md` (caught): **the single source** of bugs the team decided not to fix, each
  with its reason. People add entries with `/auto-qa reject` or by pushing to the caught branch. The
  skill reads the file every iteration and removes an entry only when someone runs
  `/auto-qa unreject`.

Rebasing rewrites history, so pushes use `git push --force-with-lease`.

## Bug lifecycle

```
candidate --verifier--> CONFIRMED -> open --test passes 3/3 after a rebase--> fixed upstream (<date>, <PR>)
    |                                 ^  |                                       |
    +--> REJECTED (noted in notes)    |  +--> rejected (team, rejected.md)       +--test fails 3/3--> open (regressed)
                                      |             |
                                      +--unreject---+   (back to open, or to fixed upstream if the test passes now)
```

- **Candidate:** a failing new test counts only if it fails 3 out of 3 runs, is caused by the code
  under test (not the environment), cites a specific javadoc, doc, standard or code invariant that
  the behavior violates ("I expected X" is not enough), and has been reduced to a minimal test.
- **Verification:** an independent verifier sees only the test, the failure, the spec reference and
  a one-line summary. Anything uncertain is REJECTED, and the reason goes into the file's notes.
- **Fixed upstream:** after each rebase, the tests of open bugs are run again. A bug whose test
  passes 3 out of 3 times is marked fixed, together with the PR or commit that touched its
  production code since the previous rebase base. Its test leaves the caught branch but stays on
  the search branch as a regression test.
- **Needs review:** a bug test that had to be repaired after an API change is never marked fixed
  automatically, because the repair may have changed what it checks.
- **Regressed:** a fixed bug whose test fails 3 out of 3 times again goes back to open, and its test
  returns to the caught branch.
- **Flaky:** a test that passes or fails only sometimes keeps its status and is listed in the report.
- **Rejected by the team:** the bug's test is removed from both branches. Its status in `bugs.md`
  points to `rejected.md`, and workers are told about it, so the behavior is not reported again.
- **Unrejected:** `/auto-qa unreject AQA-<n>` undoes a rejection, for example when the team learns
  the bug is real after all. It restores the test from git history on both branches, removes the
  entry from `rejected.md`, and reopens the bug. If the test now passes, the bug is marked fixed
  upstream instead. `bugs.md` keeps the history (`previously rejected`, `unrejected`). The entry
  leaves `rejected.md` before the restored test is pushed to the search branch, so a run in
  between can never reject the bug again.

The caught PR description keeps three sections: Open bugs, Fixed upstream and Rejected.

## Companion files

New test code never goes into an existing test file. Each test file gets a companion in the same
package and source set:

- `FooTest.java` becomes `FooAutoQATest.java`. A trailing `Test` / `Tests` is replaced, not repeated.
- `foo.yamsql` becomes `foo-auto-qa.yamsql`, registered in `YamlIntegrationTests.java` right
  after the original.
- Companion files are never picked as test-file candidates.
- A companion must not extend the original test class when it has `@Test` methods, because JUnit
  would run them all again. It extends the same base class instead. Extending an abstract helper
  base with no tests is fine.
- The license header is copied from the original with the current year. The tests follow the
  `frl-test-coding-standard` skill. Each test has a short `//` comment under its signature; bug
  tests carry `// BUG AQA-<n>: ... Spec: ...`.
- For yamsql: never commit `CORRECT_EXPECTATIONS`, because it would hide bugs. A failure that only
  happens against older versions in mixed mode is not a bug in `main`.
- Workers may modify only their companion file, its yamsql registration and its notes file. They
  never touch production code, build files or git state.
- Workers and verifiers never try to disable the sandbox or change permissions, run Gradle only
  as `./gradlew` from the checkout, and keep scratch files inside the checkout. A denied command
  is reported, not worked around, and the run's report lists it under Problems.
- yamsql companions are run with `:yaml-tests:test` (all configurations, including mixed mode).
  When the external server for mixed mode is not available, workers fall back to
  `:yaml-tests:quickTest` (embedded mode only) and say so.

## One iteration (overview)

The exact steps are in `SKILL.md`.

1. **Prepare:** take the lock, require a clean tree, fetch upstream, rebase the search branch on
   `main`, compile and repair companion files, re-check known bugs (fixed, regressed), and apply
   `rejected.md`.
2. **Pick the batch:** files whose production code changed since their last visit first, then
   never-visited files (preferring recently changed code), then files with untried ideas, then the
   oldest visits. Abstract helper bases come last within each group. At least one never-visited
   file is taken when any exist. The focus is respected strictly.
3. **Workers:** one at a time, never in parallel, because they share the checkout, Gradle, FDB and
   `YamlIntegrationTests.java`. Each worker is told the known bugs, the rejected issues and the
   candidates found so far in this iteration.
4. **Verify:** one verifier per candidate, one at a time. Confirmed bugs get the next `AQA-<n>` id.
5. **Commit and push** the search branch, then rebuild the caught branch from the open bugs (it
   must compile, pass checkstyle and fail exactly for those bugs), push it, and keep the PR
   description in sync.
6. **Report**, switch back to the starting branch, and release the lock, also when the run fails.

## Fixing bugs (`/auto-qa-fix`)

`/auto-qa-fix AQA-<n>` is a separate skill, so `/auto-qa` and `run.sh` keep the guarantee that they
never edit production code. The fixer runs interactively only (never from `run.sh`, cron or CI):

1. It checks that the bug is `open` in `bugs.md` and that no open PR or existing branch already
   fixes it. A rejected bug is fixed only if the user confirms, and an already fixed one is not.
2. It creates branch `fix-AQA-<n>` from `upstream/main` (the working tree must be clean). The
   branch lives on the user's fork (`origin`), not on upstream.
3. It moves the bug's test into the real test file as a normal regression test (no `// BUG`
   comment, project style, existing helpers), and confirms that the test fails.
4. It finds the root cause and fixes it. It asks the user first when there are several reasonable
   fixes, or when the fix is complicated: many files or layers, behavior other code may rely on,
   or anything touching on-disk or serialized formats, wire protocols, public APIs, plan-cache keys
   or compatibility with older versions. In those cases it explains the options and asks how the
   user would like it fixed.
5. It confirms that the test passes (3 out of 3), runs the original test class and the tests of
   every changed production class (and related yamsql files for SQL-reachable code), runs
   checkstyle and SpotBugs, and reviews the diff with the `code-reviewer` skill.
6. It stops with the change uncommitted for the user's review. It commits, pushes `fix-AQA-<n>`
   to the fork, or opens a draft PR (label `bug fix`) only when asked. After the PR is created,
   it switches the checkout back to where it started (a branch, or a detached commit in a
   dedicated auto-qa checkout), so the next `/auto-qa` run can start right away; `fix-AQA-<n>`
   stays available for updates to the PR.

It never modifies the auto-qa branches or state files. After the PR merges, the next
`/auto-qa check-fixed` (or full run) marks the bug `fixed upstream` with that PR and removes its
test from the caught branch.

## Permissions

Within the workflow the skill may commit to, push and force-push (`--force-with-lease`) only the two
auto-qa branches, and create or update the caught PR, without asking. It must never commit to or
push any other branch, and never modify production code.

## Triggering

| How | Notes |
|---|---|
| Interactive: `/auto-qa ...` | Best in a dedicated checkout (e.g. `~/.auto-qa/fdb-record-layer`), because the skill switches branches. Use it for the first runs, to judge quality. |
| Repeating: `/loop 2h /auto-qa` | Only while that session stays open. |
| Scheduled: `auto-qa/run.sh [args]` from cron or launchd | Suggested: a nightly `check-fixed` plus a full hunt every few days. Requires the tooling to be merged to `main`, because `run.sh` checks out `upstream/main`. |
| CI | A scheduled job calling `run.sh`. Needs FDB on the runner and an API key as a secret. |

`run.sh` works like this:

- It uses its own checkout (`AUTO_QA_DIR`, default `~/.auto-qa/fdb-record-layer`), created on first
  use. When `run.sh` runs from a git checkout with an upstream remote (the usual case on a dev
  machine), the dedicated checkout is a git worktree of it: it shares the object store, so there is
  no second copy of the history and a fetch in either checkout serves both. Otherwise (e.g. on a CI
  runner) it is a fresh clone. Build output is separate in either case. Two side effects of a
  worktree: a branch cannot be checked out in two worktrees at once (the skill holds the auto-qa
  branches only during a run and ends on a detached commit), and the worktree depends on the main
  checkout, so do not delete or move that checkout while the worktree is in use.
- It copies `fdb-environment.yaml` into that checkout and starts from the latest `upstream/main`.
- It runs `claude -p "/auto-qa <args>"` with an explicit tool allow-list and `--max-turns`. It never
  uses a blanket permission mode.
- It kills the run after `AUTO_QA_TIMEOUT` (default 4 hours) and logs to `AUTO_QA_LOG_DIR`
  (default `~/.auto-qa/logs`, kept 30 days).
- It exits non-zero when the run fails.

Lock: `<git dir>/auto-qa.lock` holds the start time and host. A lock older than the timeout is
treated as stale.

To set up a machine: clone the repo, log in to Claude Code, log in to git and `gh` for `github.com`
(the FoundationDB organization requires SAML SSO), install JDK 21 and FDB, and configure
`fdb-environment.yaml`.

## Known limitations

- The lock protects only one checkout. Two runs on different machines at the same time can fail
  each other's pushes, and can assign the same `AQA-<n>` id to different bugs.
- Until the tooling is merged, the caught branch keeps `@SuppressWarnings("checkstyle:AbbreviationAsWordInName")`
  on companion classes, because `main` does not allow the `QA` abbreviation yet.

## Out of scope (for now)

- Fixing bugs automatically. `/auto-qa` never fixes bugs; `/auto-qa-fix` does, but only
  interactively and with the user reviewing every change.
- Opening GitHub issues automatically.
- More than one iteration per invocation. Repeat it with cron, CI or `/loop`.
- Using FOCUS to steer a worker's test ideas inside a file. FOCUS only picks which files are
  processed.
