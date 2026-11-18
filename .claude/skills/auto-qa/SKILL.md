---
name: auto-qa
description: Run one iteration of the automated QA bug hunt (/auto-qa help for usage). Writes new tests for a batch of existing test files on the shared apple/jjezra/auto-qa-search branch, verifies suspected bugs, and copies confirmed bug-found tests to apple/jjezra/auto-qa-caught. Usage - /auto-qa [FOCUS] [batch=<n>] [no-push], /auto-qa check-fixed to only re-check known bugs, /auto-qa reject AQA-<n> [reason] to reject a confirmed bug, or /auto-qa unreject AQA-<n> to undo a rejection
argument-hint: "help | [FOCUS] [batch=<n>] [no-push] | check-fixed | reject AQA-<n> [reason] | unreject AQA-<n>"
disable-model-invocation: true
---

# Auto QA: one iteration

You are the orchestrator. You do the git work, pick the batch, start one `auto-qa-worker`
per test file and one `auto-qa-verifier` per candidate bug, then commit, push, and report.
You find bugs; you never fix production code.

Arguments: `$ARGUMENTS`

## Constants

- SEARCH = `apple/jjezra/auto-qa-search`
- CAUGHT = `apple/jjezra/auto-qa-caught`
- These branches are shared by the whole team. The owner name `jjezra` may change later.
- AUTO_QA_TIMEOUT = 4 hours (or the `AUTO_QA_TIMEOUT` environment variable, in seconds).
- State files (only on SEARCH): `auto-qa/progress.md`, `auto-qa/bugs.md`,
  `auto-qa/notes/<test file path>.md`.
- REJECTED_FILE (only on CAUGHT): `auto-qa/rejected.md`. The single source of truth for
  confirmed bugs that the team decided not to fix, each with its reason. People reject an
  issue by adding it to this file on CAUGHT (i.e. by pushing to the caught PR's branch). You read it
  every iteration and apply it. You never remove entries from it, except in unreject mode
  (section U), when a user explicitly asks for it. Format:
  ```
  # Auto QA rejected issues

  ## AQA-<n>: <summary>
  - rejected: <date> by <who>
  - reason: <why it will not be fixed>
  ```
- FIX_IMPORTS = `<git dir>/auto-qa-fix-unused-imports.py`, a copy of
  `auto-qa/fix-unused-imports.py` made in step A.1 (the SEARCH and CAUGHT branches may not
  contain the tooling). **Removing tests:** whenever you delete tests from a companion file
  (steps A.5b, A.7, D, F.3, R.5), afterwards run the module's `checkstyleTest` with its output
  saved to a file, run `python3 FIX_IMPORTS <that file>` to drop the imports it reports as
  unused, and run `checkstyleTest` again until it passes. Remove unused private helpers by
  hand if SpotBugs reports them.

## Permissions

Within this workflow you may, without asking: commit to, push, and force-push
(`--force-with-lease`) SEARCH and CAUGHT, and create and update the draft PR for CAUGHT.
Never commit to or push any other branch. Never modify production code. If you ever find
yourself on an unexpected branch, stop and report.

## Parse the arguments

- If the first word is `help`, `--help` or `-h`, this is HELP MODE: print the text in section
  "H. Help" exactly as written, and do nothing else (no lock, no git commands, no Gradle, no
  agents). These words are therefore never a FOCUS.
- If the first word is `reject`, this is REJECT MODE: follow section "R. Reject mode" below
  instead of steps A-G. The rest of the arguments are one or more `AQA-<n>` ids, optionally
  followed by `no-push`, then the REASON (all remaining text). `reject` is therefore never a
  FOCUS.
- If the first word is `unreject`, this is UNREJECT MODE: follow section "U. Unreject mode"
  below instead of steps A-G. The rest of the arguments are one or more `AQA-<n>` ids,
  optionally followed by `no-push`, then an optional REASON (all remaining text).
  `unreject` is therefore never a FOCUS.
- If the first word is `check-fixed`, this is CHECK-FIXED MODE: a cheap run that only
  rebases, re-checks known bugs, and updates the caught branch and PR, with no bug hunting.
  Follow section "K. Check-fixed mode" below. Only `no-push` may follow. `check-fixed` is
  therefore never a FOCUS.
- `batch=<n>` -> BATCH_SIZE (default 5).
- `no-push` -> PUSH = no: commit locally on both branches but do not push and do not create
  or update the PR. Use it for trial runs. Default PUSH = yes.
- Any other word that contains `=` or starts with `-` (other than `-h`, `--help`), for
  example the old `push=no`, is an error (this does not apply to the free-text REASON of
  reject and unreject modes): stop before step A.0 and report the unknown
  argument with a pointer to `/auto-qa help`. Never treat it as a FOCUS, and never start a
  run with it, since that run would push.
- Anything else (at most one word) -> FOCUS. Interpret it by trying, in order:
  1. `yaml` -> `.yamsql` files under `yaml-tests/src/test/resources/`.
  2. a Gradle module name (a top-level directory with a `*.gradle` file) -> test files in
     that module.
  3. a package (dots -> slashes) or a path that exists -> test files under it.
  4. a keyword -> test files whose name contains it, or that import/use a production class
     whose name contains it.
  Use the first interpretation that matches at least one test file. Remember which one you
  used for the report.

"Test files" means `*Test.java` / `*Tests.java` under `src/test/java` and `.yamsql` files
under `yaml-tests/src/test/resources/`. Companion files (`*AutoQATest.java`,
`*-auto-qa.yamsql`) are NEVER test-file candidates.

## Always clean up

If you stop for any reason after step A.0, still do step G (report, restore the branch,
release the lock).

## A. Prepare the search branch

0. **Lock.** `LOCK="$(git rev-parse --git-dir)/auto-qa.lock"`. If it exists and its
   modification time is less than AUTO_QA_TIMEOUT old, stop: another iteration is running
   in this checkout. If it is older, remove it and mention that in the report. Then create
   it containing the start time and `hostname`.
1. Remember the current branch as START_BRANCH. If `git status --porcelain` is not empty,
   stop (release the lock first). Copy `auto-qa/fix-unused-imports.py` to FIX_IMPORTS.
2. **Find UPSTREAM:** the remote from `git remote -v` whose URL contains
   `FoundationDB/fdb-record-layer`. If none, stop. `git fetch UPSTREAM`.
3. Check out SEARCH. If it doesn't exist locally, use `UPSTREAM/SEARCH` if it exists,
   otherwise create it from `UPSTREAM/main`. If the local branch exists and
   `UPSTREAM/SEARCH` is ahead of it, reset the local branch to `UPSTREAM/SEARCH` (the remote
   is the shared source of truth).
4. Remember PREV_BASE = `git merge-base SEARCH UPSTREAM/main` (the main commit the branch
   was based on before this run). Then `git rebase UPSTREAM/main`. Resolve conflicts;
   normally only the registrations in `yaml-tests/src/test/java/YamlIntegrationTests.java`
   conflict (keep both sides).
5. **Clean up after upstream changes:**
   a. For each companion file whose original test file no longer exists: delete the
      companion file (and its yamsql registration), its notes file, and its line in
      progress.md.
   b. For every module that has companion files, run `./gradlew :<module>:compileTestJava`.
      Fix compile errors in companion files only. Delete any test you cannot fix and record
      why under "Rejected" in its notes file. Remember every bug test (`// BUG` or
      `// FIXED UPSTREAM`) whose code you changed here as REPAIRED.
6. **Re-check known bugs.** Skip this step if PREV_BASE equals `UPSTREAM/main` (main did
   not move, so nothing can have changed).
   a. **Fixed upstream.** For each bug with status `open`, run its test. If it passes 3 out
      of 3 times:
      - If the test is in REPAIRED, do NOT mark it fixed: a repaired test may pass because
        the repair changed what it checks. Keep it `open` and list it under "Needs review"
        in the report.
      - Otherwise find what fixed it: `git log --format='%h %s' PREV_BASE..UPSTREAM/main --
        <production files>`, where the production files are the ones named in the bug's
        details/spec, plus the "Production code" list in the test file's notes. Prefer the
        PR number from a subject like `... (#4931)`; else the short commit hash; else the
        range `PREV_BASE..<UPSTREAM/main short>`. Call it FIX.
      - Set its status to `fixed upstream (<date>, <FIX>)`, change its comment from
        `// BUG AQA-<n>:` to `// FIXED UPSTREAM AQA-<n> (<FIX>):`, and keep the test on SEARCH
        as a regression test.
      A test that passes 1 or 2 times out of 3 stays `open`; list it as flaky in the report.
   b. **Regressions.** For each bug with status `fixed upstream ...`, run its test. If it
      fails 3 out of 3 times, the bug is back: set its status to `open`, add a line
      `- regressed: <date> (between PREV_BASE short and UPSTREAM/main short)`, keep the
      earlier `fixed upstream` information in a line `- previously fixed: <FIX>`, and change
      its comment back to `// BUG AQA-<n>: ... (regressed <date>)`. Step F copies it back to
      CAUGHT. If it fails only sometimes, keep it fixed and list it as flaky in the report.
7. **Rejected issues.** Read REJECTED_FILE from `UPSTREAM/CAUGHT` (or the local CAUGHT if
   the remote branch doesn't exist; skip if neither has the file). Remember its content as
   REJECTED. For each `AQA-<n>` in it whose status in bugs.md is not already `rejected`:
   - set its status in bugs.md to `rejected (see auto-qa/rejected.md on CAUGHT)`; do not copy
     the reason, REJECTED_FILE stays the only place for it;
   - delete its test from the companion file (and the companion file, with its yamsql
     registration, if no tests are left), and remove helpers/imports that became unused;
   - add the idea under "Rejected" in that test file's notes, with
     `rejected by the team, see AQA-<n> in auto-qa/rejected.md on CAUGHT`;
   - update the test count in progress.md.
   Run step 5b again for the affected modules.

## B. Pick the batch

0. Build the candidate list: all test files (never companion files), filtered by FOCUS if
   given. If FOCUS matches nothing, stop and report.
1. Order the candidates by priority:
   a. listed in progress.md, and some file in its notes' "Production code" list changed
      between the visit's commit and `UPSTREAM/main`
      (`git diff --quiet <commit> UPSTREAM/main -- <files>`);
   b. not in progress.md; among these, prefer files whose production code changed in the
      last 30 days (`git log --since=30.days UPSTREAM/main -- <dir>`);
   c. in progress.md and its notes have items under "Untried";
   d. all others, oldest visit first.
   Within each group, put test files that contain no `@Test`, `@ParameterizedTest` or
   `@YamlTest` (abstract helper bases) last: they cover less directly.
2. Take BATCH_SIZE files in that order, but if any (b) candidates exist, include at least
   one. If fewer than BATCH_SIZE candidates exist, take them all. Never take files outside
   the focus.

## C. Run the workers

Run one `auto-qa-worker` per file, ONE AT A TIME (never in parallel: they share the
checkout, Gradle build folders, FDB, and YamlIntegrationTests.java). Give each:

- TEST_FILE, COMPANION_FILE (per the naming rule in the worker's instructions), NOTES_FILE
  (`auto-qa/notes/<TEST_FILE>.md`),
- UPSTREAM_COMMIT (`git rev-parse UPSTREAM/main`), LAST_VISIT_COMMIT (from progress.md, or
  `none`),
- the content of auto-qa/bugs.md,
- "Rejected issues": the content of REJECTED (or `none`), so known and accepted behavior is
  not reported again,
- "Candidates so far": a one-line summary of each CANDIDATE_BUG returned by earlier workers
  in this iteration (still unverified), or `none`, so the same bug is not reported twice.

After each worker, check with `git status` that it only touched its COMPANION_FILE, its
NOTES_FILE, and (for yamsql) `YamlIntegrationTests.java`. Revert anything else and report it.

## D. Verify the candidates

For each CANDIDATE_BUG returned by the workers, run one `auto-qa-verifier`, one at a time.
Give it ONLY: test, summary, spec_reference, run_command, failure_output. Never pass the
worker's reasoning.

After each verifier, check that `git status` is unchanged; revert any leftovers.

- **CONFIRMED:** take the next id (highest `AQA-<n>` in bugs.md + 1). Add right under the
  test's signature: `// BUG AQA-<n>: <explanation>. Spec: <reference>.` Add to bugs.md:
  ```
  ## AQA-<n>: <summary>
  - status: open
  - found: <date>
  - test: <path>#<method>
  - spec: <reference>
  - details: <2-3 lines>
  ```
- **REJECTED:** fix the test so it passes, or delete it. Add the idea and the verifier's
  REASON under "Rejected" in that file's notes.

## E. Commit the search branch

1. Update progress.md: one line per processed file,
   `| <test file> | <date> | <UPSTREAM_COMMIT short> | <total tests in companion> |`
   (create the file with a header row if missing).
2. If bugs.md doesn't exist yet, create it with a `# Auto QA bugs` heading.
3. `git add` the companion files, YamlIntegrationTests.java (if changed), and `auto-qa/`.
   Commit with:
   ```
   Auto QA[ [focus: <FOCUS>]]: <N> files, <M> tests, <K> new bugs

   AQA-<n>: <short summary>            (one line per new bug)
   AQA-<n>: fixed upstream (<FIX>)     (one line per bug fixed upstream in A.6a)
   AQA-<n>: regressed                  (one line per bug that came back in A.6b)
   AQA-<n>: rejected                   (one line per bug newly rejected in A.7)
   ```
   If nothing changed on SEARCH (possible in check-fixed mode), skip the commit.
4. If PUSH = yes: `git push --force-with-lease UPSTREAM SEARCH`.

## F. Update the caught branch

1. Check out CAUGHT (local, else `UPSTREAM/CAUGHT`, else create from `UPSTREAM/main`).
2. `git rebase UPSTREAM/main`.
3. Make CAUGHT's companion files contain exactly the tests of bugs with status `open` in
   bugs.md (read them from SEARCH with `git show SEARCH:<path>`), plus the class skeleton,
   imports, setup, and helpers they need. Remove tests of fixed and rejected bugs, and delete
   companion files left with no tests (and their yamsql registrations). Do not copy the
   SEARCH state files. Keep REJECTED_FILE exactly as it is (it was written by people).
4. Compile, run `checkstyleTest` for the affected modules, and run each copied test to
   confirm it still fails.
5. If nothing changed, skip the commit. Otherwise commit with the same message as E.3.
   If PUSH = yes: `git push --force-with-lease UPSTREAM CAUGHT`.
6. If PUSH = yes and there is at least one open bug and no PR exists for CAUGHT
   (`gh pr list --repo FoundationDB/fdb-record-layer --head CAUGHT`), create it:
   `gh pr create --draft --repo FoundationDB/fdb-record-layer --base main --head CAUGHT
   --label "testing improvement"` with title `Auto QA: tests that expose known bugs` and a
   description saying that its CI is expected to fail, each failing test is a known bug
   listed with its AQA id, the branch is maintained by the auto-qa skill, and an issue can
   be rejected with `/auto-qa reject AQA-<n> <reason>` or by adding it to
   `auto-qa/rejected.md` on this branch.
7. If PUSH = yes and the PR exists and the list of open, fixed, or rejected bugs changed,
   update its description (`gh pr edit`) so that it has:
   - an "Open bugs" table listing exactly the open bugs (mark regressed ones as such);
   - a "Fixed upstream" section: one line per fixed bug with its summary and FIX, so the
     history stays visible after the test leaves this branch;
   - a "Rejected" section: one line per rejected bug with its reason from REJECTED_FILE.

## G. Report, restore, release

1. Print:
   ```
   Auto QA report
   Focus: <FOCUS> (<interpretation>) | none
   Files processed: <list>
   Tests added: <M>
   New confirmed bugs: <AQA-n: summary, ...> | none
   Rejected candidates: <test: reason, ...> | none
   Fixed upstream: <AQA-n (FIX), ...> | none
   Regressed: <AQA-n, ...> | none
   Needs review (passing after a repair, not marked fixed): <AQA-n, ...> | none
   Flaky (passed or failed only sometimes): <AQA-n, ...> | none
   Rejected by the team (newly applied): <AQA-n, ...> | none
   Pushed: yes | no (no-push) | failed (<error>)
   Problems / skipped steps: <...> | none
   ```
2. `git checkout START_BRANCH`.
3. Remove the lock.

## K. Check-fixed mode

`/auto-qa check-fixed [no-push]`: keep the bug list current without hunting for new bugs.
Cheap enough to run often, e.g. nightly from cron or on every push to main
(`auto-qa/run.sh check-fixed`).

1. Do all of step A (lock, rebase, clean up, re-check known bugs, apply rejections).
2. Skip steps B, C and D.
3. Do step E without E.1 (no files were processed). Use the commit message first line
   `Auto QA: check fixed` and the same per-bug lines. Skip the commit if nothing changed.
4. Do step F (it removes fixed tests, copies regressed tests back, and updates the PR).
   Skip committing or pushing CAUGHT if nothing changed.
5. Do step G, with `Mode: check-fixed` instead of the Focus/Files/Tests/New/Rejected
   candidates lines.

## R. Reject mode

`/auto-qa reject AQA-<n> [AQA-<m> ...] [no-push] [reason...]`: record that confirmed bugs
will not be fixed, and apply it to both branches now. No bug hunting happens in this mode.

1. **Get the reason.** If no REASON was given, ask the user for the justification (one
   question naming the ids and their summaries from bugs.md), and stop until they answer.
   If no one can answer (headless run), stop and report that the reason must be passed as
   an argument. Never invent a reason.
2. **Prepare.** Do steps A.0, A.1 and A.2 (lock, clean tree, find and fetch UPSTREAM).
3. **Validate.** Read bugs.md from `UPSTREAM/SEARCH` (or the local SEARCH). Every id must
   exist with status `open` or `fixed upstream ...`. If an id is unknown, stop and report.
   If an id is already listed in REJECTED_FILE, skip it and say so; if none are left, stop.
4. **WHO** = the GitHub login from `gh api --hostname github.com user --jq .login`, or else
   `git config user.name`.
5. **Caught branch.** Check out CAUGHT as in F.1 (do not rebase). Append to REJECTED_FILE
   (create it with its header, see Constants, if missing) one entry per id:
   ```
   ## AQA-<n>: <summary from bugs.md>
   - rejected: <date> by <WHO>
   - reason: <REASON>
   ```
   Remove the tests of these ids from CAUGHT's companion files (delete companion files left
   with no tests, and their yamsql registrations), and remove helpers/imports that became
   unused. Compile and run `checkstyleTest` for the affected modules. Commit:
   ```
   Auto QA: reject AQA-<n>[, AQA-<m>...]

   AQA-<n>: rejected (<REASON>)
   ```
   If PUSH = yes: `git push --force-with-lease UPSTREAM CAUGHT`.
6. **Search branch.** Check out SEARCH as in A.3 (do not rebase). Apply step A.7 for these
   ids (status, delete tests, notes, progress.md, step 5b). Commit with the same message,
   and if PUSH = yes: `git push --force-with-lease UPSTREAM SEARCH`.
7. **PR.** If PUSH = yes, update the caught PR description as in F.7.
8. **Report** (`Auto QA reject report`: ids rejected, ids skipped, reason, who, pushed
   yes/no), then do G.2 and G.3.

## U. Unreject mode

`/auto-qa unreject AQA-<n> [AQA-<m> ...] [no-push] [reason...]`: undo rejections, for example
when the team learns a rejected bug is real after all. No bug hunting happens in this mode.
The order matters: the entry must leave REJECTED_FILE before the restored test is pushed to
SEARCH, or the next iteration's step A.7 would reject the bug again.

1. **Prepare.** Do steps A.0, A.1 and A.2 (lock, clean tree, FIX_IMPORTS, find and fetch
   UPSTREAM). WHO as in R.4.
2. **Validate.** Every id must be listed in REJECTED_FILE on `UPSTREAM/CAUGHT` and have status
   `rejected ...` in bugs.md on `UPSTREAM/SEARCH`. Skip (and report) ids that are not; if none
   are left, stop.
3. **Find each test's last version.** The test path and method are in its bugs.md entry. On
   SEARCH, find the commit that deleted the test:
   `git log -1 --format=%H -S '<method name>' SEARCH -- <companion path>` (call it DEL), and
   read the test from `git show DEL^:<companion path>`, including its annotations, its
   `// BUG AQA-<n>:` comment, and any helpers and imports it needs.
4. **Restore on SEARCH (do not push yet).** Check out SEARCH as in A.3 (do not rebase).
   - Put the test back into its companion file. If the file was deleted, recreate it from the
     DEL^ version with only this test plus the other tests that still exist there (and restore
     its yamsql registration if needed).
   - Compile and run `checkstyleTest` for the module (use FIX_IMPORTS as in Constants).
   - Run the test 3 times. If it still fails 3 out of 3 times, set its status in bugs.md to
     `open`. If it now passes 3 out of 3 times, find FIX as in A.6a and set it to
     `fixed upstream (<date>, <FIX>)` instead. If it is flaky, set `open` and report it.
   - In bugs.md, keep the history: add `- unrejected: <date> by <WHO>[: <REASON>]` and
     `- previously rejected: <date and reason from REJECTED_FILE>`.
   - In the test file's notes, replace the `rejected by the team, see AQA-<n> ...` line under
     "Rejected" with `unrejected <date> by <WHO>`, and move the idea back under "Explored".
   - Update the test count in progress.md.
   - Commit (locally):
     ```
     Auto QA: unreject AQA-<n>[, AQA-<m>...]

     AQA-<n>: unrejected[ (<REASON>)], status <open | fixed upstream (<FIX>)>
     ```
5. **Caught branch.** Check out CAUGHT as in F.1 (do not rebase). Remove the ids' entries from
   REJECTED_FILE (keep its header even if no entries are left). For ids that are now `open`,
   copy their tests from SEARCH into CAUGHT's companion files as in F.3. Compile, run
   `checkstyleTest`, and confirm each copied test fails. Commit with the same message.
   If PUSH = yes: `git push --force-with-lease UPSTREAM CAUGHT`. If this push fails, stop and
   do not push SEARCH (the rejection is still in force, so nothing is inconsistent).
6. **Push SEARCH.** If PUSH = yes: `git push --force-with-lease UPSTREAM SEARCH`.
7. **PR.** If PUSH = yes, update the caught PR description as in F.7.
8. **Report** (`Auto QA unreject report`: ids unrejected with their new status, ids skipped
   and why, who, pushed yes/no), then do G.2 and G.3.

## H. Help

Print exactly this (between the markers), then stop:

<!-- HELP START -->
```
/auto-qa - automated QA: hunts for bugs by writing new tests for existing test files

USAGE
  /auto-qa help                      Show this help. Does nothing else.
  /auto-qa [FOCUS] [batch=<n>]       One bug-hunting iteration (default: 5 test files).
           [no-push]                 no-push: commit locally only, no push, no PR update.
  /auto-qa check-fixed [no-push]     Only rebase and re-check known bugs (cheap, no hunting).
  /auto-qa reject AQA-<n> [reason]   Mark a confirmed bug as "won't fix" (asks for the reason
                                     if missing). Several ids may share one reason.
  /auto-qa unreject AQA-<n>          Undo a rejection: restore the test on both branches
                                     and reopen the bug.

FOCUS (optional, one word; picks which test files are candidates)
  yaml                               .yamsql files under yaml-tests/
  <module>                           e.g. fdb-relational-core
  <package or path>                  e.g. com.apple.foundationdb.record.query.plan
  <keyword>                          e.g. OnlineIndexer (file or tested class name)

EXAMPLES
  /auto-qa                           /auto-qa OnlineIndexer batch=2 no-push
  /auto-qa yaml                      /auto-qa reject AQA-3 deleteWhere is not transactional

WHAT ONE ITERATION DOES
  1. Rebases apple/jjezra/auto-qa-search on main; re-checks known bugs (fixed upstream,
     regressed) and applies rejections.
  2. Picks test files: changed code first, then never-visited files, then untried ideas.
  3. A worker agent writes 1-5 tests per file in a companion *AutoQATest.java /
     *-auto-qa.yamsql file; failing tests become candidate bugs.
  4. An independent verifier agent tries to prove each candidate wrong.
  5. Confirmed bugs get an AQA-<n> id and are copied to apple/jjezra/auto-qa-caught,
     whose permanent draft PR re-runs them in CI.

WHERE THINGS ARE
  auto-qa/bugs.md, progress.md, notes/   on apple/jjezra/auto-qa-search
  auto-qa/rejected.md                    on apple/jjezra/auto-qa-caught (won't-fix list)
  Draft PR "Auto QA: tests that expose known bugs"   open, fixed and rejected bugs

TRIGGERING
  Interactive   type /auto-qa in a Claude Code session; preferably in a dedicated checkout
                (e.g. ~/.auto-qa/fdb-record-layer), since it switches branches.
  Repeating     /loop 2h /auto-qa       (only while that session stays open)
  Scheduled     auto-qa/run.sh [args]   from cron or launchd, e.g. nightly
                `auto-qa/run.sh check-fixed` plus a full hunt every few days. It uses its
                own checkout, a tool allow-list, a 4h timeout, and logs to ~/.auto-qa/logs.
                Needs JDK 21, a running FDB, and git/gh logged in to github.com.
  CI            a scheduled job calling auto-qa/run.sh (needs FDB and an API key).
```
<!-- HELP END -->
