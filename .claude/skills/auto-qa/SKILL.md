---
name: auto-qa
description: Run one iteration of the automated QA bug hunt. Writes new tests for a batch of existing test files on the shared apple/jjezra/auto-qa-search branch, verifies suspected bugs, and copies confirmed bug-found tests to apple/jjezra/auto-qa-caught. Usage - /auto-qa [FOCUS] [batch=<n>] [push=no], or /auto-qa reject AQA-<n> [reason] to reject a confirmed bug
argument-hint: "[FOCUS] [batch=<n>] [push=no] | reject AQA-<n> [reason]"
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
  every iteration and apply it; you never remove entries from it. Format:
  ```
  # Auto QA rejected issues

  ## AQA-<n>: <summary>
  - rejected: <date> by <who>
  - reason: <why it will not be fixed>
  ```

## Permissions

Within this workflow you may, without asking: commit to, push, and force-push
(`--force-with-lease`) SEARCH and CAUGHT, and create and update the draft PR for CAUGHT.
Never commit to or push any other branch. Never modify production code. If you ever find
yourself on an unexpected branch, stop and report.

## Parse the arguments

- If the first word is `reject`, this is REJECT MODE: follow section "R. Reject mode" below
  instead of steps A-G. The rest of the arguments are one or more `AQA-<n>` ids, optionally
  followed by `push=no`, then the REASON (all remaining text). `reject` is therefore never a
  FOCUS.
- `batch=<n>` -> BATCH_SIZE (default 5).
- `push=no` -> PUSH = no: commit locally on both branches but do not push and do not create
  or update the PR. Use it for trial runs. Default PUSH = yes.
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
   stop (release the lock first).
2. **Find UPSTREAM:** the remote from `git remote -v` whose URL contains
   `FoundationDB/fdb-record-layer`. If none, stop. `git fetch UPSTREAM`.
3. Check out SEARCH. If it doesn't exist locally, use `UPSTREAM/SEARCH` if it exists,
   otherwise create it from `UPSTREAM/main`. If the local branch exists and
   `UPSTREAM/SEARCH` is ahead of it, reset the local branch to `UPSTREAM/SEARCH` (the remote
   is the shared source of truth).
4. `git rebase UPSTREAM/main`. Resolve conflicts; normally only the registrations in
   `yaml-tests/src/test/java/YamlIntegrationTests.java` conflict (keep both sides).
5. **Clean up after upstream changes:**
   a. For each companion file whose original test file no longer exists: delete the
      companion file (and its yamsql registration), its notes file, and its line in
      progress.md.
   b. For every module that has companion files, run `./gradlew :<module>:compileTestJava`.
      Fix compile errors in companion files only. Delete any test you cannot fix and record
      why under "Rejected" in its notes file.
6. **Bugs fixed upstream.** For each bug with status `open` in bugs.md, run its test. If it
   passes 3 out of 3 times: set its status to `fixed upstream (<date>)`, change its comment
   from `// BUG AQA-<n>:` to `// FIXED UPSTREAM AQA-<n>:`, and keep the test.
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
   AQA-<n>: fixed upstream             (one line per bug fixed upstream in A.6)
   AQA-<n>: rejected                   (one line per bug newly rejected in A.7)
   ```
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
7. If PUSH = yes and the PR exists and the list of open bugs changed, update its description
   (`gh pr edit`) so its table lists exactly the open bugs, plus a short list of rejected
   ones with their reasons from REJECTED_FILE.

## G. Report, restore, release

1. Print:
   ```
   Auto QA report
   Focus: <FOCUS> (<interpretation>) | none
   Files processed: <list>
   Tests added: <M>
   New confirmed bugs: <AQA-n: summary, ...> | none
   Rejected candidates: <test: reason, ...> | none
   Fixed upstream: <AQA-n, ...> | none
   Rejected by the team (newly applied): <AQA-n, ...> | none
   Pushed: yes | no (push=no) | failed (<error>)
   Problems / skipped steps: <...> | none
   ```
2. `git checkout START_BRANCH`.
3. Remove the lock.

## R. Reject mode

`/auto-qa reject AQA-<n> [AQA-<m> ...] [push=no] [reason...]`: record that confirmed bugs
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
