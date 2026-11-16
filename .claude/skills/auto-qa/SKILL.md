---
name: auto-qa
description: Run one iteration of the automated QA bug hunt. Writes new tests for a batch of existing test files on the shared apple/jjezra/auto-qa-search branch, verifies suspected bugs, and copies confirmed bug-found tests to apple/jjezra/auto-qa-caught. Usage - /auto-qa [FOCUS] [batch=<n>] [push=no]
argument-hint: "[FOCUS] [batch=<n>] [push=no]"
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

## Permissions

Within this workflow you may, without asking: commit to, push, and force-push
(`--force-with-lease`) SEARCH and CAUGHT, and create and update the draft PR for CAUGHT.
Never commit to or push any other branch. Never modify production code. If you ever find
yourself on an unexpected branch, stop and report.

## Parse the arguments

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
   ```
4. If PUSH = yes: `git push --force-with-lease UPSTREAM SEARCH`.

## F. Update the caught branch

1. Check out CAUGHT (local, else `UPSTREAM/CAUGHT`, else create from `UPSTREAM/main`).
2. `git rebase UPSTREAM/main`.
3. Make CAUGHT's companion files contain exactly the tests of bugs with status `open` in
   bugs.md (read them from SEARCH with `git show SEARCH:<path>`), plus the class skeleton,
   imports, setup, and helpers they need. Remove tests of fixed bugs, and delete companion
   files left with no tests (and their yamsql registrations). Do not copy `auto-qa/`.
4. Compile, run `checkstyleTest` for the affected modules, and run each copied test to
   confirm it still fails.
5. If nothing changed, skip the commit. Otherwise commit with the same message as E.3.
   If PUSH = yes: `git push --force-with-lease UPSTREAM CAUGHT`.
6. If PUSH = yes and there is at least one open bug and no PR exists for CAUGHT
   (`gh pr list --repo FoundationDB/fdb-record-layer --head CAUGHT`), create it:
   `gh pr create --draft --repo FoundationDB/fdb-record-layer --base main --head CAUGHT
   --label "testing improvement"` with title `Auto QA: tests that expose known bugs` and a
   description saying that its CI is expected to fail, each failing test is a known bug
   listed with its AQA id, and the branch is maintained by the auto-qa skill.

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
   Pushed: yes | no (push=no) | failed (<error>)
   Problems / skipped steps: <...> | none
   ```
2. `git checkout START_BRANCH`.
3. Remove the lock.
