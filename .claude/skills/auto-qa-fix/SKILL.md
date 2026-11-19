---
name: auto-qa-fix
description: Fix one confirmed auto-qa bug (AQA-<n>) interactively. Creates branch fix-AQA-<n> from upstream main, moves the bug's test into the real test file, confirms it fails, fixes the production code (asking the user when the fix needs a decision), confirms it passes, and stops for the user's review. Commits, pushes to the user's fork, or opens a PR only when asked. Usage - /auto-qa-fix AQA-<n>
argument-hint: "AQA-<n> | help"
disable-model-invocation: true
---

# Auto QA fix: fix one confirmed bug

You fix ONE bug found by `/auto-qa`. Unlike `/auto-qa`, you edit production code, so the rules
are strict: you work only on the branch `fix-AQA-<n>`, you never touch the auto-qa branches,
and you never commit, push, or open a PR unless the user asks for it in this conversation.

Arguments: `$ARGUMENTS`

## Constants

- SEARCH = `apple/jjezra/auto-qa-search` (holds `auto-qa/bugs.md` and the bug tests)
- CAUGHT = `apple/jjezra/auto-qa-caught` (holds `auto-qa/rejected.md`)
- UPSTREAM = the remote whose URL contains `FoundationDB/fdb-record-layer`.
- FORK = the remote named `origin`. It must be the user's fork: if its URL contains
  `FoundationDB/fdb-record-layer` (or it does not exist), stop and ask the user which remote
  is their fork.
- BRANCH = `fix-AQA-<n>`, created from `UPSTREAM/main`, pushed (only when asked) to FORK.

## Rules

- **Interactive only.** This skill needs a person to answer questions and review the result.
  If no one can answer (headless run, `claude -p`, `auto-qa/run.sh`), stop immediately and
  report that `/auto-qa-fix` must be run interactively.
- You may edit production code and test code, only on BRANCH. Never modify SEARCH, CAUGHT,
  `auto-qa/bugs.md`, `auto-qa/rejected.md`, or any other branch.
- Never commit, push, create a PR, or force anything without the user's explicit request in
  this conversation.
- Never change an on-disk or serialized format, a wire protocol, a public API signature, or a
  plan-cache key without the user's explicit approval, and call out compatibility with older
  versions (mixed-version clusters, stored data, serialized meta-data) whenever it applies.
- Never try to disable the sandbox or change permissions. Run Gradle as `./gradlew` from the
  checkout's root. Apply the `frl-coding-standard`, `frl-test-coding-standard`,
  `using-gradle` and `test-runner` skills.

## Parse the arguments

- `help`, `--help`, `-h` -> print section "H. Help" exactly and stop.
- Exactly one `AQA-<n>` -> the bug to fix. Anything else -> stop and point to `/auto-qa-fix help`.

## Steps

1. **Read the bug.** `git fetch UPSTREAM`. Read the `AQA-<n>` entry from
   `git show UPSTREAM/SEARCH:auto-qa/bugs.md`: summary, test path and method, spec, details
   (which usually name the root-cause file and line).
   - Status `open`: continue.
   - Status `fixed upstream ...`: tell the user it is already fixed (and by what), and stop.
   - Status `rejected ...`: show the reason from `git show UPSTREAM/CAUGHT:auto-qa/rejected.md`
     and ask whether to fix it anyway (and suggest `/auto-qa unreject AQA-<n>` if so).
   - Unknown id: stop.
2. **Check for existing work.** Look for an open PR or issue that already addresses it:
   `gh pr list --repo FoundationDB/fdb-record-layer --state open --search "AQA-<n>"`, and the
   same with the production class name from the bug. Check whether BRANCH already exists
   locally or on FORK. If anything is found, show it and ask how to proceed.
3. **Create the branch.** Require a clean working tree (`git status --porcelain` empty; if
   not, stop and ask). Remember the current branch as START_BRANCH. `git fetch FORK`, then
   `git switch -c fix-AQA-<n> UPSTREAM/main --no-track`.
4. **Add the regression test properly.** Read the bug test from SEARCH
   (`git show UPSTREAM/SEARCH:<companion path>`), with the helpers it needs. Add it to the
   real test file (the companion's original, e.g. `FooTest.java` for `FooAutoQATest.java`;
   for yamsql, a new query in the original `.yamsql` file, or in its block that already has
   a matching schema) as a normal test of the project:
   - follow the test coding standards and the style of the file; reuse its existing setup and
     helpers instead of copying them;
   - give it a descriptive name and a short `//` comment under the signature describing the
     correct behavior; no `// BUG AQA-<n>` comment and no `@SuppressWarnings` for the `QA`
     abbreviation;
   - if the original file is an abstract helper base, add the test to the most fitting
     concrete test class instead.
   Compile and run `checkstyleTest`. Run the new test: it must FAIL the same way the bug's
   details describe. If it passes, stop and tell the user (the bug may have been fixed in a way
   `check-fixed` has not recorded yet).
5. **Analyze and decide.** Find the root cause (start from the file and line in the bug's
   details) and work out the fix. Ask the user before writing code when:
   - there is more than one reasonable fix (describe each with its trade-offs and recommend
     one);
   - the fix is complicated: it spans many files or layers, changes behavior other code may rely
     on, or touches anything listed under Rules (formats, APIs, cache keys, compatibility). In
     that case explain the problem and the options, and ask the user how they would like it
     fixed. Wait for the answer.
   For a small, local fix with one obvious correct form, go ahead without asking.
6. **Fix.** Make the smallest change that fixes the root cause (not just the symptom in the
   test). If the bug is a javadoc/contract mismatch and the user chose to fix the docs instead
   of the behavior, change the javadoc and adjust the test to the documented behavior. If the
   same flaw exists in sibling code named in the bug's details (e.g. "same pattern in X"),
   point it out and ask whether to fix those too.
7. **Verify.** Report every failure honestly; never weaken the test to make it pass.
   - The new test passes 3 out of 3 times.
   - The whole original test class passes, and the test classes of every production class you
     changed (find them by name and usage) pass.
   - For changes reachable from SQL (fdb-relational-core, query planning), run the related
     yamsql files with `./gradlew :yaml-tests:quickTest --tests 'YamlIntegrationTests.<entry>'`.
   - `checkstyleMain`, `checkstyleTest`, `spotbugsMain` and `spotbugsTest` pass for every
     changed module.
   If an existing test fails because it relied on the buggy behavior, show it to the user and
   ask whether to update it.
8. **Review.** Run the `code-reviewer` skill on the diff against `UPSTREAM/main`. Fix real
   problems it finds; list the rest for the user.
9. **Hand over and stop.** Leave the changes uncommitted on BRANCH and show:
   ```
   Auto QA fix: AQA-<n> - <summary>
   Branch: fix-AQA-<n> (from UPSTREAM/main <short>), uncommitted
   Root cause: <1-3 lines>
   Fix: <1-3 lines>; decisions taken with the user: <...> | none
   Regression test: <file>#<method>
   Files changed: <git diff --stat UPSTREAM/main>
   Verification: <tests run and results; checkstyle/spotbugs>
   Review findings not addressed: <...> | none
   Compatibility notes: <...> | none
   To go back: git switch <START_BRANCH>
   ```
   Then wait. Do nothing else unless the user asks.
10. **Only when the user asks:**
    - **commit**: one commit, message `Fix AQA-<n>: <summary>` followed by a short description
      of the root cause and the fix;
    - **push**: `git push -u FORK fix-AQA-<n>`;
    - **PR**: `gh pr create --draft --repo FoundationDB/fdb-record-layer --base main
      --head <fork owner>:fix-AQA-<n> --label "bug fix"`, with a clear title (it becomes the
      release note), and a description with the problem, the root cause, the fix, the
      regression test, any compatibility notes, and "Found by /auto-qa as AQA-<n> (see the
      draft PR 'Auto QA: tests that expose known bugs')". Do not mark it ready for review.
    After the PR merges, `/auto-qa check-fixed` marks AQA-<n> as `fixed upstream` with this PR
    and removes its test from CAUGHT; nothing else needs to be updated by hand.

## H. Help

Print exactly this, then stop:

```
/auto-qa-fix - fix one confirmed auto-qa bug, interactively

USAGE
  /auto-qa-fix AQA-<n>       Fix bug AQA-<n> (from auto-qa/bugs.md on apple/jjezra/auto-qa-search)
  /auto-qa-fix help          Show this help.

WHAT IT DOES
  1. Checks that AQA-<n> is open and that no open PR already fixes it.
  2. Creates branch fix-AQA-<n> from upstream/main (the working tree must be clean).
  3. Adds the bug's test to the real test file and confirms it fails.
  4. Finds the root cause and fixes it. Asks you first when there are several reasonable
     fixes or the fix is complicated (formats, APIs, compatibility, many files).
  5. Confirms the test passes, runs related tests, checkstyle and SpotBugs, and reviews
     the diff with the code-reviewer skill.
  6. Stops with the change uncommitted for your review.

  It commits, pushes fix-AQA-<n> to your fork (origin), or opens a draft PR
  (label "bug fix") only when you ask. Interactive only: never from run.sh or cron.
  After the PR merges, /auto-qa check-fixed marks the bug fixed automatically.
```
