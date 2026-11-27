# auto-qa: quick guide

`/auto-qa` hunts for bugs by writing new tests for existing test files. Each failing test is
checked by an independent verifier, and confirmed bugs get an id (`AQA-<n>`).
`/auto-qa-fix` then fixes one bug, interactively. Details are in [auto-qa.doc.md](auto-qa.doc.md).

## Commands

| Command | What it does |
|---|---|
| `/auto-qa` | One bug-hunting iteration over 5 test files. |
| `/auto-qa <focus>` | Hunt only in matching test files. The focus can be `yaml`, a module (`fdb-relational-core`), a package or path, or a keyword (`OnlineIndexer`). |
| `/auto-qa ... batch=<n>` | Process `<n>` test files instead of 5. |
| `/auto-qa ... no-push` | Trial run: commit locally only, with no push and no PR update. |
| `/auto-qa check-fixed` | No hunting: re-check the known bugs after `main` moves (fixed or regressed). Cheap. |
| `/auto-qa reject AQA-<n> [reason]` | Mark a bug "won't fix". It asks for the reason if you don't give one. |
| `/auto-qa unreject AQA-<n>` | Undo a rejection. |
| `/auto-qa-fix AQA-<n>` | Fix one bug on branch `fix-AQA-<n>` and stop for your review. It commits, pushes to your fork, or opens a draft PR only when you ask. |
| `/auto-qa help`, `/auto-qa-fix help` | Show the built-in help. |

## Where to look

- **Open bugs:** the draft PR "Auto QA: tests that expose known bugs". It lists the open, fixed
  and rejected bugs, and its failing CI runs are the bug tests.
- **All state:** branch `apple/jjezra/auto-qa-search`, with `auto-qa/bugs.md`,
  `auto-qa/progress.md` and `auto-qa/notes/`.
- **Rejections:** `auto-qa/rejected.md` on branch `apple/jjezra/auto-qa-caught`.

## Typical use

1. Run `/auto-qa` (or `/auto-qa <focus>`) in a dedicated checkout, e.g. a git worktree at
   `~/.auto-qa/fdb-record-layer`, because it switches branches.
2. Read the report, and the new bugs in the draft PR.
3. For each new bug, either fix it with `/auto-qa-fix AQA-<n>` or reject it with
   `/auto-qa reject AQA-<n> <reason>`.
4. After a fix merges, the next `/auto-qa` or `/auto-qa check-fixed` marks the bug fixed.

## Running it on a schedule

Call `auto-qa/run.sh [args]` from cron or launchd, for example a nightly `check-fixed` plus a
full hunt every few days. It creates its own checkout on first use (a worktree when it can) and
logs to `~/.auto-qa/logs`. It needs JDK 21, a running FDB with `fdb-environment.yaml`, and `git`
and `gh` logged in to github.com. `/auto-qa-fix` is interactive only and never runs from
`run.sh`.
