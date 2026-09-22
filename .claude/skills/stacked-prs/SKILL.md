---
name: stacked-prs
description: Create and manage stacked pull requests on `FoundationDB/fdb-record-layer` using the `gh stack` CLI extension. Applies to Record Layer team members with write access to the upstream repository. Some example usages:
  "split this change into a stack of PRs"
  "create a stacked PR for this"
  "add a branch on top of my stack"
  "rebase my stack onto main"
  "why is my PR not showing up in the stack view"
---

## Who this applies to

Team members with write access to `FoundationDB/fdb-record-layer` (check your own access with `gh api repos/FoundationDB/fdb-record-layer --jq .permissions.push`). External contributors without upstream write access must use a fork and cannot use stacked PRs.

## Branch location and naming convention

In order for stacked pull requests to work, branches must live directly on `FoundationDB/fdb-record-layer`, never on a personal fork.

As a team-wide convention, branch names must be namespaced as follows:

```
apple/«github-username»/**
```

For example, `apple/arnaud-lacurie/topic1/step2`. This naming scheme avoids collisions with other contributors’ branches on the shared upstream repository and keeps personal work clearly attributed.

## Why upstream branches instead of a fork

A pull request’s base branch must exist in the same repository as the PR itself. A PR opened from a fork can only ever target a branch that exists upstream (typically `main`); it cannot chain onto another of your own branches, because that branch lives only on the fork. Working directly on upstream branches is what makes GitHub’s native support for stacked-PR chaining and coordinated stack merges possible.

## Setup (once per clone)

```
gh extension install github/gh-stack
gh repo set-default FoundationDB/fdb-record-layer
```

The default-`repo` setting matters even when a remote named `upstream` already exists. Without it, `gh stack` and `gh pr` commands may resolve PR numbers against a fork remote (for example, `origin`) instead of upstream. If you would rather not change the default, pass `--repo FoundationDB/fdb-record-layer` on each `gh pr` and `gh api` invocation instead.

## Common workflows

- To start a new stack, use `gh stack init apple/«github-username»/«topic»/step1 apple/«github-username»/«topic»/step2 …`, which adopts existing branches or creates new ones.
- To add a layer on top of the current stack, use `gh stack add apple/«github-username»/«topic»/stepN`.
- To push the whole stack and open or update its PRs, use `gh stack submit`. (Also follow the “Project conventions when submitting” below.)
- To link branches or PRs that already exist into a stack, without adopting local tracking, use `gh stack link «branch-or-pr» «branch-or-pr» …`, which accepts branch names, PR numbers, or PR URLs, in bottom-to-top order.
- To keep local branches in sync with remote state, use `gh stack sync`.
- To rebase the whole stack (for example, onto an updated `main`), cascading through every layer, use `gh stack rebase`.
- To restructure an existing stack, use `gh stack modify`, which opens an interactive TUI that can drop, fold, insert, reorder, and rename branches. Changes are staged and applied together; run `gh stack submit` afterward to update the PRs and the stack on GitHub.
- To merge the stack, use `gh stack merge`. (See “Merging a stack” below.)
- To navigate a stack locally, use `gh stack top` / `bottom` / `up` / `down` / `switch` / `checkout`.
- To inspect stack state, use `gh stack view`, which is interactive; `--short` gives one line per branch and `--json` machine-readable output. Prefer `--json` when a script or agent needs to reason about the stack.
- To tear a stack down, use `gh stack unstack`, which removes local tracking and unstacks it on GitHub while leaving the branches and their PRs intact. `--local` drops only the local tracking, leaving GitHub alone.

## Project conventions when submitting

- New PRs must be _drafts_ (see `AGENTS.md`). The interactive editor for `gh stack submit` defaults new PRs to _ready for review_, so either switch each one with the “CREATE AS” toggle, or run `gh stack submit --auto`, which skips the editor and creates drafts. Never pass `--open` unless the human explicitly asks for PRs to be marked ready.
- PR titles feed the release notes, so the auto-generated titles from `--auto` usually need fixing up afterwards (`gh pr edit «n» --title '…'`).
- `gh stack submit` cannot apply labels, so a freshly submitted stack will land without labels. Every PR still needs one of the required labels. To add a `bug fix` label, for example, use `gh pr edit «n» --add-label 'bug fix'`.
- `gh stack submit`, `gh stack push`, and `gh stack merge` all change state on GitHub. Treat them as human-initiated. Don’t run them on your own initiative, and never merge without explicit consent.

## Merging a stack

The whole stack does _not_ need to be merged at once.

- You can merge any contiguous group starting from the lowest unmerged PR — including just the bottom PR alone. You cannot merge a mid-stack PR in isolation; everything below it merges with it in the same operation.
- When the bottom PR merges, GitHub automatically retargets the next unmerged PR’s base to point directly at the trunk branch. No manual rebase/retarget step is needed for that.
- Once the human has asked you to merge, prefer `gh stack merge` over merging PRs one-by-one through the UI. With no argument it merges the current stack (with an interactive picker for how far up to go); pass a stack or PR number to control how far. It performs the chosen range as a single all-or-nothing operation.
- If the stack’s history isn’t linear when you try to merge, a “Rebase stack” button appears in the PR merge box on GitHub, or run `gh stack rebase` from the CLI. A stack must have a linear history between its branches before it can merge.
- In a merge queue, ejecting a PR also ejects everything above it in the stack.

## Modifying a lower branch after the stack exists

Make the change in the branch it actually belongs to — don’t work around it by patching the change into a higher layer. Then propagate it upward:

1. `gh stack down` (or `gh stack checkout «branch-name»`) to the branch that needs the change.
2. Commit the change normally (using `git add` and `git commit`).
3. `gh stack rebase --upstack` to cascade the change through every branch above it (plain `gh stack rebase` also pulls in trunk updates). Each branch is rebased onto the branch below it, in order — this is required because a stack needs linear history to merge.
4. `gh stack push` to update the remote branches (uses `--force-with-lease`, safe for already-open PRs). Pushes are per-branch, not atomic. If one branch is rejected, the others may still have updated; fix that branch and re-run, the rest stay as they are.

On a conflict partway up the stack, `gh stack rebase` stops and lists the conflicted files — resolve them, `git add`, then `gh stack rebase --continue`. `gh stack rebase --abort` restores every branch to its pre-rebase state.

## Gotchas

- `gh stack view` operates on the stack of the _locally checked-out_ branch. It needs a local branch that’s part of a tracked stack, and does not take a stack number as an argument. `gh stack checkout «stack-number»` does, and switches you into that stack first.
- `gh stack link` does not require local tracking state. Use it to retroactively register a stack from PRs that were created another way (for example, manually chained base branches).
- Branch from `upstream/main`, not a fork’s `main`. A fork’s `main` can silently fall behind upstream; branches based on the stale copy will show `mergeable: CONFLICTING` even though the git ancestry within the stack itself looks fine.
