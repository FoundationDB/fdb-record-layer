---
name: stacked-prs
description: Create and manage stacked pull requests on FoundationDB/fdb-record-layer using
  the `gh stack` CLI extension. Applies to Record Layer team members with push access to the
  upstream repo. Example usages:
  "split this change into a stack of PRs"
  "create a stacked PR for this"
  "add a branch on top of my stack"
  "rebase my stack onto main"
  "why is my PR not showing up in the stack view"
---

## Who this applies to

Team members with push access to `FoundationDB/fdb-record-layer` directly (check with
`gh api repos/FoundationDB/fdb-record-layer/collaborators/<username>/permission --jq .permission`).
External contributors without upstream write access must use a fork and cannot use stacked PRs
(see "Why upstream branches instead of a fork" below).

## Branch naming convention

Branches must live directly on `FoundationDB/fdb-record-layer` (never on a personal fork),
namespaced as:

```
apple/<github-username>/**
```

e.g. `apple/arnaud-lacurie/cross-schema/pr1`. This avoids collisions with other contributors'
branches on the shared upstream repo and keeps personal work clearly attributed.

## Why upstream branches instead of a fork

A pull request's base branch must exist in the same repository as the PR itself. A PR opened
from a fork can only ever target a branch that exists upstream (typically `main`) — it cannot
chain onto another of your own branches, because that branch lives only on the fork. Working
directly on upstream branches is what makes GitHub's native stacked-PR chaining, cascading
rebases, and coordinated stack merges possible.

## Setup (once per clone)

```
gh extension install github/gh-stack
gh repo set-default FoundationDB/fdb-record-layer
```

The default-repo setting matters even when a remote named `upstream` already exists — without
it, `gh stack`/`gh pr` commands may resolve PR numbers against a fork remote (e.g. `origin`)
instead of upstream.

## Common workflows

- Start a new stack (adopts existing branches or creates new ones):
  `gh stack init apple/<user>/<topic>/step1 apple/<user>/<topic>/step2 ...`
- Add a layer on top of the current stack: `gh stack add apple/<user>/<topic>/stepN`
- Push and open/update PRs for the whole stack: `gh stack submit`
- Link branches or PRs that already exist into a stack, without adopting local tracking:
  `gh stack link <branch-or-pr> <branch-or-pr> ...` (accepts branch names, PR numbers, or PR
  URLs, in bottom-to-top order)
- Keep local branches in sync with remote state: `gh stack sync`
- Rebase the whole stack (e.g. onto an updated `main`), cascading through every layer:
  `gh stack rebase`
- Merge the stack: `gh stack merge` (supports bottom-up, individually, or in contiguous groups)
- Navigate a stack locally: `gh stack top` / `bottom` / `up` / `down` / `switch` / `checkout`

## Merging a stack

The whole stack does **not** need to merge at once:

- You can merge any contiguous group starting from the lowest unmerged PR — including just the
  bottom PR alone. You cannot merge a mid-stack PR in isolation; everything below it merges
  with it in the same operation.
- When the bottom PR(s) merge, GitHub automatically retargets the next unmerged PR's base to
  point directly at the trunk branch. No manual rebase/retarget step is needed for that.
- Prefer `gh stack merge` over merging PRs one-by-one through the UI: with no argument it merges
  the current stack (interactive picker for how far up to go); pass a stack or PR number to
  control how far. It performs the chosen range as a single all-or-nothing operation.
- If the stack's history isn't linear when you try to merge, a "Rebase stack" button appears in
  the PR merge box on GitHub, or run `gh stack rebase` from the CLI — a stack must have a linear
  history between its branches before it can merge.
- In a merge queue, ejecting a PR also ejects everything above it in the stack.

## Modifying a lower branch after the stack exists

Make the change in the branch it actually belongs to — don't work around it by patching the
change into a higher layer. Then propagate it upward:

1. `gh stack down` (or `gh stack checkout <branch-name>`) to the branch that needs the change.
2. Commit the change normally (`git add` / `git commit`).
3. `gh stack rebase --upstack` to cascade the change through every branch above it (plain
   `gh stack rebase` also pulls in trunk updates). Each branch is rebased onto the branch below
   it, in order — this is required because a stack needs linear history to merge.
4. `gh stack push` to update the remote branches (uses `--force-with-lease`, safe for
   already-open PRs).

On a conflict partway up the stack, `gh stack rebase` stops and lists the conflicted files —
resolve them, `git add`, then `gh stack rebase --continue`. `gh stack rebase --abort` restores
every branch to its pre-rebase state.

## Gotchas

- `gh stack view` operates on the *locally checked-out* branch's stack — it needs a local
  branch that's part of a tracked stack, and does not take a stack number as an argument.
  `gh stack checkout <stack-number>` does, and switches you into that stack first.
- `gh stack link` does not require local tracking state — use it to retroactively register a
  stack from PRs that were created another way (e.g. manually chained base branches).
- Branch from `upstream/main`, not a fork's `main`. A fork's `main` can silently fall behind
  upstream; branches based on the stale copy will show `mergeable: CONFLICTING` even though
  the git ancestry within the stack itself looks fine.
