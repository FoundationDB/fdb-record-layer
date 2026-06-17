---
name: context-resumption
description: Rebuild working context after a session restart. Reads branch state, recent commits, open PRs, and changed files to produce a "what was I doing" summary.
  Some example usages:
  "/context-resumption"
  "What was I working on?"
  "Resume work on PR #1234"
---

This is a read-only skill. It does not modify any files.

## Process

1. **Determine the work area**
   ```
   git status
   git branch --show-current
   ```

2. **Read recent branch commits**
   ```
   git log main...HEAD --oneline
   git log -10 --oneline   # if on main or branch unclear
   ```

3. **Check for an open PR**
   ```
   gh pr view --json title,url,state,body,reviews,comments 2>/dev/null
   ```

4. **Look for a saved context file** in `.claude/context/` or the repo root matching
   `context-*.md` or `*-notes.md`.

5. **Read key changed files**
   ```
   git diff main...HEAD --name-only
   ```
   Read the most recently modified files to understand the current state.

6. **Produce the context summary** (see format below).

## Output format

```
## Context Summary

**Branch**: <branch-name>
**PR**: <#number — title> | <open/draft/none>

### What's been done
- <chronological bullet from oldest to newest commit/change>
- ...

### Current state
<1-3 sentences: what is the code in right now? does it compile? are tests passing?>

### Open review feedback
<any unresolved PR comments or review requests, if a PR exists>

### Key files
- `path/to/file.java` — <why it matters>
- ...

### Likely next steps
- <inferred from commit messages, PR description, open comments>
```

Keep the summary tight — it should fit in one screen. If you have the PR or issue number,
link to it.

## Arguments: `$ARGUMENTS`

- PR number or URL → focus on that PR's context.
- Branch name → check out context for that branch.
- No argument → use current branch.
