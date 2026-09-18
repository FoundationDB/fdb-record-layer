# Refine

Your session retrospective specialist. Captures what worked and what didn't, then generates
actionable improvements to skills, agents, commands, and AGENTS.md.

## When to use

At the end of a session (or mid-session after significant work):
- Capture what worked well and what didn't.
- Generate improvement suggestions for the tooling you used.
- Identify gaps — missing skills, missing context in AGENTS.md, broken commands.
- Turn corrections you gave the assistant into durable improvements.

## Usage

```
/refine [focus-area]
```

### Examples

```
/refine
/refine "the yaml-test workflow was clunky"
/refine skills
/refine agents
```

## What this command does

1. **Analyzes the session** — identifies which skills, agents, and docs were used; finds
   friction points (corrections, retries, confusion, missing context) and successes.
2. **Reads the relevant source files** — loads the definitions of what was used.
3. **Generates specific suggestions** in one of four categories:
   - **Bug fixes** — something is broken or produces wrong output.
   - **Enhancements** — something works but could work better.
   - **New components** — a skill, agent, or AGENTS.md entry that should exist but doesn't.
   - **Documentation** — knowledge discovered during the session that should be captured.
4. **Asks for approval** — presents suggestions and asks which to apply.
5. **Applies approved changes on a new branch and opens a PR**.

## Instructions

### Step 1 — Analyze the session

Review the full conversation and identify:

**a. Components used**: skills invoked, agents launched, AGENTS.md sections referenced.

**b. Friction points** — look for:
- User corrections: "No, I meant...", "That's not right", "Don't do that"
- Retries: the assistant had to redo something after an error
- Missing context: the assistant lacked information that could be in a skill or AGENTS.md
- Failed tool calls or wrong Gradle tasks
- Manual steps the user had to take that could be automated

**c. Successes** — look for:
- Smooth workflows completed without correction
- Patterns the user praised or expressed satisfaction with

**d. Focus area**: if provided, prioritize it while still scanning the full session.

### Step 2 — Read relevant source files

- Skills: `.claude/skills/*/SKILL.md`
- Agents: `.claude/agents/*.md`
- Commands: `.claude/commands/*.md`
- Universal context: `AGENTS.md`
- Architecture docs: `docs/sphinx/source/architecture/` — consult these for design context that
  can reveal how to correctly improve a skill or AGENTS.md entry

### Step 3 — Generate suggestions

Format each suggestion as:

```
[category] component-name — title
File: <path>
Issue: <what happened in this session>
Suggestion: <specific improvement>
```

Categories: Bug Fix / Enhancement / New Component / Documentation

For Documentation suggestions, use the `docs-writer` skill to draft the actual doc changes.

### Step 4 — Present the retrospective report

```
## Refine — Session Retrospective

### Session Analysis
Skills used: ...
Agents used: ...
Friction points: N
Suggestions: N

---

### Bug Fixes
...

### Enhancements
...

### New Components
...

### Documentation Updates
...
```

### Step 5 — Get approval

Ask: "Which improvements should I apply?"
Options:
1. Apply all (recommended)
2. Let me pick — present each one individually
3. Save the report only — write to `.claude/retrospectives/YYYY-MM-DD-HHMMSS.md`
4. None for now

### Step 6 — Apply on a new branch and open a PR

If applying changes:

```bash
git fetch origin main
BRANCH="refine/retrospective-$(date +%Y%m%d-%H%M%S)"
git checkout -b "$BRANCH" origin/main
```

Apply edits, then:

```bash
git add -u
git commit -m "..."
git push -u origin "$BRANCH"
gh pr create --draft --title "Refine: session retrospective improvements" --body "..."
git checkout -
```

PR should only reference files tracked in the repo — not local gitignored files.

### Step 7 — Summary

```
## Refine — Changes Applied

Applied (in PR <url>):
  - [component] change description

Deferred:
  - [component] reason

Your tooling just got better.
```

## Guidelines

- Be specific: every suggestion must reference a concrete moment from the session.
- Don't over-suggest: quality over quantity.
- Preserve existing functionality: enhancements add to behavior, not replace it.
- New components should follow the established file structure and naming.
- PR descriptions must only reference files tracked in the repository.
