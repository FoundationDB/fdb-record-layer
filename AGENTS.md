<!-- NOTE: CLAUDE.md, GEMINI.md, and .github/copilot-instructions.md are symlinks to this file. Only edit AGENTS.md. -->

## Project Overview

FoundationDB Record Layer is a layered database library built on top of FoundationDB. The
repository contains two main layers:

- **Record Layer** (`fdb-record-layer-core`, `fdb-extensions`, etc.): A structured key-value
  store with rich indexing, querying, and schema evolution capabilities. The core API is
  asynchronous, built around `CompletableFuture`.
- **Relational Layer** (`fdb-relational-*`): A SQL database layer on top of the record layer,
  providing JDBC connectivity, a Cascades-based query planner, and schema templates for
  multi-tenant architectures. Also exposes a direct-access API
  (`RelationalDirectAccessStatement`) that bypasses the SQL planner for lower-overhead reads;
  this API is expected to be deprecated in the future as the SQL layer matures.

## General Guidelines

All Java, Gradle, and property files MUST end with a newline character.

JDK 21 is required to build. The code targets Java 17 language compatibility.

## PR Workflow

- The default PR target branch is `main`.
- PRs can reference the issue they address (e.g., `Fixes #492`).
- PR titles are used to generate release notes — make them clear and descriptive.
- PRs must carry one of these labels: `breaking change`, `enhancement`, `bug fix`,
  `performance`, `dependencies`, `build improvement`, `testing improvement`, `documentation`.
- Always create PRs as **drafts** (`gh pr create --draft`). Let the human decide when it's
  ready for review.
- Never merge branches or PRs without explicit user consent.

## Test Strategy

### SQL-layer tests (yaml-tests)

The primary way to test SQL-layer behavior is via `.yamsql` files in
`yaml-tests/src/test/resources/`. To add a test:

1. Create or extend a `.yamsql` file.
2. Register it in `yaml-tests/src/test/java/YamlIntegrationTests.java`.
3. Decorate the entry with `@MaintainYamlTestConfig(YamlTestConfigFilters.CORRECT_EXPECTATIONS)`
   to have the framework auto-correct expected query plans, metadata, and metrics on first run.

See `yaml-tests/src/test/resources/showcasing-tests.yamsql` for a comprehensive reference of
the yamsql format (schema templates, setup blocks, result matchers, parameterization, etc.).

### JUnit tests

Use JUnit 5 for things that operate below the SQL layer, or that cannot be easily expressed in
the yamsql framework (e.g., specific JDBC interactions, record layer API behavior, async edge
cases). Some JUnit tests require a running FDB instance — configure `fdb-environment.yaml` in
the repo root before running them.

## Agent Routing Rules

Use the appropriate specialized skill for each task type.

### Building
→ Apply the `using-gradle` skill.

### Running and diagnosing tests
→ Apply the `test-runner` skill.

### Writing or reviewing Java code
→ Apply the `frl-coding-standard` skill. For test code, also apply `frl-test-coding-standard`.

### Code review
→ Use the `code-reviewer` skill.

### Writing or updating documentation
→ Use the `docs-writer` skill.

### Working in the SQL processing layer (`fdb-relational-core`)
→ Use the `relational-query-processor` skill.

## Tooling for AI Assistants

Each AI assistant reads its own context file, all of which point here:

- **Claude Code**: `CLAUDE.md` → `AGENTS.md`. Additional tooling (agents, skills, commands)
  is available under `.claude/`. See `.claude/README.md`.
- **GitHub Copilot**: `.github/copilot-instructions.md` → `AGENTS.md`.
- **Gemini**: `GEMINI.md` → `AGENTS.md`.

The `.claude/` directory is Claude Code-specific — no equivalent directory exists yet for
other tools. Contributions adding similar tooling for Copilot, Gemini, or Cursor are welcome.
