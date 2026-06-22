# Setting up Claude Code with fdb-record-layer

## Java

Claude Code (and Gradle) require Java 21. Make sure `JAVA_HOME` points to JDK 21 in your
shell's non-interactive startup file. For zsh, add to `~/.zshenv`:

```
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
```

You can verify the setup is correct by running:

```
claude
Can you run `./gradlew :fdb-relational-core:compileJava` and show me the output?
```

If `JAVA_HOME` is not set correctly, Claude will try to fix it but will likely fail due to
its security settings.

## FDB

Some tests require a running FoundationDB instance. Before running those tests, check whether
`fdb-environment.yaml` exists in the repo root. If it does not, ask the user whether FDB is
installed and whether they need help with setup.

## Tooling overview

The `.claude/` directory contains Claude Code-specific enhancements on top of `AGENTS.md`:

- `skills/` — Reusable skills invocable via `/skill-name`
- `commands/` — Session-level commands (e.g., `/refine` for session retrospectives)

Other AI tools (Copilot, Gemini) use `AGENTS.md` at the repo root as their context file.

## Example prompts

### Review my branch
```
Code review the PR on the current branch
```

### Write a yaml test for a new SQL feature
```
Write a yaml test for the new feature in yaml-tests/src/test/resources/my-feature-tests.yamsql
```

### Explain why a query plan looks wrong
```
/relational-query-processor
Explain why this query is doing a full table scan: SELECT * FROM t WHERE id = 1
```

### Run tests for a specific class
```
/test-runner
Run the tests for EmbeddedRelationalStatement
```

### Resume work after a session break
```
/context-resumption
```

### Session retrospective (capture learnings, improve tooling)
```
/refine
```
