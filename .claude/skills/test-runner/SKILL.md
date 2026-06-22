---
name: test-runner
description: Run tests in the fdb-record-layer codebase, interpret results, and diagnose failures.
  Some example usages:
  "Run EmbeddedRelationalStatementTest"
  "Run the yaml integration tests"
  "Why is this test failing?"
  "Run the tests for the class I just modified"
---

Always apply the `using-gradle` skill for Gradle task syntax and build commands.

## How to run tests

- For yaml-tests: `./gradlew :yaml-tests:test` (or `--tests 'YamlIntegrationTests.<name>'`).
- For embedded-only yaml-tests (faster): `./gradlew :yaml-tests:quickTest`.
- For mixed-mode yaml-tests locally: `./gradlew :yaml-tests:mixedModeTest`.
- For JUnit tests in a module: `./gradlew :<module>:test --tests '<fully.qualified.ClassName>'`.

## Diagnosing failures

1. Read the full test output — look for the stack trace and the assertion message.
2. Check if the failure is in a yaml test:
   - `plan mismatch` → the actual query plan changed; edit `YamlIntegrationTests.java` to add
     `@MaintainYamlTestConfig(YamlTestConfigFilters.CORRECT_EXPECTATIONS)` to the test entry,
     then re-run — the framework will auto-correct the expected plan.
   - `result mismatch` → data or query logic changed; inspect the yamsql file.
3. Check if the failure is an async issue:
   - `BlockingInAsyncContextException` → something called `join()`/`get()` inside a future.
   - Timeout → possible deadlock; check for blocking calls in completion lambdas.
4. Check if FDB connectivity is the issue:
   - `FDBException` or connection refused → FDB is not running or cluster file is wrong.
