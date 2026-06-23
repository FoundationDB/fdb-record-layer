---
name: frl-test-coding-standard
description: Standards for writing tests in fdb-record-layer. Apply when writing or reviewing test code.
  Some example usages:
  "Write a test for the new index type"
  "Help me write a yaml test for this SQL query"
---

**Always apply the `frl-coding-standard` skill in addition to the standards below.**

## Choosing the right test type

| What you're testing | Preferred approach |
|---|---|
| SQL query behavior, query plans, result sets | yaml-tests (`.yamsql` file) |
| JDBC-level interactions | JUnit test in `fdb-relational-jdbc` or `fdb-relational-core` |
| Record layer API (below SQL) | JUnit test |
| Async / CompletableFuture edge cases | JUnit test |
| Index maintenance, key expression logic | JUnit test |

When in doubt, prefer yaml-tests for anything reachable through SQL.

## yaml-tests

### Creating a new yamsql test file

1. Create `yaml-tests/src/test/resources/<feature>-tests.yamsql`.
2. Register it in `yaml-tests/src/test/java/YamlIntegrationTests.java` with a `@YamlTest`
   annotated method:
   ```java
   @YamlTest(schemaTemplateFiles = {}, queryFiles = "my-feature-tests.yamsql")
   void myFeatureTests(YamlTest.Runner runner) throws Exception {
       runner.runYamsql();
   }
   ```
3. Decorate with `@MaintainYamlTestConfig(YamlTestConfigFilters.CORRECT_EXPECTATIONS)` to
   have the framework auto-populate expected query plans, metrics, and metadata on first run.
   Remove the annotation once the expected values are stable and reviewed.

### yamsql file structure

See `yaml-tests/src/test/resources/showcasing-tests.yamsql` for a full reference.
Key building blocks:

```yaml
options:
  supported_version: 1.0.0     # minimum FRL version required

schema_template:               # declarative schema — reused across test blocks
  create table t (id bigint, name string, primary key(id))

setup:                         # DML run once before the test block
  - query: insert into t values (1, 'Alice'), (2, 'Bob')

test_block:
  preset: single_repetition_ordered
  tests:
    - query: select * from t where id = 1
      result:
        - [1, Alice]
    - query: explain select * from t where id = 1
      explain: "COVERING(...)"   # auto-filled by CORRECT_EXPECTATIONS on first run
```

Common result matchers: `!ignore`, `!l` (Long literal), `!null`, `!not_null`,
`!sc <substring>` (string contains), `!pos <value>` (positional).

### Debugging a yaml test

Add `@DebugPlanner` to the test method to launch `PlannerRepl` and inspect the plan
interactively.

## JUnit tests

- Use **JUnit 5** exclusively. No `junit.framework` or `org.junit.Assert` (checkstyle-banned).
- Use **AssertJ** for assertions (`org.assertj.core.api.Assertions`).
- Test method naming: `<methodUnderTest><Condition><ExpectedResult>`.
- Extract shared setup into `@BeforeEach` or helper methods; DRY up after writing.

## What not to do

- Do not use `Thread.sleep()` to wait for async operations — use `asyncToSync()` or
  `CompletableFuture` composition.
- Do not call `join()` or `get()` directly in test code if `asyncToSync()` is available on
  the context — it gives better error messages and timeout behavior.
