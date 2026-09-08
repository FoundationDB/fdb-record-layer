---
name: using-gradle
description: Use this skill when you need to build the project, compile code, or run tests.
  Some example usages:
  "Check this code compiles"
  "Run the tests for EmbeddedRelationalStatement"
  "Run the yaml-tests suite"
---

# Building

```
./gradlew build                        # full build
./gradlew package                      # build + generate protobuf sources (required before first compile)
./gradlew clean                        # clean build artifacts
./gradlew -PspotbugsEnableHtmlReport check   # all checks including SpotBugs and Checkstyle
```

Compile a single module without running tests:
```
./gradlew :fdb-relational-core:compileJava
./gradlew :fdb-record-layer-core:compileJava
```

# Style / static analysis checks

**Run this before pushing or opening a PR.** CI's `style` job (`.github/workflows/pull_request.yml`)
runs:
```
./gradlew build -x test -x destructiveTest -x scalarFallbackTest -PreleaseBuild=false -PpublishBuild=false -PspotbugsEnableHtmlReport
```
which includes Checkstyle, PMD, and SpotBugs across every module. The `-PreleaseBuild=false
-PpublishBuild=false` pair matters — without it you're not running the same task graph as CI, and
some failures (e.g. dependency-version-resolution differences gated by `publishBuild`) only show
up with them set. That's slow for local iteration — scope it to the modules you actually touched,
keeping the same property flags:

```
./gradlew :fdb-relational-core:check :fdb-record-layer-core:check -x test -x destructiveTest -x scalarFallbackTest -PreleaseBuild=false -PpublishBuild=false -PspotbugsEnableHtmlReport
```

Reports on failure land at `<module>/.out/reports/checkstyle/*.html`, `<module>/.out/reports/pmd/*.html`,
and (with `-PspotbugsEnableHtmlReport`) `<module>/.out/reports/spotbugs/*.html`. The failure
output in the console also prints the exact file, line, and rule.

Common violations you'll hit when merging/rebasing branches by hand:
- Checkstyle `RedundantImport` — importing a class that's in the same package as the file, or
  the same class imported twice (easy to introduce when resolving import-block merge conflicts).
- PMD `UnnecessaryFullyQualifiedName` — using a fully-qualified name (e.g. `java.util.Map`)
  when the class is already imported under its simple name.

Gotchas when doing an incremental, module-by-module rollout of a new annotation library (e.g.
jspecify) across a dependency graph:
- SpotBugs analyzes a module's compiled classes together with an aux classpath drawn from its
  own `compileOnly`/`implementation`/`api` dependencies. If module B depends on module A and A's
  compiled bytecode now carries annotations from a library that's only a `compileOnly` dependency
  of A (not `api`), that dependency does **not** propagate to B — SpotBugs then fails B's analysis
  with "The following classes needed for analysis were missing: ..." (SpotBugs exit code 3) even
  though B's own source never references the new annotation. Fix by making the new annotation
  library a `compileOnly` dependency of every module (not just migrated ones), e.g. via a root
  `subprojects {}` block, so the class is always resolvable regardless of migration order.
- SpotBugs' `NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION` check does not treat two different
  nullability-annotation libraries (e.g. JSR-305's `javax.annotation.Nullable` and jspecify's
  `org.jspecify.annotations.Nullable`) as equivalent when comparing an overriding method's
  parameter annotation against its superclass — it flags a mismatch even when both sides
  genuinely agree the parameter is nullable. This surfaces as a wave of new findings as soon as
  a widely-subclassed base class (e.g. an exception hierarchy root) migrates to the new library,
  in every not-yet-migrated subclass module. See `NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION` in
  `gradle/codequality/spotbugs_exclude.xml` for how this project suppresses it.

# Running tests

## Standard test tasks

| Task | What it runs |
|---|---|
| `test` | Unit + integration tests, excluding `WipesFDB` and `AutomatedTest` tags |
| `quickTest` | Like `test` but faster (excludes `WipesFDB`) |
| `destructiveTest` | Tests tagged `WipesFDB` — wipes FDB data, single fork |
| `performanceTest` | Tests tagged `Performance`, assertions disabled |
| `rpcTest` | Tests via embedded RPC server rather than embedded connection |
| `mixedModeTest` | Tests tagged `MixedMode` (requires external server JARs) |
| `singleVersionTest` | Tests against a single external server version |

Run a specific test class or method in a module:
```
./gradlew :fdb-relational-core:test --tests 'com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalStatementTest'
./gradlew :fdb-relational-core:test --tests 'com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalStatementTest.testGet'
```

## yaml-tests

```
./gradlew :yaml-tests:test             # run all yaml integration tests
./gradlew :yaml-tests:test --tests 'YamlIntegrationTests.myTestName'
```

Custom seed and iteration count (for reproducing flaky yaml tests):
```
./gradlew :yaml-tests:test -Ptests.yaml.seed=12345 -Ptests.yaml.iterations=10
```

## FDB prerequisite

Tests that hit FDB require a running FoundationDB cluster and `fdb-environment.yaml` in the
repo root.

Example `fdb-environment.yaml`:
```yaml
libraryPath: /usr/local/lib/libfdb_c.dylib
clusterFiles:
  - /usr/local/etc/foundationdb/fdb.cluster
```

## Publishing locally (to use in another project)

```
./gradlew publishToMavenLocal -PpublishBuild=true
```

Jars land in `~/.m2/repository/org/foundationdb/`.

## JMH benchmarks (fdb-relational-core)

```
./gradlew :fdb-relational-core:jmh
```

Benchmark sources live in `fdb-relational-core/src/jmh/java/`. The active benchmarks are
controlled by the `includes` list in the `jmh {}` block of `fdb-relational-core.gradle`.
Useful JMH options (pass as Gradle properties or edit the block):

```
./gradlew :fdb-relational-core:jmh -Pjmh.include=DirectAccessVsQueryBenchmark
```
