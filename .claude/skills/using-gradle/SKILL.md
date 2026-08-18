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
runs `./gradlew build -x test -x destructiveTest -x scalarFallbackTest -PspotbugsEnableHtmlReport`,
which includes Checkstyle, PMD, and SpotBugs across every module. That's slow for local
iteration — scope it to the modules you actually touched:

```
./gradlew :fdb-relational-core:check :fdb-record-layer-core:check -x test -x destructiveTest -x scalarFallbackTest -PspotbugsEnableHtmlReport
```

Reports on failure land at `<module>/.out/reports/checkstyle/*.html`, `<module>/.out/reports/pmd/*.html`,
and (with `-PspotbugsEnableHtmlReport`) `<module>/.out/reports/spotbugs/*.html`. The failure
output in the console also prints the exact file, line, and rule.

Common violations you'll hit when merging/rebasing branches by hand:
- Checkstyle `RedundantImport` — importing a class that's in the same package as the file, or
  the same class imported twice (easy to introduce when resolving import-block merge conflicts).
- PMD `UnnecessaryFullyQualifiedName` — using a fully-qualified name (e.g. `java.util.Map`)
  when the class is already imported under its simple name.

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
