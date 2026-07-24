# Building the Record Layer

This page explains how to build the FoundationDB Record Layer and set up a development environment for working on it. It covers building from source, configuring your IDE for development, running the tests against a local cluster, publishing a snapshot build, and creating a patch branch.

If you intend to contribute to the FoundationDB Record Layer, please also consult our [contribution guidelines](https://github.com/FoundationDB/fdb-record-layer/blob/main/CONTRIBUTING.md).

If anything in this guide does not work smoothly for you, feel free to [create a new issue](https://github.com/FoundationDB/fdb-record-layer/issues/new) or open a thread in [the FoundationDB Forums](https://forums.foundationdb.org/c/using-layers).

## Building the project

To build the Record Layer you only need an installed Java Development Kit as a prerequisite. No separate Gradle installation is needed, as the bundled Gradle wrapper supplies the correct version.

1. **Install a JDK 21 distribution** if you don’t already have one. Our CI/CD builds use [Eclipse Temurin](https://adoptium.net/), but any distribution should work.

1. Optionally, **create your own fork** of the repository. This is only needed if you intend to contribute. All changes are made by creating pull requests from your fork into the main repository; see our [contribution guidelines](https://github.com/FoundationDB/fdb-record-layer/blob/main/CONTRIBUTING.md).

1. **Clone** the [FoundationDB/fdb-record-layer](https://github.com/FoundationDB/fdb-record-layer) repository (or your own fork of it) to your development machine.

1. Make sure that Gradle detects the intended JDK via `JAVA_HOME`. To this end, run the following command and check the “Daemon JVM” line, which reports the JDK that Gradle uses to compile the project and run the tests.
   ```sh
   ./gradlew --version
   ```

1. **Kick off a build**. The following command compiles every module and runs the static-analysis checks (SpotBugs and Checkstyle). The `-x test` flag excludes the test tasks, so this does not require a running FoundationDB cluster. Expect the command to take several minutes on a first run.
   ```sh
   ./gradlew build -x test
   ```

Our automated builds are run via **[GitHub Actions](https://github.com/FoundationDB/fdb-record-layer/actions)**. You can study the workflow definitions in [`.github/workflows`](https://github.com/FoundationDB/fdb-record-layer/tree/main/.github/workflows) to learn how they invoke Gradle precisely.

## Configuring IntelliJ

[IntelliJ IDEA](https://www.jetbrains.com/idea/) is the recommended environment for developing the FoundationDB Record Layer, although it is not strictly required to build or test it. Note that we do _not_ use the [Gradle IDEA plugin](https://docs.gradle.org/current/userguide/idea_plugin.html)—whose IDE-file generation is deprecated in modern Gradle. Instead, we rely on IntelliJ’s built-in Gradle support to generate and compile the project and to run the tests. When you open the project, IntelliJ performs a Gradle *sync* to derive a model of the project.

1. Generate the protobuf sources by running the **generateProto task** from the root directory of your local clone:

   ```sh
   ./gradlew generateProto
   ```

   This runs the _generateProto_ task in every module, which produces the protobuf-derived Java sources so that they are present for IntelliJ to resolve.

1. **Install IntelliJ IDEA** and launch it. Any reasonably recent version should work, though the exact menu paths below may differ slightly between releases.

1. In the “Welcome to IntelliJ IDEA” window, click Open and **open the project’s root directory**. If you do not see the welcome window, File › Open… can do the same.

1. Click the **Reload All Gradle Projects** button on the toolbar of the Gradle tool window. You can open this tool window from View › Tool Windows › Gradle.

1. **Compile everything** via Build › Build Project.

We recommend manually setting the following **IntelliJ preferences**. They are not strictly required, but useful.

* Under Editor › General › Auto Import, enable “Optimize imports on the fly” for the current project.

* Under Advanced Settings › Gradle, enable “Download sources” so that dependency sources are available for navigation and debugging.

If you make changes to other IntelliJ settings, they may save over the .xml files that are checked into Git under the `.idea/` directory. These changes can be shared by committing the updated files. However, other settings—such as the recommendations above—are saved in the user-specific `workspace.xml` file or other .xml files that we intentionally added to `.gitignore`. These user-specific settings must be configured manually by each developer for consistency.

## Running the tests

If you are interested in running the tests, you need to install and run a local FoundationDB cluster.

1. **Install FoundationDB** on your development machine. To find the latest FoundationDB binaries, browse the [releases of the _foundationdb_ GitHub repository](https://github.com/apple/foundationdb/releases).

1. **Make sure FoundationDB is running.** Installing an official package starts the `fdbserver` process automatically. You can confirm that the cluster is available by running:
   ```sh
   fdbcli --exec 'status minimal'
   ```
   This should print “The database is available”. For platform-specific installation and startup instructions, see FoundationDB's getting-started guides for [macOS](https://apple.github.io/foundationdb/getting-started-mac.html) and [Linux](https://apple.github.io/foundationdb/getting-started-linux.html).

1. **Run a test** to verify that you have a working configuration. In IntelliJ, pick a test that depends on FoundationDB and hit the green "play" arrow in the gutter next to it—`FDBRecordStoreCrudTest.writeRead` is a good, fast one. The first run may take a while (perhaps a minute) while the project builds and the test harness warms up; subsequent runs are faster. You can also run tests from the command line, as described below.

1. The **full suite of checks and tests** is run by the _check_ and _test_ Gradle tasks. In addition to the tests, this includes the static analysis checks (SpotBugs and Checkstyle). The `-PspotbugsEnableHtmlReport` flag additionally emits an HTML SpotBugs report.
   ```sh
   ./gradlew -PspotbugsEnableHtmlReport check test
   ```

By default, the tests run against the FoundationDB cluster named by the standard cluster file, which the official installer writes to the platform default location (`/etc/foundationdb/fdb.cluster` on Linux, `/usr/local/etc/foundationdb/fdb.cluster` on macOS), or wherever `FDB_CLUSTER_FILE` points if that variable is set. To override this behavior, you can create an **fdb-environment.yaml** file in the root of your working directory. It lets you point the tests at a non-default cluster, or even run them against _multiple_ clusters. The file specifies the directory containing the FoundationDB C API library (`libfdb_c.dylib` on macOS, `libfdb_c.so` on Linux) and a list of cluster files. If you provide more than one, most tests will choose randomly among them. Here is an example:
``` yaml
libraryPath: /opt/fdb/lib
clusterFiles:
  - /opt/fdb/clusters/fdb-one.cluster
  - /opt/fdb/clusters/fdb-two.cluster
```

The build defines several **Gradle test tasks** for different purposes. You can run any of them by passing the task name to the wrapper—for example:
```sh
./gradlew performanceTest
```

`test`
: Runs the majority of our tests. This is the go-to target and the idiomatic way of running tests in Gradle.

`destructiveTest`
: Runs the tests annotated with `@Tag(Tags.WipesFDB)`, and only those. These tests work with global state in FoundationDB and need to wipe the entire database while running (or in `@Before*` and `@After*`), which is why they are segregated into their own task.

`performanceTest`
: Runs the tests annotated with `@Tag(Tags.Performance)`, which gather performance numbers rather than check correctness. We don’t run these on any regular cadence or automatically, but once written they're valuable to keep around and re-run whenever you touch the associated production code.

`scalarFallbackTest`
: Re-runs the vector-math tests in the `fdb-extensions` module with the scalar backend forced (via `-Dfdb.vector.simd=scalar`, no `--add-modules`). The module ships two interchangeable backends: a SIMD backend based on `jdk.incubator.vector`, and a scalar fallback. The standard `test` target exercises only the SIMD one; this task covers the fallback. It is wired into `check`, so a normal build runs both. This task lives in a single module, so you have to run it as `./gradlew :fdb-extensions:scalarFallbackTest`. The selection of tests is by tag:

  * `@Tag(Tags.RequiresSIMD)` — asserts SIMD-specific behavior; runs only in the standard `test` target (the target excludes nothing extra; `scalarFallbackTest` doesn't include this tag).
  * `@Tag(Tags.RequiresScalar)` — asserts scalar-specific behavior, such as bit-exact determinism that SIMD lane-reordering would break; the standard `test` target excludes it, and `scalarFallbackTest` includes it, so it runs only under scalar.
  * `@Tag(Tags.DualScalarSIMD)` — parity/correctness tests that must hold under both backends; they run **twice**, under SIMD in `test` and under scalar in `scalarFallbackTest`. This is how SIMD-vs-scalar parity is actually exercised.

## Running the `@YamlTest` integration tests

A large part of the SQL layer is covered by **YAML integration tests**. These are declarative test cases written in `.yamsql` files and driven by the `@YamlTest` harness. These tests can be run under a variety of connection and version configurations. For example, there is a configuration that forces every query to page through a continuation after each individual row (`maxRows: 1`), which we use to exercise continuation handling. Other configurations run the tests against an external server rather than an in-process connection. The following tasks each select a different configuration:

`quickTest`
: Runs the tests in just the _embedded_ configuration. This is most useful while developing new features and tests: you can iterate quickly against the embedded configuration alone, without repeatedly running a failing test under all the other configurations.

`rpcTest`
: Like `quickTest`, but runs the tests through the JDBC driver and the gRPC protocol layer instead of a direct `EmbeddedRelationalConnection`. The server runs in the same JVM over gRPC's in-process transport (no network sockets), so this exercises the request/response marshalling of the RPC path—which the embedded connection bypasses—without the cost or flakiness of a networked server. Useful when you are working on an aspect of the RPC protocol.

`singleVersionTest`
: Runs the tests against multiple external servers that are all on the same version, rather than alternating between an external server and the current embedded connection as it normally would.

`mixedModeTest`
: Runs the tests with each test alternating between the current version and some older version. This helps ensure that we remain backward-compatible for data and continuations. By default, it runs against the 10 most recent versions, but you can make it run against a single version with `./gradlew mixedModeTest -Ptests.mixedModeVersion=${version}`. These tests are run during the nightly build and also as part of the release process. By default, they are not run during a pull request build, but you can opt into this extra validation by adding the [Run mixed-mode](https://github.com/FoundationDB/fdb-record-layer/labels/Run%20mixed-mode) label.

## Reproducing a nightly test run

We have a [“Nightly” GitHub action](https://github.com/FoundationDB/fdb-record-layer/actions/workflows/nightly.yml) that we run every night. It includes tests that are exceptionally slow (annotated with `@SuperSlow`) or have randomness (ideally using `RandomizedTestUtils`). If you wish to mimic such a test run locally, you’ll need to attach the following properties, though you don’t always need all of them. You can add these to a standard `./gradlew test` run. For example, the following invocation mirrors what’s in our [`nightly.yml`](https://github.com/FoundationDB/fdb-record-layer/blob/main/.github/workflows/nightly.yml) configuration:
```sh
./gradlew test -Ptests.nightly -Ptests.includeRandom -Ptests.iterations=2 -PspotbugsEnableHtmlReport
```

The relevant properties are:

* **`tests.nightly`**, which does the following:

   * It runs tests that are annotated with `@SuperSlow`, which have a higher timeout of 20 minutes.
   * It runs test parameters that are wrapped in `TestConfigurationUtils.onlyNightly`.
   * It sets Gradle’s `ignoreFailures` flag, so that a failing test does not fail the build. Every test task still runs to completion across all sub-modules—rather than aborting at the first failure—and the failures are still recorded in the test reports. The rationale is that these tests are more likely to be flaky, so we want to run them all and inspect the reports rather than stop early.
* **`tests.includeRandom`**, which includes tests tagged with `@Tag(Tags.Random)`. It also sets the system locale to a randomly chosen one, in order to help catch issues where we wrongly depend on the default locale being `en-US`.
* **`tests.iterations=2`**, which sets how many random seeds to generate for tests that use `RandomizedTestUtils`. It defaults to `0`, and has no effect unless `tests.includeRandom` is also set.


A note on the randomness controls: The _fdb-record-layer-lucene_ module uses the [**lucene-test-framework**](https://central.sonatype.com/artifact/org.apache.lucene/lucene-test-framework) from the Apache Lucene project to further exercise our custom file format and directory. Because that framework runs on JUnit 4 (via the JUnit Vintage engine) rather than the JUnit Jupiter API used by the rest of our tests, it does not honor the randomness properties above directly. Instead, it has its own equivalents, `-Ptests.luceneIncludeRandom` and `-Ptests.luceneIterations`, which the build translates into Lucene’s native `tests.seed` and `tests.iters` controls, so that randomness is enabled and seeded consistently across both test frameworks.

## Publishing a snapshot build

If you want to consume a local build in another project that depends on the FoundationDB Record Layer, you can publish it to your local Maven repository as follows:

```sh
./gradlew publishToMavenLocal -PpublishBuild=true
```
The published .jar files will be stored in the `~/.m2/repository/org/foundationdb/fdb-record-layer-core/n.n-SNAPSHOT/` directory.

## Creating a patch branch

By convention, a patch branch should be named “fdb-record-layer-_a.b.c_”, where _a.b.c_ is the starting version of the build it is created from.

1. Create the new patch branch based on the tag of the starting build.
    ```sh
    git checkout -b fdb-record-layer-a.b.c a.b.c.0
    ```

1. In `gradle.properties`, change `version=a.b` to `version=a.b.c` using the starting version.

1. In `docs/sphinx/source/ReleaseNotes.md`, move the template for release notes for the next release to be just above the release note for the starting version _a.b.c_.

1. Commit and push the new branch upstream.

1. Create patch fix pull requests against this new branch.

There may be conflicts in the `gradle.properties` and `docs/sphinx/source/ReleaseNotes.md` files when you try to merge the patch branch into `main`. Before you merge, make sure that the version on `main` is not accidentally changed, and that the release notes file contains all release notes from both branches after the merge.
