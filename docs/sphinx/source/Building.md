# Building the Record Layer

This page explains how to build the FoundationDB Record Layer and set up a development environment for working on it. It covers building from source, configuring your IDE for development, and running the tests against a local cluster, as well as some advanced workflows. If you intend to contribute to the FoundationDB Record Layer, please also consult our [contribution guidelines](https://github.com/FoundationDB/fdb-record-layer/blob/main/CONTRIBUTING.md). If anything in this guide does not work smoothly for you, feel free to [create a new issue](https://github.com/FoundationDB/fdb-record-layer/issues/new) or open a thread in [the FoundationDB Forums](https://forums.foundationdb.org/c/using-layers).

## Building the project

To build the Record Layer you only need an installed Java Development Kit as a prerequisite. No separate Gradle installation is needed, as the bundled Gradle wrapper supplies the correct version. A local FoundationDB cluster is not strictly needed for building, but you will need one if you intend to [run the tests](#running-the-tests).

1. **Install a current JDK distribution** if you don’t already have one. Our CI/CD builds use [Eclipse Temurin](https://adoptium.net), but any distribution should work.

1. Optionally, **create your own fork** of the [FoundationDB/fdb-record-layer](https://github.com/FoundationDB/fdb-record-layer) repository. This is only needed if you intend to contribute. All changes are made by creating pull requests from your fork into the main repository; see our [contribution guidelines](https://github.com/FoundationDB/fdb-record-layer/blob/main/CONTRIBUTING.md).

1. **Clone** the repository (or your own fork of it) to your development machine.

1. Make sure that Gradle detects your JDK and that it meets the requirements. You can check which JDKs Gradle has found on your machine with:
   ```sh
   ./gradlew javaToolchains
   ```
   If your JDK is unsuitable, the build will fail with an error explaining that no matching toolchain could be found. See Gradle’s [Java toolchain support](https://docs.gradle.org/current/userguide/toolchains.html) for more information on this process.

1. **Kick off a build**. The following command compiles every module and runs the static-analysis checks, SpotBugs and Checkstyle. Because the `-x test` flag excludes the test tasks, this does not require a running FoundationDB cluster. Expect the command to take several minutes on a first run.
   ```sh
   ./gradlew build -x test
   ```

Our automated builds are run via **[GitHub Actions](https://github.com/FoundationDB/fdb-record-layer/actions)**. You can study the workflow definitions under the `.github/workflows/` directory to learn how they invoke Gradle precisely.

## Configuring IntelliJ IDEA

[IntelliJ IDEA](https://www.jetbrains.com/idea/) is the recommended environment for developing the FoundationDB Record Layer, although it is not strictly required. Note that we do _not_ use Gradle’s [IDEA plugin](https://docs.gradle.org/current/userguide/idea_plugin.html), whose IDE file generation is deprecated. Instead, we rely on IntelliJ’s built-in Gradle integration to generate and compile the project and to run the tests. When you open the project, IntelliJ performs a Gradle *sync* to derive a model of the project.

1. **Install IntelliJ IDEA** and launch it. Any recent version should work, though the menu paths mentioned below may differ slightly between releases.

1. In the “Welcome to IntelliJ IDEA” window, click Open and **open the project’s root directory**. If you do not see the welcome window, File › Open… can do the same.

1. Click the **Reload All Gradle Projects** button on the toolbar of the Gradle tool window. You can open this tool window from View › Tool Windows › Gradle.

1. **Compile everything** via Build › Build Project.

We recommend setting the following **IntelliJ preferences**. They are not strictly required, but useful.

* Under Editor › General › Auto Import, enable “Optimize imports on the fly” for the current project.

* Under Advanced Settings › Gradle, enable “Download sources” so that dependency sources are available for navigation and debugging.

If you make changes to other IntelliJ settings, they may save over the .xml files that are checked into Git under the `.idea/` directory. These changes are meant to be shared by simply committing the updated files. However, other settings—such as the recommendations above—are saved in the user-specific `workspace.xml` file or other .xml files that we intentionally added to `.gitignore`. Such user-specific settings must be configured manually by each developer for consistency.

## Running the tests

If you are interested in running the tests, you need to install and run a local FoundationDB cluster.

1. **Install FoundationDB** on your development machine. To find the latest published binaries, refer to the [release list of the _foundationdb_ GitHub repository](https://github.com/apple/foundationdb/releases).

1. **Make sure FoundationDB is running.** If you’ve installed an official package, the `fdbserver` process will start automatically. You can confirm that the cluster is available by running:
   ```sh
   fdbcli --exec 'status minimal'
   ```
   This should print “The database is available”. For platform-specific installation and startup instructions, see FoundationDB’s getting-started guides for [macOS](https://apple.github.io/foundationdb/getting-started-mac.html) and [Linux](https://apple.github.io/foundationdb/getting-started-linux.html).

1. **Run a test** to verify that you have a working configuration. In IntelliJ, pick a test that depends on FoundationDB and hit the green "play" arrow in the gutter next to it—`FDBRecordStoreCrudTest.writeRead()` is a good, fast one. The first run may take a while (perhaps a minute) while the project builds and the test harness warms up; subsequent runs are faster. You can also run tests from the command line, as described below.

1. The **full suite of checks and tests** is run by the _check_ and _test_ Gradle tasks, as follows. In addition to the tests, this includes the static analysis checks, SpotBugs and Checkstyle. The `-PspotbugsEnableHtmlReport` flag additionally emits a SpotBugs report in HTML format.
   ```sh
   ./gradlew -PspotbugsEnableHtmlReport check test
   ```

By default, the tests run against the FoundationDB cluster named by the standard **cluster file**, which the official installer writes to the platform default location (`/etc/foundationdb/fdb.cluster` on Linux, `/usr/local/etc/foundationdb/fdb.cluster` on macOS), or wherever `FDB_CLUSTER_FILE` points if that variable is set. To override this behavior, you can create an **fdb-environment.yaml** file in the root of your working directory. It lets you point the tests at a non-default cluster, or even run them against _multiple_ clusters. The file specifies the directory containing the FoundationDB C API library (`libfdb_c.dylib` on macOS, `libfdb_c.so` on Linux) and a list of cluster files. If you provide more than one, most tests will choose randomly among them. Here is an example:
``` yaml
libraryPath: /opt/fdb/lib
clusterFiles:
  - /opt/fdb/clusters/fdb-one.cluster
  - /opt/fdb/clusters/fdb-two.cluster
```

### Gradle test tasks

The build defines several test tasks for different purposes. You can run any of them by passing the task name to the wrapper. For example:
```sh
./gradlew performanceTest
```

Here is an overview of the available tasks:

`test`
: Runs the majority of our tests. This is the go-to task and the idiomatic way of running tests in Gradle.

`destructiveTest`
: Runs the tests annotated with `@Tag(Tags.WipesFDB)`, and only those. These tests work with global state in FoundationDB and need to wipe the entire database while running (or in `@Before*` and `@After*` methods), which is why they are segregated into their own task.

`performanceTest`
: Runs the tests annotated with `@Tag(Tags.Performance)`. Such tests gather performance numbers rather than check correctness. We don’t run these on any regular cadence or automatically, but once written they’re valuable to keep around and re-run whenever you touch the associated production code.

`:fdb-extensions:scalarFallbackTest`
: This task exists only in the _fdb-extensions_ module. It re-runs the vector math tests in that module with the scalar backend forced (via `-Dfdb.vector.simd=scalar`, no `--add-modules`). The module ships two interchangeable backends: a SIMD backend based on the `jdk.incubator.vector` API, and a scalar fallback. The standard `test` task exercises only the SIMD one; the `scalarFallbackTest` task covers the fallback. It is wired into `check`, so a normal build will run both. The selection of tests is affected by the following tags:

  * `@Tag(Tags.RequiresSIMD)` — for tests that assert SIMD-specific behavior. These run only in the standard `test` task (which excludes nothing extra; whereas the `scalarFallbackTest` task doesn’t include this tag).
  * `@Tag(Tags.RequiresScalar)` — for tests that assert scalar-specific behavior, such as bit-exact determinism that SIMD lane-reordering would break. The standard `test` task excludes it, and `scalarFallbackTest` includes it, so it runs only under scalar.
  * `@Tag(Tags.DualScalarSIMD)` — for parity/correctness tests that must hold under both backends. These run _twice_, under SIMD in `test` and under scalar in `scalarFallbackTest`. This is how SIMD-vs-scalar parity is actually exercised.

## Running the `@YamlTest` integration tests

A large part of the SQL surface of the Record Layer is covered by **YAML integration tests**. These are declarative test cases written in `.yamsql` files and driven by the `@YamlTest` harness. They can be run under a variety of connection and version configurations. For example, there is a configuration that uses the `maxRows: 1` option to force every query to page through a continuation after each individual row, which we use to exercise the continuation handling. Other configurations run the tests against an external server rather than an in-process connection. The following tasks each select a different configuration:

`quickTest`
: Runs the tests in just the _embedded_ configuration. This is most useful while developing new features and tests. You can iterate quickly against the embedded configuration alone, without repeatedly running a failing test under all the other configurations.

`rpcTest`
: Like `quickTest`, but runs the tests through the JDBC driver and the gRPC protocol layer instead of a direct `EmbeddedRelationalConnection`. The server still runs in the same JVM as the test, but client and server talk over gRPC’s in-process transport (`io.grpc.inprocess`), which connects them through in-memory queues instead of network sockets. This exercises the request/response marshalling of the RPC path—which the embedded connection bypasses—without the cost or flakiness of a networked server. It will be useful when you are working on an aspect of the RPC protocol.

`singleVersionTest`
: Runs the tests against multiple external servers that are all on the same version, rather than alternating between an external server and the current embedded connection as it normally would.

`mixedModeTest`
: Runs the tests with each test alternating between the current version and some older version. This helps ensure that we remain backward-compatible for data and continuations. By default, it runs against the 10 most recent versions, but you can make it run against a single version with `./gradlew mixedModeTest -Ptests.mixedModeVersion=${version}`.

The mentioned **mixed-mode tests** are run during the [Nightly build](https://github.com/FoundationDB/fdb-record-layer/actions/workflows/nightly.yml) and also as part of the [Release workflow](https://github.com/FoundationDB/fdb-record-layer/actions/workflows/release.yml). A pull request build only runs the standard `test` and `destructiveTest` tasks against the current version by default, but you can opt into the extra mixed-mode validation by adding the [Run mixed-mode](https://github.com/FoundationDB/fdb-record-layer/labels/Run%20mixed-mode) label.

## Reproducing a nightly test run

We have a [**Nightly** GitHub action](https://github.com/FoundationDB/fdb-record-layer/actions/workflows/nightly.yml) that we run every night. It includes tests that are exceptionally slow or exhibit randomness (ideally controlled via `RandomizedTestUtils`). If you wish to mimic such a test run locally, you’ll need to attach the following properties, though you don’t always need all of them. You can add these to a standard `./gradlew test` run. For example, the following invocation mirrors what’s in our [`nightly.yml`](https://github.com/FoundationDB/fdb-record-layer/blob/main/.github/workflows/nightly.yml) configuration:
```sh
./gradlew test -Ptests.nightly -Ptests.includeRandom -Ptests.iterations=2 -PspotbugsEnableHtmlReport
```

The relevant properties are:

* **`tests.nightly`**, which does the following:

   * It runs tests that are annotated with `@SuperSlow`, under a raised timeout of 20 minutes.
   * It runs additional test parameters that are passed to the `TestConfigurationUtils.onlyNightly()` utility method, which are normally filtered out.
   * It sets Gradle’s `ignoreFailures` flag, so that a failing test will not fail the build. Rather than aborting at the first failure, every test task will run to completion across all submodules, and any failures are recorded in the test reports. The rationale is that these tests are more likely to be flaky, so we want to run them all and inspect the reports rather than stop early.
* **`tests.includeRandom`**, which includes tests tagged with `@Tag(Tags.Random)`. It also sets the system locale to a randomly chosen one, which should help catch any code that inadvertently depends on the default locale being _en-US_.
* **`tests.iterations=2`**, which sets how many random seeds to generate for tests that use `RandomizedTestUtils`. It defaults to 0, and has no effect unless `tests.includeRandom` is also set.


A note on the randomness controls: The _fdb-record-layer-lucene_ module uses the [**lucene-test-framework**](https://central.sonatype.com/artifact/org.apache.lucene/lucene-test-framework) module from the Apache Lucene project to further exercise our custom file format and directory. Because that framework runs on JUnit 4 (via the JUnit Vintage engine) rather than the JUnit Jupiter API used by the rest of our tests, it does not honor the mentioned randomness properties directly. Instead, it has its own equivalents, `-Ptests.luceneIncludeRandom` and `-Ptests.luceneIterations`, which the build translates into Lucene’s native `tests.seed` and `tests.iters` controls, so that randomness is enabled and seeded consistently across both test frameworks.

## Publishing a local snapshot build

If you want to consume a local build in another project that depends on the FoundationDB Record Layer, you can publish it to your local Maven repository as follows:

```sh
./gradlew publishToMavenLocal -PpublishBuild=true
```
The published .jar files will be stored in the `~/.m2/repository/org/foundationdb/fdb-record-layer-core/n.n-SNAPSHOT/` directory.

## Creating a patch branch

Patch branches allow us to ship a fix on top of an older release without having to pull in everything that has since landed on the `main` branch. Due to our branch protection rules on GitHub, such branches cannot be created directly. Instead, project maintainers can trigger a workflow that creates a patch branch on demand.

1. Manually trigger the [Create Release Branch](https://github.com/FoundationDB/fdb-record-layer/actions/workflows/create_branch.yml) GitHub Action, specifying the base version in the format `x.y.z`. The action will check out the corresponding `x.y.z.0` tag and push a new branch named `x.y.z-release`.

1. Once the branch exists, open a pull request for the patch against it. Remember to change the base branch to the patch branch before creating the PR, since GitHub’s “New pull request” page sets it to `main` by default.

1. From there, run the [Release](https://github.com/FoundationDB/fdb-record-layer/actions/workflows/release.yml) workflow against the patch branch. This bumps its version number and updates its release notes automatically, the same way it does for a regular release off of `main`.

1. Remember to cherry-pick or merge your changes further up once they land, since changes on a patch branch are not automatically merged back into `main`, and you’ll normally want them to be included in future releases off of `main` as well. If you merge the whole branch rather than cherry-picking individual commits, expect conflicts in `gradle.properties` and `docs/sphinx/source/ReleaseNotes.md`, since the two branches bump their version numbers and release notes independently. Make sure that the version on `main` is not accidentally changed, and that the release notes file contains all release notes from both branches afterward.
