# Building the Record Layer

## Developing the Record Layer

Before contributing to the Record Layer, please see the [contribution guide](https://github.com/FoundationDB/fdb-record-layer/blob/master/CONTRIBUTING.md).

## Building the Core

To do the full suite of checks and tests run:
```
./gradlew -PspotbugsEnableHtmlReport check test
```

If you enable the local repo in whatever uses the Record Layer, the following will make it available. The published jars will be in the directory `~/.m2/repository/org/foundationdb/fdb-record-layer-core/n.n-SNAPSHOT/`.

```
./gradlew publishToMavenLocal
```

## Configuring IntelliJ

The Record Layer project does not use the Gradle IDEA plugin, unlike many similar projects. Instead, it uses IntelliJ IDEA's builtin Gradle support to compile the project and run the tests; the repository will run a "sync" with the Gradle configuration every time the project is opened or a file referenced from `build.gradle` is changed.

If you are used to opening older `.ipr`-based IntelliJ projects, the process for opening this project will be somewhat different. Instead of selecting the `fdb-record-layer.ipr` file, open the project's root directory; IntelliJ will find the project configuration in the `.idea` directory.

If you make changes to most IntelliJ settings, they will save over the files that are checked into Git. These changes can be shared by committing the updated files.

There are a few settings that we use for consistency that are saved in the user-specific `workspace.xml` file. These settings must be enabled manually by each developer:

* Preferences > Editor > General > Auto Import > Optimize imports on the fly (for current project)

* (optional, but useful) Preferences > Build, Execution, and Deployment > Build Tools > Maven > Importing > Automatically download > Sources

Before attempting to build anything, exit IntelliJ and build once from the command line. This will generate the protobuf files.

```
./gradlew package
```

Launch IntelliJ again and Build > Build Project should succeed.

To verify that you have a working configuration, pick some tests that depend on FDB (such as those in the `FDBRecordStoreQueryTest` fixture), and check that you can run them from IntelliJ.

## Running the tests

In order to run the tests you will need to have FDB installed on your development machine. The latest FDB binaries can be found on
the [FoundationDB website](https://www.foundationdb.org/download/).

## Configuring tests for a non-standard FDB cluster file location

If a file `fdb-environment.properties` exists in the root of the working directory, it contains environment variables that specify where to find
the local FDB. These settings will apply when running inside IntelliJ as well as from a command line and so are easier to manage than a shell script.

* `FDB_CLUSTER_FILE`: the cluster file

* `DYLD_LIBRARY_PATH` (or `LD_LIBRARY_PATH`): the `libfdb_c.dylib` (or `.so`) C API library
