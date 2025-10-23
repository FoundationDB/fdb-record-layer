# Building the Record Layer

## Developing the Record Layer

Before contributing to the Record Layer, please see the [contribution guide](https://github.com/FoundationDB/fdb-record-layer/blob/main/CONTRIBUTING.md).

## Configuring IntelliJ

1. Fork and clone the [repository](https://github.com/FoundationDB/fdb-record-layer).

1. Run

        ./gradlew package

    on the root directory of the local clone.
    
1. *(Skip if you are not interested in running the tests.)*\
 Install FDB on your development machine. The latest FDB binaries can be found on
the [GitHub](https://github.com/apple/foundationdb/releases).

1. Install and launch [IntelliJ IDEA](https://www.jetbrains.com/idea/). This document is based on Version 2020.2.3.

1. In the "Welcome to IntelliJ IDEA" window, click "Open or import" and open the project's root directory. (If you do not see the welcome window, File > Open can do the same.)

1. In the Gradle tool window (expand by clicking the Gradle button on right top), click ![the Reload All Gradle Projects button](https://resources.jetbrains.com/help/img/idea/2020.2/icons.actions.refresh.svg) on the toolbar.

1. Build > Build Project.

1. Preferences > Editor > General > Auto Import > Optimize imports on the fly (for current project)

1. *(This is optional, but useful.)*\
 Preferences > Build, Execution, and Deployment > Build Tools > Maven > Importing > Automatically download > Sources

**Now you have configured IntelliJ successfully!**

*(Skip if you are not interested in running the tests.)*\
To verify that you have a working configuration, pick some tests that depend on FDB (such as those in the `FDBRecordStoreQueryTest` fixture), and check you can run them from IntelliJ.

If this does not work smoothly for you, please create a [new issue](https://github.com/FoundationDB/fdb-record-layer/issues/new) or ask in [the forum](https://forums.foundationdb.org/c/using-layers).

### Behind the Scenes

*(This section explains what happened above and why, in case you are interested.)*

The `./gradlew package` command (Step 2) generates the protobuf files.

The Record Layer project does not use the Gradle IDEA plugin, unlike many similar projects. Instead, it uses IntelliJ IDEA's builtin Gradle support to compile the project and run the tests; the repository will run a "sync" with the Gradle configuration when you load Gradle project (Step 6).

If you make changes to most IntelliJ settings, they will save over the files that are checked into Git. These changes can be shared by committing the updated files.

However, there are a few settings that we use for consistency that are saved in the user-specific `workspace.xml` file. These settings must be enabled manually by each developer. (Step 8, 9)

## Building the Core

To do the full suite of checks and tests run:
```
./gradlew -PspotbugsEnableHtmlReport check test
```

If you enable the local repo in whatever uses the Record Layer, the following will make it available. The published jars will be in the directory `~/.m2/repository/org/foundationdb/fdb-record-layer-core/n.n-SNAPSHOT/`.

```
./gradlew publishToMavenLocal -PpublishBuild=true
```

## Configuring tests for a non-standard FDB cluster file location, or to run against multiple clusters

If a file `fdb-environment.yaml` exists in the root of the working directory, it contains the configuration for the C API library
and a list of cluster files. Most tests will chose randomly from the provided cluster files when testing.

Here is an example file:
``` yaml
libraryPath: /Users/scott/fdb/bin/fdb-server-7.3.42-macos_arm64/lib
clusterFiles:
  - /Users/scott/fdb/data/fdb-one.cluster
  - /Users/scott/fdb/data/fdb-two.cluster
```



### deprecated properties file

[ this is being replaced by the yaml file described above, to better support multiple cluster files ]

If a file `fdb-environment.properties` exists in the root of the working directory, it contains environment variables that specify where to find
the local FDB. These settings will apply when running inside IntelliJ as well as from a command line and so are easier to manage than a shell script.

* `FDB_CLUSTER_FILE`: the cluster file

* `DYLD_LIBRARY_PATH` (or `LD_LIBRARY_PATH`): the `libfdb_c.dylib` (or `.so`) C API library
