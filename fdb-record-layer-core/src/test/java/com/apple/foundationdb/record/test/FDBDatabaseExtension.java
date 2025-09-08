/*
 * FDBDatabaseFactoryExtension.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.record.test;

import com.apple.foundationdb.FDB;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.APIVersion;
import com.apple.foundationdb.record.provider.foundationdb.BlockingInAsyncDetection;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactoryImpl;
import com.apple.foundationdb.test.TestExecutors;
import com.google.common.base.Strings;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Extension that allows the user to specify the database. It ensures that FDB has been properly initialized
 * and that the {@link FDBDatabase} object used during a test does not leak between test runs. This registers
 * call backs that run before and after tests, so it is suggested that users use the {@link RegisterExtension}
 * annotation to ensure that those callbacks run. Like so:
 *
 * <pre>{@code
 *     @RegisterExtension
 *     final FDBDatabaseExtension = new FDBDatabaseExtension();
 * }</pre>
 *
 * <p>
 * Because this extension creates a fresh {@link FDBDatabaseFactory} and a fresh {@link FDBDatabase} with each run,
 * tests that use this extension are free to modify what would otherwise be global state on the factory or database.
 * </p>
 */
public class FDBDatabaseExtension implements AfterEachCallback {
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBDatabaseExtension.class);
    public static final String BLOCKING_IN_ASYNC_PROPERTY = "com.apple.foundationdb.record.blockingInAsyncDetection";
    public static final String API_VERSION_PROPERTY = "com.apple.foundationdb.apiVersion";
    public static final boolean TRACE = false;
    @Nullable
    private static volatile FDB fdb;
    @Nullable
    private FDBDatabaseFactory databaseFactory;
    @Nonnull
    private static final List<String> clusterFiles;
    @Nonnull
    private final Map<String, FDBDatabase> databases = new HashMap<>();

    static {
        final String fdbEnvironment = System.getenv("FDB_ENVIRONMENT_YAML");
        if (!Strings.isNullOrEmpty(fdbEnvironment)) {
            clusterFiles = parseFDBEnvironmentYaml(fdbEnvironment);
        } else {
            clusterFiles = List.of();
        }
    }

    @SuppressWarnings("unchecked")
    private static List<String> parseFDBEnvironmentYaml(final String fdbEnvironment) {
        Yaml yaml = new Yaml();
        try (FileInputStream yamlInput = new FileInputStream(fdbEnvironment)) {
            Object fdbConfig = yaml.load(yamlInput);
            return (List<String>)((Map<?, ?>)fdbConfig).get("clusterFiles");
        } catch (IOException e) {
            throw new RecordCoreException("Could not read fdb-environment.yaml", e);
        } catch (ClassCastException e) {
            throw new RecordCoreException("Could not parse fdb environment file " + fdbEnvironment, e);
        }
    }

    public FDBDatabaseExtension() {
    }

    @Nonnull
    private static final Executor threadPoolExecutor = TestExecutors.newThreadPool("fdb-record-layer-test");

    public static APIVersion getAPIVersion() {
        String apiVersionStr = System.getProperty(API_VERSION_PROPERTY);
        if (apiVersionStr == null) {
            return APIVersion.getDefault();
        }
        return APIVersion.fromVersionNumber(Integer.parseInt(apiVersionStr));
    }

    @Nonnull
    private static FDB getInitedFDB() {
        if (fdb == null) {
            synchronized (FDBDatabaseExtension.class) {
                if (fdb == null) {
                    // Note: in some ways, this mirrors the TestDatabaseExtension abstraction in the
                    // fdb-extensions project. We could re-use this here, except that if we did, we'd
                    // never test the FDBDatabaseFactory's methods for initializing FDB
                    FDBDatabaseFactory baseFactory = FDBDatabaseFactory.instance();
                    if (TRACE) {
                        baseFactory.setTrace(".", "fdb_record_layer_test");
                    }
                    baseFactory.setAPIVersion(getAPIVersion());
                    baseFactory.setUnclosedWarning(true);
                    if (clusterFiles.isEmpty()) {
                        FDBDatabase unused = baseFactory.getDatabase();
                        unused.performNoOp(); // make sure FDB gets opened
                        unused.close();
                    }
                    for (final String clusterFile : clusterFiles) {
                        FDBDatabase unused = baseFactory.getDatabase(clusterFile);
                        unused.performNoOp(); // make sure FDB gets opened
                        unused.close();
                    }
                    fdb = FDB.instance();
                }
            }
        }
        return Objects.requireNonNull(fdb);
    }

    public void setupBlockingInAsyncDetection(@Nonnull FDBDatabaseFactory factory) {
        final String str = System.getProperty(BLOCKING_IN_ASYNC_PROPERTY);
        if (str != null) {
            final BlockingInAsyncDetection detection;
            try {
                detection = BlockingInAsyncDetection.valueOf(str);
            } catch (Exception e) {
                LOGGER.error("Illegal value provided for " + BLOCKING_IN_ASYNC_PROPERTY + ": " + str);
                return;
            }
            factory.setBlockingInAsyncDetection(detection);
            if (detection != BlockingInAsyncDetection.DISABLED) {
                LOGGER.info("Blocking-in-async is " + detection);
            }
        }
    }

    @Nonnull
    public FDBDatabaseFactory getDatabaseFactory() {
        if (databaseFactory == null) {
            // Create a new one to avoid polluting the caches or global state stored in the factory
            databaseFactory = FDBDatabaseFactoryImpl.testInstance(getInitedFDB());
            if (TRACE) {
                databaseFactory.setTransactionIsTracedSupplier(() -> true);
            }
            setupBlockingInAsyncDetection(databaseFactory);
            databaseFactory.setExecutor(threadPoolExecutor);
        }
        return databaseFactory;
    }

    @Nonnull
    public FDBDatabase getDatabase() {
        return getDatabase(randomClusterFileIndex());
    }

    public FDBDatabase getDatabase(int clusterIndex) {
        if (clusterFiles.isEmpty()) {
            if (clusterIndex > 0) {
                throw new IndexOutOfBoundsException("No cluster files specified, so there is only the default");
            } else {
                return databases.computeIfAbsent("NULL",
                        clusterFile -> {
                            LOGGER.info("Connecting to NULL cluster file");
                            return getDatabaseFactory().getDatabase();
                        });
            }
        }
        return databases.computeIfAbsent(clusterFiles.get(clusterIndex),
                clusterFile -> {
                    LOGGER.info("Connecting to cluster file: " + clusterFile);
                    return getDatabaseFactory().getDatabase(clusterFile);
                });
    }

    private static int randomClusterFileIndex() {
        if (clusterFiles.isEmpty()) {
            return 0;
        } else {
            return ThreadLocalRandom.current().nextInt(clusterFiles.size());
        }
    }

    public void checkForOpenContexts() {
        for (final Map.Entry<String, FDBDatabase> clusterFileToDatabase : databases.entrySet()) {
            assertEquals(0, clusterFileToDatabase.getValue().warnAndCloseOldTrackedOpenContexts(0),
                    clusterFileToDatabase.getKey() + " should not have left any contexts open");
        }
    }

    @Override
    public void afterEach(final ExtensionContext extensionContext) {
        // Validate that the test closes all the transactions that it opens
        checkForOpenContexts();
        for (final FDBDatabase database : databases.values()) {
            database.close();
        }
        databases.clear();
        if (databaseFactory != null) {
            getDatabaseFactory().clear();
            databaseFactory = null;
        }
    }

    /**
     * Marks the current test as skipped if there are not the desired number of clusters available to test against.
     * @param desiredCount the number of clusters to test against
     */
    public void assumeClusterCount(final int desiredCount) {
        if (desiredCount > 1) {
            Assumptions.assumeThat(clusterFiles.size()).as("Cluster file count").isGreaterThanOrEqualTo(desiredCount);
        }
    }
}
