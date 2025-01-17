/*
 * FDBSystemOperations.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.system.SystemKeyspace;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

/**
 * View of an FoundationDB database used for accessing system and special keys. These are special keys defined within
 * special system keyspaces that contain information about the underlying FoundationDB cluster, about the client
 * configuration, or other information available through another API.
 */
@API(API.Status.EXPERIMENTAL)
public class FDBSystemOperations {

    @Nullable
    private static String nullableUtf8(@Nullable byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    private static <T> T asyncToSync(@Nonnull FDBDatabaseRunner runner, @Nonnull CompletableFuture<T> operation) {
        return runner.asyncToSync(FDBStoreTimer.Waits.WAIT_LOAD_SYSTEM_KEY, operation);
    }

    /**
     * Get the primary datacenter of the underlying cluster. This will return the datacenter ID (if set)
     * of the datacenter currently serving as the primary, which (by definition) is where the transaction
     * subsystem will be recruited. This is mostly relevant for
     * <a href="https://apple.github.io/foundationdb/configuration.html#configuring-regions">multi-region configurations</a>,
     * where this value might change if the cluster decides that it needs to fail over to a secondary region. The returned
     * datacenter ID will be {@code null} if the FDB cluster's datacenter has not been set.
     *
     * <p>
     * Note that this operation must read this information from the database's storage and uses its own transaction.
     * </p>
     *
     * @param runner a runner to use to perform the operation
     * @return a future that will complete with the primary datacenter of the cluster for the database underlying {@code runner}
     */
    @Nonnull
    public static CompletableFuture<String> getPrimaryDatacenterAsync(@Nonnull FDBDatabaseRunner runner) {
        return runner.runAsync(context -> {
            final Transaction tr = context.ensureActive();
            tr.options().setReadSystemKeys();
            return tr.get(SystemKeyspace.PRIMARY_DATACENTER_KEY).thenApply(FDBSystemOperations::nullableUtf8);
        }, Arrays.asList(LogMessageKeys.TRANSACTION_NAME, "FDBSystemOperations::getPrimaryDatacenterAsync"));
    }

    /**
     * Get the primary datacenter of the underlying cluster. This is a blocking version of
     * {@link #getPrimaryDatacenterAsync(FDBDatabaseRunner)}. If the FDB cluster's primary datacenter has not
     * been set, this will return {@code null}.
     *
     * @param runner a runner to use to perform the operation
     * @return the primary datacenter of the database underlying {@code runner}
     * @see #getPrimaryDatacenterAsync(FDBDatabaseRunner)
     */
    @Nullable
    public static String getPrimaryDatacenter(@Nonnull FDBDatabaseRunner runner) {
        return asyncToSync(runner, getPrimaryDatacenterAsync(runner));
    }

    @Nonnull
    private static CompletableFuture<String> getConnectionStringAsyncInternal(@Nonnull FDBRecordContext context) {
        return context.ensureActive().get(SystemKeyspace.CONNECTION_STR_KEY).thenApply(FDBSystemOperations::nullableUtf8);
    }

    /**
     * Get the connection string used to connect to the FDB cluster. This string essentially contains the contents of
     * the cluster file, though it may change if the cluster's coordinators change. Note that even though this
     * operation requires having a database connection (in the transaction) and returns a future (as the client
     * may schedule to complete the work at a later time), it does not actually perform any network calls but reads
     * the value from the client's local memory.
     *
     * <p>
     * For more information, see the documentation on the
     * <a href="https://apple.github.io/foundationdb/administration.html#cluster-file-format">cluster file format</a>
     * in the FoundationDB documentation.
     * </p>
     *
     * @param runner a runner to use to perform the operation
     * @return a future that will contain the current cluster connection string
     */
    @Nonnull
    public static CompletableFuture<String> getConnectionStringAsync(@Nonnull FDBDatabaseRunner runner) {
        return runner.runAsync(FDBSystemOperations::getConnectionStringAsyncInternal,
                Arrays.asList(LogMessageKeys.TRANSACTION_NAME, "FDBSystemOperations::getConnectionStringAsync"));
    }

    /**
     * Get the connection string used to connect to the FDB cluster. This is a synchronous version of
     * {@link #getConnectionStringAsync(FDBDatabaseRunner)}
     *
     * @param runner a runner to use to perform the operation
     * @return the current cluster connection string
     * @see #getConnectionStringAsync(FDBDatabaseRunner)
     */
    @Nullable
    public static String getConnectionString(@Nonnull FDBDatabaseRunner runner) {
        return asyncToSync(runner, getConnectionStringAsync(runner));
    }

    private static CompletableFuture<String> getClusterFilePathAsyncInternal(@Nonnull FDBRecordContext context) {
        return context.ensureActive().get(SystemKeyspace.CLUSTER_FILE_PATH_KEY).thenApply(FDBSystemOperations::nullableUtf8);
    }

    /**
     * Get the file system path to the cluster file used. Note that this differs from the value that is returned by
     * {@link FDBDatabase#getClusterFile()} in that that function may return {@code null} if the cluster file is set to
     * the system default. This function, however, requests the information from the native client, and so it will
     * return the resolved cluster file (e.g., returning the path to the system default instead of {@code null}).
     * Note that even though this operation returns a future, it does not need to perform any network calls but instead
     * answers this question from the client's local memory.
     *
     * @param runner a runner to use to perform the operation
     * @return a future that will contain the cluster file path
     */
    @Nonnull
    public static CompletableFuture<String> getClusterFilePathAsync(@Nonnull FDBDatabaseRunner runner) {
        return runner.runAsync(FDBSystemOperations::getClusterFilePathAsyncInternal,
                Arrays.asList(LogMessageKeys.TRANSACTION_NAME, "FDBSystemOperations::getClusterFilePathAsync"));
    }

    /**
     * Get the file system path to the cluster file used. This is a synchronous version of
     * {@link #getClusterFilePathAsync(FDBDatabaseRunner)}.
     *
     * @param runner a transaction to use to perform the operation
     * @return the cluster file path
     * @see #getClusterFilePathAsync(FDBDatabaseRunner)
     */
    @Nullable
    public static String getClusterFilePath(@Nonnull FDBDatabaseRunner runner) {
        return asyncToSync(runner, getClusterFilePathAsync(runner));
    }

    private FDBSystemOperations() {
    }
}
