/*
 * IndexingThrottle.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.FDBError;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * This class provides build/commit/retry with throttling to the OnlineIndexer. In the future,
 * this class can be generalized to serve other FDB modules.
 */
@API(API.Status.INTERNAL)
public class IndexingThrottle {

    @Nonnull private static final Logger LOGGER = LoggerFactory.getLogger(IndexingThrottle.class);
    @Nonnull private final IndexingCommon common;

    private int limit;

    // These error codes represent a list of errors that can occur if there is too much work to be done
    // in a single transaction.
    private static final Set<Integer> lessenWorkCodes = new HashSet<>(Arrays.asList(
            FDBError.TIMED_OUT.code(),
            FDBError.TRANSACTION_TOO_OLD.code(),
            FDBError.NOT_COMMITTED.code(),
            FDBError.TRANSACTION_TIMED_OUT.code(),
            FDBError.COMMIT_READ_INCOMPLETE.code(),
            FDBError.TRANSACTION_TOO_LARGE.code()));

    /**
     * The number of successful transactions in a row as called by {@link #throttledRunAsync(Function, BiFunction, Consumer, List)}.
     */
    private int successCount = 0;

    IndexingThrottle(@Nonnull IndexingCommon common) {
        this.common = common;
        this.limit = common.config.getMaxLimit();
    }

    public <R> CompletableFuture<R> buildCommitRetryAsync(@Nonnull BiFunction<FDBRecordStore, AtomicLong, CompletableFuture<R>> buildFunction,
                                                          boolean limitControl,
                                                          @Nullable List<Object> additionalLogMessageKeyValues) {
        AtomicLong recordsScanned = new AtomicLong(0);
        return throttledRunAsync(store -> buildFunction.apply(store, recordsScanned),
                // Run after a single transactional call within runAsync.
                (result, exception) -> {
                    if (limitControl) {
                        tryToIncreaseLimit(exception);
                    }
                    // Update records scanned.
                    if (exception == null) {
                        common.getTotalRecordsScanned().addAndGet(recordsScanned.get());
                    } else {
                        recordsScanned.set(0);
                    }
                    return Pair.of(result, exception);
                },
                limitControl ? this::decreaseLimit : null,
                additionalLogMessageKeyValues
        );
    }

    private synchronized void loadConfig() {
        if (common.loadConfig()) {
            final int maxLimit = common.config.getMaxLimit();
            if (limit > maxLimit) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info(
                            KeyValueLogMessage.build("Decreasing the limit to the new maxLimit.",
                                    LogMessageKeys.INDEX_NAME, common.getIndex().getName(),
                                    LogMessageKeys.LIMIT, limit,
                                    LogMessageKeys.MAX_LIMIT, maxLimit).toString());
                }
                limit = maxLimit;
            }
        }
    }

    void decreaseLimit(@Nonnull List<Object> logKeysAndValues) {
        limit = Math.max(1, (3 * limit) / 4);
        logKeysAndValues.addAll(Arrays.asList(
                LogMessageKeys.LESSEN_LIMIT, true,
                LogMessageKeys.LIMIT, limit
        ));
    }

    private void tryToIncreaseLimit(@Nullable Throwable exception) {
        final int increaseLimitAfter = common.config.getIncreaseLimitAfter();
        final int maxLimit = common.config.getMaxLimit();
        if (increaseLimitAfter > 0) {
            if (exception == null) {
                successCount++;
                if (successCount >= increaseLimitAfter && limit < maxLimit) {
                    increaseLimit();
                }
            } else {
                successCount = 0;
            }
        }
    }

    private void increaseLimit() {
        final int maxLimit = common.config.getMaxLimit();
        limit = Math.min(maxLimit, Math.max(limit + 1, (4 * limit) / 3));
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(KeyValueLogMessage.of("Re-increasing limit of online index build",
                    LogMessageKeys.INDEX_NAME, common.getIndex().getName(),
                    LogMessageKeys.INDEXER_ID, common.getUuid(),
                    LogMessageKeys.LIMIT, limit));
        }
    }

    // Finds the FDBException that ultimately caused some throwable or
    // null if there is none. This can be then used to determine, for
    // example, the error code associated with this FDBException.
    @Nullable
    private FDBException getFDBException(@Nullable Throwable e) {
        Throwable curr = e;
        while (curr != null) {
            if (curr instanceof FDBException) {
                return (FDBException)curr;
            } else {
                curr = curr.getCause();
            }
        }
        return null;
    }

    <R> CompletableFuture<R> throttledRunAsync(@Nonnull final Function<FDBRecordStore, CompletableFuture<R>> function,
                                               @Nonnull final BiFunction<R, Throwable, Pair<R, Throwable>> handlePostTransaction,
                                               @Nullable final Consumer<List<Object>> handleLessenWork,
                                               @Nullable final List<Object> additionalLogMessageKeyValues) {
        List<Object> onlineIndexerLogMessageKeyValues = new ArrayList<>(Arrays.asList(
                LogMessageKeys.INDEX_NAME, common.getIndex().getName(),
                LogMessageKeys.INDEX_VERSION, common.getIndex().getLastModifiedVersion(),
                LogMessageKeys.INDEXER_ID, common.getUuid(),
                getSubspaceProvider().logKey(), getSubspaceProvider()
        ));
        if (additionalLogMessageKeyValues != null) {
            onlineIndexerLogMessageKeyValues.addAll(additionalLogMessageKeyValues);
        }

        RetriableTaskRunner.Builder<R> builder = RetriableTaskRunner.newBuilder(common.config.getMaxRetries() + 1);

        builder.setHandleAndCheckRetriable(states -> {
            final Throwable e = states.getPossibleException();
            if (e == null) {
                return false;
            } else {
                FDBException fdbE = getFDBException(e);
                if (fdbE != null && lessenWorkCodes.contains(fdbE.getCode())) {
                    states.getLocalLogs().addAll(Arrays.asList(
                            LogMessageKeys.ERROR, fdbE.getMessage(),
                            LogMessageKeys.ERROR_CODE, fdbE.getCode()
                    ));
                    return true;
                } else {
                    return false;
                }
            }
        });

        if (handleLessenWork != null) {
            builder.setHandleIfDoRetry(taskState -> handleLessenWork.accept(taskState.getLocalLogs()));
        }

        RetriableTaskRunner<R> retriableTaskRunner = builder
                .setInitDelayMillis(FDBDatabaseFactory.instance().getInitialDelayMillis())
                .setMaxDelayMillis(FDBDatabaseFactory.instance().getMaxDelayMillis())
                .setLogger(LOGGER)
                .setAdditionalLogMessageKeyValues(additionalLogMessageKeyValues)
                .setExecutor(common.getRunner().getExecutor())
                .build();

        common.addRetriableTaskRunnerToClose(retriableTaskRunner);

        return retriableTaskRunner.runAsync(ignore -> {
            loadConfig();
            final Index index = common.getIndex();
            return common.getRunner().runAsync(context -> common.getRecordStoreBuilder().copyBuilder().setContext(context).openAsync().thenCompose(store -> {
                IndexState indexState = store.getIndexState(index);
                if (indexState != IndexState.WRITE_ONLY) {
                    throw new RecordCoreStorageException("Attempted to build non-write-only index",
                            LogMessageKeys.INDEX_NAME, index.getName(),
                            getSubspaceProvider().logKey(), getSubspaceProvider().toString(context),
                            LogMessageKeys.INDEX_STATE, indexState);
                }
                return function.apply(store);
            }), handlePostTransaction, onlineIndexerLogMessageKeyValues);
        });
    }

    private SubspaceProvider getSubspaceProvider() {
        return common.getRecordStoreBuilder().getSubspaceProvider();
    }

    public int getLimit() {
        return limit;
    }
}

