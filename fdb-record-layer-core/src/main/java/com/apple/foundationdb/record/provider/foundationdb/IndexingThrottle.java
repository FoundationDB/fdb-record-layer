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
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.util.LoggableException;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * This class provides build/commit/retry with throttling to the OnlineIndexer. In the future,
 * this class can be generalized to serve other FDB modules.
 */
@API(API.Status.INTERNAL)
public class IndexingThrottle {

    @Nonnull private static final Logger LOGGER = LoggerFactory.getLogger(IndexingThrottle.class);
    @Nonnull private final IndexingCommon common;
    private final IndexState expectedIndexState;

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
     * The number of successful transactions in a row as called by {@link #throttledRunAsync(Function, BiFunction, BiConsumer, List)}.
     */
    private int successCount = 0;

    IndexingThrottle(@Nonnull IndexingCommon common, IndexState expectedIndexState) {
        this.common = common;
        this.limit = common.config.getMaxLimit();
        this.expectedIndexState = expectedIndexState;
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

    void decreaseLimit(@Nonnull FDBException fdbException,
                       @Nullable List<Object> additionalLogMessageKeyValues) {
        limit = Math.max(1, (3 * limit) / 4);
        if (LOGGER.isInfoEnabled()) {
            final KeyValueLogMessage message = KeyValueLogMessage.build("Lessening limit of online index build",
                    LogMessageKeys.ERROR, fdbException.getMessage(),
                    LogMessageKeys.ERROR_CODE, fdbException.getCode(),
                    LogMessageKeys.LIMIT, limit,
                    LogMessageKeys.INDEX_NAME, common.getIndex().getName(),
                    LogMessageKeys.INDEXER_ID, common.getUuid()
                    );
            if (additionalLogMessageKeyValues != null) {
                message.addKeysAndValues(additionalLogMessageKeyValues);
            }
            LOGGER.info(message.toString(), fdbException);
        }
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

    @Nonnull
    <R> CompletableFuture<R> throttledRunAsync(@Nonnull final Function<FDBRecordStore, CompletableFuture<R>> function,
                                               @Nonnull final BiFunction<R, Throwable, Pair<R, Throwable>> handlePostTransaction,
                                               @Nullable final BiConsumer<FDBException, List<Object>> handleLessenWork,
                                               @Nullable final List<Object> additionalLogMessageKeyValues) {
        List<Object> onlineIndexerLogMessageKeyValues = new ArrayList<>(Arrays.asList(
                LogMessageKeys.INDEX_NAME, common.getIndex().getName(),
                LogMessageKeys.INDEXER_ID, common.getUuid()));
        if (additionalLogMessageKeyValues != null) {
            onlineIndexerLogMessageKeyValues.addAll(additionalLogMessageKeyValues);
        }

        AtomicInteger tries = new AtomicInteger(0);
        CompletableFuture<R> ret = new CompletableFuture<>();
        AtomicLong toWait = new AtomicLong(common.getRunner().getDatabase().getFactory().getInitialDelayMillis());
        AsyncUtil.whileTrue(() -> {
            loadConfig();
            final Index index = common.getIndex();
            return common.getRunner().runAsync(context -> common.getRecordStoreBuilder().copyBuilder().setContext(context).openAsync().thenCompose(store -> {
                IndexState indexState = store.getIndexState(index);
                if (indexState != expectedIndexState) {
                    throw new RecordCoreStorageException("Unexpected index state",
                            LogMessageKeys.INDEX_NAME, index.getName(),
                            common.getRecordStoreBuilder().getSubspaceProvider().logKey(), common.getRecordStoreBuilder().getSubspaceProvider().toString(context),
                            LogMessageKeys.INDEX_STATE, indexState,
                            LogMessageKeys.INDEX_STATE_PRECONDITION, expectedIndexState);
                }
                return function.apply(store);
            }), handlePostTransaction, onlineIndexerLogMessageKeyValues).handle((value, e) -> {
                if (e == null) {
                    ret.complete(value);
                    return AsyncUtil.READY_FALSE;
                } else {
                    int currTries = tries.getAndIncrement();
                    FDBException fdbE = getFDBException(e);
                    if (currTries < common.config.getMaxRetries() && fdbE != null && lessenWorkCodes.contains(fdbE.getCode())) {
                        if (handleLessenWork != null) {
                            handleLessenWork.accept(fdbE, onlineIndexerLogMessageKeyValues);
                        }
                        long delay = (long)(Math.random() * toWait.get());
                        toWait.set(Math.min(toWait.get() * 2,
                                common.getRunner().getDatabase().getFactory().getMaxDelayMillis()));
                        if (LOGGER.isWarnEnabled()) {
                            final KeyValueLogMessage message = KeyValueLogMessage.build("Retrying Runner Exception",
                                    LogMessageKeys.INDEXER_CURR_RETRY, currTries,
                                    LogMessageKeys.INDEXER_MAX_RETRIES, common.config.getMaxRetries(),
                                    LogMessageKeys.DELAY, delay,
                                    LogMessageKeys.LIMIT, limit);
                            message.addKeysAndValues(onlineIndexerLogMessageKeyValues);
                            LOGGER.warn(message.toString(), e);
                        }
                        return MoreAsyncUtil.delayedFuture(delay, TimeUnit.MILLISECONDS).thenApply(vignore3 -> true);
                    } else {
                        return completeExceptionally(ret, e, onlineIndexerLogMessageKeyValues);
                    }
                }
            }).thenCompose(Function.identity());
        }, common.getRunner().getExecutor()).whenComplete((vignore, e) -> {
            if (e != null) {
                // Just update ret and ignore the returned future.
                completeExceptionally(ret, e, onlineIndexerLogMessageKeyValues);
            }
        });
        return ret;
    }

    private <R> CompletableFuture<Boolean> completeExceptionally(CompletableFuture<R> ret, Throwable e, List<Object> additionalLogMessageKeyValues) {
        if (e instanceof LoggableException) {
            ((LoggableException)e).addLogInfo(additionalLogMessageKeyValues.toArray());
        }
        ret.completeExceptionally(common.getRunner().getDatabase().mapAsyncToSyncException(e));
        return AsyncUtil.READY_FALSE;
    }

    public int getLimit() {
        return limit;
    }
}

