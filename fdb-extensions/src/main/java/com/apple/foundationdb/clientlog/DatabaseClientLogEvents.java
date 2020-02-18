/*
 * DatabaseClientLogEvents.java
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

package com.apple.foundationdb.clientlog;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.TransactionOptions;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * Parse client latency events from system keyspace.
 */
@API(API.Status.EXPERIMENTAL)
public class DatabaseClientLogEvents {
    @Nonnull
    private byte[] startKey;
    @Nonnull
    private byte[] endKey;
    @Nullable
    private Instant earliestTimestamp;
    @Nullable
    private Instant latestTimestamp;
    private int eventCount;
    private boolean more;

    /**
     * A callback with the current transaction.
     */
    @FunctionalInterface
    public interface EventConsumer {
        CompletableFuture<Void> accept(@Nonnull Transaction tr, @Nonnull FDBClientLogEvents.Event event);
    }
    
    private DatabaseClientLogEvents(@Nonnull byte[] startKey, @Nonnull byte[] endKey) {
        this.startKey = startKey;
        this.endKey = endKey;
    }

    @Nullable
    public Instant getEarliestTimestamp() {
        return earliestTimestamp;
    }

    @Nullable
    public Instant getLatestTimestamp() {
        return latestTimestamp;
    }

    public int getEventCount() {
        return eventCount;
    }

    public boolean hasMore() {
        return more;
    }

    protected static class EventRunner implements FDBClientLogEvents.EventConsumer {
        @Nonnull
        private final Database database;
        @Nonnull
        private final Executor executor;
        @Nullable
        private Transaction tr;
        @Nonnull
        private final EventConsumer callback;
        @Nullable
        private DatabaseClientLogEvents events;
        @Nullable
        private final Function<ReadTransaction, CompletableFuture<Long[]>> versionRangeProducer;
        private int eventCount;
        private final int eventCountLimit;
        private long startTimeMillis = System.currentTimeMillis();
        private final long timeLimitMillis;
        private boolean limitReached;

        public EventRunner(@Nonnull Database database, @Nonnull Executor executor, @Nonnull EventConsumer callback,
                           @Nonnull Function<ReadTransaction, CompletableFuture<Long[]>> versionRangeProducer,
                           int eventCountLimit, long timeLimitMillis) {
            this.database = database;
            this.executor = executor;
            this.callback = callback;
            this.versionRangeProducer = versionRangeProducer;
            this.eventCountLimit = eventCountLimit;
            this.timeLimitMillis = timeLimitMillis;
        }

        public EventRunner(@Nonnull Database database, @Nonnull Executor executor, @Nonnull EventConsumer callback,
                           @Nonnull DatabaseClientLogEvents events,
                           int eventCountLimit, long timeLimitMillis) {
            this.database = database;
            this.executor = executor;
            this.callback = callback;
            this.events = events;
            this.versionRangeProducer = null;
            this.eventCountLimit = eventCountLimit;
            this.timeLimitMillis = timeLimitMillis;
        }

        public CompletableFuture<DatabaseClientLogEvents> run() {
            return AsyncUtil.whileTrue(this::loop).thenApply(vignore -> {
                events.updateForRun(eventCount, limitReached);
                return events;
            });
        }

        private CompletableFuture<Boolean> loop() {
            tr = database.createTransaction(executor);
            final TransactionOptions transactionOptions = tr.options();
            transactionOptions.setReadSystemKeys();
            transactionOptions.setReadLockAware();
            if (events == null) {
                return versionRangeProducer.apply(tr).thenCompose(versions -> {
                    final Long startVersion = versions[0];
                    final byte[] startKey = startVersion == null ? FDBClientLogEvents.EVENT_KEY_PREFIX : FDBClientLogEvents.eventKeyForVersion(startVersion);
                    final Long endVersion = versions[1];
                    final byte[] endKey = endVersion == null ? ByteArrayUtil.strinc(FDBClientLogEvents.EVENT_KEY_PREFIX) : FDBClientLogEvents.eventKeyForVersion(endVersion);
                    events = new DatabaseClientLogEvents(startKey, endKey);
                    return loopBody();
                });
            } else {
                return loopBody();
            }
        }

        private CompletableFuture<Boolean> loopBody() {
            final AsyncIterable<KeyValue> range = events.getRange(tr);
            return FDBClientLogEvents.forEachEvent(range, this).thenApply(lastProcessedKey -> {
                events.updateForTransaction(lastProcessedKey);
                return false;   // Return to caller if range processed or limit reached.
            }).handle((b, t) -> {
                if (tr != null) {
                    tr.close();
                    tr = null;
                }
                if (t != null) {
                    if (t instanceof FDBException && ((FDBException)t).isRetryable()) {
                        return true;    // Continue with new transaction when too old (or too new).
                    } else {
                        throw t instanceof RuntimeException ? (RuntimeException)t : new RuntimeException(t);
                    }
                } else {
                    return b;
                }
            });
        }

        @Override
        public CompletableFuture<Void> accept(FDBClientLogEvents.Event event) {
            eventCount++;
            events.updateForEvent(event.getStartTimestamp());
            return callback.accept(tr, event);
        }

        @Override
        public boolean more() {
            if (eventCount >= eventCountLimit || System.currentTimeMillis() - startTimeMillis >= timeLimitMillis) {
                limitReached = true;
            }
            return !limitReached;
        }
    }

    private AsyncIterable<KeyValue> getRange(@Nonnull ReadTransaction tr) {
        return tr.getRange(startKey, endKey);
    }

    private void updateForEvent(@Nonnull Instant eventTimestamp) {
        if (earliestTimestamp == null) {
            earliestTimestamp = eventTimestamp;
        }
        latestTimestamp = eventTimestamp;
    }

    private void updateForTransaction(@Nullable byte[] lastProcessedKey) {
        if (lastProcessedKey != null) {
            startKey = ByteArrayUtil.join(lastProcessedKey, new byte[1]);   // The immediately following key.
        } else {
            startKey = endKey;  // Empty range.
        }
    }

    private void updateForRun(int rangeEventCount, boolean limitReached) {
        eventCount += rangeEventCount;
        more = limitReached;    // Otherwise range was processed, possibly in multiple transactions.
    }

    @Nonnull
    public static CompletableFuture<DatabaseClientLogEvents> forEachEvent(@Nonnull Database database, @Nonnull Executor executor,
                                                                          @Nonnull EventConsumer callback,
                                                                          @Nonnull Function<ReadTransaction, CompletableFuture<Long[]>> versionRangeProducer,
                                                                          int eventCountLimit, long timeLimitMillis) {
        final EventRunner runner = new EventRunner(database, executor, callback, versionRangeProducer,
                                                   eventCountLimit, timeLimitMillis);
        return runner.run();
    }

    /**
     * Apply a callback to client latency events recorded in the given database between two commit versions.
     * @param database the database to open and read events from
     * @param executor executor to use when running transactions
     * @param callback the callback to apply
     * @param startVersion the starting commit version
     * @param endVersion the exclusive end version
     * @param eventCountLimit the maximum number of events to process before returning
     * @param timeLimitMillis the maximum time to process before returning
     * @return a future which completes when the version range has been processed by the callback with an object that can be used to resume the scan
     */
    @Nonnull
    public static CompletableFuture<DatabaseClientLogEvents> forEachEventBetweenVersions(@Nonnull Database database, @Nonnull Executor executor,
                                                                                         @Nonnull EventConsumer callback,
                                                                                         @Nullable Long startVersion, @Nullable Long endVersion,
                                                                                         int eventCountLimit, long timeLimitMillis) {
        return forEachEvent(database, executor, callback,
                tignore -> CompletableFuture.completedFuture(new Long[] { startVersion, endVersion }),
                eventCountLimit, timeLimitMillis);
    }

    /**
     * Apply a callback to client latency events recorded in the given database between two commit versions.
     * @param database the database to open and read events from
     * @param executor executor to use when running transactions
     * @param callback the callback to apply
     * @param startTimestamp the starting wall-clock time
     * @param endTimestamp the exclusive end time
     * @param eventCountLimit the maximum number of events to process before returning
     * @param timeLimitMillis the maximum time to process before returning
     * @return a future which completes when the version range has been processed by the callback with an object that can be used to resume the scan
     */
    @Nonnull
    public static CompletableFuture<DatabaseClientLogEvents> forEachEventBetweenTimestamps(@Nonnull Database database, @Nonnull Executor executor,
                                                                                           @Nonnull EventConsumer callback,
                                                                                           @Nullable Instant startTimestamp, @Nullable Instant endTimestamp,
                                                                                           int eventCountLimit, long timeLimitMillis) {
        return forEachEvent(database, executor, callback, tr -> {
            final CompletableFuture<Long> startVersion = startTimestamp == null ? CompletableFuture.completedFuture(null) :
                                                         VersionFromTimestamp.lastVersionBefore(tr, startTimestamp);
            final CompletableFuture<Long> endVersion = endTimestamp == null ? CompletableFuture.completedFuture(null) :
                                                       VersionFromTimestamp.nextVersionAfter(tr, endTimestamp);
            return startVersion.thenCombine(endVersion, (s, e) -> new Long[] { s, e });
        },
                eventCountLimit, timeLimitMillis);
    }

    /**
     * Apply a callback to client latency events following an early return due to reaching a limit.
     * @param database the database to open and read events from
     * @param executor executor to use when running transactions
     * @param callback the callback to apply
     * @param eventCountLimit the maximum number of events to process before returning
     * @param timeLimitMillis the maximum time to process before returning
     * @return a future which completes when the version range has been processed by the callback with an object that can be used to resume the scan again
     */
    public CompletableFuture<DatabaseClientLogEvents> forEachEventContinued(@Nonnull Database database, @Nonnull Executor executor,
                                                                            @Nonnull EventConsumer callback,
                                                                            int eventCountLimit, long timeLimitMillis) {
        final EventRunner runner = new EventRunner(database, executor, callback, this,
                                                   eventCountLimit, timeLimitMillis);
        return runner.run();
    }
}
