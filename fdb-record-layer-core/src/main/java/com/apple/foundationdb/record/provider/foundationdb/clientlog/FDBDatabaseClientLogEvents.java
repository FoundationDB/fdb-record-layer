/*
 * FDBDatabaseClientLogEvents.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.clientlog;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.TransactionOptions;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.clientlog.FDBClientLogEvents;
import com.apple.foundationdb.clientlog.VersionFromTimestamp;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Parse client latency events from system keyspace.
 */
@API(API.Status.EXPERIMENTAL)
public class FDBDatabaseClientLogEvents {
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
    public interface RecordEventConsumer {
        CompletableFuture<Void> accept(@Nonnull FDBRecordContext context, @Nonnull FDBClientLogEvents.Event event);
    }
    
    private FDBDatabaseClientLogEvents(@Nonnull byte[] startKey, @Nonnull byte[] endKey) {
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
        private final FDBDatabase database;
        @Nullable
        private FDBRecordContext context;
        @Nonnull
        private final RecordEventConsumer callback;
        @Nullable
        private FDBDatabaseClientLogEvents events;
        @Nullable
        private final Function<FDBRecordContext, CompletableFuture<Pair<Long, Long>>> versionRangeProducer;
        private int eventCount;
        private final int eventCountLimit;
        private long startTimeMillis = System.currentTimeMillis();
        private final long timeLimitMillis;
        private boolean limitReached;

        public EventRunner(@Nonnull FDBDatabase database, @Nonnull RecordEventConsumer callback,
                           @Nonnull Function<FDBRecordContext, CompletableFuture<Pair<Long, Long>>> versionRangeProducer,
                           int eventCountLimit, long timeLimitMillis) {
            this.database = database;
            this.callback = callback;
            this.versionRangeProducer = versionRangeProducer;
            this.eventCountLimit = eventCountLimit;
            this.timeLimitMillis = timeLimitMillis;
        }

        public EventRunner(@Nonnull FDBDatabase database, @Nonnull RecordEventConsumer callback,
                           @Nonnull FDBDatabaseClientLogEvents events,
                           int eventCountLimit, long timeLimitMillis) {
            this.database = database;
            this.callback = callback;
            this.events = events;
            this.versionRangeProducer = null;
            this.eventCountLimit = eventCountLimit;
            this.timeLimitMillis = timeLimitMillis;
        }

        public CompletableFuture<FDBDatabaseClientLogEvents> run() {
            return AsyncUtil.whileTrue(this::loop).thenApply(vignore -> {
                events.updateForRun(eventCount, limitReached);
                return events;
            });
        }

        private CompletableFuture<Boolean> loop() {
            context = database.openContext();
            final TransactionOptions transactionOptions = context.ensureActive().options();
            transactionOptions.setAccessSystemKeys();
            transactionOptions.setReadLockAware();
            if (events == null) {
                return versionRangeProducer.apply(context).thenCompose(versions -> {
                    final Long startVersion = versions.getLeft();
                    final byte[] startKey = startVersion == null ? FDBClientLogEvents.EVENT_KEY_PREFIX : FDBClientLogEvents.eventKeyForVersion(startVersion);
                    final Long endVersion = versions.getRight();
                    final byte[] endKey = endVersion == null ? ByteArrayUtil.strinc(FDBClientLogEvents.EVENT_KEY_PREFIX) : FDBClientLogEvents.eventKeyForVersion(endVersion);
                    events = new FDBDatabaseClientLogEvents(startKey, endKey);
                    return loopBody();
                });
            } else {
                return loopBody();
            }
        }

        private CompletableFuture<Boolean> loopBody() {
            final AsyncIterable<KeyValue> range = events.getRange(context);
            return FDBClientLogEvents.forEachEvent(range, this).thenApply(lastProcessedKey -> {
                events.updateForTransaction(lastProcessedKey);
                return false;   // Return to caller if range processed or limit reached.
            }).handle((b, t) -> {
                if (context != null) {
                    context.close();
                    context = null;
                }
                if (t != null) {
                    final RuntimeException ex = FDBExceptions.wrapException(t);
                    if (ex instanceof FDBExceptions.FDBStoreTransactionIsTooOldException) {
                        return true;    // Continue with new transaction when too old.
                    } else {
                        throw ex;
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
            return callback.accept(context, event);
        }

        @Override
        public boolean more() {
            if (eventCount >= eventCountLimit || System.currentTimeMillis() - startTimeMillis >= timeLimitMillis) {
                limitReached = true;
            }
            return !limitReached;
        }
    }

    private AsyncIterable<KeyValue> getRange(@Nonnull FDBRecordContext context) {
        return context.ensureActive().getRange(startKey, endKey);
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
    public static CompletableFuture<FDBDatabaseClientLogEvents> forEachEvent(@Nonnull FDBDatabase database,
                                                                             @Nonnull RecordEventConsumer callback,
                                                                             @Nonnull Function<FDBRecordContext, CompletableFuture<Pair<Long, Long>>> versionRangeProducer,
                                                                             int eventCountLimit, long timeLimitMillis) {
        final EventRunner runner = new EventRunner(database, callback, versionRangeProducer,
                                                   eventCountLimit, timeLimitMillis);
        return runner.run();
    }

    /**
     * Apply a callback to client latency events recorded in the given database between two commit versions.
     * @param database the database to open and read events from
     * @param callback the callback to apply
     * @param startVersion the starting commit version
     * @param endVersion the exclusive end version
     * @param eventCountLimit the maximum number of events to process before returning
     * @param timeLimitMillis the maximum time to process before returning
     * @return a future which completes when the version range has been processed by the callback with an object that can be used to resume the scan
     */
    @Nonnull
    public static CompletableFuture<FDBDatabaseClientLogEvents> forEachEventBetweenVersions(@Nonnull FDBDatabase database,
                                                                                            @Nonnull RecordEventConsumer callback,
                                                                                            @Nullable Long startVersion, @Nullable Long endVersion,
                                                                                            int eventCountLimit, long timeLimitMillis) {
        return forEachEvent(database, callback, cignore -> CompletableFuture.completedFuture(Pair.of(startVersion, endVersion)), eventCountLimit, timeLimitMillis);
    }

    /**
     * Apply a callback to client latency events recorded in the given database between two commit versions.
     * @param database the database to open and read events from
     * @param callback the callback to apply
     * @param startTimestamp the starting wall-clock time
     * @param endTimestamp the exclusive end time
     * @param eventCountLimit the maximum number of events to process before returning
     * @param timeLimitMillis the maximum time to process before returning
     * @return a future which completes when the version range has been processed by the callback with an object that can be used to resume the scan
     */
    @Nonnull
    public static CompletableFuture<FDBDatabaseClientLogEvents> forEachEventBetweenTimestamps(@Nonnull FDBDatabase database,
                                                                                              @Nonnull RecordEventConsumer callback,
                                                                                              @Nullable Instant startTimestamp, @Nullable Instant endTimestamp,
                                                                                              int eventCountLimit, long timeLimitMillis) {
        return forEachEvent(database, callback, context -> {
            final CompletableFuture<Long> startVersion = startTimestamp == null ? CompletableFuture.completedFuture(null) :
                                                         VersionFromTimestamp.lastVersionBefore(context.readTransaction(false), startTimestamp);
            final CompletableFuture<Long> endVersion = endTimestamp == null ? CompletableFuture.completedFuture(null) :
                                                       VersionFromTimestamp.nextVersionAfter(context.readTransaction(false), endTimestamp);
            return startVersion.thenCombine(endVersion, Pair::of);
        },
                eventCountLimit, timeLimitMillis);
    }

    /**
     * Apply a callback to client latency events following an early return due to reaching a limit.
     * @param database the database to open and read events from
     * @param callback the callback to apply
     * @param eventCountLimit the maximum number of events to process before returning
     * @param timeLimitMillis the maximum time to process before returning
     * @return a future which completes when the version range has been processed by the callback with an object that can be used to resume the scan again
     */
    public CompletableFuture<FDBDatabaseClientLogEvents> forEachEventContinued(@Nonnull FDBDatabase database,
                                                                               @Nonnull RecordEventConsumer callback,
                                                                               int eventCountLimit, long timeLimitMillis) {
        final EventRunner runner = new EventRunner(database, callback, this,
                                                   eventCountLimit, timeLimitMillis);
        return runner.run();
    }

    @SuppressWarnings("PMD.SystemPrintln")
    public static void main(String[] args) {
        String cluster = null;
        Instant start = null;
        Instant end = null;
        if (args.length > 0) {
            cluster = args[0];
        }
        if (args.length > 1) {
            start = ZonedDateTime.parse(args[1]).toInstant();
        }
        if (args.length > 2) {
            end = ZonedDateTime.parse(args[2]).toInstant();
        }
        FDBDatabase database = FDBDatabaseFactory.instance().getDatabase(cluster);
        RecordEventConsumer consumer = (context, event) -> {
            System.out.println(event);
            return AsyncUtil.DONE;
        };
        forEachEventBetweenTimestamps(database, consumer, start, end, Integer.MAX_VALUE, Long.MAX_VALUE).join();
    }
}
