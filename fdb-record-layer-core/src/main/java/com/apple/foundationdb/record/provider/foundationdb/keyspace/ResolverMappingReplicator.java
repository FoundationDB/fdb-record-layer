/*
 * ResolverMappingReplicator.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.keyspace;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.KeyValueCursor;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.function.Function;

/**
 * Copies the contents of one {@link LocatableResolver} to another. This can be useful for migrating between different
 * underlying {@link LocatableResolver} implementations. The copy operation is not transactional so the resulting copy
 * will not be a consistent snapshot of the original. It is the responsibility of the caller to ensure that the  copied
 * snapshot is consistent by, for example, preventing any writes to the resolver
 * (see {@link LocatableResolver#enableWriteLock()}). After copying, the allocation window on the replica
 * will be set to the largest value seen during the copy operation. This means that all allocations in the replica will
 * be larger than any value present in the original. Note, copying to {@link ScopedDirectoryLayer} is not supported.
 */
@API(API.Status.EXPERIMENTAL)
public class ResolverMappingReplicator implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResolverMappingReplicator.class);
    @Nonnull
    private final FDBDatabaseRunner runner;
    @Nonnull
    private final LocatableResolver primary;
    @Nonnull
    private final Function<byte[], ResolverResult> valueDeserializer;
    private final int transactionRowLimit;
    private final long transactionTimeLimitMillis;

    public ResolverMappingReplicator(@Nonnull LocatableResolver primary) {
        this(primary, 10_000, 4000L);
    }

    public ResolverMappingReplicator(@Nonnull LocatableResolver primary,
                                     final int transactionRowLimit) {
        this(primary, transactionRowLimit, 4000L);
    }

    public ResolverMappingReplicator(@Nonnull LocatableResolver primary,
                                     final int transactionRowLimit,
                                     final long transactionTimeLimitMillis) {
        this.runner = primary.getDatabase().newRunner();
        this.primary = primary;
        this.valueDeserializer = primary::deserializeValue;
        this.transactionRowLimit = transactionRowLimit;
        this.transactionTimeLimitMillis = transactionTimeLimitMillis;
    }

    @Override
    public void close() {
        runner.close();
    }

    /**
     * Copy the mappings stored in a {@link LocatableResolver} to the specified replica.
     *
     * @param replica The {@link LocatableResolver} to copy to.
     */
    public void copyTo(LocatableResolver replica) {
        runner.asyncToSync(FDBStoreTimer.Waits.WAIT_LOCATABLE_RESOLVER_MAPPING_COPY, copyToAsync(replica));
    }

    /**
     * Copy the mappings stored in a {@link LocatableResolver} to the specified replica.
     *
     * @param replica The {@link LocatableResolver} to copy to.
     * @return A future that will complete when the copy is finished.
     */
    public CompletableFuture<Void> copyToAsync(@Nonnull final LocatableResolver replica) {
        if (!replica.getDatabase().equals(runner.getDatabase())) {
            throw new IllegalArgumentException("copy must be within same database");
        }

        final LongAccumulator maxAccumulator = new LongAccumulator(Long::max, 0L);
        final AtomicInteger counter = new AtomicInteger();
        return copyInternal(replica, maxAccumulator, counter)
                .thenCompose(ignore -> replica.setWindow(maxAccumulator.get()));
    }

    private CompletableFuture<Void> copyInternal(@Nonnull final LocatableResolver replica,
                                                 @Nonnull final LongAccumulator accumulator,
                                                 @Nonnull final AtomicInteger counter) {
        ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setReturnedRowLimit(transactionRowLimit)
                .setTimeLimit(transactionTimeLimitMillis)
                .setIsolationLevel(IsolationLevel.SNAPSHOT).build();

        final AtomicReference<byte[]> continuation = new AtomicReference<>(null);

        return AsyncUtil.whileTrue(() -> {
            final FDBRecordContext context = runner.openContext();

            return primary.getMappingSubspaceAsync().thenCompose(primaryMappingSubspace -> {
                RecordCursor<KeyValue> cursor = KeyValueCursor.Builder.withSubspace(primaryMappingSubspace)
                        .setScanProperties(new ScanProperties(executeProperties))
                        .setContext(context)
                        .setContinuation(continuation.get())
                        .build();

                return cursor.forEachResultAsync(result -> {
                    KeyValue kv = result.get();
                    final String mappedString = primaryMappingSubspace.unpack(kv.getKey()).getString(0);
                    final ResolverResult mappedValue = valueDeserializer.apply(kv.getValue());
                    accumulator.accumulate(mappedValue.getValue());
                    counter.incrementAndGet();
                    return replica.setMapping(context, mappedString, mappedValue);
                }).thenCompose(lastResult -> context.commitAsync().thenRun(() -> {
                    byte[] nextContinuationBytes = lastResult.getContinuation().toBytes();
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info(KeyValueLogMessage.of("committing batch",
                                        LogMessageKeys.SCANNED_SO_FAR, counter.get(),
                                        LogMessageKeys.NEXT_CONTINUATION, ByteArrayUtil2.loggable(nextContinuationBytes)));
                    }
                    continuation.set(nextContinuationBytes);
                }))
                .whenComplete((vignore, eignore) -> cursor.close())
                .thenApply(vignore -> Objects.nonNull(continuation.get()));
            });
        }, runner.getExecutor());
    }

    @Override
    public String toString() {
        return "Replicator from: " + primary;
    }

}
