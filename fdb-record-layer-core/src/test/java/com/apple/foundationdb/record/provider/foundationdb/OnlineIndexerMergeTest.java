/*
 * OnlineIndexerMergeTest.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.FDBError;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreTimeoutException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.indexes.InvalidIndexEntry;
import com.apple.foundationdb.record.provider.foundationdb.indexing.IndexingHeartbeat;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.foundationdb.synchronizedsession.SynchronizedSessionLockedException;
import com.apple.test.Tags;
import com.google.auto.service.AutoService;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests specifically of {@link OnlineIndexer#mergeIndex()}.
 */
@Tag(Tags.RequiresFDB)
public class OnlineIndexerMergeTest extends FDBRecordStoreConcurrentTestBase {

    private static final String INDEX_NAME = "mergableIndex";

    /**
     * Test repartitioning that doesn't fail, but does say that the amount repartitioned hit limits.
     * It should keep retrying until the maintainer no longer says that repartitioning was capped.
     */
    @Test
    void testRepartitionCapped() {
        final String indexType = "mergeLimitedIndex";
        List<Integer> repartitionLimits = new ArrayList<>();
        List<Long> mergeLimits = new ArrayList<>();
        AtomicReference<FDBRecordContext> lastContext = new AtomicReference<>();
        AtomicInteger toRepartition = new AtomicInteger(100);
        TestFactory.register(indexType, state -> {
            final IndexDeferredMaintenanceControl mergeControl = state.store.getIndexDeferredMaintenanceControl();
            repartitionLimits.add(mergeControl.getRepartitionDocumentCount());
            if (mergeControl.getRepartitionDocumentCount() == 0) {
                mergeControl.setRepartitionDocumentCount(9);
            }
            toRepartition.getAndUpdate(existing -> Math.max(0, existing - 9));
            mergeControl.setRepartitionCapped(toRepartition.get() > 0);
            mergeLimits.add(mergeControl.getMergesLimit());
            mergeControl.setLastStep(IndexDeferredMaintenanceControl.LastStep.MERGE);
            assertEquals(0, mergeControl.getMergesLimit());
            mergeControl.setMergesTried(17);
            mergeControl.setMergesFound(17);
            assertNotEquals(lastContext.getAndSet(state.context), state.context);
            return AsyncUtil.DONE;
        });
        final FDBRecordStore.Builder storeBuilder = createStore(indexType);
        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setRecordStoreBuilder(storeBuilder)
                .setTargetIndexesByName(List.of(INDEX_NAME))
                .setMaxAttempts(11)
                .build()) {
            indexer.mergeIndex();
        }
        assertEquals(repeat(0L, 100 / 9 + 1), mergeLimits);
        assertEquals(repeat(0, mergeLimits.size()), repartitionLimits);
    }

    /**
     * If repartitioning fails reliably it should retry at each limit {@code indexer.getMaxAttempts} times, until the
     * repartition limit is -1, at which point the index maintainer should just do merging.
     */
    @Test
    void testRepartitionTimeout() {
        final String indexType = "repartitionTimeoutIndex";
        List<Integer> repartitionLimits = new ArrayList<>();
        AtomicReference<FDBRecordContext> lastContext = new AtomicReference<>();
        TestFactory.register(indexType, state -> {
            final IndexDeferredMaintenanceControl mergeControl = state.store.getIndexDeferredMaintenanceControl();
            repartitionLimits.add(mergeControl.getRepartitionDocumentCount());
            if (mergeControl.getRepartitionDocumentCount() < 0) {
                // here, we are effectively failing before we can even find merges
                mergeControl.setLastStep(IndexDeferredMaintenanceControl.LastStep.MERGE);
            } else {
                mergeControl.setLastStep(IndexDeferredMaintenanceControl.LastStep.REPARTITION);
            }
            if (mergeControl.getRepartitionDocumentCount() == 0) {
                mergeControl.setRepartitionDocumentCount(17);
            }
            final CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(new FDBException("Timeout", FDBError.TRANSACTION_TOO_OLD.code()));
            assertNotEquals(lastContext.getAndSet(state.context), state.context);
            return future;
        });
        final FDBRecordStore.Builder storeBuilder = createStore(indexType);
        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setRecordStoreBuilder(storeBuilder)
                .setTargetIndexesByName(List.of(INDEX_NAME))
                .setMaxAttempts(9)
                .build()) {
            Assertions.assertThrows(FDBExceptions.FDBStoreTransactionIsTooOldException.class, indexer::mergeIndex);
        }
        assertEquals(
                Stream.of(0, 8, 4, 2, 1, -1).flatMap(repartitionLimit ->
                        Stream.generate(() -> repartitionLimit).limit(9)).collect(Collectors.toList()),
                repartitionLimits);
    }

    /**
     * If merge fails reliably it should retry at each limit {@code indexer.getMaxAttempts} times, until the limit would
     * be below 0.
     */
    @Test
    void testMergeTimeout() {
        final String indexType = "mergeTimeoutIndex";
        List<Integer> repartitionLimits = new ArrayList<>();
        List<Long> mergeLimits = new ArrayList<>();
        AtomicReference<FDBRecordContext> lastContext = new AtomicReference<>();
        TestFactory.register(indexType, state -> {
            final IndexDeferredMaintenanceControl mergeControl = state.store.getIndexDeferredMaintenanceControl();
            repartitionLimits.add(mergeControl.getRepartitionDocumentCount());
            mergeLimits.add(mergeControl.getMergesLimit());
            mergeControl.setLastStep(IndexDeferredMaintenanceControl.LastStep.MERGE);
            if (mergeControl.getMergesLimit() == 0) {
                mergeControl.setMergesTried(17);
                mergeControl.setMergesFound(17);
            } else {
                mergeControl.setMergesTried(mergeControl.getMergesLimit());
                mergeControl.setMergesFound(17);
            }
            final CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(new FDBException("Timeout", FDBError.TRANSACTION_TOO_OLD.code()));
            assertNotEquals(lastContext.getAndSet(state.context), state.context);
            return future;
        });
        final FDBRecordStore.Builder storeBuilder = createStore(indexType);
        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setRecordStoreBuilder(storeBuilder)
                .setTargetIndexesByName(List.of(INDEX_NAME))
                .setMaxAttempts(5)
                .build()) {
            Assertions.assertThrows(FDBExceptions.FDBStoreTransactionIsTooOldException.class, indexer::mergeIndex);
        }
        assertEquals(
                LongStream.of(0, 8, 4, 2, 1).boxed().flatMap(mergeLimit ->
                        Stream.generate(() -> mergeLimit).limit(5)).collect(Collectors.toList()),
                mergeLimits);
        assertEquals(repeat(0, mergeLimits.size()), repartitionLimits);
    }

    /**
     * Test that a RuntimeException (non-FDB, non-timeout) causes the merger to abort immediately.
     */
    @Test
    void testNonRetriableExceptionAborts() {
        final String indexType = "nonRetriableExceptionIndex";
        AtomicInteger attemptCount = new AtomicInteger(0);

        TestFactory.register(indexType, state -> {
            adjustMergeControl(state);
            attemptCount.incrementAndGet();

            // Throw a RuntimeException that is not retriable
            final CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalStateException("Non-retriable error"));
            return future;
        });

        final FDBRecordStore.Builder storeBuilder = createStore(indexType);
        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setRecordStoreBuilder(storeBuilder)
                .setTargetIndexesByName(List.of(INDEX_NAME))
                .setMaxAttempts(10)
                .build()) {
            Assertions.assertThrows(IllegalStateException.class, indexer::mergeIndex);
        }

        // Should only attempt once, no retries
        assertEquals(1, attemptCount.get());
    }

    /**
     * Test that a TimeoutException causes retry behavior (not abort).
     */
    @ParameterizedTest
    @BooleanSource
    void testTimeoutExceptionRetries(boolean customTimeout) {
        final String indexType = "timeoutExceptionIndex";
        final String exceptionMessage = "my timeout";
        AtomicInteger attemptCount = new AtomicInteger(0);

        TestFactory.register(indexType, state -> {
            adjustMergeControl(state);

            attemptCount.incrementAndGet();

            // Throw a TimeoutException (not FDB timeout)
            final CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(
                    customTimeout ?
                    new CustomOperationTimeoutException(exceptionMessage) :
                    new TimeoutException(exceptionMessage));
            return future;
        });

        final FDBRecordStore.Builder storeBuilder = createStore(indexType);
        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setRecordStoreBuilder(storeBuilder)
                .setTargetIndexesByName(List.of(INDEX_NAME))
                .build()) {
            Exception thrownException = Assertions.assertThrows(Exception.class, indexer::mergeIndex);
            // Assert that the timeout exception is in the cause chain
            var timeoutCause =
                    customTimeout ?
                    TestHelpers.findCause(thrownException, CustomOperationTimeoutException.class) :
                    TestHelpers.findCause(thrownException, TimeoutException.class);
            Assertions.assertNotNull(timeoutCause);
            Assertions.assertEquals(exceptionMessage, timeoutCause.getMessage());
        }
        // Assert multiple retries
        assertTrue(1 < attemptCount.get());
    }

    private static void adjustMergeControl(final IndexMaintainerState state) {
        final IndexDeferredMaintenanceControl mergeControl = state.store.getIndexDeferredMaintenanceControl();
        mergeControl.setLastStep(IndexDeferredMaintenanceControl.LastStep.MERGE);
        if (mergeControl.getMergesLimit() == 0) {
            mergeControl.setMergesTried(10);
            mergeControl.setMergesFound(10);
        } else {
            mergeControl.setMergesTried(mergeControl.getMergesLimit());
            mergeControl.setMergesFound(10);
        }
    }

    /**
     * Test that an FDBException wrapped in another exception still triggers retry behavior.
     */
    static Stream<Arguments> testWrappedFDBExceptionRetries() {
        return Stream.of(
                Arguments.of( FDBError.TRANSACTION_TOO_OLD.code(), true),
                Arguments.of(FDBError.COMMIT_READ_INCOMPLETE.code(), true),
                Arguments.of( FDBError.NO_CLUSTER_FILE_FOUND.code(), false)
        );
    }

    @ParameterizedTest
    @MethodSource("testWrappedFDBExceptionRetries")
    void testWrappedFDBExceptionRetries(int fdbErrorCode, boolean shouldRetry) {
        final String indexType = "wrappedFdbExceptionIndex";
        AtomicInteger attemptCount = new AtomicInteger(0);

        TestFactory.register(indexType, state -> {
            adjustMergeControl(state);

            attemptCount.incrementAndGet();

            // Wrap FDBException in multiple layers to test deep unwrapping
            final CompletableFuture<Void> future = new CompletableFuture<>();
            final FDBException fdbEx = new FDBException("my FDB Exception", fdbErrorCode);
            RuntimeException wrapper = new RuntimeException("Wrapper level 1",
                    new IllegalStateException("Wrapper level 2", fdbEx));
            future.completeExceptionally(wrapper);
            return future;
        });

        final FDBRecordStore.Builder storeBuilder = createStore(indexType);
        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setRecordStoreBuilder(storeBuilder)
                .setTargetIndexesByName(List.of(INDEX_NAME))
                .setMaxAttempts(5)
                .build()) {

            Exception thrownException = Assertions.assertThrows(Exception.class, indexer::mergeIndex);

            // Assert that FDBException is in the cause chain
            FDBException fdbCause = TestHelpers.findCause(thrownException, FDBException.class);
            Assertions.assertNotNull(fdbCause);
            assertEquals(fdbErrorCode, fdbCause.getCode());
        }

        // Assert multiple retries
        if (shouldRetry) {
            assertTrue(1 < attemptCount.get());
        } else {
            assertEquals(1, attemptCount.get());
        }
    }

    /**
     * Test that a non-retriable exception during REPARTITION phase causes immediate abort.
     */
    @Test
    void testNonRetriableExceptionDuringRepartitionAborts() {
        final String indexType = "nonRetriableRepartitionIndex";
        AtomicInteger attemptCount = new AtomicInteger(0);

        TestFactory.register(indexType, state -> {
            final IndexDeferredMaintenanceControl mergeControl = state.store.getIndexDeferredMaintenanceControl();
            mergeControl.setLastStep(IndexDeferredMaintenanceControl.LastStep.REPARTITION);
            mergeControl.setRepartitionDocumentCount(20);

            attemptCount.incrementAndGet();

            // Throw a non-retriable exception during repartition
            final CompletableFuture<Void> future = new CompletableFuture<>();
            future.completeExceptionally(new NullPointerException("Unexpected null during repartition"));
            return future;
        });

        final FDBRecordStore.Builder storeBuilder = createStore(indexType);
        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setRecordStoreBuilder(storeBuilder)
                .setTargetIndexesByName(List.of(INDEX_NAME))
                .setMaxAttempts(10)
                .build()) {
            Assertions.assertThrows(NullPointerException.class, indexer::mergeIndex);
        }

        // Should only attempt once, no retries
        assertEquals(1, attemptCount.get());
    }

    @Test
    void testMergePreCommitCallbackUpdatesHeartbeatDuringBuild() {
        final String indexType = "heartbeatDuringBuildIndex";
        final AtomicBoolean callbackPresent = new AtomicBoolean(false);
        final AtomicReference<Map<UUID, IndexBuildProto.IndexBuildHeartbeat>> heartbeatsSeen = new AtomicReference<>();
        TestFactory.register(indexType, observeHeartbeatDuringMerge(callbackPresent, heartbeatsSeen), true);

        final FDBRecordStore.Builder storeBuilder = createStoreWithUnbuiltIndex(indexType, 20);
        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setRecordStoreBuilder(storeBuilder)
                .setTargetIndexesByName(List.of(INDEX_NAME))
                .build()) {
            indexer.buildIndex();
        }

        assertThat(callbackPresent).isTrue();
        // The write-only index of this ongoing build has exactly one heartbeat: this indexer's
        assertThat(heartbeatsSeen.get()).hasSize(1);
        final IndexBuildProto.IndexBuildHeartbeat heartbeat = heartbeatsSeen.get().values().iterator().next();
        assertThat(heartbeat.getInfo()).isEqualTo(IndexBuildProto.IndexBuildIndexingStamp.Method.BY_RECORDS.toString());
        assertThat(heartbeat.getHeartbeatTimeMilliseconds()).isPositive();
        // And it is cleared once the index becomes readable
        assertNoHeartbeatsLeft(storeBuilder);
    }

    @Test
    void testMergePreCommitCallbackSkipsHeartbeatForRegularMerge() {
        final String indexType = "heartbeatRegularMergeIndex";
        final AtomicBoolean callbackPresent = new AtomicBoolean(false);
        final AtomicReference<Map<UUID, IndexBuildProto.IndexBuildHeartbeat>> heartbeatsSeen = new AtomicReference<>();
        TestFactory.register(indexType, observeHeartbeatDuringMerge(callbackPresent, heartbeatsSeen));

        final FDBRecordStore.Builder storeBuilder = createStore(indexType);
        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setRecordStoreBuilder(storeBuilder)
                .setTargetIndexesByName(List.of(INDEX_NAME))
                .build()) {
            indexer.mergeIndex();
        }

        assertThat(callbackPresent).isTrue();
        assertThat(heartbeatsSeen.get()).isEmpty();
        assertNoHeartbeatsLeft(storeBuilder);
    }

    @ParameterizedTest
    @EnumSource(value = IndexState.class, names = {"WRITE_ONLY", "WRITE_ONLY_WITH_QUEUE"})
    void testStandaloneMergeBlockedByActiveIndexingSession(IndexState indexState) {
        final String indexType = "mergeBlockedByActiveSessionIndex";
        final AtomicBoolean mergeRan = new AtomicBoolean(false);
        TestFactory.register(indexType, recordMergeRan(mergeRan));

        final FDBRecordStore.Builder storeBuilder = createStore(indexType);
        setIndexState(storeBuilder, indexState);
        addForeignHeartbeat(storeBuilder, 0);
        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setRecordStoreBuilder(storeBuilder)
                .setTargetIndexesByName(List.of(INDEX_NAME))
                .build()) {
            Assertions.assertThrows(SynchronizedSessionLockedException.class, indexer::mergeIndex);
        }
        assertThat(mergeRan).isFalse();
    }

    @ParameterizedTest
    @EnumSource(value = IndexState.class, names = {"WRITE_ONLY", "WRITE_ONLY_WITH_QUEUE"})
    void testStandaloneMergeIgnoresExpiredIndexingSession(IndexState indexState) {
        final String indexType = "mergeIgnoresExpiredSessionIndex";
        final AtomicBoolean mergeRan = new AtomicBoolean(false);
        TestFactory.register(indexType, recordMergeRan(mergeRan));

        final FDBRecordStore.Builder storeBuilder = createStore(indexType);
        setIndexState(storeBuilder, indexState);
        // A heartbeat that is far older than the lease length
        addForeignHeartbeat(storeBuilder, TimeUnit.HOURS.toMillis(1));
        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setRecordStoreBuilder(storeBuilder)
                .setTargetIndexesByName(List.of(INDEX_NAME))
                .build()) {
            indexer.mergeIndex();
        }
        assertThat(mergeRan).isTrue();
    }

    /**
     * Indexing sessions only run (and keep heartbeats) while the index is write-only. In any other state, a heartbeat
     * does not prevent a stand-alone merge.
     */
    @ParameterizedTest
    @EnumSource(value = IndexState.class, names = {"READABLE", "READABLE_UNIQUE_PENDING", "DISABLED"})
    void testStandaloneMergeOfNonWriteOnlyIndexIgnoresHeartbeats(IndexState indexState) {
        final String indexType = "mergeReadableIgnoresHeartbeatsIndex";
        final AtomicBoolean mergeRan = new AtomicBoolean(false);
        TestFactory.register(indexType, recordMergeRan(mergeRan));

        // Unique, so that the index can be made readable-unique-pending
        final FDBRecordStore.Builder storeBuilder = createStore(indexType, Map.of(IndexOptions.UNIQUE_OPTION, "true"));
        setIndexState(storeBuilder, indexState);
        addForeignHeartbeat(storeBuilder, 0);
        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setRecordStoreBuilder(storeBuilder)
                .setTargetIndexesByName(List.of(INDEX_NAME))
                .build()) {
            indexer.mergeIndex();
        }
        assertThat(mergeRan).isTrue();
    }

    @Test
    void testEnqueueFromIndexerWaitsForMergeLockLeaseBeforeRetry() {
        final String indexType = "enqueueWhileMergeLockTakenIndex";
        final long leaseRemainingMillis = 300;
        // Long enough to tell apart a delay that falls back to the heartbeat's lease length
        final long heartbeatLeaseMillis = 60_000;
        final AtomicLong lastLockTakenNanos = new AtomicLong(0);
        final AtomicLong retryNanos = new AtomicLong(0);
        final AtomicBoolean enqueueAttempted = new AtomicBoolean(false);
        // Another process holds the merge lock, hence every merge attempt fails
        final Function<IndexMaintainerState, CompletableFuture<Void>> mergeLockTaken = state -> {
            adjustMergeControl(state);
            lastLockTakenNanos.set(System.nanoTime());
            throw new FDBExceptions.FDBStoreLockTakenException("Lock taken", null, leaseRemainingMillis);
        };
        // The first record update attempts to enqueue, the following ones (after the retry) succeed
        final Function<IndexMaintainerState, CompletableFuture<Void>> enqueueOnce = state -> {
            if (enqueueAttempted.compareAndSet(false, true)) {
                throw new IndexingBase.EnqueueFromIndexerException("Indexer attempted to enqueue a record", state.index);
            }
            retryNanos.compareAndSet(0, System.nanoTime());
            return AsyncUtil.DONE;
        };
        TestFactory.register(indexType, mergeLockTaken, true, enqueueOnce);

        final FDBRecordStore.Builder storeBuilder = createStoreWithUnbuiltIndex(indexType, 20);
        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setRecordStoreBuilder(storeBuilder)
                .setTargetIndexesByName(List.of(INDEX_NAME))
                .setMaxAttempts(1)
                .setLeaseLengthMillis(heartbeatLeaseMillis)
                .build()) {
            indexer.buildIndex();
        }

        assertThat(enqueueAttempted).isTrue();
        assertThat(lastLockTakenNanos.get()).isPositive();
        assertThat(retryNanos.get()).isPositive();
        // The retry waited for the time left for the lock's lease, not for the heartbeat's lease length
        assertThat(TimeUnit.NANOSECONDS.toMillis(retryNanos.get() - lastLockTakenNanos.get()))
                .isGreaterThanOrEqualTo(leaseRemainingMillis)
                .isLessThan(heartbeatLeaseMillis / 2);
    }

    @Nonnull
    private static Function<IndexMaintainerState, CompletableFuture<Void>> recordMergeRan(final AtomicBoolean mergeRan) {
        return state -> {
            adjustMergeControl(state);
            mergeRan.set(true);
            return AsyncUtil.DONE;
        };
    }

    private void setIndexState(@Nonnull final FDBRecordStore.Builder storeBuilder, @Nonnull IndexState indexState) {
        try (FDBRecordContext context = openContext(RecordLayerPropertyStorage.getEmptyInstance())) {
            final FDBRecordStore store = storeBuilder.copyBuilder().setContext(context).open();
            final Index index = store.getRecordMetaData().getIndex(INDEX_NAME);
            switch (indexState) {
                case READABLE:
                    // A new store's index is readable
                    break;
                case WRITE_ONLY:
                    store.markIndexWriteOnly(index).join();
                    break;
                case WRITE_ONLY_WITH_QUEUE:
                    store.markIndexWriteOnly(index, IndexState.WRITE_ONLY_WITH_QUEUE).join();
                    break;
                case READABLE_UNIQUE_PENDING:
                    // The test maintainer reports a uniqueness violation for a unique index
                    store.markIndexWriteOnly(index).join();
                    store.markIndexReadableOrUniquePending(index).join();
                    break;
                case DISABLED:
                    store.markIndexDisabled(index).join();
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected index state " + indexState);
            }
            assertThat(store.getIndexState(index)).isEqualTo(indexState);
            context.commit();
        }
    }

    private void addForeignHeartbeat(@Nonnull final FDBRecordStore.Builder storeBuilder, long heartbeatAgeMillis) {
        try (FDBRecordContext context = openContext(RecordLayerPropertyStorage.getEmptyInstance())) {
            final FDBRecordStore store = storeBuilder.copyBuilder().setContext(context).open();
            final Index index = store.getRecordMetaData().getIndex(INDEX_NAME);
            // Written directly, as IndexingHeartbeat always stamps the current time
            final long heartbeatTimeMillis = System.currentTimeMillis() - heartbeatAgeMillis;
            final byte[] key = IndexingSubspaces.indexHeartbeatSubspaceBytes(store, index, UUID.randomUUID());
            final byte[] value = IndexBuildProto.IndexBuildHeartbeat.newBuilder()
                    .setInfo("Test")
                    .setCreateTimeMilliseconds(heartbeatTimeMillis)
                    .setHeartbeatTimeMilliseconds(heartbeatTimeMillis)
                    .build().toByteArray();
            store.ensureContextActive().set(key, value);
            context.commit();
        }
    }

    @Nonnull
    private static Function<IndexMaintainerState, CompletableFuture<Void>> observeHeartbeatDuringMerge(
            final AtomicBoolean callbackPresent,
            final AtomicReference<Map<UUID, IndexBuildProto.IndexBuildHeartbeat>> heartbeatsSeen) {
        return state -> {
            adjustMergeControl(state);
            final Function<FDBRecordStore, CompletableFuture<Void>> preCommitCallback =
                    state.store.getIndexDeferredMaintenanceControl().getPreCommitCallback();
            callbackPresent.set(preCommitCallback != null);
            if (preCommitCallback == null) {
                return AsyncUtil.DONE;
            }
            return preCommitCallback.apply(state.store)
                    .thenCompose(ignore -> IndexingHeartbeat.getIndexingHeartbeats(state.store, state.index, 0))
                    .thenAccept(heartbeatsSeen::set);
        };
    }

    @Nonnull
    private FDBRecordStore.Builder createStore(@Nonnull final String indexType) {
        return createStore(indexType, Map.of());
    }

    @Nonnull
    private FDBRecordStore.Builder createStore(@Nonnull final String indexType, @Nonnull final Map<String, String> indexOptions) {
        Index index = new Index(INDEX_NAME, Key.Expressions.field("num_value_2"),
                indexType, indexOptions);
        final RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.addIndex("MySimpleRecord", index);
        final RecordMetaData metadata = metaDataBuilder.getRecordMetaData();
        final KeySpacePath path = pathManager.createPath();
        FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext(RecordLayerPropertyStorage.getEmptyInstance())) {
            storeBuilder = createOrOpenRecordStore(context, metadata, path).getLeft().asBuilder();
            context.commit();
        }
        return storeBuilder;
    }

    @Nonnull
    private FDBRecordStore.Builder createStoreWithUnbuiltIndex(@Nonnull final String indexType, final int numRecords) {
        final FDBRecordStore.Builder storeBuilder = createStore(indexType);
        try (FDBRecordContext context = openContext(RecordLayerPropertyStorage.getEmptyInstance())) {
            final FDBRecordStore store = storeBuilder.copyBuilder().setContext(context).open();
            store.markIndexDisabled(INDEX_NAME).join();
            context.commit();
        }
        try (FDBRecordContext context = openContext(RecordLayerPropertyStorage.getEmptyInstance())) {
            // The index is disabled, hence saving these records does not call its maintainer
            final FDBRecordStore store = storeBuilder.copyBuilder().setContext(context).open();
            for (int i = 0; i < numRecords; i++) {
                store.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(i)
                        .setNumValue2(i % 5)
                        .build());
            }
            context.commit();
        }
        return storeBuilder;
    }

    private void assertNoHeartbeatsLeft(@Nonnull final FDBRecordStore.Builder storeBuilder) {
        try (FDBRecordContext context = openContext(RecordLayerPropertyStorage.getEmptyInstance())) {
            final FDBRecordStore store = storeBuilder.copyBuilder().setContext(context).open();
            final Index index = store.getRecordMetaData().getIndex(INDEX_NAME);
            assertThat(IndexingHeartbeat.getIndexingHeartbeats(store, index, 0).join()).isEmpty();
        }
    }

    private static <T> @Nonnull List<T> repeat(final T value, final int count) {
        return Stream.generate(() -> value).limit(count).collect(Collectors.toList());
    }


    /**
     * Singleton factory for tests in this class.
     */
    @AutoService(IndexMaintainerFactory.class)
    public static class TestFactory implements IndexMaintainerFactory {
        static Map<String, Function<IndexMaintainerState, IndexMaintainer>> maintainers = new HashMap<>();

        private static void register(String name, Function<IndexMaintainerState, CompletableFuture<Void>> mergeImplementation) {
            register(name, mergeImplementation, false);
        }

        private static void register(String name, Function<IndexMaintainerState, CompletableFuture<Void>> mergeImplementation,
                             boolean buildable) {
            register(name, mergeImplementation, buildable, null);
        }

        private static void register(String name, Function<IndexMaintainerState, CompletableFuture<Void>> mergeImplementation,
                                     boolean buildable,
                                     @Nullable Function<IndexMaintainerState, CompletableFuture<Void>> updateImplementation) {
            maintainers.put(name, state -> new IndexMaintainer(state) {
                @Nonnull
                @Override
                public RecordCursor<IndexEntry> scan(@Nonnull final IndexScanType scanType, @Nonnull final TupleRange range, @Nullable final byte[] continuation, @Nonnull final ScanProperties scanProperties) {
                    throw new UnsupportedOperationException();
                }

                @Nonnull
                @Override
                public <M extends Message> CompletableFuture<Void> update(@Nullable final FDBIndexableRecord<M> oldRecord, @Nullable final FDBIndexableRecord<M> newRecord) {
                    if (!buildable) {
                        throw new UnsupportedOperationException();
                    }
                    if (updateImplementation != null) {
                        return updateImplementation.apply(state);
                    }
                    state.store.getIndexDeferredMaintenanceControl().setMergeRequiredIndexes(state.index);
                    return AsyncUtil.DONE;
                }

                @Nonnull
                @Override
                public <M extends Message> CompletableFuture<Void> updateWhileWriteOnly(@Nullable final FDBIndexableRecord<M> oldRecord, @Nullable final FDBIndexableRecord<M> newRecord) {
                    if (!buildable) {
                        throw new UnsupportedOperationException();
                    }
                    return update(oldRecord, newRecord);
                }

                @Nonnull
                @Override
                public RecordCursor<IndexEntry> scanUniquenessViolations(@Nonnull final TupleRange range, @Nullable final byte[] continuation, @Nonnull final ScanProperties scanProperties) {
                    if (!state.index.isUnique()) {
                        throw new UnsupportedOperationException();
                    }
                    // A single violation: the value 1 is shared by the records 1 and 2
                    return RecordCursor.fromList(state.store.getExecutor(),
                            List.of(new IndexEntry(state.index, Tuple.from(1, 1L), Tuple.from(2L))), continuation);
                }

                @Override
                public CompletableFuture<Void> clearUniquenessViolations() {
                    throw new UnsupportedOperationException();
                }

                @Nonnull
                @Override
                public RecordCursor<InvalidIndexEntry> validateEntries(@Nullable final byte[] continuation, @Nullable final ScanProperties scanProperties) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean canEvaluateRecordFunction(@Nonnull final IndexRecordFunction<?> function) {
                    throw new UnsupportedOperationException();
                }

                @Nullable
                @Override
                public <M extends Message> List<IndexEntry> evaluateIndex(@Nonnull final FDBRecord<M> record) {
                    throw new UnsupportedOperationException();
                }

                @Nullable
                @Override
                public <M extends Message> List<IndexEntry> filteredIndexEntries(@Nullable final FDBIndexableRecord<M> savedRecord) {
                    throw new UnsupportedOperationException();
                }

                @Nonnull
                @Override
                public <T, M extends Message> CompletableFuture<T> evaluateRecordFunction(@Nonnull final EvaluationContext context, @Nonnull final IndexRecordFunction<T> function, @Nonnull final FDBRecord<M> record) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean canEvaluateAggregateFunction(@Nonnull final IndexAggregateFunction function) {
                    throw new UnsupportedOperationException();
                }

                @Nonnull
                @Override
                public CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull final IndexAggregateFunction function, @Nonnull final TupleRange range, @Nonnull final IsolationLevel isolationLevel) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean isIdempotent() {
                    if (!buildable) {
                        throw new UnsupportedOperationException();
                    }
                    return true;
                }

                @Nonnull
                @Override
                public CompletableFuture<Boolean> addedRangeWithKey(@Nonnull final Tuple primaryKey) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean canDeleteWhere(@Nonnull final QueryToKeyMatcher matcher, @Nonnull final Key.Evaluated evaluated) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public CompletableFuture<Void> deleteWhere(@Nonnull final Transaction tr, @Nonnull final Tuple prefix) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public CompletableFuture<IndexOperationResult> performOperation(@Nonnull final IndexOperation operation) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public CompletableFuture<Void> mergeIndex() {
                    return mergeImplementation.apply(state);
                }
            });
        }

        @Nonnull
        @Override
        public Iterable<String> getIndexTypes() {
            return List.of(
                    "repartitionTimeoutIndex",
                    "mergeLimitedIndex",
                    "nonRetriableExceptionIndex",
                    "wrappedFdbExceptionIndex",
                    "mergeTimeoutIndex",
                    "timeoutExceptionIndex",
                    "nonRetriableRepartitionIndex",
                    "heartbeatDuringBuildIndex",
                    "heartbeatRegularMergeIndex",
                    "mergeBlockedByActiveSessionIndex",
                    "mergeIgnoresExpiredSessionIndex",
                    "mergeReadableIgnoresHeartbeatsIndex",
                    "enqueueWhileMergeLockTakenIndex"
            );
        }

        @Nonnull
        @Override
        public IndexValidator getIndexValidator(Index index) {
            return new IndexValidator(index);
        }

        @Nonnull
        @Override
        public IndexMaintainer getIndexMaintainer(@Nonnull IndexMaintainerState state) {
            return maintainers.get(state.index.getType()).apply(state);
        }
    }

    @SuppressWarnings("serial")
    private static class CustomOperationTimeoutException extends RecordCoreTimeoutException {
        public CustomOperationTimeoutException(String message) {
            super(message);
        }
    }
}
