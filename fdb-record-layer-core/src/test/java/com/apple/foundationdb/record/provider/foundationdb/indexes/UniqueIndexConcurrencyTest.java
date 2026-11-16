/*
 * UniqueIndexConcurrencyTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordIndexUniquenessViolation;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.auto.service.AutoService;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Stress tests that try to get two records with the same value committed despite a unique index on that value.
 *
 * <p>Within a single transaction, {@link StandardIndexMaintainer} relies on synchronizing the uniqueness check with the
 * write of the index entry, so that concurrent saves in the same transaction observe one another through
 * read-your-writes. This matters because a range read does not observe writes made after it was issued, even if
 * the read has not completed yet. By default, the store's concurrency manager also serializes all record mutations,
 * so the same-transaction tests run both with and without it, to exercise the synchronization on its own. Across
 * transactions, uniqueness relies on the read conflict range of the uniqueness check. These tests issue saves from
 * many threads at once to try to break both.
 */
@Tag(Tags.RequiresFDB)
class UniqueIndexConcurrencyTest extends FDBRecordStoreTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(UniqueIndexConcurrencyTest.class);

    private static final int ITERATIONS = 50;
    private static final int WRITERS = 8;
    private static final int GROUPS_PER_TRANSACTION = 32;
    private static final int VALUES_PER_TRANSACTION = GROUPS_PER_TRANSACTION * WRITERS / 2;

    private static final String UNIQUE_INDEX_NAME = "MySimpleRecord$num_value_unique";
    private static final String UNSYNCHRONIZED_UNIQUE_VALUE = "unsynchronized_unique_value";
    private static final String UNSYNCHRONIZED_INDEX_NAME = "unsynchronized_num_value_unique";
    private static final RecordMetaDataHook UNSYNCHRONIZED_HOOK = metaDataBuilder -> {
        metaDataBuilder.removeIndex(UNIQUE_INDEX_NAME);
        metaDataBuilder.addIndex("MySimpleRecord", new Index(UNSYNCHRONIZED_INDEX_NAME, field("num_value_unique"),
                UNSYNCHRONIZED_UNIQUE_VALUE, IndexOptions.UNIQUE_OPTIONS));
    };

    /**
     * Writers block on a barrier so that they start together, which is fine here as the pool is unbounded.
     */
    private static final Executor WRITER_POOL = TestExecutors.defaultThreadPool();

    /**
     * Many writers save pairs of records that share a value in the same transaction at the same time.
     * Every such transaction must fail to commit.
     *
     * @param disableConcurrencyManagement whether to disable the store's serialization of record mutations
     */
    @ParameterizedTest
    @BooleanSource("disableConcurrencyManagement")
    void sameTransactionConcurrentInsertsAlwaysFail(boolean disableConcurrencyManagement) {
        int escapes = 0;
        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            try (FDBRecordContext context = openContext()) {
                openStore(context, NO_HOOK, disableConcurrencyManagement);
                saveConcurrentPairs(context, iteration);
                try {
                    commit(context);
                } catch (RecordIndexUniquenessViolation e) {
                    continue;
                }
            }
            escapes++;
            LOGGER.warn(KeyValueLogMessage.of("committed duplicate values despite unique index",
                    "iteration", iteration,
                    "duplicates", duplicateValues(NO_HOOK)));
        }
        assertThat(escapes)
                .as("transactions that committed duplicate values out of %d", ITERATIONS)
                .isZero();
    }

    /**
     * Many writers save pairs of records that share a value in the same transaction at the same time, while the index
     * is write-only. A uniqueness check then records the violation rather than throwing, so every pair whose checks
     * both missed each other shows up as a value without any recorded violation. Such a pair would let the index be
     * marked readable with duplicate entries.
     *
     * @param disableConcurrencyManagement whether to disable the store's serialization of record mutations
     */
    @ParameterizedTest
    @BooleanSource("disableConcurrencyManagement")
    void sameTransactionConcurrentInsertsWhileWriteOnlyRecordEveryViolation(boolean disableConcurrencyManagement) {
        final int missed = countMissedViolationsWhileWriteOnly(NO_HOOK, UNIQUE_INDEX_NAME, disableConcurrencyManagement);
        assertThat(missed)
                .as("duplicate pairs without a recorded violation out of %d", ITERATIONS * VALUES_PER_TRANSACTION)
                .isZero();
    }

    /**
     * The same as {@link #sameTransactionConcurrentInsertsWhileWriteOnlyRecordEveryViolation(boolean)}, but against a
     * maintainer that does not synchronize its uniqueness check with the write of the index entry. This does not
     * assert anything, because hitting the race is not deterministic, but the number of missed violations it logs
     * indicates whether these stress tests are capable of detecting a lost check at all.
     *
     * @param disableConcurrencyManagement whether to disable the store's serialization of record mutations
     */
    @ParameterizedTest
    @BooleanSource("disableConcurrencyManagement")
    void sameTransactionConcurrentInsertsWithoutSynchronization(boolean disableConcurrencyManagement) {
        UnsynchronizedUniqueValueIndexMaintainer.MAX_IN_FLIGHT.set(0);
        final int missed = countMissedViolationsWhileWriteOnly(UNSYNCHRONIZED_HOOK, UNSYNCHRONIZED_INDEX_NAME,
                disableConcurrencyManagement);
        LOGGER.info(KeyValueLogMessage.of("concurrent inserts without synchronization",
                "disableConcurrencyManagement", disableConcurrencyManagement,
                "missed", missed,
                "pairs", ITERATIONS * VALUES_PER_TRANSACTION,
                "maxInFlight", UnsynchronizedUniqueValueIndexMaintainer.MAX_IN_FLIGHT.get()));
    }

    /**
     * Each iteration starts with pairs of records {@code A = a} and {@code B = b}, and then concurrently, in one
     * transaction, moves {@code A} to {@code b} and {@code B} to a fresh value. This races the clear of {@code B}'s old
     * entry, which happens outside of the synchronized block, with the uniqueness check of {@code A}. Depending on
     * the order, the commit may or may not fail, but if it succeeds the index must be unique and consistent.
     * With {@code competingInsert}, a new record {@code C = b} is also saved at the same time, so the final state would
     * have two records with {@code b}, and the commit must fail.
     *
     * @param competingInsert whether to also insert a new record taking the value vacated by {@code B}
     * @param disableConcurrencyManagement whether to disable the store's serialization of record mutations
     */
    @ParameterizedTest
    @BooleanSource({"competingInsert", "disableConcurrencyManagement"})
    void sameTransactionConcurrentUpdates(boolean competingInsert, boolean disableConcurrencyManagement) {
        int committed = 0;
        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            final int iterationBase = iteration * GROUPS_PER_TRANSACTION * 3;
            try (FDBRecordContext context = openContext()) {
                openStore(context, NO_HOOK, disableConcurrencyManagement);
                // Start each iteration empty, so that checking consistency does not get slower as the test goes on.
                recordStore.deleteAllRecords();
                for (int group = 0; group < GROUPS_PER_TRANSACTION; group++) {
                    final int base = iterationBase + group * 3;
                    recordStore.saveRecord(record(base, base));
                    recordStore.saveRecord(record(base + 1, base + 1));
                }
                commit(context);
            }

            final int writers = competingInsert ? 3 : 2;
            try (FDBRecordContext context = openContext()) {
                openStore(context, NO_HOOK, disableConcurrencyManagement);
                final FDBRecordStore store = recordStore;
                runConcurrently(context, writers, writer -> {
                    final List<CompletableFuture<FDBStoredRecord<Message>>> saves = new ArrayList<>();
                    for (int group = 0; group < GROUPS_PER_TRANSACTION; group++) {
                        final int base = iterationBase + group * 3;
                        switch (writer) {
                            case 0:
                                saves.add(store.saveRecordAsync(record(base, base + 1)));
                                break;
                            case 1:
                                saves.add(store.saveRecordAsync(record(base + 1, base + 2)));
                                break;
                            default:
                                saves.add(store.saveRecordAsync(record(base + 2, base + 1)));
                                break;
                        }
                    }
                    return AsyncUtil.whenAll(saves);
                });
                if (competingInsert) {
                    assertThatThrownBy(() -> commit(context))
                            .as("commit with two records claiming each vacated value, iteration %d", iteration)
                            .isInstanceOf(RecordIndexUniquenessViolation.class);
                } else {
                    try {
                        commit(context);
                        committed++;
                    } catch (RecordIndexUniquenessViolation e) {
                        // The new entry for A was checked before the old entry for B was cleared.
                        continue;
                    }
                }
            }
            assertUniqueAndConsistent(NO_HOOK);
        }
        LOGGER.info(KeyValueLogMessage.of("concurrent updates",
                "competingInsert", competingInsert,
                "disableConcurrencyManagement", disableConcurrencyManagement,
                "committed", committed,
                "iterations", ITERATIONS));
    }

    /**
     * Many writers each save a distinct record with the same value in their own transaction, and commit at the same
     * time. At most one of them may succeed.
     */
    @Test
    void crossTransactionConcurrentInsertsAtMostOneCommits() {
        try (FDBRecordContext context = openContext()) {
            openStore(context, NO_HOOK, false);
            commit(context);
        }
        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            final int value = iteration;
            // Contexts are opened on the test thread, as opening one copies the test's MDC.
            final List<FDBRecordContext> contexts = new ArrayList<>();
            try {
                final List<FDBRecordStore> stores = new ArrayList<>();
                for (int writer = 0; writer < WRITERS; writer++) {
                    final FDBRecordContext context = openContext();
                    contexts.add(context);
                    stores.add(getStoreBuilder(context, simpleMetaData(NO_HOOK)).open());
                }
                final CyclicBarrier barrier = new CyclicBarrier(WRITERS);
                final List<CompletableFuture<Boolean>> outcomes = new ArrayList<>();
                for (int writer = 0; writer < WRITERS; writer++) {
                    final int writerForTask = writer;
                    outcomes.add(CompletableFuture.supplyAsync(() -> {
                        awaitBarrier(barrier);
                        return stores.get(writerForTask).saveRecordAsync(record((long)value * WRITERS + writerForTask, value))
                                .thenCompose(ignore -> contexts.get(writerForTask).commitAsync())
                                .handle((ignore, err) -> committed(err));
                    }, WRITER_POOL).thenCompose(outcome -> outcome));
                }
                final List<Boolean> committed = fdb.asyncToSync(timer, FDBStoreTimer.Waits.WAIT_COMMIT,
                        AsyncUtil.getAll(outcomes));
                assertThat(committed)
                        .as("transactions that committed value %d", value)
                        .filteredOn(Boolean::booleanValue)
                        .hasSizeLessThanOrEqualTo(1);
            } finally {
                contexts.forEach(FDBRecordContext::close);
            }
        }
        assertUniqueAndConsistent(NO_HOOK);
    }

    /**
     * Whether a commit that completed with the given error succeeded. A uniqueness violation or a conflict means it
     * lost to another writer, and any other error is rethrown.
     */
    private static boolean committed(@Nullable Throwable err) {
        if (err == null) {
            return true;
        }
        final RuntimeException wrapped = FDBExceptions.wrapException(err);
        if (wrapped instanceof RecordIndexUniquenessViolation
                || wrapped instanceof FDBExceptions.FDBStoreTransactionConflictException) {
            return false;
        }
        throw wrapped;
    }

    /**
     * Mark the index write-only, run {@link #ITERATIONS} transactions of {@link #saveConcurrentPairs}, and then count
     * the values that were saved twice but have no recorded uniqueness violation.
     */
    private int countMissedViolationsWhileWriteOnly(@Nonnull RecordMetaDataHook hook, @Nonnull String indexName,
                                                    boolean disableConcurrencyManagement) {
        try (FDBRecordContext context = openContext()) {
            openStore(context, hook, disableConcurrencyManagement);
            context.asyncToSync(FDBStoreTimer.Waits.WAIT_INDEX_OPERATION, recordStore.markIndexWriteOnly(indexName));
            commit(context);
        }
        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            try (FDBRecordContext context = openContext()) {
                openStore(context, hook, disableConcurrencyManagement);
                saveConcurrentPairs(context, iteration);
                commit(context);
            }
        }
        try (FDBRecordContext context = openContext()) {
            openStore(context, hook, disableConcurrencyManagement);
            final Index index = recordStore.getRecordMetaData().getIndex(indexName);
            final Set<Tuple> violatedValues = new HashSet<>(context.asyncToSync(FDBStoreTimer.Waits.WAIT_SCAN_INDEX_RECORDS,
                    recordStore.scanUniquenessViolations(index).map(violation -> violation.getIndexEntry().getKey()).asList()));
            int missed = 0;
            for (int value = 0; value < ITERATIONS * VALUES_PER_TRANSACTION; value++) {
                if (!violatedValues.contains(Tuple.from(value))) {
                    missed++;
                }
            }
            return missed;
        }
    }

    /**
     * Save records in pairs that share a value, with the two records of each pair saved by different writers at about
     * the same time, all in the given transaction.
     */
    private void saveConcurrentPairs(@Nonnull FDBRecordContext context, int iteration) {
        final int iterationBase = iteration * VALUES_PER_TRANSACTION;
        final FDBRecordStore store = recordStore;
        runConcurrently(context, WRITERS, writer -> {
            final List<CompletableFuture<FDBStoredRecord<Message>>> saves = new ArrayList<>();
            for (int group = 0; group < GROUPS_PER_TRANSACTION; group++) {
                final int value = iterationBase + group * (WRITERS / 2) + writer / 2;
                saves.add(store.saveRecordAsync(record(value * 2L + writer % 2, value)));
            }
            return AsyncUtil.whenAll(saves);
        });
    }

    /**
     * Have {@code writers} threads, released at the same moment, each start their share of work in the given
     * transaction, and then wait for all of it to complete.
     */
    private void runConcurrently(@Nonnull FDBRecordContext context, int writers,
                                 @Nonnull IntFunction<CompletableFuture<Void>> work) {
        final CyclicBarrier barrier = new CyclicBarrier(writers);
        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int writer = 0; writer < writers; writer++) {
            final int writerForTask = writer;
            futures.add(CompletableFuture.supplyAsync(() -> {
                awaitBarrier(barrier);
                return work.apply(writerForTask);
            }, WRITER_POOL).thenCompose(future -> future));
        }
        context.asyncToSync(FDBStoreTimer.Waits.WAIT_SAVE_RECORD, AsyncUtil.whenAll(futures));
    }

    private void openStore(@Nonnull FDBRecordContext context, @Nonnull RecordMetaDataHook hook,
                           boolean disableConcurrencyManagement) {
        recordStore = getStoreBuilder(context, simpleMetaData(hook))
                .setDisableConcurrencyManagement(disableConcurrencyManagement)
                .createOrOpen();
    }

    private static void awaitBarrier(@Nonnull CyclicBarrier barrier) {
        try {
            barrier.await(1, TimeUnit.MINUTES);
        } catch (Exception e) {
            throw new IllegalStateException("writers did not all start", e);
        }
    }

    @Nonnull
    private static TestRecords1Proto.MySimpleRecord record(long recNo, int numValueUnique) {
        return TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(recNo)
                .setNumValueUnique(numValueUnique)
                .build();
    }

    /**
     * Assert that no two committed records share a value, and that the unique index contains exactly one entry for
     * each record.
     */
    private void assertUniqueAndConsistent(@Nonnull RecordMetaDataHook hook) {
        assertThat(duplicateValues(hook)).as("values held by more than one record").isEmpty();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            final Set<Tuple> expectedEntries = new HashSet<>();
            for (TestRecords1Proto.MySimpleRecord rec : loadRecords(context)) {
                expectedEntries.add(Tuple.from(rec.getNumValueUnique(), rec.getRecNo()));
            }
            final List<IndexEntry> indexEntries = context.asyncToSync(FDBStoreTimer.Waits.WAIT_SCAN_INDEX_RECORDS,
                    recordStore.scanIndex(recordStore.getRecordMetaData().getIndex(UNIQUE_INDEX_NAME),
                            IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).asList());
            final List<Tuple> actualEntries = new ArrayList<>();
            for (IndexEntry entry : indexEntries) {
                actualEntries.add(entry.getKey());
            }
            assertThat(actualEntries).as("unique index entries").containsExactlyInAnyOrderElementsOf(expectedEntries);
        }
    }

    @Nonnull
    private Map<Integer, Set<Tuple>> duplicateValues(@Nonnull RecordMetaDataHook hook) {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            final Map<Integer, Set<Tuple>> byValue = new HashMap<>();
            for (TestRecords1Proto.MySimpleRecord rec : loadRecords(context)) {
                byValue.computeIfAbsent(rec.getNumValueUnique(), ignore -> new HashSet<>()).add(Tuple.from(rec.getRecNo()));
            }
            byValue.values().removeIf(primaryKeys -> primaryKeys.size() < 2);
            return byValue;
        }
    }

    @Nonnull
    private List<TestRecords1Proto.MySimpleRecord> loadRecords(@Nonnull FDBRecordContext context) {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_SCAN_RECORDS,
                recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN)
                        .map(rec -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).build())
                        .asList());
    }

    /**
     * Factory for {@link UnsynchronizedUniqueValueIndexMaintainer}.
     */
    @AutoService(IndexMaintainerFactory.class)
    public static class UnsynchronizedUniqueValueIndexMaintainerFactory implements IndexMaintainerFactory {
        @Nonnull
        @Override
        public Iterable<String> getIndexTypes() {
            return Collections.singletonList(UNSYNCHRONIZED_UNIQUE_VALUE);
        }

        @Nonnull
        @Override
        public IndexValidator getIndexValidator(Index index) {
            return new IndexValidator(index);
        }

        @Nonnull
        @Override
        public IndexMaintainer getIndexMaintainer(@Nonnull IndexMaintainerState state) {
            return new UnsynchronizedUniqueValueIndexMaintainer(state);
        }
    }

    /**
     * A {@link ValueIndexMaintainer} that writes index entries the same way as {@link StandardIndexMaintainer}, except
     * that it does not synchronize the uniqueness check with the write of the entry.
     */
    private static class UnsynchronizedUniqueValueIndexMaintainer extends ValueIndexMaintainer {
        /**
         * The most index entries this maintainer has seen being written at the same time, to tell whether the writers
         * actually overlapped.
         */
        static final AtomicInteger MAX_IN_FLIGHT = new AtomicInteger();
        private static final AtomicInteger IN_FLIGHT = new AtomicInteger();

        UnsynchronizedUniqueValueIndexMaintainer(@Nonnull IndexMaintainerState state) {
            super(state);
        }

        @Nonnull
        @Override
        protected <M extends Message> CompletableFuture<Void> updateOneKeyAsync(@Nonnull FDBIndexableRecord<M> savedRecord,
                                                                                boolean remove,
                                                                                @Nonnull IndexEntry indexEntry) {
            if (remove) {
                return super.updateOneKeyAsync(savedRecord, remove, indexEntry);
            }
            final int inFlight = IN_FLIGHT.incrementAndGet();
            MAX_IN_FLIGHT.accumulateAndGet(inFlight, Math::max);
            try {
                return updateOneKeyAsyncUnsynchronized(savedRecord, indexEntry);
            } finally {
                IN_FLIGHT.decrementAndGet();
            }
        }

        private <M extends Message> CompletableFuture<Void> updateOneKeyAsyncUnsynchronized(@Nonnull FDBIndexableRecord<M> savedRecord,
                                                                                            @Nonnull IndexEntry indexEntry) {
            final Tuple valueKey = indexEntry.getKey();
            final Tuple value = indexEntry.getValue();
            final byte[] keyBytes = state.indexSubspace.pack(indexEntryKey(valueKey, savedRecord.getPrimaryKey()));
            final byte[] valueBytes = value.pack();
            checkKeyValueSizes(savedRecord, valueKey, value, keyBytes, valueBytes);
            if (!indexEntry.keyContainsNonUniqueNull()) {
                checkUniqueness(savedRecord, indexEntry);
            }
            state.transaction.set(keyBytes, valueBytes);
            return AsyncUtil.DONE;
        }
    }
}
