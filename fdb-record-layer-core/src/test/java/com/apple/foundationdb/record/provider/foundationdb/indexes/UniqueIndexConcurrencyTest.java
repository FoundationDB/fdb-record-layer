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
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;

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
        final UniqueStore store = new UniqueStore(NO_HOOK, UNIQUE_INDEX_NAME, disableConcurrencyManagement);
        int escapes = 0;
        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            if (store.commitPairs(iteration)) {
                escapes++;
                LOGGER.warn(KeyValueLogMessage.of("committed duplicate values despite unique index",
                        "iteration", iteration,
                        "duplicates", store.duplicateValues()));
            }
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
        final UniqueStore store = new UniqueStore(NO_HOOK, UNIQUE_INDEX_NAME, disableConcurrencyManagement);
        assertThat(store.countMissedViolationsWhileWriteOnly())
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
        final UniqueStore store = new UniqueStore(UNSYNCHRONIZED_HOOK, UNSYNCHRONIZED_INDEX_NAME,
                disableConcurrencyManagement);
        final int missed = store.countMissedViolationsWhileWriteOnly();
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
     * the order, the commit may or may not fail, but the index must be unique and consistent afterwards.
     * With {@code competingInsert}, a new record {@code C = b} is also saved at the same time, so the final state would
     * have two records with {@code b}, and the commit must fail.
     *
     * @param competingInsert whether to also insert a new record taking the value vacated by {@code B}
     * @param disableConcurrencyManagement whether to disable the store's serialization of record mutations
     */
    @ParameterizedTest
    @BooleanSource({"competingInsert", "disableConcurrencyManagement"})
    void sameTransactionConcurrentUpdates(boolean competingInsert, boolean disableConcurrencyManagement) {
        final UniqueStore store = new UniqueStore(NO_HOOK, UNIQUE_INDEX_NAME, disableConcurrencyManagement);
        final int writers = competingInsert ? 3 : 2;
        int committed = 0;
        for (int iteration = 0; iteration < ITERATIONS; iteration++) {
            final UpdateRound round = new UpdateRound(iteration);
            store.inTransaction(context -> round.seed(recordStore));
            final boolean succeeded = store.tryCommit(context -> saveConcurrently(context, writers, round::move));
            if (competingInsert) {
                assertThat(succeeded)
                        .as("commit with two records claiming each vacated value, iteration %d", iteration)
                        .isFalse();
            }
            if (succeeded) {
                committed++;
            }
            store.assertUniqueAndConsistent();
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
        final UniqueStore store = new UniqueStore(NO_HOOK, UNIQUE_INDEX_NAME, false);
        // Create the store up front, so that the writers do not race to create it.
        store.inTransaction(context -> { });
        for (int value = 0; value < ITERATIONS; value++) {
            try (ConcurrentWriters writers = new ConcurrentWriters(store)) {
                assertThat(writers.saveAndCommitAll(value))
                        .as("transactions that committed value %d", value)
                        .filteredOn(Boolean::booleanValue)
                        .hasSizeLessThanOrEqualTo(1);
            }
        }
        store.assertUniqueAndConsistent();
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
     * Which record the given writer saves for the given group of a transaction.
     */
    @FunctionalInterface
    private interface RecordSource {
        TestRecords1Proto.MySimpleRecord record(int writer, int group);
    }

    /**
     * Have {@code writers} threads, released at the same moment, each save one record per group in the given
     * transaction, and then wait for all of the saves to complete.
     */
    private void saveConcurrently(@Nonnull FDBRecordContext context, int writers, @Nonnull RecordSource source) {
        final FDBRecordStore store = recordStore;
        final CyclicBarrier barrier = new CyclicBarrier(writers);
        final List<CompletableFuture<Void>> saves = IntStream.range(0, writers)
                .mapToObj(writer -> CompletableFuture.supplyAsync(() -> {
                    awaitBarrier(barrier);
                    return saveGroups(store, writer, source);
                }, WRITER_POOL).thenCompose(future -> future))
                .toList();
        context.asyncToSync(FDBStoreTimer.Waits.WAIT_SAVE_RECORD, AsyncUtil.whenAll(saves));
    }

    @Nonnull
    private static CompletableFuture<Void> saveGroups(@Nonnull FDBRecordStore store, int writer,
                                                      @Nonnull RecordSource source) {
        return AsyncUtil.whenAll(IntStream.range(0, GROUPS_PER_TRANSACTION)
                .mapToObj(group -> store.saveRecordAsync(source.record(writer, group)))
                .toList());
    }

    /**
     * Save records in pairs that share a value, with the two records of each pair saved by different writers at about
     * the same time, all in the given transaction.
     */
    private void saveConcurrentPairs(@Nonnull FDBRecordContext context, int iteration) {
        final int iterationBase = iteration * VALUES_PER_TRANSACTION;
        saveConcurrently(context, WRITERS, (writer, group) -> {
            final int value = iterationBase + group * (WRITERS / 2) + writer / 2;
            return record(value * 2L + writer % 2, value);
        });
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

    @Nonnull
    private List<TestRecords1Proto.MySimpleRecord> loadRecords(@Nonnull FDBRecordContext context) {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_SCAN_RECORDS,
                recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN)
                        .map(rec -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).build())
                        .asList());
    }

    /**
     * A store with a unique index on {@code num_value_unique}, which every test operation opens in its own
     * transaction. This keeps the meta-data hook, the index name, and whether to disable the concurrency manager
     * in one place, instead of passing them to every operation.
     */
    private final class UniqueStore {
        @Nonnull
        private final RecordMetaDataHook hook;
        @Nonnull
        private final String indexName;
        private final boolean disableConcurrencyManagement;

        UniqueStore(@Nonnull RecordMetaDataHook hook, @Nonnull String indexName, boolean disableConcurrencyManagement) {
            this.hook = hook;
            this.indexName = indexName;
            this.disableConcurrencyManagement = disableConcurrencyManagement;
        }

        /**
         * Open a new transaction with the store open in it. The store is left in {@code recordStore}.
         */
        @Nonnull
        FDBRecordContext open() {
            final FDBRecordContext context = openContext();
            try {
                recordStore = getStoreBuilder(context, simpleMetaData(hook))
                        .setDisableConcurrencyManagement(disableConcurrencyManagement)
                        .createOrOpen();
                return context;
            } catch (RuntimeException e) {
                context.close();
                throw e;
            }
        }

        /**
         * Run the work and commit, in a new transaction.
         */
        void inTransaction(@Nonnull Consumer<FDBRecordContext> work) {
            try (FDBRecordContext context = open()) {
                work.accept(context);
                commit(context);
            }
        }

        /**
         * Run the work and commit, in a new transaction.
         *
         * @return {@code true} if the commit succeeded, or {@code false} if it failed with a uniqueness violation
         */
        boolean tryCommit(@Nonnull Consumer<FDBRecordContext> work) {
            try (FDBRecordContext context = open()) {
                work.accept(context);
                return commitSucceeds(context);
            }
        }

        private boolean commitSucceeds(@Nonnull FDBRecordContext context) {
            try {
                commit(context);
                return true;
            } catch (RecordIndexUniquenessViolation e) {
                return false;
            }
        }

        /**
         * Concurrently save pairs of records that share a value, and commit.
         *
         * @return whether the commit succeeded
         */
        boolean commitPairs(int iteration) {
            return tryCommit(context -> saveConcurrentPairs(context, iteration));
        }

        /**
         * Mark the index write-only, save {@link #ITERATIONS} transactions of pairs, and then count the values that
         * were saved twice but have no recorded uniqueness violation.
         */
        int countMissedViolationsWhileWriteOnly() {
            inTransaction(context ->
                    context.asyncToSync(FDBStoreTimer.Waits.WAIT_INDEX_OPERATION, recordStore.markIndexWriteOnly(indexName)));
            for (int iteration = 0; iteration < ITERATIONS; iteration++) {
                assertThat(commitPairs(iteration))
                        .as("commit while the index is write-only, iteration %d", iteration)
                        .isTrue();
            }
            final Set<Tuple> violatedValues = violatedValues();
            return (int)IntStream.range(0, ITERATIONS * VALUES_PER_TRANSACTION)
                    .filter(value -> !violatedValues.contains(Tuple.from(value)))
                    .count();
        }

        @Nonnull
        private Set<Tuple> violatedValues() {
            try (FDBRecordContext context = open()) {
                final Index index = recordStore.getRecordMetaData().getIndex(indexName);
                return new HashSet<>(context.asyncToSync(FDBStoreTimer.Waits.WAIT_SCAN_INDEX_RECORDS,
                        recordStore.scanUniquenessViolations(index)
                                .map(violation -> violation.getIndexEntry().getKey())
                                .asList()));
            }
        }

        /**
         * The primary keys of the records for each value that more than one record has.
         */
        @Nonnull
        Map<Integer, Set<Tuple>> duplicateValues() {
            try (FDBRecordContext context = open()) {
                final Map<Integer, Set<Tuple>> byValue = loadRecords(context).stream()
                        .collect(Collectors.groupingBy(TestRecords1Proto.MySimpleRecord::getNumValueUnique, HashMap::new,
                                Collectors.mapping(rec -> Tuple.from(rec.getRecNo()), Collectors.toSet())));
                byValue.values().removeIf(primaryKeys -> primaryKeys.size() < 2);
                return byValue;
            }
        }

        /**
         * Assert that no two committed records share a value, and that the unique index contains exactly one entry for
         * each record.
         */
        void assertUniqueAndConsistent() {
            assertThat(duplicateValues()).as("values held by more than one record").isEmpty();
            try (FDBRecordContext context = open()) {
                final Set<Tuple> expectedEntries = loadRecords(context).stream()
                        .map(rec -> Tuple.from(rec.getNumValueUnique(), rec.getRecNo()))
                        .collect(Collectors.toSet());
                final List<Tuple> actualEntries = context.asyncToSync(FDBStoreTimer.Waits.WAIT_SCAN_INDEX_RECORDS,
                        recordStore.scanIndex(recordStore.getRecordMetaData().getIndex(indexName),
                                        IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                                .map(IndexEntry::getKey)
                                .asList());
                assertThat(actualEntries).as("unique index entries").containsExactlyInAnyOrderElementsOf(expectedEntries);
            }
        }
    }

    /**
     * One iteration of {@link #sameTransactionConcurrentUpdates}. The records come in groups of three values,
     * {@code A = base}, {@code B = base + 1}, and a fresh {@code base + 2}. Writer 0 moves {@code A} onto {@code B}'s
     * value, writer 1 moves {@code B} to the fresh value, and writer 2 inserts a new record {@code C} that also takes
     * {@code B}'s old value.
     */
    private static final class UpdateRound {
        /**
         * For each writer, the offset within the group of the record it saves, and of the value it gives that record.
         */
        private static final int[][] MOVES = {{0, 1}, {1, 2}, {2, 1}};

        private final int iterationBase;

        UpdateRound(int iteration) {
            this.iterationBase = iteration * GROUPS_PER_TRANSACTION * 3;
        }

        @Nonnull
        TestRecords1Proto.MySimpleRecord move(int writer, int group) {
            final int base = iterationBase + group * 3;
            return record(base + MOVES[writer][0], base + MOVES[writer][1]);
        }

        /**
         * Replace the contents of the store with the starting records {@code A} and {@code B} of every group.
         * Starting empty keeps the consistency checks from getting slower as the test goes on.
         */
        void seed(@Nonnull FDBRecordStore store) {
            store.deleteAllRecords();
            for (int group = 0; group < GROUPS_PER_TRANSACTION; group++) {
                final int base = iterationBase + group * 3;
                store.saveRecord(record(base, base));
                store.saveRecord(record(base + 1, base + 1));
            }
        }
    }

    /**
     * Several transactions on the same store, all opened up front, that each save a record with the same value and
     * commit at the same time.
     */
    private final class ConcurrentWriters implements AutoCloseable {
        private final List<FDBRecordContext> contexts = new ArrayList<>();
        private final List<FDBRecordStore> stores = new ArrayList<>();

        ConcurrentWriters(@Nonnull UniqueStore store) {
            // Contexts are opened on the test thread, as opening one copies the test's MDC.
            try {
                for (int writer = 0; writer < WRITERS; writer++) {
                    contexts.add(store.open());
                    stores.add(recordStore);
                }
            } catch (RuntimeException e) {
                close();
                throw e;
            }
        }

        /**
         * Have every writer save its own record with the given value and commit, all at the same moment.
         *
         * @return for each writer, whether its commit succeeded
         */
        @Nonnull
        List<Boolean> saveAndCommitAll(int value) {
            final CyclicBarrier barrier = new CyclicBarrier(WRITERS);
            final List<CompletableFuture<Boolean>> outcomes = IntStream.range(0, WRITERS)
                    .mapToObj(writer -> CompletableFuture.supplyAsync(() -> {
                        awaitBarrier(barrier);
                        return saveAndCommit(writer, value);
                    }, WRITER_POOL).thenCompose(outcome -> outcome))
                    .toList();
            return fdb.asyncToSync(timer, FDBStoreTimer.Waits.WAIT_COMMIT, AsyncUtil.getAll(outcomes));
        }

        @Nonnull
        private CompletableFuture<Boolean> saveAndCommit(int writer, int value) {
            return stores.get(writer).saveRecordAsync(record((long)value * WRITERS + writer, value))
                    .thenCompose(ignore -> contexts.get(writer).commitAsync())
                    .handle((ignore, err) -> committed(err));
        }

        @Override
        public void close() {
            contexts.forEach(FDBRecordContext::close);
        }
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
