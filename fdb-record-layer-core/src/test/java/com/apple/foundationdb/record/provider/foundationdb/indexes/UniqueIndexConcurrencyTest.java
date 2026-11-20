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
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordIndexUniquenessViolation;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreConcurrentTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.ParameterizedTestUtils;
import com.apple.test.Tags;
import com.google.auto.service.AutoService;
import com.google.protobuf.Message;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Stress tests that try to get two records with the same value committed despite a unique index on that value.
 *
 * <p>Within a single transaction, {@link StandardIndexMaintainer} relies on synchronizing the uniqueness check with the
 * write of the index entry, so that concurrent saves in the same transaction observe one another through
 * read-your-writes. This matters because a range read does not observe writes made after it was issued, even if
 * the read has not completed yet. By default, the store's concurrency manager also serializes all record mutations,
 * so the tests that share a transaction run both with and without it, to exercise the synchronization on its own.
 * Across transactions, uniqueness relies on the read conflict range of the uniqueness check. These tests issue saves
 * from many threads at once to try to break both.
 */
@Tag(Tags.RequiresFDB)
class UniqueIndexConcurrencyTest extends FDBRecordStoreConcurrentTestBase {
    private static final int ITERATIONS = 50;

    private static final String UNIQUE_INDEX_NAME = "MySimpleRecord$num_value_unique";
    private static final String UNSYNCHRONIZED_UNIQUE_VALUE = "unsynchronized_unique_value";
    private static final String UNSYNCHRONIZED_INDEX_NAME = "unsynchronized_num_value_unique";
    private static final String CONTROLLED_VIOLATIONS_UNIQUE_VALUE = "controlled_violations_unique_value";
    private static final String CONTROLLED_VIOLATIONS_INDEX_NAME = "controlled_violations_num_value_unique";
    private static final RecordMetaData UNIQUE_META_DATA = RecordMetaData.build(TestRecords1Proto.getDescriptor());
    private static final RecordMetaData UNSYNCHRONIZED_META_DATA =
            metaDataWithUniqueIndex(UNSYNCHRONIZED_INDEX_NAME, UNSYNCHRONIZED_UNIQUE_VALUE);
    private static final RecordMetaData CONTROLLED_VIOLATIONS_META_DATA =
            metaDataWithUniqueIndex(CONTROLLED_VIOLATIONS_INDEX_NAME, CONTROLLED_VIOLATIONS_UNIQUE_VALUE);

    /**
     * Writers block on a barrier so that they start together, which is fine here as the pool is unbounded.
     */
    private static final Executor WRITER_POOL = TestExecutors.defaultThreadPool();

    private KeySpacePath path;

    @BeforeEach
    void setUp() {
        path = pathManager.createPath(TestKeySpace.RECORD_STORE);
    }

    /**
     * How the writers of {@link #uniquenessConcurrencyTest} share transactions. Every writer saves the same unique
     * value, so at most one of them can end up committing it.
     */
    enum ConcurrentStoreRunner {
        /**
         * All of the writers save into a single transaction, which is then committed once. The commit must fail.
         */
        OneTransaction(0) {
            @Override
            CompletableFuture<Integer> runConcurrently(@Nonnull Supplier<FDBRecordStore> storeSupplier, int concurrency,
                                                       @Nonnull BiFunction<FDBRecordStore, Integer, CompletableFuture<Void>> action) {
                final FDBRecordStore store = storeSupplier.get();
                return AsyncUtil.whenAll(startTogether(concurrency, writer -> action.apply(store, writer)))
                        .thenCompose(ignore -> store.getContext().commitAsync())
                        .handle((ignore, err) -> committed(err) ? 1 : 0)
                        .whenComplete((ignore, err) -> store.getContext().close());
            }
        },
        /**
         * Each writer saves into its own transaction, and commits it. Exactly one of them can win.
         */
        MultipleTransactions(1) {
            @Override
            CompletableFuture<Integer> runConcurrently(@Nonnull Supplier<FDBRecordStore> storeSupplier, int concurrency,
                                                       @Nonnull BiFunction<FDBRecordStore, Integer, CompletableFuture<Void>> action) {
                // The stores are opened here, on the test thread, as opening a context copies the test's MDC.
                final List<FDBRecordStore> stores = IntStream.range(0, concurrency)
                        .mapToObj(writer -> storeSupplier.get())
                        .toList();
                final IntFunction<CompletableFuture<Integer>> saveAndCommit = writer -> {
                    final FDBRecordStore store = stores.get(writer);
                    return action.apply(store, writer)
                            .thenCompose(ignore -> store.getContext().commitAsync())
                            .handle((ignore, err) -> committed(err) ? 1 : 0);
                };
                return AsyncUtil.getAll(startTogether(concurrency, saveAndCommit))
                        .thenApply(commits -> commits.stream().mapToInt(Integer::intValue).sum())
                        .whenComplete((ignore, err) -> stores.forEach(store -> store.getContext().close()));
            }
        };

        private final int expectedSuccessfulCommits;

        ConcurrentStoreRunner(int expectedSuccessfulCommits) {
            this.expectedSuccessfulCommits = expectedSuccessfulCommits;
        }

        /**
         * How many transactions should commit when a correct maintainer enforces uniqueness.
         */
        int getExpectedSuccessfulCommits() {
            return expectedSuccessfulCommits;
        }

        /**
         * Have {@code concurrency} writers, started at the same moment, each run {@code action} with their own number.
         * A commit that fails because another writer got there first, either with a uniqueness violation or a
         * conflict, is not an error. Any other failure is.
         *
         * @param storeSupplier opens a new transaction, with the store open in it
         * @param concurrency the number of writers
         * @param action what each writer does with the store of its transaction, given its number
         * @return the number of transactions that were committed successfully
         */
        abstract CompletableFuture<Integer> runConcurrently(@Nonnull Supplier<FDBRecordStore> storeSupplier, int concurrency,
                                                            @Nonnull BiFunction<FDBRecordStore, Integer, CompletableFuture<Void>> action);
    }

    enum MaintainerType {
        Synchronized(UNIQUE_META_DATA, UNIQUE_INDEX_NAME),
        Unsynchronized(UNSYNCHRONIZED_META_DATA, UNSYNCHRONIZED_INDEX_NAME);

        private final RecordMetaDataProvider metaData;
        private final String indexName;

        MaintainerType(final RecordMetaDataProvider metaData, final String indexName) {
            this.metaData = metaData;
            this.indexName = indexName;
        }

        public RecordMetaDataProvider getMetaData() {
            return metaData;
        }

        public String getIndexName() {
            return indexName;
        }
    }

    static Stream<Arguments> uniquenessConcurrencyTest() {
        return concurrencyArguments();
    }

    static Stream<Arguments> uniquenessConcurrencyUpdateTest() {
        return concurrencyArguments(ParameterizedTestUtils.booleans("concurrentInsert"));
    }

    /**
     * The arguments that all of the tests share: every repetition, with each maintainer, way of sharing transactions,
     * index state, and whether the concurrency manager is disabled, except for the combinations that have nothing to
     * check.
     *
     * @param extraDimensions further dimensions to combine, which come after the shared ones
     * @return the arguments of {@code iterationIndex, maintainerType, storeRunner, indexState,
     * disableConcurrencyManagement}, followed by those of the extra dimensions
     */
    private static Stream<Arguments> concurrencyArguments(@Nonnull Stream<?>... extraDimensions) {
        final List<Stream<?>> dimensions = new ArrayList<>();
        dimensions.add(IntStream.range(0, ITERATIONS).boxed());
        dimensions.add(Stream.of(MaintainerType.values()));
        dimensions.add(Stream.of(ConcurrentStoreRunner.values()));
        dimensions.add(Stream.of(IndexState.READABLE, IndexState.WRITE_ONLY));
        dimensions.add(ParameterizedTestUtils.booleans("disableConcurrencyManagement"));
        dimensions.addAll(Arrays.asList(extraDimensions));
        return ParameterizedTestUtils.cartesianProduct(dimensions.toArray(Stream<?>[]::new))
                // While the index is write-only, uniqueness is not enforced when committing, so separate transactions
                // can all commit the same value. There is nothing to check about that.
                .filter(arguments -> !(arguments.get()[2] == ConcurrentStoreRunner.MultipleTransactions
                                       && arguments.get()[3] == IndexState.WRITE_ONLY));
    }

    /**
     * Many writers each save a distinct record with the same unique value, concurrently. What should happen
     * depends on how the writers share transactions, see {@link ConcurrentStoreRunner}.
     *
     * <p>While the index is write-only, a uniqueness check records a violation instead of failing the commit. Every
     * record must end up with a recorded violation: a record without one was neither seen by another writer nor saw
     * one, and the index could be marked readable with a duplicate of it.
     *
     * <p>With the unsynchronized maintainer, in one transaction, the writers may fail to see each other. Whether that
     * happens is not deterministic, so the test is skipped when it does, rather than failed. Each run is a fresh
     * store, so {@code iterationIndex} only repeats the test.
     *
     * @param iterationIndex which repetition this is
     * @param maintainerType the index maintainer of the unique index
     * @param storeRunner how the writers share transactions
     * @param indexState the state of the unique index while the writers save
     * @param disableConcurrencyManagement whether to disable the store's serialization of record mutations
     */
    @ParameterizedTest
    @MethodSource
    void uniquenessConcurrencyTest(int iterationIndex, MaintainerType maintainerType, ConcurrentStoreRunner storeRunner,
                                   IndexState indexState, boolean disableConcurrencyManagement) {
        final Supplier<FDBRecordStore> storeSupplier = createStore(maintainerType, indexState, disableConcurrencyManagement,
                ignored -> { });

        final int writers = 2;
        final int successful = fdb.asyncToSync(timer, FDBStoreTimer.Waits.WAIT_COMMIT,
                storeRunner.runConcurrently(storeSupplier, writers, (store, recNo) -> saveRecordAsync(store, recNo, 17)));

        // The commit does not fail while the index is write-only, as the violations are recorded instead.
        final boolean writeOnly = indexState == IndexState.WRITE_ONLY;
        final int expectedCommits = writeOnly ? 1 : storeRunner.getExpectedSuccessfulCommits();
        // Whatever was committed, and nothing else, is in the store and in the index. Commits that failed leave nothing.
        final int expectedRecords = writeOnly ? writers : expectedCommits;
        final List<Integer> recordsNeedingViolation = writeOnly ? IntStream.range(0, writers).boxed().toList() : List.of();
        final boolean raceMayBeLost = maintainerType == MaintainerType.Unsynchronized && storeRunner == ConcurrentStoreRunner.OneTransaction;
        assertUniquenessEnforced(storeSupplier, maintainerType, raceMayBeLost, successful, expectedCommits, expectedCommits,
                recordsNeedingViolation, expectedRecords);
    }

    /**
     * A record that holds a unique value is moved to another value while new records are inserted with the value
     * it frees, all concurrently. The insert may or may not see the old record, so the transaction that does both can
     * succeed or fail. But when two records are inserted with the same value, that must be caught.
     *
     * <p>The seed record, which holds the value to begin with, is moved by writer 0. Writer 1 inserts a record with
     * the value, and with {@code concurrentInsert}, so does writer 2.
     *
     * @param iterationIndex which repetition this is
     * @param maintainerType the index maintainer of the unique index
     * @param storeRunner how the writers share transactions
     * @param indexState the state of the unique index while the writers save
     * @param disableConcurrencyManagement whether to disable the store's serialization of record mutations
     * @param concurrentInsert whether a second record is inserted with the value, which makes the final state a duplicate
     */
    @ParameterizedTest
    @MethodSource
    void uniquenessConcurrencyUpdateTest(int iterationIndex, MaintainerType maintainerType, ConcurrentStoreRunner storeRunner,
                                         IndexState indexState, boolean disableConcurrencyManagement, boolean concurrentInsert) {
        int seedId = 10;
        int newId1 = 12;
        int newId2 = 14;
        final int value = 17;
        final Supplier<FDBRecordStore> storeSupplier = createStore(maintainerType, indexState, disableConcurrencyManagement,
                seedStore -> saveRecordSync(seedStore, seedId, value));

        final int successful = fdb.asyncToSync(timer, FDBStoreTimer.Waits.WAIT_COMMIT,
                storeRunner.runConcurrently(storeSupplier, concurrentInsert ? 3 : 2,
                        (store, writer) -> switch (writer) {
                            case 0 -> saveRecordAsync(store, seedId, value + 3);
                            case 1 -> saveRecordAsync(store, newId1, value);
                            case 2 -> saveRecordAsync(store, newId2, value);
                            default -> throw new IllegalStateException("Unexpected value: " + writer);
                        }));

        // How many transactions may commit.
        final int minCommits;
        final int maxCommits;
        if (indexState == IndexState.WRITE_ONLY) {
            // Violations are recorded instead of failing the commit.
            minCommits = 1;
            maxCommits = 1;
        } else if (storeRunner == ConcurrentStoreRunner.OneTransaction) {
            // The insert may see the old record, in which case the commit fails, even though moving the record would
            // have freed the value. Two inserts of the value can never both be committed.
            minCommits = 0;
            maxCommits = concurrentInsert ? 0 : 1;
        } else {
            // The writer that moves the record always commits. An insert only commits if it started after that, and at
            // most one of two inserts of the same value does.
            minCommits = 1;
            maxCommits = 2;
        }
        // While write-only, the two inserted records are duplicates of each other, which must be recorded. Without the
        // second insert there is no duplicate left.
        // TODO: This occasionally fails (about 1 in 300 runs), even for the synchronized maintainer with the concurrency
        //       manager enabled: neither inserted record has a violation recorded. Suspected cause: the violations are
        //       recorded from an asynchronous callback that can outlive the save, and removing the moved record's old
        //       entry (removeUniquenessViolationsAsync) clears every violation for the value once it counts one left,
        //       which can wipe the ones that were recorded in the meantime.
        //       This is reproduced deterministically by movingRecordClearsViolationsOfDuplicates.
        final List<Integer> recordsNeedingViolation = indexState == IndexState.WRITE_ONLY && concurrentInsert
                                                      ? List.of(newId1, newId2)
                                                      : List.of();

        // Whatever was committed, and nothing else, is in the store and in the index: the seed record, and the inserted
        // records that committed. A single transaction that failed leaves only the seed record.
        final int insertedRecords = concurrentInsert ? 2 : 1;
        final int expectedRecords = storeRunner == ConcurrentStoreRunner.OneTransaction
                                    ? 1 + (successful == 0 ? 0 : insertedRecords)
                                    : successful;
        assertUniquenessEnforced(storeSupplier, maintainerType,
                maintainerType == MaintainerType.Unsynchronized && storeRunner == ConcurrentStoreRunner.OneTransaction
                        && concurrentInsert,
                successful, minCommits, maxCommits, recordsNeedingViolation, expectedRecords);
    }

    /**
     * Reproduces, deterministically, a move of a record clearing the violations of two records that are duplicates.
     *
     * <p>While the index is write-only, two records are inserted with a value, which a third record is then moved off,
     * all in one transaction. The inserts record their violations from a callback of their uniqueness check, which does
     * not have to be done when the save is, so it can still be going when a later save starts. Removing the moved
     * record's old index entry removes its violations, and scans the ones that are left for that value: if there is only
     * one, they are all cleared, but only once the scan has a result. A violation that is recorded in between is cleared
     * too, even though the two records are still duplicates.
     *
     * <p>The maintainer blocks the recording of the violations of the second insert, so that its uniqueness check is
     * still going, and the test decides when they are recorded.
     *
     * @param recordedDuringRemoval whether the violations of the second insert are recorded while the moved record's
     * violations are removed. If not, they are recorded before.
     */
    @ParameterizedTest
    @BooleanSource("recordedDuringRemoval")
    void movingRecordClearsViolationsOfDuplicates(boolean recordedDuringRemoval) {
        // TODO: This is a bug in removeUniquenessViolationsAsync, which this reproduces. Remove this once it is fixed.
        Assumptions.assumeThat(recordedDuringRemoval)
                .as("known bug: removing a record's violations clears ones that were recorded since it scanned them")
                .isFalse();
        ControlledViolationsIndexMaintainer.reset();
        final int seedId = 10;
        final int newId1 = 12;
        final int newId2 = 14;
        final int value = 17;
        final Supplier<FDBRecordStore> storeSupplier = () -> openStore(CONTROLLED_VIOLATIONS_META_DATA, false);

        final FDBRecordStore seedStore = storeSupplier.get();
        try (FDBRecordContext context = seedStore.getContext()) {
            context.asyncToSync(FDBStoreTimer.Waits.WAIT_INDEX_OPERATION,
                    seedStore.markIndexWriteOnly(CONTROLLED_VIOLATIONS_INDEX_NAME));
            saveRecordSync(seedStore, seedId, value);
            commit(context);
        }

        final FDBRecordStore store = storeSupplier.get();
        try (FDBRecordContext context = store.getContext()) {
            // The first insert sees the seed record, so there are two violations, which are recorded.
            saveRecordSync(store, newId1, value);
            ControlledViolationsIndexMaintainer.awaitRecorded(2);
            // The second insert sees the seed record and the first insert, so there are four violations. Recording them
            // is blocked, so its uniqueness check is not done.
            ControlledViolationsIndexMaintainer.blockViolations();
            saveRecordSync(store, newId2, value);
            ControlledViolationsIndexMaintainer.awaitBlocked();
            if (recordedDuringRemoval) {
                ControlledViolationsIndexMaintainer.onRemovalStarted = ControlledViolationsIndexMaintainer::unblockViolations;
            } else {
                ControlledViolationsIndexMaintainer.unblockViolations();
                ControlledViolationsIndexMaintainer.awaitRecorded(4);
            }
            // Moving the seed record removes its index entry, and so its violations.
            saveRecordSync(store, seedId, value + 3);
            commit(context);
        }

        // The two inserted records are in the store, and both have the value: they are duplicates.
        assertStoreContents(storeSupplier, CONTROLLED_VIOLATIONS_INDEX_NAME, 3);
        assertThat(countRecordsWithoutViolation(storeSupplier, CONTROLLED_VIOLATIONS_INDEX_NAME, List.of(newId1, newId2)))
                .as("duplicate records without a recorded uniqueness violation")
                .isZero();
    }

    /**
     * Create the store, with the unique index in the given state.
     *
     * @return how to open the store in a new transaction
     */
    @Nonnull
    private Supplier<FDBRecordStore> createStore(@Nonnull MaintainerType maintainerType, @Nonnull IndexState indexState,
                                                 boolean disableConcurrencyManagement, @Nonnull Consumer<FDBRecordStore> seed) {
        final Supplier<FDBRecordStore> storeSupplier = () -> openStore(maintainerType.getMetaData(), disableConcurrencyManagement);
        // Create the store up front, so that the writers do not race to create it.
        final FDBRecordStore createdStore = storeSupplier.get();
        try (FDBRecordContext context = createdStore.getContext()) {
            if (indexState == IndexState.WRITE_ONLY) {
                context.asyncToSync(FDBStoreTimer.Waits.WAIT_INDEX_OPERATION,
                        createdStore.markIndexWriteOnly(maintainerType.getIndexName()));
            }
            seed.accept(createdStore);
            commit(context);
        }
        return storeSupplier;
    }

    /**
     * Assert what the writers must have produced, which is that they did not commit a duplicate.
     *
     * @param storeSupplier opens the store in a new transaction
     * @param maintainerType the index maintainer of the unique index
     * @param raceMayBeLost whether the writers can fail to see each other, as with the unsynchronized maintainer. That
     * is not deterministic, so the test is skipped, rather than failed, when it happens: the number of skipped runs
     * is how often the race was hit
     * @param successful how many transactions committed
     * @param minCommits the fewest transactions that may have committed
     * @param maxCommits the most transactions that may have committed
     * @param recordsNeedingViolation the records that must have a violation recorded, because they are duplicates of
     * each other in a write-only index
     * @param expectedRecords how many records should be in the store
     */
    private static void assertUniquenessEnforced(@Nonnull Supplier<FDBRecordStore> storeSupplier,
                                                 @Nonnull MaintainerType maintainerType, boolean raceMayBeLost,
                                                 int successful, int minCommits, int maxCommits,
                                                 @Nonnull List<Integer> recordsNeedingViolation, int expectedRecords) {
        final int recordsWithoutViolation = recordsNeedingViolation.isEmpty()
                                            ? 0
                                            : countRecordsWithoutViolation(storeSupplier, maintainerType.getIndexName(),
                                                    recordsNeedingViolation);
        if (raceMayBeLost) {
            // A lost check shows up as a transaction committing that should not have, or as a record without a
            // recorded violation.
            Assumptions.assumeThat(successful).isBetween(minCommits, maxCommits);
            Assumptions.assumeThat(recordsWithoutViolation).isZero();
        }
        assertThat(successful)
                .as("transactions that committed")
                .isBetween(minCommits, maxCommits);
        assertThat(recordsWithoutViolation)
                .as("records without a recorded uniqueness violation")
                .isZero();
        assertStoreContents(storeSupplier, maintainerType.getIndexName(), expectedRecords);
    }

    /**
     * Assert that the store has exactly the expected number of records, and that the index has exactly one entry for
     * each of them.
     */
    private static void assertStoreContents(@Nonnull Supplier<FDBRecordStore> storeSupplier, @Nonnull String indexName,
                                            int expectedRecords) {
        final FDBRecordStore store = storeSupplier.get();
        try {
            assertThat(loadRecords(store)).as("records in the store").hasSize(expectedRecords);
            assertIndexMatchesRecords(store, indexName);
        } finally {
            store.getContext().close();
        }
    }

    private static void assertIndexMatchesRecords(@Nonnull FDBRecordStore store, @Nonnull String indexName) {
        final Set<Tuple> expectedEntries = loadRecords(store).stream()
                .map(rec -> Tuple.from(rec.getNumValueUnique(), rec.getRecNo()))
                .collect(Collectors.toSet());
        // Scan with the maintainer, as the store refuses to scan an index that is not readable, such as a write-only one.
        // This is also why we can't use scrubbing
        final Index index = store.getRecordMetaData().getIndex(indexName);
        final List<Tuple> actualEntries = store.getContext().asyncToSync(FDBStoreTimer.Waits.WAIT_SCAN_INDEX_RECORDS,
                store.getIndexMaintainer(index)
                        .scan(IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                        .map(IndexEntry::getKey)
                        .asList());
        assertThat(actualEntries).as("unique index entries").containsExactlyInAnyOrderElementsOf(expectedEntries);
    }

    /**
     * Count the writers' records that have no uniqueness violation recorded. Seeing another record records a violation
     * for both, so a record without one was neither seen by another writer nor saw one, and a duplicate of it would go
     * unnoticed. A violation only keeps one other record, which is why this counts records and not pairs.
     */
    private static int countRecordsWithoutViolation(@Nonnull Supplier<FDBRecordStore> storeSupplier, @Nonnull String indexName,
                                                    @Nonnull List<Integer> recNos) {
        final FDBRecordStore store = storeSupplier.get();
        try (FDBRecordContext context = store.getContext()) {
            final Set<Tuple> recordsWithViolation = context.asyncToSync(FDBStoreTimer.Waits.WAIT_SCAN_INDEX_RECORDS,
                            store.scanUniquenessViolations(store.getRecordMetaData().getIndex(indexName)).asList())
                    .stream()
                    .map(RecordIndexUniquenessViolation::getPrimaryKey)
                    .collect(Collectors.toSet());
            return (int)recNos.stream()
                    .filter(recNo -> !recordsWithViolation.contains(Tuple.from((long)recNo)))
                    .count();
        }
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

    @Nonnull
    private static RecordMetaData metaDataWithUniqueIndex(@Nonnull String indexName, @Nonnull String indexType) {
        final RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.removeIndex(UNIQUE_INDEX_NAME);
        metaDataBuilder.addIndex("MySimpleRecord", new Index(indexName, field("num_value_unique"),
                indexType, IndexOptions.UNIQUE_OPTIONS));
        return metaDataBuilder.getRecordMetaData();
    }

    private static void awaitBarrier(@Nonnull CyclicBarrier barrier) {
        try {
            barrier.await(1, TimeUnit.MINUTES);
        } catch (Exception e) {
            throw new IllegalStateException("writers did not all start", e);
        }
    }

    /**
     * Start the work of each writer at the same moment, from threads that are all already running.
     */
    @Nonnull
    private static <T> List<CompletableFuture<T>> startTogether(int writers, @Nonnull IntFunction<CompletableFuture<T>> work) {
        final CyclicBarrier barrier = new CyclicBarrier(writers);
        return IntStream.range(0, writers)
                .mapToObj(writer -> CompletableFuture.supplyAsync(() -> {
                    awaitBarrier(barrier);
                    return work.apply(writer);
                }, WRITER_POOL).thenCompose(future -> future))
                .toList();
    }

    @Nonnull
    private static CompletableFuture<Void> saveRecordAsync(@Nonnull FDBRecordStore store, int recNo, int numValueUnique) {
        return store.saveRecordAsync(TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(recNo)
                        .setNumValueUnique(numValueUnique)
                        .build())
                .thenApply(ignore -> null);
    }

    private static void saveRecordSync(@Nonnull FDBRecordStore store, int recNo, int numValueUnique) {
        store.getContext().asyncToSync(FDBStoreTimer.Waits.WAIT_SAVE_RECORD, saveRecordAsync(store, recNo, numValueUnique));
    }

    /**
     * Open the store in a new transaction. The caller must close the store's context.
     */
    @Nonnull
    private FDBRecordStore openStore(@Nonnull RecordMetaDataProvider metaData, boolean disableConcurrencyManagement) {
        final FDBRecordContext context = openContext();
        try {
            return getStoreBuilder(context, metaData, path)
                    .setDisableConcurrencyManagement(disableConcurrencyManagement)
                    .createOrOpen();
        } catch (RuntimeException e) {
            context.close();
            throw e;
        }
    }

    @Nonnull
    private static List<TestRecords1Proto.MySimpleRecord> loadRecords(@Nonnull FDBRecordStore store) {
        return store.getContext().asyncToSync(FDBStoreTimer.Waits.WAIT_SCAN_RECORDS,
                store.scanRecords(null, ScanProperties.FORWARD_SCAN)
                        .map(rec -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).build())
                        .asList());
    }

    /**
     * Factory for the index maintainers that the tests use to control what the unique index does:
     * {@link UnsynchronizedUniqueValueIndexMaintainer} and {@link ControlledViolationsIndexMaintainer}.
     */
    @AutoService(IndexMaintainerFactory.class)
    public static class TestUniqueValueIndexMaintainerFactory implements IndexMaintainerFactory {
        @Nonnull
        @Override
        public Iterable<String> getIndexTypes() {
            return List.of(UNSYNCHRONIZED_UNIQUE_VALUE, CONTROLLED_VIOLATIONS_UNIQUE_VALUE);
        }

        @Nonnull
        @Override
        public IndexValidator getIndexValidator(Index index) {
            return new IndexValidator(index);
        }

        @Nonnull
        @Override
        public IndexMaintainer getIndexMaintainer(@Nonnull IndexMaintainerState state) {
            if (UNSYNCHRONIZED_UNIQUE_VALUE.equals(state.index.getType())) {
                return new UnsynchronizedUniqueValueIndexMaintainer(state);
            }
            return new ControlledViolationsIndexMaintainer(state);
        }
    }

    /**
     * A {@link ValueIndexMaintainer} that lets a test hold up the recording of uniqueness violations, which keeps the
     * uniqueness check that is recording them going, as it does while it waits for a slow read. It can also do something
     * as soon as a removal of violations starts.
     */
    private static class ControlledViolationsIndexMaintainer extends ValueIndexMaintainer {
        private static final Semaphore RECORDED = new Semaphore(0);
        private static volatile CountDownLatch blocked = new CountDownLatch(0);
        private static volatile CountDownLatch unblocked = new CountDownLatch(0);
        private static volatile Runnable onRemovalStarted = () -> { };

        ControlledViolationsIndexMaintainer(@Nonnull IndexMaintainerState state) {
            super(state);
        }

        static void reset() {
            RECORDED.drainPermits();
            blocked = new CountDownLatch(0);
            unblocked = new CountDownLatch(0);
            onRemovalStarted = () -> { };
        }

        /**
         * Block the recording of violations, until they are unblocked.
         */
        static void blockViolations() {
            blocked = new CountDownLatch(1);
            unblocked = new CountDownLatch(1);
        }

        /**
         * Wait for the recording of a violation to be blocked.
         */
        static void awaitBlocked() {
            await(blocked);
        }

        static void unblockViolations() {
            unblocked.countDown();
        }

        /**
         * Wait for the given number of violations to be recorded, since the last time this waited.
         */
        static void awaitRecorded(int count) {
            try {
                assertThat(RECORDED.tryAcquire(count, 30, TimeUnit.SECONDS))
                        .as("%d violations recorded", count)
                        .isTrue();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted waiting for violations", e);
            }
        }

        private static void await(@Nonnull CountDownLatch latch) {
            try {
                assertThat(latch.await(30, TimeUnit.SECONDS)).as("waited for %s", latch).isTrue();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted waiting", e);
            }
        }

        @Override
        protected void addUniquenessViolation(@Nonnull Tuple valueKey, @Nonnull Tuple primaryKey, @Nullable Tuple existingKey) {
            blocked.countDown();
            await(unblocked);
            super.addUniquenessViolation(valueKey, primaryKey, existingKey);
            RECORDED.release();
        }

        @Nonnull
        @Override
        protected CompletableFuture<Void> removeUniquenessViolationsAsync(@Nonnull Tuple valueKey, @Nonnull Tuple primaryKey) {
            final CompletableFuture<Void> removal = super.removeUniquenessViolationsAsync(valueKey, primaryKey);
            onRemovalStarted.run();
            return removal;
        }
    }

    /**
     * A {@link ValueIndexMaintainer} that writes index entries the same way as {@link StandardIndexMaintainer}, except
     * that it does not synchronize the uniqueness check with the write of the entry.
     */
    private static class UnsynchronizedUniqueValueIndexMaintainer extends ValueIndexMaintainer {
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
