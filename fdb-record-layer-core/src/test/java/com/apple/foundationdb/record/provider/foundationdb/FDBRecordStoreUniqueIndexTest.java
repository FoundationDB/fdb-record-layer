/*
 * FDBRecordStoreUniquenessTest.java
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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordIndexUniquenessViolation;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsBytesProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.indexes.InvalidIndexEntry;
import com.apple.foundationdb.record.provider.foundationdb.indexes.ValueIndexMaintainer;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.auto.service.AutoService;
import com.google.protobuf.Message;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of uniqueness checks.
 */
@Tag(Tags.RequiresFDB)
public class FDBRecordStoreUniqueIndexTest extends FDBRecordStoreTestBase {

    private static final String NO_UNIQUE_CLEAR_INDEX_TYPE = "no_unique_clear";

    @Test
    public void writeUniqueByteString() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openBytesRecordStore(context);

            recordStore.saveRecord(TestRecordsBytesProto.ByteStringRecord.newBuilder()
                    .setPkey(byteString(0, 1, 2)).setSecondary(byteString(0, 1, 2)).setUnique(byteString(0, 2))
                    .setName("foo").build());
            recordStore.saveRecord(TestRecordsBytesProto.ByteStringRecord.newBuilder()
                    .setPkey(byteString(0, 1, 5)).setSecondary(byteString(0, 1, 3)).setUnique(byteString(0, 2))
                    .setName("box").build());
            assertThrows(RecordIndexUniquenessViolation.class, () -> commit(context));
        }
    }

    @Test
    public void asyncUniqueInserts() throws Exception {
        List<TestRecords1Proto.MySimpleRecord> records = new ArrayList<>();
        Random r = new Random(0xdeadc0deL);
        for (int i = 0; i < 100; i++) {
            records.add(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(r.nextLong())
                    .setNumValueUnique(r.nextInt())
                    .build()
            );
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            CompletableFuture<?>[] futures = new CompletableFuture<?>[records.size()];
            for (int i = 0; i < records.size(); i++) {
                futures[i] = recordStore.saveRecordAsync(records.get(i));
            }

            CompletableFuture.allOf(futures).get();
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            for (TestRecords1Proto.MySimpleRecord record : records) {
                assertEquals(record.toString(), recordStore.loadRecord(Tuple.from(record.getRecNo())).getRecord().toString());
            }
        }
    }

    @Test
    public void asyncNotUniqueInserts() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            CompletableFuture<?>[] futures = new CompletableFuture<?>[2];
            futures[0] = recordStore.saveRecordAsync(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .setNumValueUnique(42)
                    .build()
            );
            futures[1] = recordStore.saveRecordAsync(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1776L)
                    .setNumValueUnique(42)
                    .build()
            );

            CompletableFuture.allOf(futures).get();
            assertThrows(RecordIndexUniquenessViolation.class, () -> commit(context));
        }
    }

    /**
     * Validate the behavior when a unique index is added on existing data that already has duplicates on the same data.
     * The new index should not be built, as that is impossible to do without breaking the constraints of the index, but
     * it also shouldn't fail the store opening or commit, as otherwise, the store would never be able to be opened.
     *
     * @throws Exception from store opening code
     */
    @Test
    public void buildUniqueInCheckVersion() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            // create a uniqueness violation on a field without a unique index
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .setNumValue2(42)
                    .build());
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1412L)
                    .setNumValue2(42)
                    .build());

            commit(context);
        }

        final Index uniqueIndex = new Index("unique_num_value_2_index", field("num_value_2"),
                IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        assertTrue(uniqueIndex.isUnique());
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", uniqueIndex));

            assertFalse(recordStore.isIndexReadable(uniqueIndex), "index with uniqueness violations should not be readable after being added to the meta-data");
            final List<RecordIndexUniquenessViolation> uniquenessViolations = recordStore.scanUniquenessViolations(uniqueIndex).asList().get();
            assertThat(uniquenessViolations, not(empty()));
            for (RecordIndexUniquenessViolation uniquenessViolation : uniquenessViolations) {
                assertThat(uniquenessViolation.getPrimaryKey(), either(equalTo(Tuple.from(1066L))).or(equalTo(Tuple.from(1412L))));
            }

            commit(context);
        }
    }

    static Stream<Arguments> removeUniquenessConstraintArguments() {
        return Stream.of(true, false)
                .flatMap(withViolations -> Stream.of(true, false)
                        .map(allowReadableUniquePending -> Arguments.of(withViolations, allowReadableUniquePending)));
    }

    @Test
    public void uniquenessChecks() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            Index index1 = recordStore.getRecordMetaData().getIndex("MySimpleRecord$str_value_indexed");
            Index index2 = recordStore.getRecordMetaData().getIndex("MySimpleRecord$num_value_unique");

            AtomicBoolean check1Run = new AtomicBoolean(false);
            CompletableFuture<Void> check1 = MoreAsyncUtil.delayedFuture(1L, TimeUnit.MILLISECONDS)
                    .thenRun(() -> check1Run.set(true));
            recordStore.addIndexUniquenessCommitCheck(index1, check1);

            CompletableFuture<Void> check2 = new CompletableFuture<>();
            RecordCoreException err = new RecordCoreException("unable to run check");
            check2.completeExceptionally(err);
            recordStore.addIndexUniquenessCommitCheck(index2, check2);

            // Checks For index 1 should complete successfully and mark the "check1Run" boolean as completed. It
            // should not throw an error from check 2 completing exceptionally.
            recordStore.whenAllIndexUniquenessCommitChecks(index1).get();
            assertTrue(check1Run.get(), "check1 should have marked check1Run as having completed");

            // For index 2, the error should be caught when the uniqueness checks are waited on
            ExecutionException thrownExecutionException = assertThrows(ExecutionException.class, () -> recordStore.whenAllIndexUniquenessCommitChecks(index2).get());
            assertSame(err, thrownExecutionException.getCause());

            // The error from the "uniqueness check" should block the transaction from committing
            RecordCoreException thrownRecordCoreException = assertThrows(RecordCoreException.class, context::commit);
            assertSame(err, thrownRecordCoreException);
        }
    }

    @Test
    public void changeIndexAtFixedSubspaceKey() throws Exception {
        final Object subspaceKey = "fixed_subspace_key";

        final List<TestRecords1Proto.MySimpleRecord> records = List.of(
                TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(1066L)
                        .setStrValueIndexed("foo")
                        .setNumValue2(1)
                        .build(),
                TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(1415L)
                        .setStrValueIndexed("bar")
                        .setNumValue2(1)
                        .build(),
                TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(1815L)
                        .setStrValueIndexed("baz")
                        .setNumValue2(1)
                        .build()
        );

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            records.forEach(recordStore::saveRecord);
            commit(context);
        }

        final Index firstIndex = new Index("first_index", field("num_value_2"), IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        firstIndex.setSubspaceKey(subspaceKey);
        assertTrue(firstIndex.isUnique());
        final RecordMetaDataHook hook1 = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", firstIndex);
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook1);
            assertTrue(recordStore.isVersionChanged());

            assertThat(recordStore.getIndexState(firstIndex), either(equalTo(IndexState.READABLE_UNIQUE_PENDING)).or(equalTo(IndexState.WRITE_ONLY)));
            assertThat(recordStore.scanUniquenessViolations(firstIndex).asList().get(), hasSize(3));

            commit(context);
        }

        final Index secondIndex = new Index("second_index", field("num_value_2"));
        secondIndex.setSubspaceKey(subspaceKey);
        assertFalse(secondIndex.isUnique());
        final RecordMetaDataHook hook2 = metaDataBuilder -> {
            metaDataBuilder.setVersion(metaDataBuilder.getVersion() + 1);
            metaDataBuilder.addIndex("MySimpleRecord", secondIndex);
        };
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook2);
            assertTrue(recordStore.isVersionChanged());

            assertEquals(IndexState.READABLE, recordStore.getIndexState(secondIndex));
            List<IndexEntry> entries = recordStore.scanIndex(secondIndex, new IndexScanRange(IndexScanType.BY_VALUE, TupleRange.ALL), null, ScanProperties.FORWARD_SCAN)
                    .asList()
                    .get();
            assertThat(entries, hasSize(3));
            assertEquals(List.of(Tuple.from(1, 1066L), Tuple.from(1, 1415L), Tuple.from(1, 1815L)),
                    entries.stream().map(IndexEntry::getKey).collect(Collectors.toList()));
            assertThat(recordStore.scanUniquenessViolations(secondIndex).asList().get(), empty());

            commit(context);
        }

        final Index thirdIndex = new Index("third_index", field("str_value_indexed"), IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        thirdIndex.setSubspaceKey(subspaceKey);
        assertTrue(thirdIndex.isUnique());

        final RecordMetaDataHook hook3 = metaDataBuilder -> {
            metaDataBuilder.setVersion(metaDataBuilder.getVersion() + 2);
            metaDataBuilder.addIndex("MySimpleRecord", thirdIndex);
        };
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook3);
            assertTrue(recordStore.isVersionChanged());

            assertEquals(IndexState.READABLE, recordStore.getIndexState(thirdIndex));
            List<IndexEntry> entries = recordStore.scanIndex(thirdIndex, new IndexScanRange(IndexScanType.BY_VALUE, TupleRange.ALL), null, ScanProperties.FORWARD_SCAN)
                    .asList()
                    .get();
            assertThat(entries, hasSize(3));
            assertEquals(List.of(Tuple.from("bar", 1415L), Tuple.from("baz", 1815L), Tuple.from("foo", 1066L)),
                    entries.stream().map(IndexEntry::getKey).collect(Collectors.toList()));
            assertThat(recordStore.scanUniquenessViolations(thirdIndex).asList().get(), empty());

            commit(context);
        }
    }

    @Test
    public void removeUniquenessConstraint() throws Exception {
        final DropUniquenessConstraint dropUniquenessConstraint = new DropUniquenessConstraint(false);
        dropUniquenessConstraint.setupStore();
        dropUniquenessConstraint.addUniqueIndexViaCheckVersion();
        dropUniquenessConstraint.openWithNonUnique(false, true);
    }

    @ParameterizedTest(name = "removeUniquenessConstraintAfterBuild(withViolations={0}, allowReadableUniquePending={1})")
    @MethodSource("removeUniquenessConstraintArguments")
    void removeUniquenessConstraintAfterBuild(boolean withViolations, boolean allowReadableUniquePending) throws Exception {
        final DropUniquenessConstraint dropUniquenessConstraint = new DropUniquenessConstraint(allowReadableUniquePending);
        dropUniquenessConstraint.setupStore();
        dropUniquenessConstraint.addUniqueIndexViaBuild();
        if (!withViolations) {
            dropUniquenessConstraint.removeUniquenessViolations();
        }
        dropUniquenessConstraint.openWithNonUnique(false, true);
    }

    @ParameterizedTest(name = "removeUniquenessConstraintDuringTransaction(clearViolations={0}, allowReadableUniquePending={1})")
    @MethodSource("removeUniquenessConstraintArguments")
    void removeUniquenessConstraintDuringTransaction(boolean clearViolations, boolean allowReadableUniquePending) throws Exception {
        final DropUniquenessConstraint dropUniquenessConstraint = new DropUniquenessConstraint(allowReadableUniquePending);
        dropUniquenessConstraint.setupStore();
        dropUniquenessConstraint.addUniqueIndexViaBuild();
        dropUniquenessConstraint.changeToNonUnique(clearViolations, false, false);
    }

    /**
     * This test covers the situation where you change the index to non-unique, and then, in the same transaction, add
     * what would be more violations if the index were still unique.
     * @param allowReadableUniquePending whether to allow {@link IndexState#READABLE_UNIQUE_PENDING}
     * @throws Exception if there is an issue
     */
    @ParameterizedTest(name = "removeUniquenessConstraintDuringTransactionWithNewDuplications(addViolations={0}, allowReadableUniquePending={1})")
    @MethodSource("removeUniquenessConstraintArguments")
    void removeUniquenessConstraintDuringTransactionWithNewDuplications(boolean allowReadableUniquePending) throws Exception {
        final DropUniquenessConstraint dropUniquenessConstraint = new DropUniquenessConstraint(allowReadableUniquePending);
        dropUniquenessConstraint.setupStore();
        dropUniquenessConstraint.addUniqueIndexViaBuild();
        dropUniquenessConstraint.changeToNonUnique(false, false, true);
    }

    /**
     * This test covers the situation where you add some violations, and then, in the same transaction, change the index
     * to no longer be unique.
     * <p>
     *     Right now this will fail {@link FDBRecordStore#checkVersion(FDBRecordStoreBase.UserVersionChecker, FDBRecordStoreBase.StoreExistenceCheck)}
     *     and fail the commit. In theory, we could support this clearing out the index violation checks, but that
     *     proved tricky, and this seems like pathological use case.
     * </p>
     * @param allowReadableUniquePending whether to allow {@link IndexState#READABLE_UNIQUE_PENDING}
     * @throws Exception if there is an issue
     */
    @ParameterizedTest(name = "removeUniquenessConstraintDuringTransactionWithNewViolations(allowReadableUniquePending={0})")
    @BooleanSource
    void removeUniquenessConstraintDuringTransactionWithNewViolations(boolean allowReadableUniquePending) throws Exception {
        final DropUniquenessConstraint dropUniquenessConstraint = new DropUniquenessConstraint(allowReadableUniquePending);
        dropUniquenessConstraint.setupStore();
        dropUniquenessConstraint.addUniqueIndexViaBuild();
        dropUniquenessConstraint.changeToNonUnique(false, true, false);
    }

    /**
     * This test is primarily here to make sure that if there is an index maintainer that doesn't implement the new
     * {@link IndexMaintainer#clearUniquenessViolations}, the old flow of increasing the version when changing the state
     * still works.
     * @param allowReadableUniquePending whether to allow {@link IndexState#READABLE_UNIQUE_PENDING}
     */
    @ParameterizedTest(name = "bumpVersionWhenChangingToNonUnique(allowReadableUniquePending={0})")
    @BooleanSource
    void bumpVersionWhenChangingToNonUnique(boolean allowReadableUniquePending) throws Exception {
        final DropUniquenessConstraint dropUniquenessConstraint = new DropUniquenessConstraint(
                allowReadableUniquePending, NO_UNIQUE_CLEAR_INDEX_TYPE, true);
        dropUniquenessConstraint.setupStore();
        dropUniquenessConstraint.addUniqueIndexViaBuild();
        dropUniquenessConstraint.openWithNonUnique(true, true);
    }

    /**
     * Test to show what happens if you drop the uniqueness constraint on an index that doesn't implement the new
     * {@link IndexMaintainer#clearUniquenessViolations} without bumping {@code lastModifiedVersion}, despite the
     * instructions on {@link IndexOptions#UNIQUE_OPTION}.
     * @param allowReadableUniquePending whether to test with allowing {@link IndexState#READABLE_UNIQUE_PENDING}
     */
    @ParameterizedTest(name = "unsupportedChangeToNonUniqueWithoutBumpingVersion(allowReadableUniquePending={0})")
    @BooleanSource
    void unsupportedChangeToNonUniqueWithoutBumpingVersion(boolean allowReadableUniquePending) throws Exception {
        // this won't fail, it will just leave the violations sitting around, but as per the
        final DropUniquenessConstraint dropUniquenessConstraint = new DropUniquenessConstraint(
                allowReadableUniquePending, NO_UNIQUE_CLEAR_INDEX_TYPE, false);
        dropUniquenessConstraint.setupStore();
        dropUniquenessConstraint.addUniqueIndexViaBuild();
        dropUniquenessConstraint.openWithNonUnique(false, false);
    }

    private class DropUniquenessConstraint {
        private static final String TYPE_NAME = "MySimpleRecord";
        private static final int nonUniqueNumValue = 42;
        private final List<Long> recordNumbers;
        private final Index uniqueIndex;
        private final Index nonUniqueIndex;
        private final boolean allowReadableUniquePending;
        private final IndexState readableUniquePendingState;
        private final RecordMetaData uniqueMetadata;
        private final RecordMetaData nonUniqueMetadata;

        private DropUniquenessConstraint(final boolean allowReadableUniquePending) {
            this(allowReadableUniquePending, IndexTypes.VALUE, false);
        }

        private DropUniquenessConstraint(final boolean allowReadableUniquePending, final String indexType, final boolean bumpVersion) {
            this.allowReadableUniquePending = allowReadableUniquePending;
            readableUniquePendingState = allowReadableUniquePending ? IndexState.READABLE_UNIQUE_PENDING : IndexState.WRITE_ONLY;
            uniqueIndex = new Index("initiallyUniqueIndex", field("num_value_2"), indexType, IndexOptions.UNIQUE_OPTIONS);
            assertTrue(uniqueIndex.isUnique());
            uniqueMetadata = simpleMetaData(metaDataBuilder ->
                    metaDataBuilder.addIndex(TYPE_NAME, uniqueIndex));

            // Copy the first index, but drop the uniqueness constraint. This keeps the index as is, including the
            // last_modified_version, so adding it to the meta-data won't cause the index to be rebuilt during
            // check version. However, bump the meta-data version to ensure that anyone with an old meta-data version
            // (who will expect the index to be unique, if READABLE) knows to reload the meta-data.
            nonUniqueIndex = new IndexWithOptions(uniqueIndex, IndexOptions.UNIQUE_OPTION, Boolean.FALSE.toString());
            if (bumpVersion) {
                // we need to ensure that this will be greater than the metadata version created with the unique index
                nonUniqueIndex.setLastModifiedVersion(uniqueMetadata.getVersion() + 1);
            } else {
                assertEquals(uniqueIndex.getLastModifiedVersion(), nonUniqueIndex.getLastModifiedVersion());
                assertEquals(uniqueIndex.getSubspaceKey(), nonUniqueIndex.getSubspaceKey());
            }

            assertFalse(nonUniqueIndex.isUnique());
            nonUniqueMetadata = simpleMetaData(metaDataBuilder -> {
                metaDataBuilder.addIndex("MySimpleRecord", nonUniqueIndex);
                metaDataBuilder.setVersion(metaDataBuilder.getVersion() + 2);
            });
            recordNumbers = new ArrayList<>();
            recordNumbers.add(1066L);
            recordNumbers.add(1415L);
            recordNumbers.add(1815L);
        }

        public void setupStore() throws Exception {
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context);
                for (final Long recordNumber : recordNumbers) {
                    saveRecord(recordNumber);
                }

                commit(context);
            }
        }

        private void addViolation() {
            saveRecord(3980L);
            recordNumbers.add(3980L);
        }

        private void saveRecord(final Long recordNumber) {
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(recordNumber)
                    .setNumValue2(nonUniqueNumValue)
                    .build());
        }

        public void addUniqueIndexViaCheckVersion() throws ExecutionException, InterruptedException {
            timer.reset();
            try (FDBRecordContext context = openContext()) {
                createOrOpenRecordStore(context, uniqueMetadata);
                assertTrue(recordStore.isVersionChanged());
                assertEquals(1L, timer.getCount(FDBStoreTimer.Events.REBUILD_INDEX));
                assertThat(recordStore.getIndexState(uniqueIndex), either(equalTo(IndexState.WRITE_ONLY)).or(equalTo(IndexState.READABLE_UNIQUE_PENDING)));
                assertThat(recordStore.scanUniquenessViolations(uniqueIndex)
                                .map(RecordIndexUniquenessViolation::getPrimaryKey).asList().get(),
                        containsAllPrimaryKeys());
                commit(context);
            }
        }

        private @Nonnull Matcher<Iterable<? extends Tuple>> containsAllPrimaryKeys() {
            return containsInAnyOrder(
                    recordNumbers.stream()
                            .map(items -> Matchers.equalTo(Tuple.from(items)))
                            .collect(Collectors.toList()));
        }

        public void addUniqueIndexViaBuild() {
            try (FDBRecordContext context = openContext()) {
                createOrOpenRecordStore(context, uniqueMetadata);
                final OnlineIndexer.IndexingPolicy.Builder indexingPolicy = OnlineIndexer.IndexingPolicy.newBuilder();
                if (allowReadableUniquePending) {
                    indexingPolicy.allowUniquePendingState();
                }
                try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                        .setRecordStore(recordStore)
                        .setTargetIndexesByName(List.of(uniqueIndex.getName()))
                        .setIndexingPolicy(indexingPolicy
                                .build()).build()) {
                    if (allowReadableUniquePending) {
                        indexer.buildIndex();
                    } else {
                        assertThrows(RecordIndexUniquenessViolation.class, indexer::buildIndex);
                    }
                }
            }
        }

        public void removeUniquenessViolations() throws Exception {
            try (FDBRecordContext context = openContext()) {
                createOrOpenRecordStore(context, uniqueMetadata);
                removeUniquenessViolations(recordStore);
                commit(context);
            }
            try (FDBRecordContext context = openContext()) {
                createOrOpenRecordStore(context, uniqueMetadata);

                assertEquals(readableUniquePendingState, recordStore.getIndexState(uniqueIndex.getName()),
                        "Index should still be " + readableUniquePendingState.getLogName());
            }
        }

        private void removeUniquenessViolations(final FDBRecordStore store) {
            for (int i = 1; i < recordNumbers.size(); i++) {
                final Long recordNumber = recordNumbers.get(i);
                store.deleteRecord(Tuple.from(recordNumber));
            }
            while (recordNumbers.size() > 1) {
                recordNumbers.remove(1);
            }
        }

        public void openWithNonUnique(final boolean expectRebuild, final boolean shouldClearViolations)
                throws ExecutionException, InterruptedException {
            timer.reset();
            final Matcher<Collection<? extends KeyValue>> violationMatcher;
            if (shouldClearViolations) {
                violationMatcher = Matchers.hasSize(0);
            } else {
                violationMatcher = Matchers.hasSize(Matchers.greaterThan(0));
            }
            try (FDBRecordContext context = openContext()) {
                createOrOpenRecordStore(context, nonUniqueMetadata);
                assertTrue(recordStore.isVersionChanged());
                assertUniquenessViolations(context, violationMatcher);
                if (expectRebuild) {
                    assertEquals(1L, timer.getCount(FDBStoreTimer.Events.REBUILD_INDEX));
                    assertEquals(IndexState.READABLE, recordStore.getIndexState(nonUniqueIndex));
                } else {
                    assertEquals(0L, timer.getCount(FDBStoreTimer.Events.REBUILD_INDEX));
                    assertOrMarkReadable();
                }

                assertUniquenessViolations(context, violationMatcher);
                assertIndexEntries();
                commit(context);
            }
        }

        private void assertOrMarkReadable() throws InterruptedException, ExecutionException {
            // Given that transitioning indexes from Unique to non-unique is probably rare, we are opting to not have
            // checkVersion check all WriteOnly indexes to see if they are only WriteOnly because of uniqueness violations,
            // on an index that is no longer unique.
            if (allowReadableUniquePending) {
                assertEquals(IndexState.READABLE, recordStore.getIndexState(nonUniqueIndex));
            } else {
                assertEquals(IndexState.WRITE_ONLY, recordStore.getIndexState(nonUniqueIndex));
                assertTrue(recordStore.markIndexReadable(nonUniqueIndex).get());
                assertEquals(IndexState.READABLE, recordStore.getIndexState(nonUniqueIndex));
            }
        }

        private void assertIndexEntries() throws InterruptedException, ExecutionException {
            List<IndexEntry> indexEntries = recordStore.scanIndex(
                            nonUniqueIndex,
                            new IndexScanRange(IndexScanType.BY_VALUE, TupleRange.ALL),
                            null, ScanProperties.FORWARD_SCAN)
                    .asList().get();
            assertThat(indexEntries.stream().map(IndexEntry::getPrimaryKey).collect(Collectors.toList()),
                    containsAllPrimaryKeys());
            Set<Long> indexKeys = indexEntries.stream()
                    .map(IndexEntry::getKey)
                    .map(indexKey -> indexKey.getLong(0))
                    .collect(Collectors.toSet());
            assertEquals(Set.of((long) nonUniqueNumValue), indexKeys);
        }

        public void changeToNonUnique(final boolean clearViolations, final boolean addViolationsBefore,
                                      final boolean addViolationsAfter) throws Exception {
            // The important things are:
            // 1. You should never have an index that is ReadableUniquePending, but not unique
            // 2. If it is ReadableUniquePending, and you didn't add any violations in the current transaction, it should
            //    transition to Readable during checkVersion if the index is now not unique
            // 3. A non-unique index should not have uniquess violations on disk
            timer.reset();
            try (FDBRecordContext context = openContext()) {
                final AtomicReference<RecordMetaData> metadataProvider = new AtomicReference<>(uniqueMetadata);
                createOrOpenRecordStore(context, metadataProvider::get);
                final FDBRecordStore.Builder storeBuilder = getStoreBuilder(context, metadataProvider.get());
                assertFalse(recordStore.isVersionChanged());
                assertEquals(0L, timer.getCount(FDBStoreTimer.Events.REBUILD_INDEX));

                if (clearViolations) {
                    removeUniquenessViolations(recordStore);
                }

                if (addViolationsBefore) {
                    addViolation();
                }

                metadataProvider.set(nonUniqueMetadata);
                final Callable<Void> checkVersion = () -> {
                    recordStore.checkVersion(storeBuilder.getUserVersionChecker(), FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS)
                            .get();
                    return null;
                };
                if (allowReadableUniquePending && addViolationsBefore) {
                    final ExecutionException executionException = assertThrows(ExecutionException.class, checkVersion::call);
                    assertThat(executionException.getCause(), Matchers.instanceOf(RecordIndexUniquenessViolation.class));
                    assertThrows(RecordIndexUniquenessViolation.class, () -> commit(context));
                    return;
                }
                checkVersion.call();
                assertTrue(recordStore.isVersionChanged());
                assertEquals(0L, timer.getCount(FDBStoreTimer.Events.REBUILD_INDEX));

                if (addViolationsAfter) {
                    addViolation();
                }

                assertOrMarkReadable();

                assertUniquenessViolations(context, Matchers.hasSize(0));
                assertIndexEntries();
                commit(context);
            }
        }

        private void assertUniquenessViolations(final FDBRecordContext context,
                                                final Matcher<Collection<? extends KeyValue>> matcher)
                throws InterruptedException, ExecutionException {
            assertThat(recordStore.scanUniquenessViolations(nonUniqueIndex).asList().get(), empty());
            assertThat(context.ensureActive().getRange(recordStore.indexUniquenessViolationsSubspace(nonUniqueIndex)
                            .range()).asList().get(),
                    matcher);
        }
    }

    /**
     * Factory for {@link NeverAllowClearUniquenessViolations}.
     */
    @AutoService(IndexMaintainerFactory.class)
    public static class NeverAllowClearUniquenessViolationsFactory implements IndexMaintainerFactory {

        @Nonnull
        @Override
        public Iterable<String> getIndexTypes() {
            return Collections.singletonList(NO_UNIQUE_CLEAR_INDEX_TYPE);
        }

        @Nonnull
        @Override
        public IndexValidator getIndexValidator(Index index) {
            return new IndexValidator(index);
        }

        @Nonnull
        @Override
        public IndexMaintainer getIndexMaintainer(@Nonnull IndexMaintainerState state) {
            return new NeverAllowClearUniquenessViolations(state);
        }
    }

    private static class NeverAllowClearUniquenessViolations extends IndexMaintainer {
        IndexMaintainer underlying;

        public NeverAllowClearUniquenessViolations(final IndexMaintainerState state) {
            super(state);
            underlying = new ValueIndexMaintainer(state);
        }

        @Nonnull
        @Override
        public RecordCursor<IndexEntry> scan(@Nonnull final IndexScanType scanType, @Nonnull final TupleRange range, @Nullable final byte[] continuation, @Nonnull final ScanProperties scanProperties) {
            return underlying.scan(scanType, range, continuation, scanProperties);
        }

        @Nonnull
        @Override
        public <M extends Message> CompletableFuture<Void> update(@Nullable final FDBIndexableRecord<M> oldRecord, @Nullable final FDBIndexableRecord<M> newRecord) {
            return underlying.update(oldRecord,  newRecord);
        }

        @Nonnull
        @Override
        public <M extends Message> CompletableFuture<Void> updateWhileWriteOnly(@Nullable final FDBIndexableRecord<M> oldRecord, @Nullable final FDBIndexableRecord<M> newRecord) {
            return underlying.updateWhileWriteOnly(oldRecord, newRecord);
        }

        @Nonnull
        @Override
        public RecordCursor<IndexEntry> scanUniquenessViolations(@Nonnull final TupleRange range, @Nullable final byte[] continuation, @Nonnull final ScanProperties scanProperties) {
            return underlying.scanUniquenessViolations(range, continuation, scanProperties);
        }

        @Override
        public CompletableFuture<Void> clearUniquenessViolations() {
            return super.clearUniquenessViolations();
        }

        @Nonnull
        @Override
        public RecordCursor<InvalidIndexEntry> validateEntries(@Nullable final byte[] continuation, @Nullable final ScanProperties scanProperties) {
            return underlying.validateEntries(continuation, scanProperties);
        }

        @Override
        public boolean canEvaluateRecordFunction(@Nonnull final IndexRecordFunction<?> function) {
            return false;
        }

        @Nullable
        @Override
        public <M extends Message> List<IndexEntry> evaluateIndex(@Nonnull final FDBRecord<M> record) {
            return underlying.evaluateIndex(record);
        }

        @Nullable
        @Override
        public <M extends Message> List<IndexEntry> filteredIndexEntries(@Nullable final FDBIndexableRecord<M> savedRecord) {
            return underlying.filteredIndexEntries(savedRecord);
        }

        @Nonnull
        @Override
        public <T, M extends Message> CompletableFuture<T> evaluateRecordFunction(@Nonnull final EvaluationContext context, @Nonnull final IndexRecordFunction<T> function, @Nonnull final FDBRecord<M> record) {
            return underlying.evaluateRecordFunction(context, function, record);
        }

        @Override
        public boolean canEvaluateAggregateFunction(@Nonnull final IndexAggregateFunction function) {
            return underlying.canEvaluateAggregateFunction(function);
        }

        @Nonnull
        @Override
        public CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull final IndexAggregateFunction function, @Nonnull final TupleRange range, @Nonnull final IsolationLevel isolationLevel) {
            return underlying.evaluateAggregateFunction(function, range, isolationLevel);
        }

        @Override
        public boolean isIdempotent() {
            return underlying.isIdempotent();
        }

        @Nonnull
        @Override
        public CompletableFuture<Boolean> addedRangeWithKey(@Nonnull final Tuple primaryKey) {
            return addedRangeWithKey(primaryKey);
        }

        @Override
        public boolean canDeleteWhere(@Nonnull final QueryToKeyMatcher matcher, @Nonnull final Key.Evaluated evaluated) {
            return underlying.canDeleteWhere(matcher, evaluated);
        }

        @Override
        public CompletableFuture<Void> deleteWhere(@Nonnull final Transaction tr, @Nonnull final Tuple prefix) {
            return underlying.deleteWhere(tr, prefix);
        }

        @Override
        public CompletableFuture<IndexOperationResult> performOperation(@Nonnull final IndexOperation operation) {
            return underlying.performOperation(operation);
        }

        @Override
        public CompletableFuture<Void> mergeIndex() {
            return underlying.mergeIndex();
        }
    }
}
