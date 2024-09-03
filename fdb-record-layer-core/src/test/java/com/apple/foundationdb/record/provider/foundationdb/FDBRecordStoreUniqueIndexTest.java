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

import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordIndexUniquenessViolation;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsBytesProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
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
        dropUniquenessConstraint.openWithNonUnique();
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
        dropUniquenessConstraint.openWithNonUnique();
    }

    @ParameterizedTest(name = "removeUniquenessConstraintDuringTransaction(clearViolations={0}, allowReadableUniquePending={1})")
    @MethodSource("removeUniquenessConstraintArguments")
    void removeUniquenessConstraintDuringTransaction(boolean clearViolations, boolean allowReadableUniquePending) throws Exception {
        final DropUniquenessConstraint dropUniquenessConstraint = new DropUniquenessConstraint(allowReadableUniquePending);
        dropUniquenessConstraint.setupStore();
        dropUniquenessConstraint.addUniqueIndexViaBuild();
        dropUniquenessConstraint.changeToNonUnique(clearViolations, false);
    }

    @ParameterizedTest(name = "removeUniquenessConstraintDuringTransactionWithNewDuplications(addViolations={0}, allowReadableUniquePending={1})")
    @MethodSource("removeUniquenessConstraintArguments")
    void removeUniquenessConstraintDuringTransactionWithNewDuplications(boolean allowReadableUniquePending) throws Exception {
        final DropUniquenessConstraint dropUniquenessConstraint = new DropUniquenessConstraint(allowReadableUniquePending);
        dropUniquenessConstraint.setupStore();
        dropUniquenessConstraint.addUniqueIndexViaBuild();
        dropUniquenessConstraint.changeToNonUnique(false, true);
    }

    @ParameterizedTest(name = "removeUniquenessConstraintDuringTransactionWithNewViolations(allowReadableUniquePending={0})")
    @BooleanSource
    void removeUniquenessConstraintDuringTransactionWithNewViolations(boolean allowReadableUniquePending) throws Exception {
        final DropUniquenessConstraint dropUniquenessConstraint = new DropUniquenessConstraint(allowReadableUniquePending);
        dropUniquenessConstraint.setupStore();
        dropUniquenessConstraint.addUniqueIndexViaBuild();
        dropUniquenessConstraint.changeToNonUniqueWithViolations();
    }

    private class DropUniquenessConstraint {
        private static final String TYPE_NAME = "MySimpleRecord";
        private static final int nonUniqueNumValue = 42;
        private final List<Long> recordNumbers;
        private final Index uniqueIndex;
        private final Index nonUniqueIndex;
        private final boolean allowReadableUniquePending;
        private final IndexState readableUniquePendingState;
        private final RecordMetaDataHook nonUniqueHook;
        private final RecordMetaDataHook uniqueHook;

        private DropUniquenessConstraint(final boolean allowReadableUniquePending) {
            this.allowReadableUniquePending = allowReadableUniquePending;
            readableUniquePendingState = allowReadableUniquePending ? IndexState.READABLE_UNIQUE_PENDING : IndexState.WRITE_ONLY;
            // Copy the first index, but drop the uniqueness constraint. This keeps the index as is, including the
            // last_modified_version, so adding it to the meta-data won't cause the index to be rebuilt during
            // check version. However, bump the meta-data version to ensure that anyone with an old meta-data version
            // (who will expect the index to be unique, if READABLE) knows to reload the meta-data.
            uniqueIndex = new Index("initiallyUniqueIndex", field("num_value_2"), IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
            assertTrue(uniqueIndex.isUnique());
            nonUniqueIndex = new IndexWithOptions(uniqueIndex, IndexOptions.UNIQUE_OPTION, Boolean.FALSE.toString());
            assertFalse(nonUniqueIndex.isUnique());
            nonUniqueHook = metaDataBuilder -> {
                metaDataBuilder.addIndex("MySimpleRecord", nonUniqueIndex);
                metaDataBuilder.setVersion(metaDataBuilder.getVersion() + 1);
            };
            uniqueHook = metaDataBuilder -> metaDataBuilder.addIndex(TYPE_NAME, uniqueIndex);
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

        private void saveRecord(final Long recordNumber) {
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(recordNumber)
                    .setNumValue2(nonUniqueNumValue)
                    .build());
        }

        public void addUniqueIndexViaCheckVersion() throws ExecutionException, InterruptedException {
            timer.reset();
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context, uniqueHook);
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
                openSimpleRecordStore(context, uniqueHook);
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
                openSimpleRecordStore(context, uniqueHook);
                removeUniquenessViolations(recordStore);
                commit(context);
            }
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context, uniqueHook);

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

        public void openWithNonUnique() throws ExecutionException, InterruptedException {
            timer.reset();
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context, nonUniqueHook);
                assertTrue(recordStore.isVersionChanged());
                assertEquals(0L, timer.getCount(FDBStoreTimer.Events.REBUILD_INDEX));
                assertOrMarkReadable();

                assertThat(recordStore.scanUniquenessViolations(nonUniqueIndex).asList().get(), empty());
                assertIndexEntries();
                commit(context);
            }
        }

        private void assertOrMarkReadable() throws InterruptedException, ExecutionException {
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

        public void changeToNonUnique(final boolean clearViolations, final boolean addViolations) throws ExecutionException, InterruptedException {
            timer.reset();
            try (FDBRecordContext context = openContext()) {
                final RecordMetaData uniqueMetadata = simpleMetaData(uniqueHook);
                final RecordMetaData nonUniqueMetadata = simpleMetaData(nonUniqueHook);
                final AtomicReference<RecordMetaData> metadataProvider = new AtomicReference<>(uniqueMetadata);
                createOrOpenRecordStore(context, metadataProvider::get);
                final FDBRecordStore.Builder storeBuilder = getStoreBuilder(context, metadataProvider.get());
                assertFalse(recordStore.isVersionChanged());
                assertEquals(0L, timer.getCount(FDBStoreTimer.Events.REBUILD_INDEX));

                if (clearViolations) {
                    removeUniquenessViolations(recordStore);
                }

                metadataProvider.set(nonUniqueMetadata);
                recordStore.checkVersion(storeBuilder.getUserVersionChecker(), FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS)
                        .get();
                assertTrue(recordStore.isVersionChanged());
                assertEquals(0L, timer.getCount(FDBStoreTimer.Events.REBUILD_INDEX));

                // Note: if you add a violation before calling checkVersion, it will still fail to commit, this seems
                // fine.
                if (addViolations) {
                    saveRecord(3980L);
                    recordNumbers.add(3980L);
                }

                assertOrMarkReadable();

                assertThat(recordStore.scanUniquenessViolations(nonUniqueIndex).asList().get(), empty());
                assertIndexEntries();
                commit(context);
            }
        }

        public void changeToNonUniqueWithViolations() throws ExecutionException, InterruptedException {
            timer.reset();
            try (FDBRecordContext context = openContext()) {
                final RecordMetaData uniqueMetadata = simpleMetaData(uniqueHook);
                final RecordMetaData nonUniqueMetadata = simpleMetaData(nonUniqueHook);
                final AtomicReference<RecordMetaData> metadataProvider = new AtomicReference<>(uniqueMetadata);
                createOrOpenRecordStore(context, metadataProvider::get);
                final FDBRecordStore.Builder storeBuilder = getStoreBuilder(context, metadataProvider.get());
                assertFalse(recordStore.isVersionChanged());
                assertEquals(0L, timer.getCount(FDBStoreTimer.Events.REBUILD_INDEX));

                saveRecord(3980L);
                recordNumbers.add(3980L);

                metadataProvider.set(nonUniqueMetadata);
                recordStore.checkVersion(storeBuilder.getUserVersionChecker(), FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS)
                        .get();
                assertTrue(recordStore.isVersionChanged());
                assertEquals(0L, timer.getCount(FDBStoreTimer.Events.REBUILD_INDEX));


                assertOrMarkReadable();

                assertThat(recordStore.scanUniquenessViolations(nonUniqueIndex).asList().get(), empty());
                assertIndexEntries();
                if (allowReadableUniquePending) {
                    assertThrows(RecordIndexUniquenessViolation.class, () -> commit(context));
                } else {
                    commit(context);
                }
            }
        }
    }
}
