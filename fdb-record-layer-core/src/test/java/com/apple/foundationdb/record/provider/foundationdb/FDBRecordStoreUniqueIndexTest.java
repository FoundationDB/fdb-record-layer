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
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsBytesProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.hamcrest.MatcherAssert.assertThat;
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

    @Test
    public void uniquenessChecks() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            Index index1 = recordStore.getRecordMetaData().getIndex("MySimpleRecord$str_value_indexed");
            Index index2 = recordStore.getRecordMetaData().getIndex("MySimpleRecord$num_value_unique");

            AtomicBoolean check1Run = new AtomicBoolean(false);
            CompletableFuture<Void> check1 = MoreAsyncUtil.delayedFuture(1L, TimeUnit.MILLISECONDS)
                    .thenRun(() -> check1Run.set(true));
            recordStore.addIndexUniquenessCommitCheck(index1, recordStore.indexSubspace(index1), check1);

            CompletableFuture<Void> check2 = new CompletableFuture<>();
            RecordCoreException err = new RecordCoreException("unable to run check");
            check2.completeExceptionally(err);
            recordStore.addIndexUniquenessCommitCheck(index2, recordStore.indexSubspace(index2), check2);

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
    public void uniquenessChecksShouldBeStoreScoped() throws Exception {
        final KeySpacePath otherPath = pathManager.createPath(TestKeySpace.RECORD_STORE);
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore firstStore = createOrOpenRecordStore(context, simpleMetaData(NO_HOOK), path).getLeft();
            final FDBRecordStore otherStore = createOrOpenRecordStore(context, simpleMetaData(NO_HOOK), otherPath).getLeft();
            Index index = firstStore.getRecordMetaData().getIndex("MySimpleRecord$str_value_indexed");

            AtomicBoolean check1Run = new AtomicBoolean(false);
            CompletableFuture<Void> check1 = MoreAsyncUtil.delayedFuture(1L, TimeUnit.MILLISECONDS)
                    .thenRun(() -> check1Run.set(true));
            firstStore.addIndexUniquenessCommitCheck(index, firstStore.indexSubspace(index), check1);

            CompletableFuture<Void> check2 = new CompletableFuture<>();
            RecordCoreException err = new RecordCoreException("unable to run check");
            check2.completeExceptionally(err);
            otherStore.addIndexUniquenessCommitCheck(index, otherStore.indexSubspace(index), check2);

            // Checks For index 1 should complete successfully and mark the "check1Run" boolean as completed. It
            // should not throw an error from check 2 completing exceptionally.
            firstStore.whenAllIndexUniquenessCommitChecks(index).get();
            assertTrue(check1Run.get(), "check1 should have marked check1Run as having completed");

            // For index 2, the error should be caught when the uniqueness checks are waited on
            ExecutionException thrownExecutionException = assertThrows(ExecutionException.class, () -> otherStore.whenAllIndexUniquenessCommitChecks(index).get());
            assertSame(err, thrownExecutionException.getCause());

            // The error from the "uniqueness check" should block the transaction from committing
            RecordCoreException thrownRecordCoreException = assertThrows(RecordCoreException.class, context::commit);
            assertSame(err, thrownRecordCoreException);
        }
    }

    @Test
    void multipleStores() throws Exception {
        final KeySpacePath otherPath = pathManager.createPath(TestKeySpace.RECORD_STORE);
        for (final KeySpacePath keySpacePath : List.of(path, otherPath)) {
            try (FDBRecordContext context = openContext()) {
                Pair<FDBRecordStore, QueryPlanner> recordStoreQueryPlannerPair = createOrOpenRecordStore(context, simpleMetaData(NO_HOOK), keySpacePath);
                recordStore = recordStoreQueryPlannerPair.getLeft();
                planner = recordStoreQueryPlannerPair.getRight();

                recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(1066L)
                        .setNumValue2(42)
                        .build());
                commit(context);
            }
        }

        final Index uniqueIndex = new Index("uniqueIndex", field("num_value_2"), IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        assertTrue(uniqueIndex.isUnique());
        final RecordMetaDataHook uniqueHook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", uniqueIndex);

        try (FDBRecordContext context = openContext()) {
            recordStore = createOrOpenRecordStore(context, simpleMetaData(uniqueHook), path).getLeft();
            commit(context);

            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setRecordStore(recordStore)
                    .setTargetIndexes(List.of(uniqueIndex))
                    .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                            .allowUniquePendingState(true))
                    .build()) {
                indexBuilder.buildIndex();
            }
        }

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore otherStore = createOrOpenRecordStore(context, simpleMetaData(uniqueHook), otherPath).getLeft();
            otherStore.markIndexWriteOnly(uniqueIndex).get();
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            recordStore = createOrOpenRecordStore(context, simpleMetaData(uniqueHook), path).getLeft();
            final FDBRecordStore otherStore = createOrOpenRecordStore(context, simpleMetaData(uniqueHook), otherPath).getLeft();

            assertEquals(IndexState.READABLE, recordStore.getIndexState(uniqueIndex));
            assertEquals(IndexState.WRITE_ONLY, otherStore.getIndexState(uniqueIndex));
            // add a violation to one
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1415L)
                    .setNumValue2(42)
                    .build());
            // mark the other as readable
            // this should not fail, because this store does not have violations
            otherStore.markIndexReadable(uniqueIndex).get();

            assertThrows(RecordIndexUniquenessViolation.class, () -> commit(context));
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
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .setNumValue2(42)
                    .build());
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1415L)
                    .setNumValue2(42)
                    .build());
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1815L)
                    .setNumValue2(42)
                    .build());

            commit(context);
        }

        final Index uniqueIndex = new Index("initiallyUniqueIndex", field("num_value_2"), IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        assertTrue(uniqueIndex.isUnique());
        final RecordMetaDataHook uniqueHook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", uniqueIndex);

        timer.reset();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, uniqueHook);
            assertTrue(recordStore.isVersionChanged());
            assertEquals(1L, timer.getCount(FDBStoreTimer.Events.REBUILD_INDEX));
            assertThat(recordStore.getIndexState(uniqueIndex), either(equalTo(IndexState.WRITE_ONLY)).or(equalTo(IndexState.READABLE_UNIQUE_PENDING)));
            assertThat(recordStore.scanUniquenessViolations(uniqueIndex).asList().get(), hasSize(3));
            commit(context);
        }

        // Copy the first index, but drop the uniqueness constraint. This keeps the index as is, including the
        // last_modified_version, so adding it to the meta-data won't cause the index to be rebuilt during
        // check version. However, bump the meta-data version to ensure that anyone with an old meta-data version
        // (who will expect the index to be unique, if READABLE) knows to reload the meta-data.
        final Index nonUniqueIndex = new IndexWithOptions(uniqueIndex, IndexOptions.UNIQUE_OPTION, Boolean.FALSE.toString());
        assertFalse(nonUniqueIndex.isUnique());
        final RecordMetaDataHook nonUniqueHook = metaDataBuilder -> {
            metaDataBuilder.addIndex("MySimpleRecord", nonUniqueIndex);
            metaDataBuilder.setVersion(metaDataBuilder.getVersion() + 1);
        };

        timer.reset();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, nonUniqueHook);
            assertTrue(recordStore.isVersionChanged());
            assertEquals(0L, timer.getCount(FDBStoreTimer.Events.REBUILD_INDEX));

            // We don't want a full rebuild of the index here, but it's possible that we'd want this to be done
            // automatically during checkVersion. Perhaps the best thing would be for checkVersion to look
            // for READABLE_UNIQUE_PENDING indexes that are not unique, which should only happen if a uniqueness
            // constraint is dropped. For any such index, checkVersion can clear out the uniqueness space and then
            // mark the index as READABLE.
            // See: https://github.com/FoundationDB/fdb-record-layer/issues/1991
            assertThat(recordStore.getIndexState(nonUniqueIndex), either(equalTo(IndexState.WRITE_ONLY)).or(equalTo(IndexState.READABLE_UNIQUE_PENDING)));
            assertTrue(recordStore.markIndexReadable(nonUniqueIndex).get());

            assertThat(recordStore.scanUniquenessViolations(nonUniqueIndex).asList().get(), empty());
            List<IndexEntry> indexEntries = recordStore.scanIndex(nonUniqueIndex, new IndexScanRange(IndexScanType.BY_VALUE, TupleRange.ALL), null, ScanProperties.FORWARD_SCAN).asList().get();
            assertThat(indexEntries, hasSize(3));
            Set<Long> indexKeys = indexEntries.stream()
                    .map(IndexEntry::getKey)
                    .map(indexKey -> indexKey.getLong(0))
                    .collect(Collectors.toSet());
            assertThat(indexKeys, hasSize(1));
            commit(context);
        }
    }
}
