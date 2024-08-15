/*
 * OnlineIndexerUniqueIndexTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreRetriableTransactionException;
import com.apple.foundationdb.record.RecordIndexUniquenessViolation;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for building unique indexes with {@link OnlineIndexer}.
 */
public class OnlineIndexerUniqueIndexTest extends OnlineIndexerTest {
    @Tag(Tags.Slow)
    @Test
    void uniquenessViolations() {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 10).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(((int)val) % 5).build()
        ).collect(Collectors.toList());
        Index index = new Index("simple$value_2", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);

        // Case 1: Entirely in build.
        openSimpleMetaData();
        try (FDBRecordContext context = openContext())  {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            context.commit();
        }
        try (OnlineIndexer indexBuilder = newIndexerBuilder(index).build()) {
            buildIndexAssertThrowUniquenessViolation(indexBuilder);

            // Case 2: While in write-only mode.
            try (FDBRecordContext context = openContext()) {
                recordStore.deleteAllRecords();
                recordStore.markIndexWriteOnly(index).join();
                context.commit();
            }
            try (FDBRecordContext context = openContext()) {
                records.forEach(recordStore::saveRecord);
                context.commit();
            }
            try (FDBRecordContext context = openContext()) {
                assertEquals(10, (int)recordStore.scanUniquenessViolations(index).getCount().join());
                context.commit();
            }
            buildIndexAssertThrowUniquenessViolation(indexBuilder);
        }

        // Case 3: Some in write-only mode.
        fdb.run(context -> {
            FDBRecordStore.deleteStore(context, path);
            return null;
        });
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < 5; i++) {
                recordStore.saveRecord(records.get(i));
            }
            context.commit();
        }
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            for (int i = 5; i < records.size(); i++) {
                recordStore.saveRecord(records.get(i));
            }
            context.commit();
        }
        try (OnlineIndexer indexBuilder = newIndexerBuilder(index).build()) {
            buildIndexAssertThrowUniquenessViolation(indexBuilder);
        }

        // Case 4: Some in write-only mode with an initial range build that shouldn't affect anything.
        fdb.run(context -> {
            FDBRecordStore.deleteStore(context, path);
            return null;
        });
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            for (int i = 5; i < records.size(); i++) {
                recordStore.saveRecord(records.get(i));
            }
            context.commit();
        }
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            for (int i = 0; i < 5; i++) {
                recordStore.saveRecord(records.get(i));
            }
            context.commit();
        }
        try (OnlineIndexer indexBuilder = newIndexerBuilder(index).build()) {
            buildIndexAssertThrowUniquenessViolation(indexBuilder);
        }

        // Case 5: Should be caught by write-only writes after build.
        fdb.run(context -> {
            FDBRecordStore.deleteStore(context, path);
            return null;
        });
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < 5; i++) {
                recordStore.saveRecord(records.get(i));
            }
            context.commit();
        }
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            context.commit();
        }
        try (OnlineIndexer indexBuilder = newIndexerBuilder(index).build()) {
            indexBuilder.buildIndex();
        }
        try (FDBRecordContext context = openContext()) {
            for (int i = 5; i < records.size(); i++) {
                recordStore.saveRecord(records.get(i));
            }
            context.commit();
            fail("Did not catch uniqueness violation when done after build by write-only writes");
        } catch (RecordIndexUniquenessViolation e) {
            // passed.
        }

        // Case 6: Should be caught by write-only writes after partial build.
        fdb.run(context -> {
            FDBRecordStore.deleteStore(context, path);
            return null;
        });
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < 5; i++) {
                recordStore.saveRecord(records.get(i));
            }
            context.commit();
        }
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            context.commit();
        }
        try (OnlineIndexer indexBuilder = newIndexerBuilder(index).build()) {
            try (FDBRecordContext context = openContext()) {
                for (int i = 5; i < records.size(); i++) {
                    recordStore.saveRecord(records.get(i));
                }
                context.commit();
            }
            buildIndexAssertThrowUniquenessViolation(indexBuilder);
            try (FDBRecordContext context = openContext()) {
                assertEquals(10, (int)recordStore.scanUniquenessViolations(index).getCount().join());
                context.commit();
            }
        }

        // Case 7: The second of these two transactions should fail on not_committed, and then
        // there should be a uniqueness violation.
        fdb.run(context -> {
            FDBRecordStore.deleteStore(context, path);
            return null;
        });
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < 5; i++) {
                recordStore.saveRecord(records.get(i));
            }
            context.commit();
        }
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            context.commit();
        }
        try (OnlineIndexer indexBuilder = newIndexerBuilder(index).build()) {
            try (FDBRecordContext context = openContext()) {
                context.getReadVersion();
                try (FDBRecordContext context2 = fdb.openContext()) {
                    context2.getReadVersion();
                    FDBRecordStore recordStore2 = recordStore.asBuilder().setContext(context2).build();

                    indexBuilder.buildIndexAsync(false).join();
                    recordStore2.saveRecord(records.get(8));

                    context.commit();
                    context2.commitAsync().handle((ignore, e) -> {
                        assertNotNull(e);
                        RuntimeException runE = FDBExceptions.wrapException(e);
                        assertThat(runE, instanceOf(RecordCoreRetriableTransactionException.class));
                        assertNotNull(runE.getCause());
                        assertThat(runE.getCause(), instanceOf(FDBException.class));
                        FDBException fdbE = (FDBException)runE.getCause();
                        assertEquals(FDBError.NOT_COMMITTED.code(), fdbE.getCode());
                        return null;
                    }).join();
                }
            }
            try (FDBRecordContext context = openContext()) {
                for (int i = 5; i < records.size(); i++) {
                    recordStore.saveRecord(records.get(i));
                }
                context.commit();
            }
            try (FDBRecordContext context = openContext()) {
                assertEquals(10, (int)recordStore.scanUniquenessViolations(index).getCount().join());
                context.commit();
            }
            buildIndexAssertThrowUniquenessViolation(indexBuilder);
        }
    }

    private void buildIndexAssertThrowUniquenessViolation(OnlineIndexer indexer) {
        indexer.buildIndexAsync().handle((ignore, e) -> {
            assertNotNull(e);
            RuntimeException runE = FDBExceptions.wrapException(e);
            assertNotNull(runE);
            assertThat(runE, instanceOf(RecordIndexUniquenessViolation.class));
            return null;
        }).join();
    }

    @Test
    void resolveUniquenessViolations() {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 10).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(((int)val) % 5).build()
        ).collect(Collectors.toList());
        Index index = new Index("simple$value_2", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);

        openSimpleMetaData();
        try (FDBRecordContext context = openContext())  {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            context.commit();
        }
        try (OnlineIndexer indexBuilder = newIndexerBuilder(index).build()) {
            buildIndexAssertThrowUniquenessViolation(indexBuilder);
        }

        try (FDBRecordContext context = openContext()) {
            Set<Tuple> indexEntries = new HashSet<>(recordStore.scanUniquenessViolations(index)
                    .map( v -> v.getIndexEntry().getKey() )
                    .asList().join());

            for (Tuple indexKey : indexEntries) {
                List<Tuple> primaryKeys = recordStore.scanUniquenessViolations(index, indexKey).map(RecordIndexUniquenessViolation::getPrimaryKey).asList().join();
                assertEquals(2, primaryKeys.size());
                recordStore.resolveUniquenessViolation(index, indexKey, primaryKeys.get(0)).join();
                assertEquals(0, (int)recordStore.scanUniquenessViolations(index, indexKey).getCount().join());
            }

            for (int i = 0; i < 5; i++) {
                assertNotNull(recordStore.loadRecord(Tuple.from(i)));
            }
            for (int i = 5; i < records.size(); i++) {
                assertNull(recordStore.loadRecord(Tuple.from(i)));
            }

            recordStore.markIndexReadable(index).join();
            context.commit();
        }
    }

    @Test
    void testReadableUniquePendingSingle() {
        final String indexName = "simple$value_2";
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 10).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(((int)val) % 5).build()
        ).collect(Collectors.toList());
        Index index = new Index(indexName, field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);

        openSimpleMetaData();
        try (FDBRecordContext context = openContext())  {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            context.commit();
        }
        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(index)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .allowUniquePendingState())
                .build()) {
            indexBuilder.buildIndex(true);
        }
        try (FDBRecordContext context = openContext()) {
            final IndexState indexState = recordStore.getIndexState(indexName);
            assertEquals(IndexState.READABLE_UNIQUE_PENDING, indexState);
            assertTrue(recordStore.isIndexReadableUniquePending(indexName));
            context.commit();
        }
        // now try resolving it, and marking readable with another build
        try (FDBRecordContext context = openContext()) {
            Set<Tuple> indexEntries = new HashSet<>(recordStore.scanUniquenessViolations(index)
                    .map( v -> v.getIndexEntry().getKey() )
                    .asList().join());

            for (Tuple indexKey : indexEntries) {
                List<Tuple> primaryKeys = recordStore.scanUniquenessViolations(index, indexKey).map(RecordIndexUniquenessViolation::getPrimaryKey).asList().join();
                assertEquals(2, primaryKeys.size());
                recordStore.resolveUniquenessViolation(index, indexKey, primaryKeys.get(0)).join();
                assertEquals(0, (int)recordStore.scanUniquenessViolations(index, indexKey).getCount().join());
            }

            for (int i = 0; i < 5; i++) {
                assertNotNull(recordStore.loadRecord(Tuple.from(i)));
            }
            for (int i = 5; i < records.size(); i++) {
                assertNull(recordStore.loadRecord(Tuple.from(i)));
            }
            context.commit();
        }

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(index)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .allowUniquePendingState())
                .build()) {
            indexBuilder.buildIndex(true);
        }
        assertReadable(index);
    }

    @Test
    void testUniquenessMultiTargetsForbidUniquePending() {
        testUniquenessMultiTarget(false);
    }

    @Test
    void testUniquenessMultiTargetsAllowUniquePending() {
        testUniquenessMultiTarget(true);
    }

    private void testUniquenessMultiTarget(boolean allowUniquePending) {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 10).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(((int)val) % 5).build()
        ).collect(Collectors.toList());

        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);

        openSimpleMetaData();
        try (FDBRecordContext context = openContext())  {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }
        openSimpleMetaData(hook);
        disableAll(indexes);

        // Force a RecordCoreException failure
        final String throwMsg = "Intentionally crash during test";
        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes)
                .setLimit(1)
                .setConfigLoader(old -> {
                    throw new RecordCoreException(throwMsg);
                })
                .build()) {

            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains(throwMsg));
            // The index should be partially built
        }

        // build normally
        disableAll(indexes);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .allowUniquePendingState(allowUniquePending))
                .build()) {
            if (allowUniquePending) {
                indexBuilder.buildIndex();
            } else {
                buildIndexAssertThrowUniquenessViolation(indexBuilder);
            }
        }

        try (FDBRecordContext context = openContext()) {
            // unique index with uniqueness violation:
            assertEquals(10, (int)recordStore.scanUniquenessViolations(indexes.get(0)).getCount().join());
            if (allowUniquePending) {
                assertTrue(recordStore.isIndexReadableUniquePending(indexes.get(0)));
                final List<IndexEntry> scanned = recordStore.scanIndex(indexes.get(0), IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                        .asList().join();
                assertEquals(scanned.size(), records.size());
                List<Long> numValues = records.stream().map(TestRecords1Proto.MySimpleRecord::getNumValue2).map(Integer::longValue).collect(Collectors.toList());
                List<Long> scannedValues = scanned.stream().map(IndexEntry::getKey).map(tuple -> tuple.getLong(0)).collect(Collectors.toList());
                assertTrue(numValues.containsAll(scannedValues));
                assertTrue(scannedValues.containsAll(numValues));
            } else {
                assertTrue(recordStore.isIndexWriteOnly(indexes.get(0)));
                RecordCoreException e = assertThrows(ScanNonReadableIndexException.class,
                        () -> recordStore.scanIndex(indexes.get(0), IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN));
                assertTrue(e.getMessage().contains("Cannot scan non-readable index"));
            }
            // non-unique index:
            assertTrue(recordStore.isIndexReadable(indexes.get(1)));
            // unique index of unique numbers:
            assertTrue(recordStore.isIndexReadable(indexes.get(2)));
            context.commit();
        }

        // now try resolving the duplications, and marking readable with another build
        final Index index = indexes.get(0);
        try (FDBRecordContext context = openContext()) {
            Set<Tuple> indexEntries = new HashSet<>(recordStore.scanUniquenessViolations(index)
                    .map( v -> v.getIndexEntry().getKey() )
                    .asList().join());

            for (Tuple indexKey : indexEntries) {
                List<Tuple> primaryKeys = recordStore.scanUniquenessViolations(index, indexKey).map(RecordIndexUniquenessViolation::getPrimaryKey).asList().join();
                assertEquals(2, primaryKeys.size());
                recordStore.resolveUniquenessViolation(index, indexKey, primaryKeys.get(0)).join();
                assertEquals(0, (int)recordStore.scanUniquenessViolations(index, indexKey).getCount().join());
            }

            for (int i = 0; i < 5; i++) {
                assertNotNull(recordStore.loadRecord(Tuple.from(i)));
            }
            for (int i = 5; i < records.size(); i++) {
                assertNull(recordStore.loadRecord(Tuple.from(i)));
            }
            context.commit();
        }
        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder()
                .setIndex(index)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .allowTakeoverContinue()
                        .allowUniquePendingState())
                .build()) {
            indexBuilder.buildIndex(true);
        }
        try (FDBRecordContext context = openContext()) {
            assertTrue(recordStore.isIndexReadable(index.getName()));
            assertEquals(0, (int)recordStore.scanUniquenessViolations(indexes.get(0)).getCount().join());
            context.commit();
        }
    }

    @Test
    void testMarkReadableOrUniquePendingUnchanged() {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 10).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(((int)val) % 5).build()
        ).collect(Collectors.toList());

        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);

        openSimpleMetaData();
        try (FDBRecordContext context = openContext())  {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }
        openSimpleMetaData(hook);
        disableAll(indexes);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .allowUniquePendingState(true))
                .build()) {
            indexBuilder.buildIndex();
        }

        // verify that unique pending state is unchanged
        try (FDBRecordContext context = openContext()) {
            for (Index index: indexes) {
                assertTrue(recordStore.isIndexReadableUniquePending(index));
                boolean changed = recordStore.markIndexReadableOrUniquePending(index).join();
                assertFalse(changed);
            }
            context.commit();
        }

        // scan violations
        try (FDBRecordContext context = openContext()) {
            assertEquals(10, (int)recordStore.scanUniquenessViolations(indexes.get(0)).getCount().join());
            context.commit();
        }

        // resolve violations
        try (FDBRecordContext context = openContext()) {
            for (int i = 5 ; i < 10; i++) {
                TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(i).setNumValue2(i).build();
                recordStore.saveRecord(rec);
            }
            context.commit();
        }

        // scan violations
        try (FDBRecordContext context = openContext()) {
            assertEquals(0, (int)recordStore.scanUniquenessViolations(indexes.get(0)).getCount().join());
            context.commit();
        }

        // assert change after resolving
        try (FDBRecordContext context = openContext()) {
            for (Index index: indexes) {
                assertTrue(recordStore.isIndexReadableUniquePending(index));
                boolean changed = recordStore.markIndexReadableOrUniquePending(index).join();
                assertTrue(changed);
            }
            context.commit();
        }

        // assert readable state
        assertReadable(indexes);
    }

    @Test
    void testRepeatingAndNewUniquenessViolation() {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 20).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(((int)val) % 4).build()
        ).collect(Collectors.toList());

        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);

        openSimpleMetaData();
        try (FDBRecordContext context = openContext())  {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }
        openSimpleMetaData(hook);
        disableAll(indexes);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .allowUniquePendingState(true))
                .build()) {
            indexBuilder.buildIndex();
        }

        // verify that unique pending state is unchanged
        try (FDBRecordContext context = openContext()) {
            for (Index index: indexes) {
                assertTrue(recordStore.isIndexReadableUniquePending(index));
                boolean changed = recordStore.markIndexReadableOrUniquePending(index).join();
                assertFalse(changed);
            }
            context.commit();
        }

        // add new violation -- this should fail while the index's state is READABLE_UNIQUE_PENDING
        try (FDBRecordContext context = openContext()) {
            for (int i = 20 ; i < 25; i++) {
                TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(i).setNumValue2(100).build();
                recordStore.saveRecord(rec);
            }
            assertThrows(RecordIndexUniquenessViolation.class, context::commit);
        }

        // verify that unique pending state is unchanged
        try (FDBRecordContext context = openContext()) {
            for (Index index: indexes) {
                assertTrue(recordStore.isIndexReadableUniquePending(index));
                boolean changed = recordStore.markIndexReadableOrUniquePending(index).join();
                assertFalse(changed);
                assertEquals(20, (int)recordStore.scanUniquenessViolations(index).getCount().join());
            }
            context.commit();
        }

        // resolve duplications (keep the first items unchanged)
        try (FDBRecordContext context = openContext()) {
            for (int i = 4 ; i < 25; i++) {
                TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(i).setNumValue2(i).build();
                recordStore.saveRecord(rec);
            }
            context.commit();
        }

        // mark readable via build index's short circuit
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .allowUniquePendingState(true))
                .build()) {
            indexBuilder.buildIndex();
        }

        // assert readable state
        try (FDBRecordContext context = openContext()) {
            for (Index index: indexes) {
                assertTrue(recordStore.isIndexReadable(index));
                assertEquals(0, (int)recordStore.scanUniquenessViolations(index).getCount().join());
            }
            context.commit();
        }
    }

    @Test
    void testPartlyBuiltMarkReadableFailure() {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 10).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(((int)val) % 5).build()
        ).collect(Collectors.toList());

        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);

        openSimpleMetaData();
        try (FDBRecordContext context = openContext())  {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }
        openSimpleMetaData(hook);
        disableAll(indexes);

        // partly build the index
        final AtomicLong counter = new AtomicLong(0);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes)
                .setLimit(1)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .allowUniquePendingState(true))
                .setConfigLoader(old -> {
                    if (counter.incrementAndGet() > 4) {
                        throw new RecordCoreException("Intentionally crash during test");
                    }
                    return old;
                })
                .build()) {
            assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            // The index should be partially built
        }

        // assert failure to mark readable or unique pending
        try (FDBRecordContext context = openContext()) {
            for (Index index: indexes) {
                assertTrue(recordStore.isIndexWriteOnly(index));
                assertThrows(Exception.class, () -> recordStore.markIndexReadableOrUniquePending(index).join());
                assertThrows(Exception.class, () -> recordStore.markIndexReadable(index).join());
            }
            context.commit();
        }
    }

    @Test
    void testIndexingFromReadableUniquePendingIndex() {
        Index srcIndex = new Index("src_index", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index tgtIndex = new Index("tgt_index", field("num_value_3_indexed").ungrouped(), IndexTypes.SUM);
        List<Index> indexes = new ArrayList<>();
        indexes.add(srcIndex);
        indexes.add(tgtIndex);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);

        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            for (int i = 0 ; i < 11; i++) {
                TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(1003 + i).setNumValue2(i % 5).build();
                recordStore.saveRecord(rec);
            }
            context.commit();
        }

        openSimpleMetaData(hook);
        disableAll(indexes);
        // build source index
        try (OnlineIndexer indexBuilder = newIndexerBuilder()
                .setIndex(srcIndex)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .allowUniquePendingState())
                .build()) {
            indexBuilder.buildIndex();
        }

        // assert source index state is unique pending
        try (FDBRecordContext context = openContext()) {
            assertTrue(recordStore.isIndexReadableUniquePending(srcIndex));
            context.commit();
        }

        // build target index from source index (note - may fall back to by-records)
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex(srcIndex.getName())
                        .build())
                .build()) {
            indexBuilder.buildIndex(true);
        }

        // assert target index state is readable
        assertReadable(tgtIndex);
    }
}
