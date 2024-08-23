/*
 * LucenOnlineIndexingTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.FDBError;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.common.text.AllSuffixesTextTokenizer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexDeferredMaintenanceControl;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.RandomSeedSource;
import com.apple.test.RandomizedTestUtils;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMap;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.lucene.LuceneIndexOptions.INDEX_PARTITION_BY_FIELD_NAME;
import static com.apple.foundationdb.record.lucene.LuceneIndexOptions.INDEX_PARTITION_HIGH_WATERMARK;
import static com.apple.foundationdb.record.lucene.LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED;
import static com.apple.foundationdb.record.lucene.LuceneIndexTest.ENGINEER_JOKE;
import static com.apple.foundationdb.record.lucene.LuceneIndexTest.WAYLON;
import static com.apple.foundationdb.record.lucene.LuceneIndexTest.complexPartitionedIndex;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.NGRAM_LUCENE_INDEX;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.QUERY_ONLY_SYNONYM_LUCENE_INDEX;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.SIMPLE_TEXT_SUFFIXES;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.TEXT_AND_STORED;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.createComplexDocument;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.createSimpleDocument;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.COMPLEX_DOC;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.MAP_DOC;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.SIMPLE_DOC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LuceneOnlineIndexingTest extends FDBRecordStoreTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneOnlineIndexingTest.class);


    private void rebuildIndexMetaData(final FDBRecordContext context, final String document, final Index index) {
        Pair<FDBRecordStore, QueryPlanner> pair = LuceneIndexTestUtils.rebuildIndexMetaData(context, path, document, index, isUseCascadesPlanner());
        this.recordStore = pair.getLeft();
        this.planner = pair.getRight();
    }

    private void disableIndex(Index index, String document) {
        try (final FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, document, index);
            recordStore.markIndexDisabled(index).join();
            context.commit();
        }
    }

    @Test
    void luceneOnlineIndexingTestSimple() {
        Index index = SIMPLE_TEXT_SUFFIXES;
        disableIndex(index, SIMPLE_DOC);
        try (final FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
            recordStore.saveRecord(createSimpleDocument(2222L, WAYLON + " who?", 1));
            context.commit();
        }
        try (final FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
            recordStore.saveRecord(createSimpleDocument(2222L, WAYLON + " who?", 1));
            context.commit();
        }
        try (final FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setRecordStore(recordStore)
                    .setIndex(index)
                    .build()) {
                assertTrue(recordStore.isIndexDisabled(index));
                indexBuilder.buildIndex(true);
            }
        }
        try (final FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            assertTrue(recordStore.isIndexReadable(index));
        }
        String[] allFiles = listFiles(index);
        assertTrue(allFiles.length < 12);
    }

    @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
    @Test
    void luceneOnlineIndexingTestWithRecordUpdates() throws IOException {
        final Map<String, String> options = Map.of(
                INDEX_PARTITION_BY_FIELD_NAME, "timestamp",
                PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED, "true",
                INDEX_PARTITION_HIGH_WATERMARK, String.valueOf(2));

        Index index = complexPartitionedIndex(options);
        disableIndex(index, COMPLEX_DOC);

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 2)
                .build();

        long startTime = System.currentTimeMillis();

        long docId_1 = 1623L;
        long docId_2 = 1547L;
        long docId_3 = 2222L;
        long docId_4 = 899L;
        Tuple primaryKey_1 = Tuple.from(1, docId_1);
        Tuple primaryKey_2 = Tuple.from(1, docId_2);
        Tuple primaryKey_3 = Tuple.from(1, docId_3);
        Tuple primaryKey_4 = Tuple.from(1, docId_4);
        try (final FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, COMPLEX_DOC, index);
            recordStore.saveRecord(createComplexDocument(docId_1, WAYLON, 1, startTime));
            recordStore.saveRecord(createComplexDocument(docId_2, WAYLON, 1, startTime + 1000));
            recordStore.saveRecord(createComplexDocument(docId_3, WAYLON + " who?", 1, startTime + 2000));
            recordStore.saveRecord(createComplexDocument(docId_4, ENGINEER_JOKE, 1, startTime + 3000));
            context.commit();
        }

        // do not index any records and leave the index writeOnly
        try (final FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, COMPLEX_DOC, index);
            final RuntimeException stopBuildException = new RuntimeException("stop build");
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setRecordStore(recordStore)
                    .setIndex(index)
                    .setInitialLimit(1)
                    .setConfigLoader(config -> {
                        throw stopBuildException;
                    })
                    .build()) {
                assertTrue(recordStore.isIndexDisabled(index));
                final RuntimeException thrown = assertThrows(RuntimeException.class, () -> indexBuilder.buildIndex(true));
                assertSame(stopBuildException, thrown);
            }
        }

        final LuceneIndexTestValidator luceneIndexTestValidator = new LuceneIndexTestValidator(() -> openContext(contextProps), context -> {
            rebuildIndexMetaData(context, COMPLEX_DOC, index);
            return recordStore;
        });

        final List<LucenePartitionInfoProto.LucenePartitionInfo> partitionMeta = luceneIndexTestValidator.getPartitionMeta(index, Tuple.from(1L));
        assertTrue(partitionMeta.isEmpty());

        // update a record; no partition information exist yet
        // this will succeed
        try (final FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, COMPLEX_DOC, index);
            assertTrue(recordStore.isIndexWriteOnly(index));
            recordStore.saveRecord(createComplexDocument(docId_1, ENGINEER_JOKE, 1, startTime));
            context.commit();
        }

        // validate a new partition meta has been created
        List<LucenePartitionInfoProto.LucenePartitionInfo> partitionInfos = luceneIndexTestValidator.getPartitionMeta(index, Tuple.from(1));
        assertEquals(1, partitionInfos.size());
        assertEquals(1, partitionInfos.get(0).getCount());
        assertEquals(Tuple.from(startTime).addAll(primaryKey_1), LucenePartitioner.getPartitionKey(partitionInfos.get(0)));
        assertEquals(Tuple.from(startTime).addAll(primaryKey_1), LucenePartitioner.getToTuple(partitionInfos.get(0)));

        // update another record; partition information exist, but the record isn't indexed yet
        // this will also succeed
        try (final FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, COMPLEX_DOC, index);
            assertTrue(recordStore.isIndexWriteOnly(index));
            recordStore.saveRecord(createComplexDocument(docId_2, ENGINEER_JOKE, 1, startTime + 1_000));
            // update record a second time
            recordStore.saveRecord(createComplexDocument(docId_1, ENGINEER_JOKE + " XANADU", 1, startTime));
            context.commit();
        }

        // validate partition meta has been correctly updated
        partitionInfos = luceneIndexTestValidator.getPartitionMeta(index, Tuple.from(1));
        assertEquals(1, partitionInfos.size());
        assertEquals(2, partitionInfos.get(0).getCount());
        assertEquals(Tuple.from(startTime).addAll(primaryKey_1), LucenePartitioner.getPartitionKey(partitionInfos.get(0)));
        assertEquals(Tuple.from(startTime + 1_000).addAll(primaryKey_2), LucenePartitioner.getToTuple(partitionInfos.get(0)));

        // finish the indexing process
        try (final FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, COMPLEX_DOC, index);
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setRecordStore(recordStore)
                    .setIndex(index)
                    .build()) {
                assertTrue(recordStore.isIndexWriteOnly(index));
                indexBuilder.buildIndex(true);
            }
        }

        // validate index is sane
        luceneIndexTestValidator.validate(
                index,
                Map.of(
                        Tuple.from(1), Map.of(
                                primaryKey_4, Tuple.from(startTime + 3_000),
                                primaryKey_3, Tuple.from(startTime + 2_000),
                                primaryKey_2, Tuple.from(startTime + 1_000),
                                primaryKey_1, Tuple.from(startTime)
                        )
                ),
                "*:*");

        // validate that the records that were updated actually got updated
        try (final FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, COMPLEX_DOC, index);

            LuceneScanQuery query = (LuceneScanQuery)LuceneIndexTestValidator.groupedSortedTextSearch(recordStore, index, "text:software", null, 1);
            try (RecordCursor<IndexEntry> indexEntryCursor = recordStore.scanIndex(
                    index,
                    query,
                    null,
                    ExecuteProperties.newBuilder().build().asScanProperties(false))) {
                List<IndexEntry> entries = indexEntryCursor.asList().join();

                assertEquals(3, entries.size());
                assertEquals(Set.of(primaryKey_4, primaryKey_2, primaryKey_1), entries.stream().map(IndexEntry::getPrimaryKey).collect(Collectors.toSet()));
            }

            query = (LuceneScanQuery)LuceneIndexTestValidator.groupedSortedTextSearch(recordStore, index, "text:XANADU", null, 1);
            try (RecordCursor<IndexEntry> indexEntryCursor = recordStore.scanIndex(
                    index,
                    query,
                    null,
                    ExecuteProperties.newBuilder().build().asScanProperties(false))) {
                List<IndexEntry> entries = indexEntryCursor.asList().join();

                assertEquals(1, entries.size());
                assertEquals(Set.of(primaryKey_1), entries.stream().map(IndexEntry::getPrimaryKey).collect(Collectors.toSet()));
            }
        }
    }

    @ParameterizedTest
    @RandomSeedSource({
            6997773450764782661L, // original passing seed
            3416978384487730594L, // this one failed reliably when most seeds passed
            6096618498708109618L // this one failed too
    })
    void luceneOnlineIndexingTestWithAllRecordUpdates(long seed) {
        final Map<String, String> options = Map.of(
                INDEX_PARTITION_BY_FIELD_NAME, "timestamp",
                PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED, "true",
                INDEX_PARTITION_HIGH_WATERMARK, String.valueOf(2));

        // store time field
        Index index = new Index("Complex$partitioned",
                concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
                        function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp")),
                        function(LuceneFunctionNames.LUCENE_STORED, field("time")))
                        .groupBy(field("group")),
                LuceneIndexTypes.LUCENE,
                options);

        disableIndex(index, COMPLEX_DOC);

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 2)
                .build();

        Random random = new Random(seed);

        final int docCount = 5;
        Map<Long, Long> primaryKeyToTimestamp = new HashMap<>();
        try (final FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, COMPLEX_DOC, index);
            for (int i = 0; i < docCount; i++) {
                Long docId = RandomizedTestUtils.randomNotIn(primaryKeyToTimestamp.keySet(), () -> (long) (random.nextInt(1000) + 1000));
                Long timestamp = RandomizedTestUtils.randomNotIn(primaryKeyToTimestamp.values(), () -> (long) (random.nextInt(100) + 1));
                primaryKeyToTimestamp.put(docId, timestamp);
                recordStore.saveRecord(createComplexDocument(docId, ENGINEER_JOKE, 1, timestamp));
            }
            context.commit();
        }

        AtomicInteger counter = new AtomicInteger(0);
        try (final FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, COMPLEX_DOC, index);
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setRecordStore(recordStore)
                    .setIndex(index)
                    .setInitialLimit(1)
                    .setLimit(1)
                    .setConfigLoader(old -> {
                        try (final FDBRecordContext context2 = fdb.openContext(context.getConfig())) {
                            rebuildIndexMetaData(context2, COMPLEX_DOC, index);
                            for (Map.Entry<Long, Long> entry : primaryKeyToTimestamp.entrySet()) {
                                final long newTime = counter.getAndIncrement();
                                TestRecordsTextProto.ComplexDocument doc = TestRecordsTextProto.ComplexDocument.newBuilder()
                                        .setDocId(entry.getKey())
                                        .setTimestamp(entry.getValue())
                                        .setGroup(1)
                                        .setTime(newTime)
                                        .build();
                                recordStore.saveRecord(doc);
                            }
                            if (counter.get() == 2 * docCount) { // on the "second" iteration
                                // add a new doc
                                TestRecordsTextProto.ComplexDocument doc = TestRecordsTextProto.ComplexDocument.newBuilder()
                                        .setDocId(100)
                                        .setGroup(1)
                                        .setText("extra record")
                                        .setTimestamp(200)
                                        .setTime(Math.pow(docCount, 2) - 1)
                                        .build();
                                recordStore.saveRecord(doc);
                            }
                            context2.commit();
                        }
                        return old.toBuilder()
                                .setMaxLimit(1)
                                .setMaxRetries(Integer.MAX_VALUE)
                                .build();
                    }).build()) {
                indexBuilder.buildIndex();
            }
        }

        try (final FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, COMPLEX_DOC, index);

            assertTrue(recordStore.isIndexReadable(index));

            // search for docs with Math.pow(docCount, 2) - docCount < time < Math.pow(docCount, 2)
            // which should return all docs, if their updates completed successfully during index build
            final String search = "time:[" + (Math.pow(docCount, 2) - docCount) + " TO " + Math.pow(docCount, 2) + "]";
            LuceneScanQuery query = new LuceneScanQueryParameters(
                    Verify.verifyNotNull(ScanComparisons.from(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 1))),
                    new LuceneQuerySearchClause(LuceneQueryType.QUERY, search, false),
                    new Sort(new SortField("timestamp", SortField.Type.LONG, false)),
                    List.of("time"),
                    List.of(LuceneIndexExpressions.DocumentFieldType.DOUBLE),
                    null).bind(recordStore, index, EvaluationContext.EMPTY);

            try (RecordCursor<IndexEntry> indexEntryCursor = recordStore.scanIndex(
                    index,
                    query,
                    null,
                    ExecuteProperties.newBuilder().build().asScanProperties(false))) {
                List<IndexEntry> entries = indexEntryCursor.asList().join();

                // find all 6 documents
                assertEquals(6, entries.size());
            }
        }
    }

    @Test
    void luceneOnlineIndexingTest1() {
        luceneOnlineIndexingTestAny(QUERY_ONLY_SYNONYM_LUCENE_INDEX, COMPLEX_DOC, 17, 7, 0, 20);
    }

    @Test
    void luceneOnlineIndexingTest2() {
        luceneOnlineIndexingTestAny(QUERY_ONLY_SYNONYM_LUCENE_INDEX, SIMPLE_DOC, 15, 100, 300, 4);
    }

    @Test
    void luceneOnlineIndexingTest3() {
        luceneOnlineIndexingTestAny(NGRAM_LUCENE_INDEX, SIMPLE_DOC, 44, 7, 2, 34);
    }

    @Test
    void luceneOnlineIndexingTest4() {
        luceneOnlineIndexingTestAny(TEXT_AND_STORED, COMPLEX_DOC, 8, 100, 1, 4);
    }

    @Test
    void luceneOnlineIndexingTest5() {
        luceneOnlineIndexingTestAny(LuceneIndexTestUtils.COMPLEX_MULTIPLE_GROUPED, COMPLEX_DOC, 77, 20, 2, 20);
    }

    @Test
    void luceneOnlineIndexingTest6() {
        luceneOnlineIndexingTestAny(LuceneIndexTestUtils.COMPLEX_MULTIPLE_GROUPED, COMPLEX_DOC, 77, 20, 0, 20);
    }

    private String randomText(Random rn) {
        switch (rn.nextInt() % 4) {
            case 0: return ENGINEER_JOKE;
            case 1: return WAYLON;
            case 2: return WAYLON + " says who?";
            case 3: return ENGINEER_JOKE + " is it really funny?";
            default: return ENGINEER_JOKE + WAYLON ;
        }
    }

    private int randomGroup(Random rn) {
        return rn.nextInt() % 2;
    }


    void luceneOnlineIndexingTestAny(Index index, String document, int numRecords, int transactionLimit, int mergesLimit, final int expectedFileCount) {
        assertTrue(numRecords > 3);
        final Random rn = new Random();
        rn.nextInt();
        disableIndex(index, document);

        long[] docIds = new long[numRecords * 2];
        for (int i = 0; i < numRecords * 2; i ++) {
            docIds[i] = rn.nextLong();
        }
        int middle = 1 + rn.nextInt(numRecords - 2);
        // write records
        try (final FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, document, index);
            for (int i = 0; i < numRecords; i ++) {
                long docId = docIds[i];
                if (document.equals(SIMPLE_DOC)) {
                    recordStore.saveRecord(createSimpleDocument(docId, randomText(rn), randomGroup(rn)));
                } else if (document.equals(COMPLEX_DOC)) {
                    recordStore.saveRecord(createComplexDocument(docId, randomText(rn), randomText(rn), randomGroup(rn)));
                } else {
                    Assertions.fail("Unexpected document type: " + document);
                    return;
                }
            }
            context.commit();
        }
        // overwrite some records (to enforce merge), write others as new
        try (final FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, document, index);
            for (int i = 0; i < middle; i ++) {
                long docId = docIds[i];
                recordStore.saveRecord(createSimpleDocument(docId, randomText(rn), randomGroup(rn)));
            }
            for (int i = middle; i < numRecords; i ++) {
                long docId = docIds[numRecords + i];
                recordStore.saveRecord(createSimpleDocument(docId, randomText(rn), randomGroup(rn)));
            }
            context.commit();
        }
        // build the index ..
        try (final FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, document, index);
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setRecordStore(recordStore)
                    .setIndex(index)
                    .setLimit(transactionLimit)
                    .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                            .setInitialMergesCountLimit(mergesLimit)
                            .build())
                    .build()) {
                assertTrue(recordStore.isIndexDisabled(index));
                indexBuilder.buildIndex(true);
            }
            context.commit();
        }
        // .. and assert readable mode
        try (final FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, document, index);
            assertTrue(recordStore.isIndexReadable(index));
        }
        // assert number of segments, the number below is based on previous runs of this test.
        // the key thing is to make sure that while it was building the index it actually did the merges.
        final String[] files = listFiles(index);
        // assert that it is greater than 1, as a sanity check, if it's less than 1, it means we didn't save any docs
        MatcherAssert.assertThat(String.join(", ", files),
                files, Matchers.arrayWithSize(Matchers.allOf(Matchers.greaterThan(1),
                        Matchers.lessThanOrEqualTo(expectedFileCount))));
    }

    @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
    @Test
    void luceneOnlineIndexingTestMulti() {
        int numRecords = 47;
        int transactionLimit = 10;
        int groupingCount = 1;
        final Random rn = new Random();
        int numIndexes = 4;
        rn.nextInt();
        List<Index> indexes = new ArrayList<>();
        for (int i = 0; i < numIndexes; i++) {
            indexes.add(new Index(
                    "Map_with_auto_complete$entry-value-" + i + "-" + groupingCount,
                    new GroupingKeyExpression(field("entry",
                            KeyExpression.FanType.FanOut).nest(concat(LuceneIndexTestUtils.keys)), groupingCount + 1),
                    LuceneIndexTypes.LUCENE,
                    ImmutableMap.of()));
        }

        long[] docIds = new long[numRecords * 2];
        for (int i = 0; i < numRecords * 2; i ++) {
            docIds[i] = rn.nextLong();
        }
        int middle = 1 + rn.nextInt(numRecords - 2);
        // write records
        RecordMetaDataHook hook = metaDataBuilder -> {
            for (Index index: indexes) {
                metaDataBuilder.addIndex(MAP_DOC, index);
            }
            metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
            TextIndexTestUtils.addRecordTypePrefix(metaDataBuilder);
        };
        try (final FDBRecordContext context = openContext()) {
            recordStore = LuceneIndexTestUtils.openRecordStore(context, path, hook);
            for (Index index: indexes) {
                recordStore.markIndexDisabled(index).join();
            }
            context.commit();
        }
        try (final FDBRecordContext context = openContext()) {
            recordStore = LuceneIndexTestUtils.openRecordStore(context, path, hook);
            for (int i = 0; i < numRecords; i ++) {
                long docId = docIds[i];
                recordStore.saveRecord(createSimpleDocument(docId, randomText(rn), randomGroup(rn)));
            }
            context.commit();
        }
        // overwrite some records (to enforce merge), write others as new
        try (final FDBRecordContext context = openContext()) {
            recordStore = LuceneIndexTestUtils.openRecordStore(context, path, hook);
            for (int i = 0; i < middle; i ++) {
                long docId = docIds[i];
                recordStore.saveRecord(createSimpleDocument(docId, randomText(rn), randomGroup(rn)));
            }
            for (int i = middle; i < numRecords; i ++) {
                long docId = docIds[numRecords + i];
                recordStore.saveRecord(createSimpleDocument(docId, randomText(rn), randomGroup(rn)));
            }
            context.commit();
        }
        // build the index ..
        try (final FDBRecordContext context = openContext()) {
            recordStore = LuceneIndexTestUtils.openRecordStore(context, path, hook);
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setRecordStore(recordStore)
                    .setTargetIndexes(indexes)
                    .setLimit(transactionLimit)
                    .build()) {
                for (Index index: indexes) {
                    assertTrue(recordStore.isIndexDisabled(index));
                }
                indexBuilder.buildIndex(true);
            }
        }
        // .. and assert readable mode
        try (final FDBRecordContext context = openContext()) {
            recordStore = LuceneIndexTestUtils.openRecordStore(context, path, hook);
            for (Index index: indexes) {
                assertTrue(recordStore.isIndexReadable(index));
            }
        }
        // assert number of segments
        for (Index index: indexes) {
            String[] allFiles = listFiles(index);
            assertTrue(allFiles.length < 12);
        }
    }


    protected void openRecordStore(FDBRecordContext context, FDBRecordStoreTestBase.RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsTextProto.getDescriptor());
        metaDataBuilder.getRecordType(COMPLEX_DOC).setPrimaryKey(concatenateFields("group", "doc_id"));
        hook.apply(metaDataBuilder);
        recordStore = getStoreBuilder(context, metaDataBuilder.getRecordMetaData())
                .setSerializer(TextIndexTestUtils.COMPRESSING_SERIALIZER)
                .createOrOpen();
        setupPlanner(null);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void luceneOnlineIndexingTestGroupingKeys(int groupingCount) {
        Index index = new Index(
                "Map_with_auto_complete$entry-value",
                new GroupingKeyExpression(field("entry",
                        KeyExpression.FanType.FanOut).nest(concat(LuceneIndexTestUtils.keys)), groupingCount),
                LuceneIndexTypes.LUCENE,
                ImmutableMap.of());

        RecordMetaDataHook hook = metaDataBuilder -> {
            metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
            TextIndexTestUtils.addRecordTypePrefix(metaDataBuilder);
            metaDataBuilder.addIndex(MAP_DOC, index);
        };
        int group = 3;

        // disable the index
        try (final FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            recordStore.markIndexDisabled(index).join();
            context.commit();
        }

        // write records
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            for (long i = 1; i < 42; i++) {
                recordStore.saveRecord(multiEntryMapDoc(77L * i, ENGINEER_JOKE, group));
            }
            commit(context);
        }

        // overwrite few
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            for (long i = 1; i < 37; i += 2) {
                recordStore.saveRecord(multiEntryMapDoc(77L * i, WAYLON,  group));
            }
            commit(context);
        }

        // build the index ..
        try (final FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setRecordStore(recordStore)
                    .setIndex(index)
                    .build()) {
                assertTrue(recordStore.isIndexDisabled(index));
                indexBuilder.buildIndex(true);
            }
        }
        // .. and assert readable mode
        try (final FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            assertTrue(recordStore.isIndexReadable(index));
        }
    }

    @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void luceneOnlineIndexingTestGroupingKeysBackgroundMerge(int groupingCount) {
        final int groupedCount = 4 - groupingCount;
        Index index = new Index(
                "Map_with_auto_complete$entry-value",
                new GroupingKeyExpression(field("entry",
                        KeyExpression.FanType.FanOut).nest(concat(LuceneIndexTestUtils.keys)), groupedCount),
                LuceneIndexTypes.LUCENE,
                ImmutableMap.of());

        RecordMetaDataHook hook = metaDataBuilder -> {
            metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
            TextIndexTestUtils.addRecordTypePrefix(metaDataBuilder);
            metaDataBuilder.addIndex(MAP_DOC, index);
        };
        int group = 3;

        // write/overwrite records
        boolean needMerge = false;
        for (int iLast = 60; iLast > 40; iLast --) {
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context, hook);
                for (int i = 0; i < iLast; i++) {
                    recordStore.saveRecord(multiEntryMapDoc(77L * i, ENGINEER_JOKE + iLast, group));
                }
                final Set<Index> indexSet = recordStore.getIndexDeferredMaintenanceControl().getMergeRequiredIndexes();
                if (indexSet != null && !indexSet.isEmpty()) {
                    assertEquals(1, indexSet.size());
                    assertEquals(indexSet.stream().findFirst().get().getName(), index.getName());
                    needMerge = true;
                }
                commit(context);
            }
        }
        assertTrue(needMerge);

        Tuple tuple = Tuple.from("Text 2, and 1", "Text 3, plus 1", "I am text 4, with 1");
        String[] allFiles = listFiles(index, tuple, groupingCount);
        int oldLength = allFiles.length;

        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setRecordStore(recordStore)
                .setIndex(index)
                .build()) {
            indexBuilder.mergeIndex();
        }

        allFiles = listFiles(index, tuple, groupingCount);
        int newLength = allFiles.length;
        LOGGER.debug("Merge test: number of files: old=" + oldLength + " new=" + newLength);
        assertTrue(newLength > 0);
        assertTrue(newLength < oldLength);
    }

    private TestRecordsTextProto.MapDocument multiEntryMapDoc(long id, String text, int group) {
        assertTrue(group < 4);
        String text2 = "Text 2, and " + (id % 2);
        String text3 = "Text 3, plus " + (id % 3);
        String text4 = "I am text 4, with " + (id % 5);
        return TestRecordsTextProto.MapDocument.newBuilder()
                .setDocId(id)
                .setGroup(group)
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder()
                        .setKey(text2)
                        .setValue(text3)
                        .setSecondValue(text4)
                        .setThirdValue(text))
                .build();

    }

    private String[] listFiles(Index index) {
        try (FDBRecordContext context = openContext()) {
            if (index.getRootExpression() instanceof GroupingKeyExpression) {
                List<String> files = new ArrayList<>();
                for (int i = 0; i < 2; i++) {
                    final FDBDirectory directory = new FDBDirectory(
                            recordStore.indexSubspace(index).subspace(Tuple.from(i)),
                            context, index.getOptions());
                    files.addAll(Arrays.asList(directory.listAll()));
                }
                return files.toArray(new String[0]);
            } else {
                final FDBDirectory directory = new FDBDirectory(recordStore.indexSubspace(index), context, index.getOptions());
                return directory.listAll();
            }
        }
    }

    private String[] listFiles(Index index, Tuple tuple, int groupingCount) {
        try (FDBRecordContext context = openContext()) {
            final Subspace subspace = recordStore.indexSubspace(index);
            final FDBDirectory directory = new FDBDirectory(subspace.subspace(Tuple.fromItems(tuple.getItems().subList(0, groupingCount))), context, index.getOptions());
            return directory.listAll();
        }
    }

    @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
    @Test
    void testRecordUpdateBackgroundMerge() {
        boolean needMerge = false;
        Index index = SIMPLE_TEXT_SUFFIXES;
        // Need to reach 10 files to trigger a real merge
        int[] iLimits = {0, 20, 10, 50, 40, 100, 0, 10, 0, 9, 0, 8, 0, 7, 0, 6, 0, 5, 0, 4, 0, 3, 0, 2};
        for (int iIndex = 0; iIndex < iLimits.length;) {
            int iFIrst = iLimits[iIndex ++];
            int iLast = iLimits[iIndex ++];
            try (FDBRecordContext context = openContext()) {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                for (int i = iFIrst; i < iLast; i++) {
                    recordStore.saveRecord(createSimpleDocument(1623L + i, ENGINEER_JOKE + iIndex, 2));
                }
                final Set<Index> indexSet = recordStore.getIndexDeferredMaintenanceControl().getMergeRequiredIndexes();
                if (indexSet != null && !indexSet.isEmpty()) {
                    needMerge = true;
                }
                commit(context);
            }
        }

        assertTrue(needMerge);

        String[] allFiles = listFiles(index);
        int oldLength = allFiles.length;

        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setRecordStore(recordStore)
                .setIndex(index)
                .build()) {
            indexBuilder.mergeIndex();
        }

        allFiles = listFiles(index);
        int newLength = allFiles.length;
        LOGGER.debug("Merge test: number of files: old=" + oldLength + " new=" + newLength);
        assertTrue(newLength < oldLength);
        // Note: Running in a debugger shows that the explicit merge reduces the number of files from 21 to 3. Asserting
        // these magic numbers, however, seems to be fragile. The assertion above seems to be enough.
    }


    private boolean populateDataSplitSegments(Index index, int high, int low) {
        boolean needMerge = false;
        for (int iLast = high; iLast > low; iLast --) {
            try (FDBRecordContext context = openContext()) {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                for (int i = 0; i < iLast; i++) {
                    recordStore.saveRecord(createSimpleDocument(1623L + i, ENGINEER_JOKE + iLast, 2));
                }
                final Set<Index> indexSet = recordStore.getIndexDeferredMaintenanceControl().getMergeRequiredIndexes();
                if (indexSet != null && !indexSet.isEmpty()) {
                    final Optional<Index> first = indexSet.stream().findFirst();
                    assertEquals(1, indexSet.size());
                    assertEquals(first.get().getName(), index.getName());
                    needMerge = true;
                }
                commit(context);
            }
        }
        return needMerge;
    }

    @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
    @Test
    void testRecordUpdateBackgroundMerge2() {
        Index index = SIMPLE_TEXT_SUFFIXES;
        boolean needMerge = populateDataSplitSegments(index, 20, 5);
        assertTrue(needMerge);

        String[] allFiles = listFiles(index);
        int oldLength = allFiles.length;

        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setRecordStore(recordStore)
                .setIndex(index)
                .build()) {
            indexBuilder.mergeIndex();
        }

        allFiles = listFiles(index);
        int newLength = allFiles.length;
        LOGGER.debug("Merge test: number of files: old=" + oldLength + " new=" + newLength);
        assertTrue(newLength < oldLength);
    }

    @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
    @Test
    void testRecordUpdateMergeByAnotherRecordUpdate() {
        Index index = SIMPLE_TEXT_SUFFIXES;
        // update records while skipping merge
        boolean needMerge = populateDataSplitSegments(index, 20, 5);
        assertTrue(needMerge);

        String[] allFiles = listFiles(index);
        int oldLength = allFiles.length;

        // Now update records with merge
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(true);
            for (int i = 17; i < 22; i++) {
                recordStore.saveRecord(createSimpleDocument(1623L + i, ENGINEER_JOKE + " Sababa", 2));
            }
            commit(context);
        }

        allFiles = listFiles(index);
        int newLength = allFiles.length;
        LOGGER.debug("Merge test: number of files: old=" + oldLength + " new=" + newLength);
        assertTrue(newLength < oldLength);
    }

    @Test
    void testRecordUpdateReducedMerge() {
        // emulate repeating merge until until unchanged.
        Index index = SIMPLE_TEXT_SUFFIXES;
        boolean needMerge = populateDataSplitSegments(index, 40, 7);
        assertTrue(needMerge);

        int loopCounter = 0;
        for (boolean allDone = false; !allDone; loopCounter++) {
            int oldLength = listFiles(index).length;
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setRecordStore(recordStore)
                    .setIndex(index)
                    .build()) {
                indexBuilder.mergeIndex();
            }
            int newLength = listFiles(index).length;
            LOGGER.debug("Merge test: number of files: old=" + oldLength + " new=" + newLength +
                         " needMerge=" + recordStore.getIndexDeferredMaintenanceControl().getMergeRequiredIndexes());

            allDone = oldLength <= newLength;
        }
        assertTrue(loopCounter > 1);
        // Observed: 70 => 13, 13 => 2, 2 => 2
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 400})
    void luceneOnlineIndexingTestMergesLimit(int mergesLimit) {
        // emulate repeating merge until until unchanged with different merges limits
        Index index = SIMPLE_TEXT_SUFFIXES;
        boolean needMerge = populateDataSplitSegments(index, 40, 7);
        assertTrue(needMerge);

        int loopCounter = 0;
        for (boolean allDone = false; !allDone; loopCounter++) {
            int oldLength = listFiles(index).length;
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setRecordStore(recordStore)
                    .setIndex(index)
                    .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                            .setInitialMergesCountLimit(mergesLimit)
                            .build())
                    .build()) {
                indexBuilder.mergeIndex();
            }
            int newLength = listFiles(index).length;
            LOGGER.debug("Merge test with limits: merges_limit: " + mergesLimit + " number of files: old=" + oldLength + " new=" + newLength +
                         " needMerge=" + recordStore.getIndexDeferredMaintenanceControl().getMergeRequiredIndexes());

            allDone = oldLength <= newLength;
        }
        assertTrue(loopCounter > 1);
    }


    private static class TerribleMergingIndexMaintainer extends LuceneIndexMaintainer {
        private final IndexMaintainerState state;
        private static int pseodoFound = 0;

        protected TerribleMergingIndexMaintainer(final IndexMaintainerState state) {
            super(state, state.context.getExecutor());
            this.state = state;
        }

        @Override
        public CompletableFuture<Void> mergeIndex() {
            final IndexDeferredMaintenanceControl mergeControl = state.store.getIndexDeferredMaintenanceControl();
            mergeControl.setLastStep(IndexDeferredMaintenanceControl.LastStep.MERGE);
            final long limit = mergeControl.getMergesLimit();
            if (limit == 0) {
                mergeControl.setMergesFound(10);
                mergeControl.setMergesTried(10);
                throw new FDBException("transaction_too_old", FDBError.TRANSACTION_TOO_OLD.code());
            }
            if (limit > 3) {
                Assertions.assertEquals(5, limit);
                mergeControl.setMergesFound(10);
                mergeControl.setMergesTried(limit);
                throw new FDBException("transaction_too_old", FDBError.TRANSACTION_TOO_OLD.code());
            }
            // Here: pseudo success
            if (pseodoFound <= 0) {
                pseodoFound = 10;
            } else {
                // Here: if passed the failures above, the limit should be down to 2
                Assertions.assertEquals(2, limit);
            }
            mergeControl.setMergesFound(pseodoFound);
            int pseudoTried = Math.min((int)limit, pseodoFound);
            mergeControl.setMergesTried(pseudoTried);
            pseodoFound -= pseudoTried;
            return AsyncUtil.DONE;
        }
    }

    @AutoService(IndexMaintainerFactory.class)
    public static class TerribleMergingIndexMaintainerFactory implements IndexMaintainerFactory {
        @Nonnull
        @Override
        public Iterable<String> getIndexTypes() {
            return Collections.singletonList("terribleMerger");
        }

        @Nonnull
        @Override
        public IndexValidator getIndexValidator(Index index) {
            return new IndexValidator(index);
        }

        @Nonnull
        @Override
        public IndexMaintainer getIndexMaintainer(@Nonnull IndexMaintainerState state) {
            return new TerribleMergingIndexMaintainer(state);
        }
    }

    @Test
    void luceneOnlineIndexingTestMerger() {
        Index index = new Index("Simple$text_suffixes",
                function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
                "terribleMerger",
                ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME));

        boolean needMerge = populateDataSplitSegments(index, 4, 1);
        assertTrue(needMerge);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setRecordStore(recordStore)
                .setIndex(index)
                .setMaxAttempts(1)
                .build()) {
            indexBuilder.mergeIndex();
        }
    }

    private static class Terrible2MergingIndexMaintainer extends LuceneIndexMaintainer {
        private final IndexMaintainerState state;
        private static int pseodoFound = 0;

        protected Terrible2MergingIndexMaintainer(final IndexMaintainerState state) {
            super(state, state.context.getExecutor());
            this.state = state;
        }

        @Override
        public CompletableFuture<Void> mergeIndex() {
            final IndexDeferredMaintenanceControl mergeControl = state.store.getIndexDeferredMaintenanceControl();
            mergeControl.setLastStep(IndexDeferredMaintenanceControl.LastStep.MERGE);
            mergeControl.setMergesFound(1);
            mergeControl.setMergesTried(1);
            final long timeQuota = mergeControl.getTimeQuotaMillis();
            if (timeQuota <= 0) {
                mergeControl.setTimeQuotaMillis(1000);
                throw new FDBException("transaction_too_old", FDBError.TRANSACTION_TOO_OLD.code());
            }
            if (timeQuota > 10) {
                throw new FDBException("transaction_too_old", FDBError.TRANSACTION_TOO_OLD.code());
            }
            return AsyncUtil.DONE;
        }
    }

    @AutoService(IndexMaintainerFactory.class)
    public static class Terrible2MergingIndexMaintainerFactory implements IndexMaintainerFactory {
        @Nonnull
        @Override
        public Iterable<String> getIndexTypes() {
            return Collections.singletonList("terrible2Merger");
        }

        @Nonnull
        @Override
        public IndexValidator getIndexValidator(Index index) {
            return new IndexValidator(index);
        }

        @Nonnull
        @Override
        public IndexMaintainer getIndexMaintainer(@Nonnull IndexMaintainerState state) {
            return new Terrible2MergingIndexMaintainer(state);
        }
    }

    @Test
    void luceneOnlineIndexingTestTerrible2Merger() {
        Index index = new Index("Simple$text_suffixes",
                function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
                "terrible2Merger",
                ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME));

        boolean needMerge = populateDataSplitSegments(index, 4, 1);
        assertTrue(needMerge);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setRecordStore(recordStore)
                .setIndex(index)
                .setMaxAttempts(1)
                .build()) {
            indexBuilder.mergeIndex(); // retries under the hood until time quota is under 10 milli
            assertTrue(recordStore.getIndexDeferredMaintenanceControl().getTimeQuotaMillis() <= 10);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRecordUpdateReducedMergeForcingAgileSizeQuota(boolean disableAgilityContext) {
        // emulate repeating merge until until unchanged, with a tiny size quota
        final RecordLayerPropertyStorage.Builder insertProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_AGILE_COMMIT_SIZE_QUOTA, 100)
                .addProp(LuceneRecordContextProperties.LUCENE_AGILE_DISABLE_AGILITY_CONTEXT, disableAgilityContext);

        Index index = SIMPLE_TEXT_SUFFIXES;

        RecordMetaDataHook hook = metaDataBuilder -> {
            metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
            metaDataBuilder.addIndex(SIMPLE_DOC, index);
        };

        // write records
        populateDataSplitSegments(index, 42, 10);

        int oldLength = listFiles(index).length;
        int newLength;
        FDBStoreTimer timer = new FDBStoreTimer();
        try (FDBRecordContext context = openContext(insertProps)) {
            openRecordStore(context, hook);
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setRecordStore(recordStore)
                    .setIndex(index)
                    .setTimer(timer)
                    .build()) {
                indexBuilder.mergeIndex();
            }
            commit(context);
        }
        newLength = listFiles(index).length;
        assertTrue(newLength < oldLength);
        final StoreTimer.Counter counter = timer.getCounter(LuceneEvents.Counts.LUCENE_AGILE_COMMITS_SIZE_QUOTA);

        if (disableAgilityContext) {
            assertNull(counter);
            return;
        }

        assertNotNull(counter);
        final int sizeCommitCount = counter.getCount();
        LOGGER.debug("Merge test: number of files: old=" + oldLength + " new=" + newLength +
                     " needMerge=" + recordStore.getIndexDeferredMaintenanceControl().getMergeRequiredIndexes() +
                     " sizeCommitCount: " + sizeCommitCount);
        // Got "old=161 new=19 needMerge=null sizeCommitCount: 64"
        assertTrue(sizeCommitCount > 10);
    }

    @Test
    void testRecordUpdateReducedMergeForcingAgileTimeQuota() {
        // emulate repeating merge until until unchanged, with a tiny size quota
        final RecordLayerPropertyStorage.Builder insertProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_AGILE_COMMIT_TIME_QUOTA, 1);

        Index index = SIMPLE_TEXT_SUFFIXES;

        RecordMetaDataHook hook = metaDataBuilder -> {
            metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
            metaDataBuilder.addIndex(SIMPLE_DOC, index);
        };

        // write records
        populateDataSplitSegments(index, 42, 10);

        int oldLength = listFiles(index).length;
        int newLength;
        FDBStoreTimer timer = new FDBStoreTimer();
        try (FDBRecordContext context = openContext(insertProps)) {
            openRecordStore(context, hook);
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setRecordStore(recordStore)
                    .setIndex(index)
                    .setTimer(timer)
                    .build()) {
                indexBuilder.mergeIndex();
            }
            commit(context);
        }
        newLength = listFiles(index).length;
        final int timeCommitCount = timer.getCounter(LuceneEvents.Counts.LUCENE_AGILE_COMMITS_TIME_QUOTA).getCount();
        LOGGER.debug("Merge test: number of files: old=" + oldLength + " new=" + newLength +
                     " needMerge=" + recordStore.getIndexDeferredMaintenanceControl().getMergeRequiredIndexes() +
                     " timeCommitCount: " + timeCommitCount);
        // Got "old=161 new=19 needMerge=null timeCommitCount: 58"

        assertTrue(newLength < oldLength);
        assertTrue(timeCommitCount > 0);
    }

    private static class TerribleRebalanceIndexMaintainer extends LuceneIndexMaintainer {
        private final IndexMaintainerState state;
        private static int pseodoFound = 0;

        protected TerribleRebalanceIndexMaintainer(final IndexMaintainerState state) {
            super(state, state.context.getExecutor());
            this.state = state;
        }

        @Override
        public CompletableFuture<Void> mergeIndex() {
            final IndexDeferredMaintenanceControl mergeControl = state.store.getIndexDeferredMaintenanceControl();
            mergeControl.setLastStep(IndexDeferredMaintenanceControl.LastStep.REPARTITION);
            final int documentCount = mergeControl.getRepartitionDocumentCount();
            if (documentCount <= 0) {
                mergeControl.setRepartitionDocumentCount(16);
                throw new FDBException("transaction_too_old", FDBError.TRANSACTION_TOO_OLD.code());
            }
            if (documentCount > 2) {
                throw new FDBException("transaction_too_old", FDBError.TRANSACTION_TOO_OLD.code());
            }
            return AsyncUtil.DONE;
        }
    }

    @AutoService(IndexMaintainerFactory.class)
    public static class TerribleRebalanceIndexMaintainerFactory implements IndexMaintainerFactory {
        @Nonnull
        @Override
        public Iterable<String> getIndexTypes() {
            return Collections.singletonList("terribleRebalance");
        }

        @Nonnull
        @Override
        public IndexValidator getIndexValidator(Index index) {
            return new IndexValidator(index);
        }

        @Nonnull
        @Override
        public IndexMaintainer getIndexMaintainer(@Nonnull IndexMaintainerState state) {
            return new TerribleRebalanceIndexMaintainer(state);
        }
    }

    @Test
    void luceneOnlineIndexingTestTerribleRebalance() {
        Index index = new Index("Simple$text_suffixes",
                function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
                "terribleRebalance",
                ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME));

        boolean needMerge = populateDataSplitSegments(index, 4, 1);
        assertTrue(needMerge);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setRecordStore(recordStore)
                .setIndex(index)
                .setMaxAttempts(1)
                .build()) {
            indexBuilder.mergeIndex(); // retries under the hood until document count reaches minimum
            assertTrue(recordStore.getIndexDeferredMaintenanceControl().getRepartitionDocumentCount() <= 2);
        }
    }

    private static class Terriblerebalnce2ndChanceIndexMaintainer extends LuceneIndexMaintainer {
        private final IndexMaintainerState state;
        static boolean isSecondPass = false;
        static boolean gotSecondChance = false;

        protected Terriblerebalnce2ndChanceIndexMaintainer(final IndexMaintainerState state) {
            super(state, state.context.getExecutor());
            this.state = state;
        }

        @Override
        public CompletableFuture<Void> mergeIndex() {
            final IndexDeferredMaintenanceControl mergeControl = state.store.getIndexDeferredMaintenanceControl();
            final int documentCount = mergeControl.getRepartitionDocumentCount();
            if (isSecondPass) {
                mergeControl.setLastStep(IndexDeferredMaintenanceControl.LastStep.REPARTITION);
                assertEquals(0, documentCount);
                gotSecondChance = true;
                return AsyncUtil.DONE;
            }
            if (documentCount == -1) {
                isSecondPass = true;
                mergeControl.setLastStep(IndexDeferredMaintenanceControl.LastStep.MERGE);
                return AsyncUtil.DONE;
            }
            mergeControl.setLastStep(IndexDeferredMaintenanceControl.LastStep.REPARTITION);
            if (documentCount <= 0) {
                mergeControl.setRepartitionDocumentCount(16);
                throw new FDBException("transaction_too_old", FDBError.TRANSACTION_TOO_OLD.code());
            }
            throw new FDBException("transaction_too_old", FDBError.TRANSACTION_TOO_OLD.code());
        }
    }

    @AutoService(IndexMaintainerFactory.class)
    public static class Terriblerebalnce2ndChanceIndexMaintainerFactory implements IndexMaintainerFactory {
        @Nonnull
        @Override
        public Iterable<String> getIndexTypes() {
            return Collections.singletonList("terribleRebalance2ndChance");
        }

        @Nonnull
        @Override
        public IndexValidator getIndexValidator(Index index) {
            return new IndexValidator(index);
        }

        @Nonnull
        @Override
        public IndexMaintainer getIndexMaintainer(@Nonnull IndexMaintainerState state) {
            return new Terriblerebalnce2ndChanceIndexMaintainer(state);
        }
    }

    @Test
    void luceneOnlineIndexingTestTerribleRebalance2ndChance() {
        Terriblerebalnce2ndChanceIndexMaintainer.isSecondPass = false;
        Terriblerebalnce2ndChanceIndexMaintainer.gotSecondChance = false;
        Index index = new Index("Simple$text_suffixes",
                function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
                "terribleRebalance2ndChance",
                ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME));

        boolean needMerge = populateDataSplitSegments(index, 4, 1);
        assertTrue(needMerge);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setRecordStore(recordStore)
                .setIndex(index)
                .setMaxAttempts(1)
                .build()) {
            indexBuilder.mergeIndex(); // retries under the hood + second chance
            assertTrue(Terriblerebalnce2ndChanceIndexMaintainer.gotSecondChance);
            assertTrue(Terriblerebalnce2ndChanceIndexMaintainer.isSecondPass);
        }
    }
}

