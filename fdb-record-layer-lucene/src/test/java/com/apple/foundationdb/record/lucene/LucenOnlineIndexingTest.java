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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.text.AllSuffixesTextTokenizer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexDeferredMaintenanceControl;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.apple.foundationdb.record.lucene.LuceneIndexTest.COMPLEX_MULTIPLE_GROUPED;
import static com.apple.foundationdb.record.lucene.LuceneIndexTest.ENGINEER_JOKE;
import static com.apple.foundationdb.record.lucene.LuceneIndexTest.WAYLON;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.NGRAM_LUCENE_INDEX;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.QUERY_ONLY_SYNONYM_LUCENE_INDEX;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.SIMPLE_TEXT_SUFFIXES;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.TEXT_AND_STORED;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.createSimpleDocument;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.COMPLEX_DOC;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.MAP_DOC;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.SIMPLE_DOC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LucenOnlineIndexingTest extends FDBRecordStoreTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(LucenOnlineIndexingTest.class);


    private void rebuildIndexMetaData(final FDBRecordContext context, final String document, final Index index) {
        Pair<FDBRecordStore, QueryPlanner> pair = LuceneIndexTestUtils.rebuildIndexMetaData(context, path, document, index, useRewritePlanner);
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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void luceneOnlineIndexingTestSimple(boolean preventMergeDuringIndexing) {
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
                    .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                            .setDeferMergeDuringIndexing(preventMergeDuringIndexing))
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

    @Test
    void luceneOnlineIndexingTest1() {
        luceneOnlineIndexingTestAny(QUERY_ONLY_SYNONYM_LUCENE_INDEX, COMPLEX_DOC, 17, 7, 0);
    }

    @Test
    void luceneOnlineIndexingTest2() {
        luceneOnlineIndexingTestAny(QUERY_ONLY_SYNONYM_LUCENE_INDEX, SIMPLE_DOC, 15, 100, 300);
    }

    @Test
    void luceneOnlineIndexingTest3() {
        luceneOnlineIndexingTestAny(NGRAM_LUCENE_INDEX, SIMPLE_DOC, 44, 7, 2);
    }

    @Test
    void luceneOnlineIndexingTest4() {
        luceneOnlineIndexingTestAny(TEXT_AND_STORED, COMPLEX_DOC, 8, 100, 1);
    }

    @Test
    void luceneOnlineIndexingTest5() {
        luceneOnlineIndexingTestAny(COMPLEX_MULTIPLE_GROUPED, COMPLEX_DOC, 77, 20, 2);
    }

    @Test
    void luceneOnlineIndexingTest6() {
        luceneOnlineIndexingTestAny(COMPLEX_MULTIPLE_GROUPED, COMPLEX_DOC, 77, 20, 0);
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


    void luceneOnlineIndexingTestAny(Index index, String document, int numRecords, int transactionLimit, int mergesLimit) {
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
                recordStore.saveRecord(createSimpleDocument(docId, randomText(rn), randomGroup(rn)));
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
                            .setDeferMergeDuringIndexing(true)
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
        // assert number of segments
        final int newLength = listFiles(index).length;
        LOGGER.debug("Merge test: number of files: new=" + newLength);
        assertTrue(newLength < 12);
    }

    @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void luceneOnlineIndexingTestMulti(boolean preventMergeDuringIndexing) {
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
                            KeyExpression.FanType.FanOut).nest(concat(LuceneIndexTest.keys)), groupingCount + 1),
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
                    .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                            .setDeferMergeDuringIndexing(preventMergeDuringIndexing)
                            .build())
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
                        KeyExpression.FanType.FanOut).nest(concat(LuceneIndexTest.keys)), groupingCount),
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
                    .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                            .setDeferMergeDuringIndexing(true)
                            .build())
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
                        KeyExpression.FanType.FanOut).nest(concat(LuceneIndexTest.keys)), groupedCount),
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
                recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(false);
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
            final FDBDirectory directory = new FDBDirectory(recordStore.indexSubspace(index), context, true);
            return directory.listAll();
        }
    }

    private String[] listFiles(Index index, Tuple tuple, int groupingCount) {
        try (FDBRecordContext context = openContext()) {
            recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(false);
            final Subspace subspace = recordStore.indexSubspace(index);
            final FDBDirectory directory = new FDBDirectory(subspace.subspace(Tuple.fromItems(tuple.getItems().subList(0, groupingCount))), context, true);
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
                recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(false);
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
                recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(false);
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
        oldLength = newLength;

        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setRecordStore(recordStore)
                .setIndex(index)
                .build()) {
            indexBuilder.mergeIndex();
        }

        allFiles = listFiles(index);
        newLength = allFiles.length;
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
                .build()) {
            indexBuilder.mergeIndex();
        }
    }
}
