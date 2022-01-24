/*
 * LuceneIndexTest.java
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorProto;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.lucene.directory.FDBLuceneFileReference;
import com.apple.foundationdb.record.lucene.ngram.NgramAnalyzer;
import com.apple.foundationdb.record.lucene.synonym.SynonymAnalyzer;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.common.text.AllSuffixesTextTokenizer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContextConfig;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.IndexFileNames;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.COMPLEX_DOC;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.SIMPLE_DOC;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;

/**
 * Tests for {@code LUCENE} type indexes.
 */
@Tag(Tags.RequiresFDB)
public class LuceneIndexTest extends FDBRecordStoreTestBase {

    private static final String MAP_DOC = "MapDocument";

    private static final Index SIMPLE_TEXT_SUFFIXES = new Index("Simple$text_suffixes",
            function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME));

    private static final Index COMPLEX_MULTIPLE_TEXT_INDEXES = new Index("Complex$text_multipleIndexes",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME));

    private static final Index NGRAM_LUCENE_INDEX = new Index("ngram_index", function(LuceneFunctionNames.LUCENE_TEXT, field("text")), LuceneIndexTypes.LUCENE,
            ImmutableMap.of(IndexOptions.TEXT_ANALYZER_NAME_OPTION, NgramAnalyzer.NgramAnalyzerFactory.ANALYZER_NAME,
                    IndexOptions.TEXT_TOKEN_MIN_SIZE, "3",
                    IndexOptions.TEXT_TOKEN_MAX_SIZE, "5"));

    private static final Index SYNONYM_LUCENE_INDEX = new Index("synonym_index", function(LuceneFunctionNames.LUCENE_TEXT, field("text")), LuceneIndexTypes.LUCENE,
            ImmutableMap.of(IndexOptions.TEXT_ANALYZER_NAME_OPTION, SynonymAnalyzer.SynonymAnalyzerFactory.ANALYZER_NAME));

    private static final Index SPELLCHECK_INDEX = new Index(
            "spellcheck_index",
            function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
            LuceneIndexTypes.LUCENE,
            Collections.emptyMap());

    private static final Index SPELLCHECK_INDEX_COMPLEX = new Index(
            "spellcheck_index_complex",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
            LuceneIndexTypes.LUCENE,
            Collections.emptyMap());

    private static final List<KeyExpression> keys = List.of(
            field("key"),
            function(LuceneFunctionNames.LUCENE_TEXT, field("value")),
            function(LuceneFunctionNames.LUCENE_TEXT, field("second_value")),
            function(LuceneFunctionNames.LUCENE_TEXT, field("third_value")));

    private static final Index MAP_ON_VALUE_INDEX = new Index("Map$entry-value", new GroupingKeyExpression(field("entry", KeyExpression.FanType.FanOut).nest(concat(keys)), 3), IndexTypes.LUCENE);

    private static final String ENGINEER_JOKE = "A software engineer, a hardware engineer, and a departmental manager were driving down a steep mountain road when suddenly the brakes on their car failed. The car careened out of control down the road, bouncing off the crash barriers, ground to a halt scraping along the mountainside. The occupants were stuck halfway down a mountain in a car with no brakes. What were they to do?" +
                                                "'I know,' said the departmental manager. 'Let's have a meeting, propose a Vision, formulate a Mission Statement, define some Goals, and by a process of Continuous Improvement find a solution to the Critical Problems, and we can be on our way.'" +
                                                "'No, no,' said the hardware engineer. 'That will take far too long, and that method has never worked before. In no time at all, I can strip down the car's braking system, isolate the fault, fix it, and we can be on our way.'" +
                                                "'Wait, said the software engineer. 'Before we do anything, I think we should push the car back up the road and see if it happens again.'";

    private static final String WAYLON = "There's always one more way to do things and that's your way, and you have a right to try it at least once.";

    protected void openRecordStore(FDBRecordContext context, FDBRecordStoreTestBase.RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsTextProto.getDescriptor());
        metaDataBuilder.getRecordType(COMPLEX_DOC).setPrimaryKey(concatenateFields("group", "doc_id"));
        hook.apply(metaDataBuilder);
        recordStore = getStoreBuilder(context, metaDataBuilder.getRecordMetaData())
                .setSerializer(TextIndexTestUtils.COMPRESSING_SERIALIZER)
                .uncheckedOpen();
        setupPlanner(null);
    }

    @Nonnull
    protected FDBRecordStore.Builder getStoreBuilder(@Nonnull FDBRecordContext context, @Nonnull RecordMetaData metaData) {
        return FDBRecordStore.newBuilder()
                .setFormatVersion(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION) // set to max to test newest features (unsafe for real deployments)
                .setKeySpacePath(path)
                .setContext(context)
                .setMetaDataProvider(metaData);
    }

    private TestRecordsTextProto.SimpleDocument createSimpleDocument(long docId, int group) {
        return TestRecordsTextProto.SimpleDocument.newBuilder()
                .setDocId(docId)
                .setGroup(group)
                .build();
    }

    private TestRecordsTextProto.SimpleDocument createSimpleDocument(long docId, String text, int group) {
        return TestRecordsTextProto.SimpleDocument.newBuilder()
                .setDocId(docId)
                .setText(text)
                .setGroup(group)
                .build();
    }

    private TestRecordsTextProto.ComplexDocument createComplexDocument(long docId, String text, String text2, int group) {
        return TestRecordsTextProto.ComplexDocument.newBuilder()
                .setDocId(docId)
                .setText(text)
                .setText2(text2)
                .setGroup(group)
                .build();
    }

    private TestRecordsTextProto.MapDocument createComplexMapDocument(long docId, String text, String text2, int group) {
        return TestRecordsTextProto.MapDocument.newBuilder()
                .setDocId(docId)
                .setGroup(group)
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder()
                        .setKey(text2)
                        .setValue(text)
                        .setSecondValue("secondValue" + docId)
                        .setThirdValue("thirdValue" + docId)
                        .build())
                .build();
    }

    private TestRecordsTextProto.MapDocument createMultiEntryMapDoc(long docId, String text, String text2, String text3,
                                                                    String text4, int group) {
        return TestRecordsTextProto.MapDocument.newBuilder()
                .setDocId(docId)
                .setGroup(group)
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder()
                    .setKey(text2)
                    .setValue(text)
                    .setSecondValue("firstEntrySecondValue")
                    .setThirdValue("firstEntryThirdValue"))
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder()
                        .setKey(text4)
                        .setValue(text3)
                        .setSecondValue("secondEntrySecondValue")
                        .setThirdValue("secondEntryThirdValue"))
                .build();
    }

    private TestRecordsTextProto.MapDocument createMapDocument(long docId, String text, String text2, int group) {
        return TestRecordsTextProto.MapDocument.newBuilder()
                .setDocId(docId)
                .setGroup(group)
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("text").setValue(text).build())
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("text2").setValue(text2).build())
                .build();
    }

    private void assertCorrectMetricCount(StoreTimer.Event metric, int expectedValue) {
        //check that metrics were collected
        final Collection<StoreTimer.Event> events = timer.getEvents();
        Assertions.assertTrue(events.contains(metric), "Did not count get increment calls!");
        Assertions.assertEquals(expectedValue, timer.getCounter(metric).getCount(), "Incorrect call count ");
    }

    @Override
    public FDBRecordContext openContext() {
        final FDBRecordContextConfig config = FDBRecordContextConfig.newBuilder()
                .setTimer(timer)
                .setMdcContext(ImmutableMap.of("uuid", UUID.randomUUID().toString()))
                .setSaveOpenStackTrace(true)
                .setRecordContextProperties(RecordLayerPropertyStorage.newBuilder().addProp(LuceneRecordContextProperties.LUCENE_INDEX_COMPRESSION_ENABLED, true).build())
                .build();
        return fdb.openContext(config);
    }

    @Test
    void simpleInsertAndSearch() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE_FULL_TEXT, TupleRange.allOf(Tuple.from("Vision")), null, ScanProperties.FORWARD_SCAN);
            assertEquals(1, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE_FULL_TEXT,
                    TupleRange.allOf(Tuple.from("Vision")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            assertEquals(1, context.getTimer().getCounter(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs", true);
        }
    }

    @Test
    void simpleInsertAndSearchNumFDBFetches() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE_FULL_TEXT, TupleRange.allOf(Tuple.from("Vision")), null, ScanProperties.FORWARD_SCAN);
            assertEquals(1, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE_FULL_TEXT,
                            TupleRange.allOf(Tuple.from("Vision")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            assertEquals(1, context.getTimer().getCounter(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            assertCorrectMetricCount(FDBStoreTimer.Events.LUCENE_GET_FILE_REFERENCE,1);

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs", true);
        }
    }

    @Test
    void testContinuation() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1624L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1625L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1626L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
            RecordCursorProto.LuceneIndexContinuation continuation =  RecordCursorProto.LuceneIndexContinuation.newBuilder()
                    .setDoc(1)
                    .setScore(0.27130663F)
                    .setShard(0)
                    .build();
            RecordCursor<IndexEntry> recordCursor = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE_FULL_TEXT,
                    TupleRange.allOf(Tuple.from("Vision")), continuation.toByteArray(), ScanProperties.FORWARD_SCAN);
            assertEquals(2, recordCursor.getCount().join());

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs", true);
        }
    }

    @Test
    void testNullValue() throws ExecutionException, InterruptedException {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            recordStore.saveRecord(createSimpleDocument(1623L, 2));
            recordStore.saveRecord(createSimpleDocument(1632L, ENGINEER_JOKE, 2));
            RecordCursor<IndexEntry> recordCursor = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE_FULL_TEXT,
                    TupleRange.allOf(Tuple.from("*:* AND NOT text:[* TO *]")), null, ScanProperties.FORWARD_SCAN);
            List<IndexEntry> indexEntries = recordCursor.asList().join();
            assertEquals(1, indexEntries.size());
            assertEquals(1623L, indexEntries.get(0).getKeyValue(1));

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs", true);
        }
    }

    @Test
    void testLimit() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            for (int i = 0; i < 200; i++) {
                recordStore.saveRecord(createSimpleDocument(1623L + i, ENGINEER_JOKE, 2));
            }
            assertEquals(50, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE_FULL_TEXT,
                    TupleRange.allOf(Tuple.from("Vision")), null,
                    ExecuteProperties.newBuilder().setReturnedRowLimit(50).build().asScanProperties(false))
                    .getCount().join());

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs", true);
        }
    }

    @Test
    void testSkip() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            for (int i = 0; i < 50; i++) {
                recordStore.saveRecord(createSimpleDocument(1623L + i, ENGINEER_JOKE, 2));
            }
            assertEquals(40, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE_FULL_TEXT,
                    TupleRange.allOf(Tuple.from("Vision")), null,
                    ExecuteProperties.newBuilder().setReturnedRowLimit(50).setSkip(10).build().asScanProperties(false))
                    .getCount().join());

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs", true);
        }
    }

    @Test
    void testSkipWithLimit() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            for (int i = 0; i < 50; i++) {
                recordStore.saveRecord(createSimpleDocument(1623L + i, ENGINEER_JOKE, 2));
            }
            assertEquals(40, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE_FULL_TEXT,
                    TupleRange.allOf(Tuple.from("Vision")), null,
                    ExecuteProperties.newBuilder().setReturnedRowLimit(50).setSkip(10).build().asScanProperties(false))
                    .getCount().join());

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs", true);
        }
    }

    @Test
    void testLimitWithContinuation() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            for (int i = 0; i < 200; i++) {
                recordStore.saveRecord(createSimpleDocument(1623L + i, ENGINEER_JOKE, 2));
            }
            RecordCursorProto.LuceneIndexContinuation continuation = RecordCursorProto.LuceneIndexContinuation.newBuilder()
                    .setDoc(151)
                    .setScore(0.0025435707F)
                    .setShard(0)
                    .build();
            assertEquals(48, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE_FULL_TEXT,
                    TupleRange.allOf(Tuple.from("Vision")), continuation.toByteArray(),
                    ExecuteProperties.newBuilder().setReturnedRowLimit(50).build().asScanProperties(false))
                    .getCount().join());

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs", true);
        }
    }

    @Test
    void testLimitNeedsMultipleScans() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            for (int i = 0; i < 800; i++) {
                recordStore.saveRecord(createSimpleDocument(1623L + i, ENGINEER_JOKE, 2));
            }
            assertEquals(251, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE_FULL_TEXT,
                    TupleRange.allOf(Tuple.from("Vision")), null,
                    ExecuteProperties.newBuilder().setReturnedRowLimit(251).build().asScanProperties(false))
                    .getCount().join());
            assertEquals(2, recordStore.getTimer().getCount(FDBStoreTimer.Events.LUCENE_INDEX_SCAN));
            assertEquals(251, recordStore.getTimer().getCount(FDBStoreTimer.Counts.LUCENE_SCAN_MATCHED_DOCUMENTS));

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs", true);
        }
    }

    @Test
    void testSkipOverMaxPageSize() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            for (int i = 0; i < 251; i++) {
                recordStore.saveRecord(createSimpleDocument(1623L + i, ENGINEER_JOKE, 2));
            }
            assertEquals(50, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE_FULL_TEXT,
                    TupleRange.allOf(Tuple.from("Vision")), null,
                    ExecuteProperties.newBuilder().setReturnedRowLimit(251).setSkip(201).build().asScanProperties(false))
                    .getCount().join());
            assertEquals(2, recordStore.getTimer().getCount(FDBStoreTimer.Events.LUCENE_INDEX_SCAN));
            assertEquals(251, recordStore.getTimer().getCount(FDBStoreTimer.Counts.LUCENE_SCAN_MATCHED_DOCUMENTS));

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs", true);
        }
    }

    @Test
    void testNestedFieldSearch() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, MAP_DOC, MAP_ON_VALUE_INDEX);
            recordStore.saveRecord(createComplexMapDocument(1623L, ENGINEER_JOKE, "sampleTextSong", 2));
            recordStore.saveRecord(createComplexMapDocument(1547L, WAYLON, "sampleTextPhrase",  1));
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(MAP_ON_VALUE_INDEX, IndexScanType.BY_LUCENE, TupleRange.allOf(Tuple.from("value:Vision", "sampleTextSong")), null, ScanProperties.FORWARD_SCAN);
            assertEquals(1, indexEntries.getCount().join());
            assertEquals(1, context.getTimer().getCounter(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(MAP_ON_VALUE_INDEX).subspace(Tuple.from("sampleTextSong")), context, "_0.cfs", true);
        }
    }

    @Test
    void testGroupedRecordSearch() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, MAP_DOC, MAP_ON_VALUE_INDEX);
            recordStore.saveRecord(createMultiEntryMapDoc(1623L, ENGINEER_JOKE, "sampleTextPhrase", WAYLON, "sampleTextSong", 2));
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(MAP_ON_VALUE_INDEX, IndexScanType.BY_LUCENE, TupleRange.allOf(Tuple.from("value:Vision", "sampleTextPhrase")), null, ScanProperties.FORWARD_SCAN);
            assertEquals(1, indexEntries.getCount().join());
            assertEquals(1, context.getTimer().getCounter(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(MAP_ON_VALUE_INDEX).subspace(Tuple.from("sampleTextPhrase")), context, "_0.cfs", true);
        }
    }

    @Test
    void testMultipleFieldSearch() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_MULTIPLE_TEXT_INDEXES);
            recordStore.saveRecord(createComplexDocument(1623L, ENGINEER_JOKE, "john_leach@apple.com", 2));
            recordStore.saveRecord(createComplexDocument(1547L, WAYLON, "hering@gmail.com", 2));
            assertEquals(1, recordStore.scanIndex(COMPLEX_MULTIPLE_TEXT_INDEXES, IndexScanType.BY_LUCENE_FULL_TEXT,
                    TupleRange.allOf(Tuple.from("text:\"Vision\" AND text2:\"john_leach@apple.com\"")),
                    null, ScanProperties.FORWARD_SCAN).getCount().join());

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(COMPLEX_MULTIPLE_TEXT_INDEXES), context, "_0.cfs", true);
        }
    }

    @Test
    void testFuzzySearchWithDefaultEdit2() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_MULTIPLE_TEXT_INDEXES);
            recordStore.saveRecord(createComplexDocument(1623L, ENGINEER_JOKE, "john_leach@apple.com", 2));
            recordStore.saveRecord(createComplexDocument(1547L, WAYLON, "hering@gmail.com", 2));
            assertEquals(1, recordStore.scanIndex(COMPLEX_MULTIPLE_TEXT_INDEXES, IndexScanType.BY_LUCENE_FULL_TEXT,
                    TupleRange.allOf(Tuple.from("text:\"Vision\" AND text2:jonleach@apple.com\\~")),
                    null, ScanProperties.FORWARD_SCAN).getCount().join());

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(COMPLEX_MULTIPLE_TEXT_INDEXES), context, "_0.cfs", true);
        }
    }

    @Test
    void simpleInsertDeleteAndSearch() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1624L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 2));
            assertEquals(2, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE_FULL_TEXT,
                    TupleRange.allOf(Tuple.from("Vision")), null, ScanProperties.FORWARD_SCAN).getCount().join());
            assertTrue(recordStore.deleteRecord(Tuple.from(1624L)));
            assertEquals(1, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE_FULL_TEXT,
                    TupleRange.allOf(Tuple.from("Vision")), null, ScanProperties.FORWARD_SCAN).getCount().join());

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs", true);
        }
    }

    @Test
    void testCommit() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
            assertEquals(1, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE_FULL_TEXT,
                    TupleRange.allOf(Tuple.from("Vision")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs", false);

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            assertEquals(1, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE_FULL_TEXT,
                    TupleRange.allOf(Tuple.from("Vision")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs", true);
        }
    }

    @Test
    void testRollback() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
            assertEquals(1, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE_FULL_TEXT,
                    TupleRange.allOf(Tuple.from("Vision")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs", false);

            context.ensureActive().cancel();
        }
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
            assertEquals(0, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE_FULL_TEXT,
                    TupleRange.allOf(Tuple.from("Vision")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            assertEquals(1, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE_FULL_TEXT,
                    TupleRange.allOf(Tuple.from("Vision")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs", true);
        }
    }

    @Test
    void testDataLoad() {
        FDBRecordContext context = openContext();
        for (int i = 0; i < 2000; i++) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            String[] randomWords = generateRandomWords(500);
            final TestRecordsTextProto.SimpleDocument dylan = TestRecordsTextProto.SimpleDocument.newBuilder()
                    .setDocId(i)
                    .setText(randomWords[1])
                    .setGroup(2)
                    .build();
            recordStore.saveRecord(dylan);
            if ((i + 1) % 50 == 0) {
                commit(context);
                context = openContext();
                assertOnlyCompoundFileExisting(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, null, true);
            }
        }
        context.close();
    }

    @Test
    void scanWithSynonymIndex() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SYNONYM_LUCENE_INDEX);
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
            assertEquals(1, recordStore.scanIndex(SYNONYM_LUCENE_INDEX, IndexScanType.BY_LUCENE_FULL_TEXT,
                            TupleRange.allOf(Tuple.from("hullo record layer")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello recor layer", 1));
            assertEquals(1, recordStore.scanIndex(SYNONYM_LUCENE_INDEX, IndexScanType.BY_LUCENE_FULL_TEXT,
                            TupleRange.allOf(Tuple.from("hullo record layer")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            // "hullo" is synonym of "hello"
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
            assertEquals(1, recordStore.scanIndex(SYNONYM_LUCENE_INDEX, IndexScanType.BY_LUCENE_FULL_TEXT,
                            TupleRange.allOf(Tuple.from("text:(+\"hullo\" AND +\"record\" AND +\"layer\")")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            // it doesn't match due to the "recor"
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
            assertEquals(0, recordStore.scanIndex(SYNONYM_LUCENE_INDEX, IndexScanType.BY_LUCENE_FULL_TEXT,
                            TupleRange.allOf(Tuple.from("text:(+\"hullo\" AND +\"recor\" AND +\"layer\")")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            // "hullo" is synonym of "hello", and "show" is synonym of "record"
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
            assertEquals(1, recordStore.scanIndex(SYNONYM_LUCENE_INDEX, IndexScanType.BY_LUCENE_FULL_TEXT,
                            TupleRange.allOf(Tuple.from("text:(+\"hullo\" AND +\"show\" AND +\"layer\")")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
        }
    }

    @Test
    void scanWithNgramIndex() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, NGRAM_LUCENE_INDEX);
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
            assertEquals(1, recordStore.scanIndex(NGRAM_LUCENE_INDEX, IndexScanType.BY_LUCENE_FULL_TEXT,
                            TupleRange.allOf(Tuple.from("hello record layer")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            assertEquals(1, recordStore.scanIndex(NGRAM_LUCENE_INDEX, IndexScanType.BY_LUCENE_FULL_TEXT,
                            TupleRange.allOf(Tuple.from("hello")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            assertEquals(1, recordStore.scanIndex(NGRAM_LUCENE_INDEX, IndexScanType.BY_LUCENE_FULL_TEXT,
                            TupleRange.allOf(Tuple.from("hel")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            assertEquals(1, recordStore.scanIndex(NGRAM_LUCENE_INDEX, IndexScanType.BY_LUCENE_FULL_TEXT,
                            TupleRange.allOf(Tuple.from("ell")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            assertEquals(1, recordStore.scanIndex(NGRAM_LUCENE_INDEX, IndexScanType.BY_LUCENE_FULL_TEXT,
                            TupleRange.allOf(Tuple.from("ecord")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            assertEquals(1, recordStore.scanIndex(NGRAM_LUCENE_INDEX, IndexScanType.BY_LUCENE_FULL_TEXT,
                            TupleRange.allOf(Tuple.from("hel ord aye")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            assertEquals(1, recordStore.scanIndex(NGRAM_LUCENE_INDEX, IndexScanType.BY_LUCENE_FULL_TEXT,
                            TupleRange.allOf(Tuple.from("ello record")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            // The term "ella" is not expected
            assertEquals(0, recordStore.scanIndex(NGRAM_LUCENE_INDEX, IndexScanType.BY_LUCENE_FULL_TEXT,
                            TupleRange.allOf(Tuple.from("ella")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
        }
    }

    private void spellCheckHelper(final Index index, @Nonnull String query, List<Pair<String, String>> expectedSuggestions) throws ExecutionException, InterruptedException {
        List<IndexEntry> suggestions = recordStore.scanIndex(index,
                IndexScanType.BY_LUCENE_SPELLCHECK,
                TupleRange.allOf(Tuple.from(query)),
                null,
                ScanProperties.FORWARD_SCAN).asList().get();

        assertEquals(expectedSuggestions.size(), suggestions.size());
        for (int i = 0 ; i < expectedSuggestions.size(); ++i) {
            assertThat(suggestions.get(i).getKey().get(0), equalTo(expectedSuggestions.get(i).getKey()));
            assertThat(suggestions.get(i).getValue().get(0), equalTo(expectedSuggestions.get(i).getValue()));
        }
    }

    @Test
    void spellCheck() throws Exception {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SPELLCHECK_INDEX);
            long docId = 1623L;
            for (String word : List.of("hello", "monitor", "keyboard", "mouse", "trackpad", "cable", "help", "elmo", "elbow", "helps", "helm", "helms", "gulps")) {
                recordStore.saveRecord(createSimpleDocument(docId++, word, 1));
            }
            spellCheckHelper(SPELLCHECK_INDEX, "keyboad", List.of(Pair.of("keyboard", "text")));
            spellCheckHelper(SPELLCHECK_INDEX, "text:keyboad", List.of(Pair.of("keyboard", "text")));
            spellCheckHelper(SPELLCHECK_INDEX, "helo", List.of(
                    Pair.of("hello", "text"),
                    Pair.of("helm", "text"),
                    Pair.of("help", "text"),
                    Pair.of("helms", "text"),
                    Pair.of("helps", "text")
                    ));
            spellCheckHelper(SPELLCHECK_INDEX, "hello", List.of());
            spellCheckHelper(SPELLCHECK_INDEX, "mous", List.of(Pair.of("mouse", "text")));

            assertThrows(RecordCoreException.class,
                    () -> spellCheckHelper(SPELLCHECK_INDEX, "wrongField:helo", List.of()),
                    "Invalid field name in Lucene index query");
        }
    }

    @Test
    void spellCheckComplexDocument() throws Exception {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, SPELLCHECK_INDEX_COMPLEX);
            long docId = 1623L;
            List<String> text = List.of("beaver", "leopard", "hello", "help", "helm", "boat", "road", "fowl", "foot");
            List<String> text2 = List.of("beavers", "lizards", "hell", "helps", "helms", "boot", "read", "fowl", "fool");
            assertThat(text2, hasSize(text.size()));
            for (int i = 0; i < text.size(); ++i) {
                recordStore.saveRecord(createComplexDocument(docId++, text.get(i), text2.get(i), 1));
            }
            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "baver", List.of(Pair.of("beaver", "text"), Pair.of("beavers", "text2")));
            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "text:baver", List.of(Pair.of("beaver", "text")));
            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "text2:baver", List.of(Pair.of("beavers", "text2")));

            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "lepard", List.of(Pair.of("leopard", "text")));
            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "text:lepard", List.of(Pair.of("leopard", "text")));
            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "text2:lepard", List.of());

            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "lizerds", List.of(Pair.of("lizards", "text2")));
            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "text:lizerds", List.of());
            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "text2:lizerds", List.of(Pair.of("lizards", "text2")));

            // Apply the limit of 5 fields so do not return "helms" which has a lower score than the rest
            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "hela", List.of(
                    Pair.of("hell", "text2"),
                    Pair.of("helm", "text"),
                    Pair.of("help", "text"),
                    Pair.of("hello", "text"),
                    Pair.of("helms", "text2")));

            // Same score and same frequency, this is sorted by field name
            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "bost", List.of(
                    Pair.of("boat", "text"),
                    Pair.of("boot", "text2")));
            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "rlad", List.of(
                    Pair.of("read", "text2"),
                    Pair.of("road", "text")));

            // Same score but different frequency, priority to the more frequent item
            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "foml", List.of(
                    Pair.of("fowl", "text"),
                    Pair.of("fool", "text2"),
                    Pair.of("foot", "text")));

            // TODO add tests for grouping
        }
    }

    private static String[] generateRandomWords(int numberOfWords) {
        assert numberOfWords > 0 : "Number of words have to be greater than 0";
        StringBuilder builder = new StringBuilder();
        Random random = new Random();
        char[] word = null;
        for (int i = 0; i < numberOfWords; i++) {
            word = new char[random.nextInt(8) + 3]; // words of length 3 through 10. (1 and 2 letter words are boring.)
            for (int j = 0; j < word.length; j++) {
                word[j] = (char)('a' + random.nextInt(26));
            }
            if (i != numberOfWords - 1) {
                builder.append(word).append(" ");
            }
        }
        String[] returnValue = new String[2];
        returnValue[0] = new String(word);
        returnValue[1] = builder.toString();
        return returnValue;
    }

    private static void assertEntriesAndSegmentInfoStoredInCompoundFile(@Nonnull Subspace subspace, @Nonnull FDBRecordContext context, @Nonnull String segment, boolean cleanFiles) {
        try (final FDBDirectory directory = new FDBDirectory(subspace, context)) {
            final FDBLuceneFileReference reference = directory.getFDBLuceneFileReference(segment).join();
            Assertions.assertNotNull(reference);
            Assertions.assertTrue(reference.getEntries().length > 0);
            Assertions.assertTrue(reference.getSegmentInfo().length > 0);
            assertOnlyCompoundFileExisting(subspace, context, directory, cleanFiles);
        }
    }

    private static void assertOnlyCompoundFileExisting(@Nonnull Subspace subspace, @Nonnull FDBRecordContext context, @Nullable FDBDirectory fdbDirectory, boolean cleanFiles) {
        final FDBDirectory directory = fdbDirectory == null ? new FDBDirectory(subspace, context) : fdbDirectory;
        String[] allFiles = directory.listAll();
        for (String file : allFiles) {
            Assertions.assertTrue(FDBDirectory.isCompoundFile(file) || file.startsWith(IndexFileNames.SEGMENTS));
            if (cleanFiles) {
                try {
                    directory.deleteFile(file);
                } catch (IOException ex) {
                    Assertions.fail("Failed to clean file: " + file);
                }
            }
        }
    }

    private void rebuildIndexMetaData(final FDBRecordContext context, final String document, final Index index) {
        openRecordStore(context, metaDataBuilder -> {
            metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
            metaDataBuilder.addIndex(document, index);
        });
    }
}
