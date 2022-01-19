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
import com.apple.foundationdb.record.RecordCoreArgumentException;
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
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.common.text.AllSuffixesTextTokenizer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContextConfig;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.lucene.index.IndexFileNames;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.COMPLEX_DOC;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.SIMPLE_DOC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    private static final Index SIMPLE_TEXT_WITH_AUTO_COMPLETE = new Index("Simple_with_auto_complete",
            function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(IndexOptions.AUTO_COMPLETE_ENABLED, "true",
                    IndexOptions.AUTO_COMPLETE_MIN_PREFIX_SIZE, "3"));

    private static final Index SIMPLE_TEXT_WITH_AUTO_COMPLETE_WITH_HIGHLIGHT = new Index("Simple_with_auto_complete",
            function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(IndexOptions.AUTO_COMPLETE_ENABLED, "true",
                    IndexOptions.AUTO_COMPLETE_MIN_PREFIX_SIZE, "3",
                    IndexOptions.AUTO_COMPLETE_HIGHLIGHT, "true"));

    private static final Index COMPLEX_MULTIPLE_TEXT_INDEXES = new Index("Complex$text_multipleIndexes",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME));

    private static final Index COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE = new Index("Complex$text_multipleIndexes",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(IndexOptions.AUTO_COMPLETE_ENABLED, "true",
                    IndexOptions.AUTO_COMPLETE_MIN_PREFIX_SIZE, "3"));

    private static final Index NGRAM_LUCENE_INDEX = new Index("ngram_index", function(LuceneFunctionNames.LUCENE_TEXT, field("text")), LuceneIndexTypes.LUCENE,
            ImmutableMap.of(IndexOptions.TEXT_ANALYZER_NAME_OPTION, NgramAnalyzer.NgramAnalyzerFactory.ANALYZER_NAME,
                    IndexOptions.TEXT_TOKEN_MIN_SIZE, "3",
                    IndexOptions.TEXT_TOKEN_MAX_SIZE, "5"));

    private static final Index SYNONYM_LUCENE_INDEX = new Index("synonym_index", function(LuceneFunctionNames.LUCENE_TEXT, field("text")), LuceneIndexTypes.LUCENE,
            ImmutableMap.of(IndexOptions.TEXT_ANALYZER_NAME_OPTION, SynonymAnalyzer.SynonymAnalyzerFactory.ANALYZER_NAME));

    private static final Index SPELLCHECK_LUCENE_INDEX = new Index(
            "spellcheck_index",
            function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
            LuceneIndexTypes.LUCENE,
            Collections.emptyMap());

    private static final List<KeyExpression> keys = List.of(
            field("key"),
            function(LuceneFunctionNames.LUCENE_TEXT, field("value")),
            function(LuceneFunctionNames.LUCENE_TEXT, field("second_value")),
            function(LuceneFunctionNames.LUCENE_TEXT, field("third_value")));

    private static final Index MAP_ON_VALUE_INDEX = new Index("Map$entry-value", new GroupingKeyExpression(field("entry", KeyExpression.FanType.FanOut).nest(concat(keys)), 3), IndexTypes.LUCENE);

    private static final Index MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE = new Index("Map_with_auto_complete$entry-value",
            new GroupingKeyExpression(field("entry", KeyExpression.FanType.FanOut).nest(concat(keys)), 3),
            IndexTypes.LUCENE,
            ImmutableMap.of(IndexOptions.AUTO_COMPLETE_ENABLED, "true"));

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
    protected FDBRecordContextConfig.Builder contextConfig() {
        return super.contextConfig()
                .setRecordContextProperties(RecordLayerPropertyStorage.newBuilder().addProp(LuceneRecordContextProperties.LUCENE_INDEX_COMPRESSION_ENABLED, true).build());
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
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(MAP_ON_VALUE_INDEX, IndexScanType.BY_LUCENE, TupleRange.allOf(Tuple.from("entry_value:Vision", "sampleTextSong")), null, ScanProperties.FORWARD_SCAN);
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
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(MAP_ON_VALUE_INDEX, IndexScanType.BY_LUCENE, TupleRange.allOf(Tuple.from("entry_value:Vision", "sampleTextPhrase")), null, ScanProperties.FORWARD_SCAN);
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

    @Test
    void searchForAutoComplete() throws Exception {
        searchForAutoCompleteAndAssert("good", true, false);
    }

    @Test
    void searchForAutoCompleteWithoutFieldWithoutTerm() throws Exception {
        assertThrows(RecordCoreArgumentException.class,
                () -> searchForAutoCompleteAndAssert("", true, false),
                "Invalid query for auto-complete search");
    }

    @Test
    void searchForAutoCompleteWithPrefix() throws Exception {
        searchForAutoCompleteAndAssert("goo", true, false);
    }

    @Test
    void searchForAutoCompleteWithHighlight() throws Exception {
        searchForAutoCompleteAndAssert("good", true, true);
    }

    @Test
    void searchForAutoCompleteCrossingMultipleFields() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(COMPLEX_DOC, COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE);
            });

            // Write 8 texts and 6 of them contain the key "good"
            recordStore.saveRecord(createComplexDocument(1623L, "Good morning", "", 1));
            recordStore.saveRecord(createComplexDocument(1624L, "Good afternoon", "", 1));
            recordStore.saveRecord(createComplexDocument(1625L, "good evening", "", 1));
            recordStore.saveRecord(createComplexDocument(1626L, "Good night", "", 1));
            recordStore.saveRecord(createComplexDocument(1627L, "", "That's really good!", 1));
            recordStore.saveRecord(createComplexDocument(1628L, "", "I'm good", 1));
            recordStore.saveRecord(createComplexDocument(1629L, "", "Hello Record Layer", 1));
            RecordType recordType = recordStore.saveRecord(createComplexDocument(1630L, "", "Hello FoundationDB!", 1)).getRecordType();

            List<IndexEntry> results = recordStore.scanIndex(COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE,
                    IndexScanType.BY_LUCENE_AUTO_COMPLETE,
                    TupleRange.allOf(Tuple.from("good")),
                    null,
                    ScanProperties.FORWARD_SCAN).asList().get();

            results.stream().forEach(i -> assertDocumentPartialRecordFromIndexEntry(recordType, i,
                    (String) i.getKey().get(i.getKeySize() - 1),
                    (String) i.getKey().get(i.getKeySize() - 2)));

            assertEquals(6, context.getTimer().getCounter(FDBStoreTimer.Counts.LUCENE_SCAN_MATCHED_AUTO_COMPLETE_SUGGESTIONS).getCount());
            assertEntriesAndSegmentInfoStoredInCompoundFile(DirectoryCommitCheckAsync.getSuggestionIndexSubspace(recordStore.indexSubspace(COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE), TupleHelpers.EMPTY),
                    context, "_0.cfs", true);

            // Assert the count of suggestions
            assertEquals(6, results.size());

            // Assert the suggestions' keys
            List<String> suggestions = results.stream().map(i -> (String) i.getKey().get(i.getKeySize() - 1)).collect(Collectors.toList());
            assertEquals(ImmutableList.of("good evening", "Good night", "Good morning", "Good afternoon", "I'm good", "That's really good!"), suggestions);

            // Assert the corresponding field for the suggestions
            List<String> fields = results.stream().map(i -> (String) i.getKey().get(i.getKeySize() - 2)).collect(Collectors.toList());
            assertEquals(ImmutableList.of("text", "text", "text", "text", "text2", "text2"), fields);

            // Assert the suggestions are sorted according to their values, which are determined by the position of the term into the indexed text
            List<Long> values = results.stream().map(i -> (Long) i.getValue().get(0)).collect(Collectors.toList());
            List<Long> valuesSorted = new ArrayList<>(values);
            Collections.sort(valuesSorted, Collections.reverseOrder());
            assertEquals(valuesSorted, values);
            assertEquals(values.get(0), values.get(1));
            assertEquals(values.get(1), values.get(2));
            assertEquals(values.get(2), values.get(3));
            assertTrue(values.get(3) > values.get(4));
            assertTrue(values.get(4) > values.get(5));
        }
    }

    @Test
    void searchForAutoCompleteWithContinueTyping() throws Exception {
        try (FDBRecordContext context = openContext()) {
            addIndexAndSaveRecordForAutoComplete(context, false);
            List<IndexEntry> results = recordStore.scanIndex(SIMPLE_TEXT_WITH_AUTO_COMPLETE,
                    IndexScanType.BY_LUCENE_AUTO_COMPLETE,
                    TupleRange.allOf(Tuple.from("good mor")),
                    null,
                    ScanProperties.FORWARD_SCAN).asList().get();

            // Assert only 1 suggestion returned
            assertEquals(1, results.size());

            // Assert the suggestion's key and field
            IndexEntry result = results.get(0);
            assertEquals("Good morning", result.getKey().get(result.getKeySize() - 1));
            assertEquals("text", result.getKey().get(result.getKeySize() - 2));

            assertEquals(1, context.getTimer().getCounter(FDBStoreTimer.Counts.LUCENE_SCAN_MATCHED_AUTO_COMPLETE_SUGGESTIONS).getCount());
            assertEntriesAndSegmentInfoStoredInCompoundFile(DirectoryCommitCheckAsync.getSuggestionIndexSubspace(recordStore.indexSubspace(SIMPLE_TEXT_WITH_AUTO_COMPLETE), TupleHelpers.EMPTY),
                    context, "_0.cfs", true);
        }
    }

    @Test
    void searchForAutoCompleteForGroupedRecord() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(MAP_DOC, MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE);
            });
            FDBStoredRecord<Message> fdbRecord = recordStore.saveRecord(createMultiEntryMapDoc(1623L, ENGINEER_JOKE, "sampleTextPhrase", WAYLON, "sampleTextSong", 2));
            List<IndexEntry> indexEntries = recordStore.scanIndex(MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE,
                    IndexScanType.BY_LUCENE_AUTO_COMPLETE,
                    TupleRange.allOf(Tuple.from("Vision", "sampleTextPhrase")),
                    null,
                    ScanProperties.FORWARD_SCAN).asList().get();

            assertEquals(1, indexEntries.size());
            IndexEntry indexEntry = indexEntries.get(0);

            Descriptors.Descriptor recordDescriptor = TestRecordsTextProto.MapDocument.getDescriptor();
            IndexKeyValueToPartialRecord toPartialRecord = LuceneIndexQueryPlan.getToPartialRecord(MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE, fdbRecord.getRecordType());
            Message message = toPartialRecord.toRecord(recordDescriptor, indexEntry);

            Descriptors.FieldDescriptor entryDescriptor = recordDescriptor.findFieldByName("entry");
            Message entry = (Message)message.getRepeatedField(entryDescriptor, 0);

            Descriptors.FieldDescriptor keyDescriptor = entryDescriptor.getMessageType().findFieldByName("key");
            Descriptors.FieldDescriptor valueDescriptor = entryDescriptor.getMessageType().findFieldByName("value");

            assertEquals("sampleTextPhrase", entry.getField(keyDescriptor));
            assertEquals(ENGINEER_JOKE, entry.getField(valueDescriptor));

            assertEquals(1, context.getTimer().getCounter(FDBStoreTimer.Counts.LUCENE_SCAN_MATCHED_AUTO_COMPLETE_SUGGESTIONS).getCount());
            assertEntriesAndSegmentInfoStoredInCompoundFile(DirectoryCommitCheckAsync.getSuggestionIndexSubspace(recordStore.indexSubspace(MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE), Tuple.from("sampleTextPhrase")),
                    context, "_0.cfs", true);
        }
    }

    @Test
    void spellCheck() throws Exception {
        try (FDBRecordContext context = openContext()) {
            addIndexAndSaveRecordForSpellcheck(context);
            List<IndexEntry> results = recordStore.scanIndex(SPELLCHECK_LUCENE_INDEX,
                    IndexScanType.BY_LUCENE_SPELLCHECK,
                    TupleRange.allOf(Tuple.from("keyboad")),
                    null,
                    ScanProperties.FORWARD_SCAN).asList().get();

            assertEquals(1, results.size());
            IndexEntry result = results.get(0);
            assertEquals("keyboard", result.getKey().get(0));
            assertEquals("text", result.getValue().get(0));
        }
    }

    private void addIndexAndSaveRecordForSpellcheck(@Nonnull FDBRecordContext context) {
        rebuildIndexMetaData(context, SIMPLE_DOC, SPELLCHECK_LUCENE_INDEX);
        long docId = 1623L;
        for (String word : List.of("hello", "monitor", "keyboard", "mouse", "trackpad", "cable")) {
            recordStore.saveRecord(createSimpleDocument(docId++, word, 1));
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

    private void searchForAutoCompleteAndAssert(String query, boolean matches, boolean highlight) throws Exception {
        try (FDBRecordContext context = openContext()) {
            final RecordType recordType = addIndexAndSaveRecordForAutoComplete(context, highlight);
            List<IndexEntry> results = recordStore.scanIndex(highlight ? SIMPLE_TEXT_WITH_AUTO_COMPLETE_WITH_HIGHLIGHT : SIMPLE_TEXT_WITH_AUTO_COMPLETE,
                    IndexScanType.BY_LUCENE_AUTO_COMPLETE,
                    TupleRange.allOf(Tuple.from(query)),
                    null,
                    ScanProperties.FORWARD_SCAN).asList().get();

            if (!matches) {
                // Assert no suggestions
                assertTrue(results.isEmpty());
                return;
            }

            // Assert the count of suggestions
            assertEquals(6, results.size());

            // Assert the suggestions' keys
            List<String> suggestions = results.stream().map(i -> (String) i.getKey().get(i.getKeySize() - 1)).collect(Collectors.toList());
            if (highlight) {
                assertEquals(ImmutableList.of("<b>good</b> evening", "<b>Good</b> night", "<b>Good</b> morning", "<b>Good</b> afternoon", "I'm <b>good</b>", "That's really <b>good</b>!"), suggestions);
            } else {
                assertEquals(ImmutableList.of("good evening", "Good night", "Good morning", "Good afternoon", "I'm good", "That's really good!"), suggestions);
            }

            // Assert the corresponding field for the suggestions
            List<String> fields = results.stream().map(i -> (String) i.getKey().get(i.getKeySize() - 2)).collect(Collectors.toList());
            assertEquals(ImmutableList.of("text", "text", "text", "text", "text", "text"), fields);

            // Assert the suggestions are sorted according to their values, which are determined by the position of the term into the indexed text
            List<Long> values = results.stream().map(i -> (Long) i.getValue().get(0)).collect(Collectors.toList());
            List<Long> valuesSorted = new ArrayList<>(values);
            Collections.sort(valuesSorted, Collections.reverseOrder());
            assertEquals(valuesSorted, values);
            assertEquals(values.get(0), values.get(1));
            assertEquals(values.get(1), values.get(2));
            assertEquals(values.get(2), values.get(3));
            assertTrue(values.get(3) > values.get(4));
            assertTrue(values.get(4) > values.get(5));

            results.stream().forEach(i -> assertDocumentPartialRecordFromIndexEntry(recordType, i,
                    (String) i.getKey().get(i.getKeySize() - 1),
                    (String) i.getKey().get(i.getKeySize() - 2)));

            assertEquals(6, context.getTimer().getCounter(FDBStoreTimer.Counts.LUCENE_SCAN_MATCHED_AUTO_COMPLETE_SUGGESTIONS).getCount());
            assertEntriesAndSegmentInfoStoredInCompoundFile(DirectoryCommitCheckAsync.getSuggestionIndexSubspace(recordStore.indexSubspace(SIMPLE_TEXT_WITH_AUTO_COMPLETE), TupleHelpers.EMPTY),
                    context, "_0.cfs", true);
        }
    }

    private RecordType addIndexAndSaveRecordForAutoComplete(@Nonnull FDBRecordContext context, boolean highlight) {
        openRecordStore(context, metaDataBuilder -> {
            metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
            metaDataBuilder.addIndex(SIMPLE_DOC, highlight ? SIMPLE_TEXT_WITH_AUTO_COMPLETE_WITH_HIGHLIGHT : SIMPLE_TEXT_WITH_AUTO_COMPLETE);
        });
        
        // Write 8 texts and 6 of them contain the key "good"
        recordStore.saveRecord(createSimpleDocument(1623L, "Good morning", 1));
        recordStore.saveRecord(createSimpleDocument(1624L, "Good afternoon", 1));
        recordStore.saveRecord(createSimpleDocument(1625L, "good evening", 1));
        recordStore.saveRecord(createSimpleDocument(1626L, "Good night", 1));
        recordStore.saveRecord(createSimpleDocument(1627L, "That's really good!", 1));
        recordStore.saveRecord(createSimpleDocument(1628L, "I'm good", 1));
        recordStore.saveRecord(createSimpleDocument(1629L, "Hello Record Layer", 1));
        return recordStore.saveRecord(createSimpleDocument(1630L, "Hello FoundationDB!", 1)).getRecordType();
    }

    private void assertDocumentPartialRecordFromIndexEntry(@Nonnull RecordType recordType, @Nonnull IndexEntry indexEntry,
                                                           @Nonnull String expectedSuggestion, @Nonnull String fieldName) {
        Descriptors.Descriptor recordDescriptor = recordType.getDescriptor();

        Message message = LuceneIndexQueryPlan.getToPartialRecord(indexEntry.getIndex(), recordType)
                .toRecord(recordDescriptor, indexEntry);
        Descriptors.FieldDescriptor textDescriptor = recordDescriptor.findFieldByName(fieldName);

        assertEquals(expectedSuggestion, message.getField(textDescriptor));
    }

    private void rebuildIndexMetaData(final FDBRecordContext context, final String simpleDoc, final Index simpleTextSuffixes) {
        openRecordStore(context, metaDataBuilder -> {
            metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
            metaDataBuilder.addIndex(simpleDoc, simpleTextSuffixes);
        });
    }
}
