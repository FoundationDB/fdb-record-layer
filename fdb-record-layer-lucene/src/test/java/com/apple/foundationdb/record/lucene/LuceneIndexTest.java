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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.lucene.directory.FDBLuceneFileReference;
import com.apple.foundationdb.record.lucene.ngram.NgramAnalyzer;
import com.apple.foundationdb.record.lucene.synonym.EnglishSynonymMapConfig;
import com.apple.foundationdb.record.lucene.synonym.SynonymAnalyzer;
import com.apple.foundationdb.record.lucene.synonym.SynonymMapConfig;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.common.text.AllSuffixesTextTokenizer;
import com.apple.foundationdb.record.provider.common.text.TextSamples;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContextConfig;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.test.Tags;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.index.IndexFileNames;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.lucene.LucenePlanMatchers.group;
import static com.apple.foundationdb.record.lucene.LucenePlanMatchers.query;
import static com.apple.foundationdb.record.lucene.LucenePlanMatchers.scanParams;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.metadata.Key.Expressions.recordType;
import static com.apple.foundationdb.record.metadata.Key.Expressions.value;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.COMPLEX_DOC;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.MAP_DOC;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.SIMPLE_DOC;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@code LUCENE} type indexes.
 */
@Tag(Tags.RequiresFDB)
public class LuceneIndexTest extends FDBRecordStoreTestBase {
    private static final Index SIMPLE_TEXT_SUFFIXES = new Index("Simple$text_suffixes",
            function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME));

    private static final Index SIMPLE_TEXT_WITH_AUTO_COMPLETE = new Index("Simple_with_auto_complete",
            function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(LuceneIndexOptions.AUTO_COMPLETE_ENABLED, "true",
                    LuceneIndexOptions.AUTO_COMPLETE_MIN_PREFIX_SIZE, "3"));

    private static final Index SIMPLE_TEXT_WITH_AUTO_COMPLETE_NO_FREQS_POSITIONS = new Index("Simple_with_auto_complete",
            function(LuceneFunctionNames.LUCENE_TEXT, concat(field("text"),
                    function(LuceneFunctionNames.LUCENE_AUTO_COMPLETE_FIELD_INDEX_OPTIONS, value(LuceneFunctionNames.LuceneFieldIndexOptions.DOCS.name())),
                    function(LuceneFunctionNames.LUCENE_FULL_TEXT_FIELD_INDEX_OPTIONS, value(LuceneFunctionNames.LuceneFieldIndexOptions.DOCS.name())))),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(LuceneIndexOptions.AUTO_COMPLETE_ENABLED, "true",
                    LuceneIndexOptions.AUTO_COMPLETE_MIN_PREFIX_SIZE, "3"));

    private static final Index COMPLEX_MULTIPLE_TEXT_INDEXES = new Index("Complex$text_multipleIndexes",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME));

    private static final Index COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE = new Index("Complex$text_multipleIndexes",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(LuceneIndexOptions.AUTO_COMPLETE_ENABLED, "true",
                    LuceneIndexOptions.AUTO_COMPLETE_MIN_PREFIX_SIZE, "3"));

    private static final Index COMPLEX_MULTIPLE_GROUPED = new Index("Complex$text_multiple_grouped",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))).groupBy(field("group")),
            LuceneIndexTypes.LUCENE);

    private static final Index COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE = new Index("Complex$text_multiple_grouped_autocomplete",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))).groupBy(field("group")),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(LuceneIndexOptions.AUTO_COMPLETE_ENABLED, "true",
                    LuceneIndexOptions.AUTO_COMPLETE_MIN_PREFIX_SIZE, "3"));

    private static final Index NGRAM_LUCENE_INDEX = new Index("ngram_index", function(LuceneFunctionNames.LUCENE_TEXT, field("text")), LuceneIndexTypes.LUCENE,
            ImmutableMap.of(LuceneIndexOptions.TEXT_ANALYZER_NAME_OPTION, NgramAnalyzer.NgramAnalyzerFactory.ANALYZER_FACTORY_NAME,
                    IndexOptions.TEXT_TOKEN_MIN_SIZE, "3",
                    IndexOptions.TEXT_TOKEN_MAX_SIZE, "5"));

    private static final Index QUERY_ONLY_SYNONYM_LUCENE_INDEX = new Index("synonym_index", function(LuceneFunctionNames.LUCENE_TEXT, field("text")), LuceneIndexTypes.LUCENE,
            ImmutableMap.of(
                    LuceneIndexOptions.TEXT_ANALYZER_NAME_OPTION, SynonymAnalyzer.QueryOnlySynonymAnalyzerFactory.ANALYZER_FACTORY_NAME,
                    LuceneIndexOptions.TEXT_SYNONYM_SET_NAME_OPTION, EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig.CONFIG_NAME));

    private static final Index AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX = new Index("synonym_index", function(LuceneFunctionNames.LUCENE_TEXT, field("text")), LuceneIndexTypes.LUCENE,
            ImmutableMap.of(
                    LuceneIndexOptions.TEXT_ANALYZER_NAME_OPTION, SynonymAnalyzer.AuthoritativeSynonymOnlyAnalyzerFactory.ANALYZER_FACTORY_NAME,
                    LuceneIndexOptions.TEXT_SYNONYM_SET_NAME_OPTION, EnglishSynonymMapConfig.AuthoritativeOnlyEnglishSynonymMapConfig.CONFIG_NAME));

    private static final String COMBINED_SYNONYM_SETS = "COMBINED_SYNONYM_SETS";

    private static final Index QUERY_ONLY_SYNONYM_LUCENE_COMBINED_SETS_INDEX = new Index("synonym_combined_sets_index", function(LuceneFunctionNames.LUCENE_TEXT, field("text")), LuceneIndexTypes.LUCENE,
            ImmutableMap.of(
                    LuceneIndexOptions.TEXT_ANALYZER_NAME_OPTION, SynonymAnalyzer.QueryOnlySynonymAnalyzerFactory.ANALYZER_FACTORY_NAME,
                    LuceneIndexOptions.TEXT_SYNONYM_SET_NAME_OPTION, COMBINED_SYNONYM_SETS));

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

    private static final Index MAP_ON_VALUE_INDEX = new Index("Map$entry-value", new GroupingKeyExpression(field("entry", KeyExpression.FanType.FanOut).nest(concat(keys)), 3), LuceneIndexTypes.LUCENE);

    private static final Index MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE = new Index("Map_with_auto_complete$entry-value",
            new GroupingKeyExpression(field("entry", KeyExpression.FanType.FanOut).nest(concat(keys)), 3),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(LuceneIndexOptions.AUTO_COMPLETE_ENABLED, "true"));

    private static final Index ANALYZER_CHOOSER_TEST_LUCENE_INDEX = new Index("analyzer_chooser_test_index", function(LuceneFunctionNames.LUCENE_TEXT, field("text")), LuceneIndexTypes.LUCENE,
            ImmutableMap.of(
                    LuceneIndexOptions.TEXT_ANALYZER_NAME_OPTION, TestAnalyzerFactory.ANALYZER_FACTORY_NAME));

    private static final String ENGINEER_JOKE = "A software engineer, a hardware engineer, and a departmental manager were driving down a steep mountain road when suddenly the brakes on their car failed. The car careened out of control down the road, bouncing off the crash barriers, ground to a halt scraping along the mountainside. The occupants were stuck halfway down a mountain in a car with no brakes. What were they to do?" +
                                                "'I know,' said the departmental manager. 'Let's have a meeting, propose a Vision, formulate a Mission Statement, define some Goals, and by a process of Continuous Improvement find a solution to the Critical Problems, and we can be on our way.'" +
                                                "'No, no,' said the hardware engineer. 'That will take far too long, and that method has never worked before. In no time at all, I can strip down the car's braking system, isolate the fault, fix it, and we can be on our way.'" +
                                                "'Wait, said the software engineer. 'Before we do anything, I think we should push the car back up the road and see if it happens again.'";

    private static final String WAYLON = "There's always one more way to do things and that's your way, and you have a right to try it at least once.";

    private static final int DEFAULT_AUTO_COMPLETE_TEXT_SIZE_LIMIT = LuceneRecordContextProperties.LUCENE_AUTO_COMPLETE_TEXT_SIZE_UPPER_LIMIT.getDefaultValue().intValue();

    protected void openRecordStore(FDBRecordContext context, FDBRecordStoreTestBase.RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsTextProto.getDescriptor());
        metaDataBuilder.getRecordType(COMPLEX_DOC).setPrimaryKey(concatenateFields("group", "doc_id"));
        hook.apply(metaDataBuilder);
        recordStore = getStoreBuilder(context, metaDataBuilder.getRecordMetaData())
                .setSerializer(TextIndexTestUtils.COMPRESSING_SERIALIZER)
                .createOrOpen();
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
    protected FDBRecordContextConfig.Builder contextConfig(@Nonnull final RecordLayerPropertyStorage.Builder propsBuilder) {
        return super.contextConfig(propsBuilder.addProp(LuceneRecordContextProperties.LUCENE_INDEX_COMPRESSION_ENABLED, true));
    }

    private LuceneScanBounds fullTextSearch(Index index, String search) {
        LuceneScanParameters scan = new LuceneScanQueryParameters(
                ScanComparisons.EMPTY,
                new LuceneQueryMultiFieldSearchClause(search, false));
        return scan.bind(recordStore, index, EvaluationContext.EMPTY);
    }

    private LuceneScanBounds groupedTextSearch(Index index, String search, Object group) {
        LuceneScanParameters scan = new LuceneScanQueryParameters(
                ScanComparisons.from(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, group)),
                new LuceneQuerySearchClause(search, false));
        return scan.bind(recordStore, index, EvaluationContext.EMPTY);
    }

    private LuceneScanBounds autoComplete(Index index, String search, boolean highlight) {
        LuceneScanParameters scan = new LuceneScanAutoCompleteParameters(
                ScanComparisons.EMPTY,
                search, false, highlight);
        return scan.bind(recordStore, index, EvaluationContext.EMPTY);
    }

    private LuceneScanBounds groupedAutoComplete(Index index, String search, Object group, boolean highlight) {
        LuceneScanParameters scan = new LuceneScanAutoCompleteParameters(
                ScanComparisons.from(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, group)),
                search, false, highlight);
        return scan.bind(recordStore, index, EvaluationContext.EMPTY);
    }

    private LuceneScanBounds spellCheck(Index index, String search) {
        LuceneScanParameters scan = new LuceneScanSpellCheckParameters(
                ScanComparisons.EMPTY,
                search, false);
        return scan.bind(recordStore, index, EvaluationContext.EMPTY);
    }

    private LuceneScanBounds groupedSpellCheck(Index index, String search, Object group) {
        LuceneScanParameters scan = new LuceneScanSpellCheckParameters(
                ScanComparisons.from(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, group)),
                search, false);
        return scan.bind(recordStore, index, EvaluationContext.EMPTY);
    }

    @Test
    void simpleInsertAndSearch() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "\"propose a Vision\""), null, ScanProperties.FORWARD_SCAN));
            assertEquals(1, context.getTimer().getCounter(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs", true);
        }
    }

    @Test
    void simpleEmptyIndex() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "something"), null, ScanProperties.FORWARD_SCAN)) {
                assertEquals(RecordCursorResult.exhausted(), cursor.getNext());
            }
        }
    }

    @Test
    void simpleEmptyAutoComplete() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_WITH_AUTO_COMPLETE);
            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(SIMPLE_TEXT_WITH_AUTO_COMPLETE, autoComplete(SIMPLE_TEXT_WITH_AUTO_COMPLETE, "something", false), null, ScanProperties.FORWARD_SCAN)) {
                assertEquals(RecordCursorResult.exhausted(), cursor.getNext());
            }
        }
    }

    @Test
    void simpleInsertAndSearchNumFDBFetches() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), null, ScanProperties.FORWARD_SCAN));
            assertEquals(1, context.getTimer().getCounter(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

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
            LuceneContinuationProto.LuceneIndexContinuation continuation =  LuceneContinuationProto.LuceneIndexContinuation.newBuilder()
                    .setDoc(1)
                    .setScore(0.12324655F)
                    .setShard(0)
                    .build();
            assertIndexEntryPrimaryKeys(List.of(1625L, 1626L),
                    recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), continuation.toByteArray(), ScanProperties.FORWARD_SCAN));

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs", true);
        }
    }

    @Test
    void testNullValue() throws ExecutionException, InterruptedException {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            recordStore.saveRecord(createSimpleDocument(1623L, 2));
            recordStore.saveRecord(createSimpleDocument(1632L, ENGINEER_JOKE, 2));
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "*:* AND NOT text:[* TO *]"), null, ScanProperties.FORWARD_SCAN));

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
            assertEquals(50, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), null, ExecuteProperties.newBuilder().setReturnedRowLimit(50).build().asScanProperties(false))
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
            assertEquals(40, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), null, ExecuteProperties.newBuilder().setReturnedRowLimit(50).setSkip(10).build().asScanProperties(false))
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
            assertEquals(40, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), null, ExecuteProperties.newBuilder().setReturnedRowLimit(50).setSkip(10).build().asScanProperties(false))
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
            LuceneContinuationProto.LuceneIndexContinuation continuation = LuceneContinuationProto.LuceneIndexContinuation.newBuilder()
                    .setDoc(151)
                    .setScore(0.0011561684F)
                    .setShard(0)
                    .build();
            assertEquals(48, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), continuation.toByteArray(), ExecuteProperties.newBuilder().setReturnedRowLimit(50).build().asScanProperties(false))
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
            assertEquals(251, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), null, ExecuteProperties.newBuilder().setReturnedRowLimit(251).build().asScanProperties(false))
                    .getCount().join());
            assertEquals(2, recordStore.getTimer().getCount(LuceneEvents.Events.LUCENE_INDEX_SCAN));
            assertEquals(251, recordStore.getTimer().getCount(LuceneEvents.Counts.LUCENE_SCAN_MATCHED_DOCUMENTS));

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
            assertEquals(50, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), null, ExecuteProperties.newBuilder().setReturnedRowLimit(251).setSkip(201).build().asScanProperties(false))
                    .getCount().join());
            assertEquals(2, recordStore.getTimer().getCount(LuceneEvents.Events.LUCENE_INDEX_SCAN));
            assertEquals(251, recordStore.getTimer().getCount(LuceneEvents.Counts.LUCENE_SCAN_MATCHED_DOCUMENTS));

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs", true);
        }
    }

    @Test
    void testLimitForAutoCompleteWithMultipleMatchingFields() {
        final RecordLayerPropertyStorage.Builder storageBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_AUTO_COMPLETE_SEARCH_LIMITATION, Integer.MAX_VALUE);
        try (FDBRecordContext context = openContext(storageBuilder)) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(COMPLEX_DOC, COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE);
            });

            for (int i = 0; i < 200; i++) {
                recordStore.saveRecord(createComplexDocument(1623L + i, "auto complete suggestions " + 1623 + 2 * i, "auto complete suggestions " + + 1623 + 2 * i + 1, 2));
            }
            // Only 50 results are returned, although each matching doc has 2 fields that contain the search key
            assertEquals(50, recordStore.scanIndex(COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE, autoComplete(COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE, "complete", false), null,
                    ExecuteProperties.newBuilder().setReturnedRowLimit(50).build().asScanProperties(false)).getCount().join());

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE), context, "_0.cfs", true);
        }
    }

    @Test
    void testLimitAndSkipForAutoCompleteWithMultipleMatchingFields() {
        final RecordLayerPropertyStorage.Builder storageBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_AUTO_COMPLETE_SEARCH_LIMITATION, Integer.MAX_VALUE);
        try (FDBRecordContext context = openContext(storageBuilder)) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(COMPLEX_DOC, COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE);
            });

            for (int i = 0; i < 200; i++) {
                recordStore.saveRecord(createComplexDocument(1623L + i, "auto complete suggestions " + 1623 + 2 * i, "auto complete suggestions " + + 1623 + 2 * i + 1, 2));
            }
            // 50 results are returned, although each matching doc has 2 fields that contain the search key, and the first 10 results are skipped
            assertEquals(50, recordStore.scanIndex(COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE, autoComplete(COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE, "complete", false), null,
                    ExecuteProperties.newBuilder().setReturnedRowLimit(50).setSkip(10).build().asScanProperties(false)).getCount().join());

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE), context, "_0.cfs", true);
        }
    }

    @Test
    void testLimitAndSkipForAutoCompleteWithSingleMatchingField() {
        final RecordLayerPropertyStorage.Builder storageBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_AUTO_COMPLETE_SEARCH_LIMITATION, Integer.MAX_VALUE);
        try (FDBRecordContext context = openContext(storageBuilder)) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(COMPLEX_DOC, COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE);
            });

            for (int i = 0; i < 200; i++) {
                recordStore.saveRecord(createComplexDocument(1623L + i, "auto complete suggestions " + 1623 + 2 * i, "other suggestions " + + 1623 + 2 * i + 1, 2));
            }
            // 50 results are returned, with one result for each matching doc, although the first 10 results are skipped
            assertEquals(50, recordStore.scanIndex(COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE, autoComplete(COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE, "complete", false), null,
                    ExecuteProperties.newBuilder().setReturnedRowLimit(50).setSkip(10).build().asScanProperties(false)).getCount().join());

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE), context, "_0.cfs", true);
        }
    }

    @Test
    void testNestedFieldSearch() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, MAP_DOC, MAP_ON_VALUE_INDEX);
            recordStore.saveRecord(createComplexMapDocument(1623L, ENGINEER_JOKE, "sampleTextSong", 2));
            recordStore.saveRecord(createComplexMapDocument(1547L, WAYLON, "sampleTextPhrase",  1));
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(MAP_ON_VALUE_INDEX, groupedTextSearch(MAP_ON_VALUE_INDEX, "entry_value:Vision", "sampleTextSong"), null, ScanProperties.FORWARD_SCAN));
            assertEquals(1, context.getTimer().getCounter(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(MAP_ON_VALUE_INDEX).subspace(Tuple.from("sampleTextSong")), context, "_0.cfs", true);
        }
    }

    @Test
    void testGroupedRecordSearch() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, MAP_DOC, MAP_ON_VALUE_INDEX);
            recordStore.saveRecord(createMultiEntryMapDoc(1623L, ENGINEER_JOKE, "sampleTextPhrase", WAYLON, "sampleTextSong", 2));
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(MAP_ON_VALUE_INDEX, groupedTextSearch(MAP_ON_VALUE_INDEX, "entry_value:Vision", "sampleTextPhrase"), null, ScanProperties.FORWARD_SCAN));
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
            assertIndexEntryPrimaryKeyTuples(List.of(Tuple.from(2L, 1623L)),
                    recordStore.scanIndex(COMPLEX_MULTIPLE_TEXT_INDEXES, fullTextSearch(COMPLEX_MULTIPLE_TEXT_INDEXES, "text:\"Vision\" AND text2:\"john_leach@apple.com\""), null, ScanProperties.FORWARD_SCAN));

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(COMPLEX_MULTIPLE_TEXT_INDEXES), context, "_0.cfs", true);
        }
    }

    @Test
    void testFuzzySearchWithDefaultEdit2() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_MULTIPLE_TEXT_INDEXES);
            recordStore.saveRecord(createComplexDocument(1623L, ENGINEER_JOKE, "john_leach@apple.com", 2));
            recordStore.saveRecord(createComplexDocument(1547L, WAYLON, "hering@gmail.com", 2));
            assertIndexEntryPrimaryKeyTuples(List.of(Tuple.from(2L, 1623L)),
                    recordStore.scanIndex(COMPLEX_MULTIPLE_TEXT_INDEXES, fullTextSearch(COMPLEX_MULTIPLE_TEXT_INDEXES, "text:\"Vision\" AND text2:jonleach@apple.com\\~"), null, ScanProperties.FORWARD_SCAN));

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
            assertIndexEntryPrimaryKeys(List.of(1623L, 1624L),
                    recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), null, ScanProperties.FORWARD_SCAN));
            assertTrue(recordStore.deleteRecord(Tuple.from(1624L)));
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), null, ScanProperties.FORWARD_SCAN));

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs", true);
        }
    }

    @Test
    void simpleInsertAndSearchSingleTransaction() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            // Save one record and try and search for it
            for (int docId = 0; docId < 100; docId++) {
                recordStore.saveRecord(createSimpleDocument(docId, ENGINEER_JOKE, 2));
                assertEquals(docId + 1, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "formulate"), null, ScanProperties.FORWARD_SCAN)
                        .getCount().join());
            }

            commit(context);
        }
    }

    @Test
    void testCommit() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), null, ScanProperties.FORWARD_SCAN));

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs", false);

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), null, ScanProperties.FORWARD_SCAN));

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs", true);
        }
    }

    @Test
    void testRollback() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), null, ScanProperties.FORWARD_SCAN));

            assertEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs", false);

            context.ensureActive().cancel();
        }
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
            assertIndexEntryPrimaryKeys(Collections.emptyList(),
                    recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), null, ScanProperties.FORWARD_SCAN));
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), null, ScanProperties.FORWARD_SCAN));

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

    private String matchAll(String... words) {
        return "text:(" +
               Arrays.stream(words).map(word -> "+\"" + word + "\"").collect(Collectors.joining(" AND ")) +
               ")";
    }

    @Test
    void scanWithQueryOnlySynonymIndex() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, QUERY_ONLY_SYNONYM_LUCENE_INDEX);
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "hullo record layer"), null, ScanProperties.FORWARD_SCAN));
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello recor layer", 1));
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "hullo record layer"), null, ScanProperties.FORWARD_SCAN));
            // "hullo" is synonym of "hello"
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, matchAll("hullo", "record", "layer")), null, ScanProperties.FORWARD_SCAN));
            // it doesn't match due to the "recor"
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
            assertIndexEntryPrimaryKeys(Collections.emptyList(),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, matchAll("hullo", "recor", "layer")), null, ScanProperties.FORWARD_SCAN));
            // "hullo" is synonym of "hello", and "show" is synonym of "record"
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, matchAll("hullo", "show", "layer")), null, ScanProperties.FORWARD_SCAN));
        }
    }

    @Test
    void scanWithAuthoritativeSynonymOnlyIndex() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX);
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
            assertEquals(1, recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, "hullo record layer"), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello recor layer", 1));
            assertEquals(1, recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, "hullo record layer"), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            // "hullo" is synonym of "hello"
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
            assertEquals(1, recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX,
                            fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, matchAll("hullo", "record", "layer")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            // it doesn't match due to the "recor"
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
            assertEquals(0, recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX,
                            fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, matchAll("hullo", "recor", "layer")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            // "hullo" is synonym of "hello", and "show" is synonym of "record"
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
            assertEquals(1, recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX,
                            fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, matchAll("hullo", "show", "layer")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
        }
    }

    @Test
    void phraseSearchBasedOnQueryOnlySynonymIndex() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, QUERY_ONLY_SYNONYM_LUCENE_INDEX);
            // Save a document to verify synonym search based on the group {'motivation','motive','need'}
            recordStore.saveRecord(createSimpleDocument(1623L, "I think you need to search with Lucene index", 1));
            // Search for original phrase
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "\"you need to\""), null, ScanProperties.FORWARD_SCAN));
            // Search for phrase with changed order of tokens, no match is expected
            assertIndexEntryPrimaryKeys(Collections.emptyList(),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "\"need you to\""), null, ScanProperties.FORWARD_SCAN));
            // Search for phrase with "motivation" as synonym of "need"
            // "Motivation" is the authoritative term of the group {'motivation','motive','need'}
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "\"you motivation to\""), null, ScanProperties.FORWARD_SCAN));
            // Search for phrase with "motivation" with changed order of tokens, no match is expected
            assertIndexEntryPrimaryKeys(Collections.emptyList(),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "\"motivation you to\""), null, ScanProperties.FORWARD_SCAN));
            // Search for phrase with "motive" as synonym of "need"
            // "Motive" is not the authoritative term of the group {'motivation','motive','need'}
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "\"you motive to\""), null, ScanProperties.FORWARD_SCAN));
            // Search for phrase with "motive" with changed order of tokens, no match is expected
            assertIndexEntryPrimaryKeys(Collections.emptyList(),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "\"motive you to\""), null, ScanProperties.FORWARD_SCAN));
            // Term query rather than phrase query, so match is expected although the order is changed
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "motivation you to"), null, ScanProperties.FORWARD_SCAN));

            // Save a document to verify synonym search based on the group {'departure','going','going away','leaving'}
            // This group contains multi-word term "going away", and also the single-word term "going"
            recordStore.saveRecord(createSimpleDocument(1624L, "He is leaving for New York next week", 1));
            assertIndexEntryPrimaryKeys(List.of(1624L),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "\"is departure for\""), null, ScanProperties.FORWARD_SCAN));

            // Search for phrase with "going away"
            assertIndexEntryPrimaryKeys(List.of(1624L),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "\"is going away for\""), null, ScanProperties.FORWARD_SCAN));

            //Search for phrase with "going"
            assertIndexEntryPrimaryKeys(List.of(1624L),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "\"is going for\""), null, ScanProperties.FORWARD_SCAN));

            // Search for phrase with only "away", no match is expected
            assertIndexEntryPrimaryKeys(Collections.emptyList(),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "\"is away for\""), null, ScanProperties.FORWARD_SCAN));
        }
    }

    @Test
    void phraseSearchBasedOnAuthoritativeSynonymOnlyIndex() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX);
            // Save a document to verify synonym search based on the group {'motivation','motive','need'}
            recordStore.saveRecord(createSimpleDocument(1623L, "I think you need to search with Lucene index", 1));
            // Search for original phrase
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, "\"you need to\""), null, ScanProperties.FORWARD_SCAN));
            // Search for phrase with changed order of tokens, no match is expected
            assertIndexEntryPrimaryKeys(Collections.emptyList(),
                    recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, "\"need you to\""), null, ScanProperties.FORWARD_SCAN));
            // Search for phrase with "motivation" as synonym of "need"
            // "Motivation" is the authoritative term of the group {'motivation','motive','need'}
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, "\"you motivation to\""), null, ScanProperties.FORWARD_SCAN));
            // Search for phrase with "motivation" with changed order of tokens, no match is expected
            assertIndexEntryPrimaryKeys(Collections.emptyList(),
                    recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, "\"motivation you to\""), null, ScanProperties.FORWARD_SCAN));
            // Search for phrase with "motive" as synonym of "need"
            // "Motive" is not the authoritative term of the group {'motivation','motive','need'}
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, "\"you motive to\""), null, ScanProperties.FORWARD_SCAN));
            // Search for phrase with "motive" with changed order of tokens, no match is expected
            assertIndexEntryPrimaryKeys(Collections.emptyList(),
                    recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, "\"motive you to\""), null, ScanProperties.FORWARD_SCAN));
            // Term query rather than phrase query, so match is expected although the order is changed
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, "motivation you to"), null, ScanProperties.FORWARD_SCAN));

            // Save a document to verify synonym search based on the group {'departure','going','going away','leaving'}
            // This group contains multi-word term "going away", and also the single-word term "going"
            recordStore.saveRecord(createSimpleDocument(1624L, "He is leaving for New York next week", 1));
            assertIndexEntryPrimaryKeys(List.of(1624L),
                    recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, "\"is departure for\""), null, ScanProperties.FORWARD_SCAN));

            // Search for phrase with "going away"
            assertIndexEntryPrimaryKeys(List.of(1624L),
                    recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, "\"is going away for\""), null, ScanProperties.FORWARD_SCAN));

            //Search for phrase with "going"
            assertIndexEntryPrimaryKeys(List.of(1624L),
                    recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, "\"is going for\""), null, ScanProperties.FORWARD_SCAN));

            // Search for phrase with only "away", the correct behavior is to return no match. But match is still hit due to the poor handling of positional data for multi-word synonym by this analyzer
            assertIndexEntryPrimaryKeys(List.of(1624L),
                    recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, "\"is away for\""), null, ScanProperties.FORWARD_SCAN));
        }
    }

    /**
     * Test config with a combined set of synonyms.
     */
    @AutoService(SynonymMapConfig.class)
    public static class CombinedSynonymSetsConfig implements SynonymMapConfig {
        @Override
        public String getName() {
            return COMBINED_SYNONYM_SETS;
        }

        @Override
        public boolean expand() {
            return true;
        }

        @Override
        public InputStream getSynonymInputStream() {
            InputStream is1 = null;
            InputStream is2 = null;
            try {
                is1 = new EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig().getSynonymInputStream();
                is2 = SynonymMapConfig.openFile("test.txt");
                return new SequenceInputStream(is1, is2);
            } catch (RecordCoreException e) {
                try {
                    if (is1 != null) {
                        is1.close();
                    }
                    if (is2 != null) {
                        is2.close();
                    }
                    throw e;
                } catch (IOException ignored) {
                    throw e;
                }
            }
        }
    }

    @Test
    void scanWithCombinedSetsSynonymIndex() throws IOException {
        // The COMBINED_SYNONYM_SETS adds this extra line to our synonym set:
        // 'synonym', 'nonsynonym'
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, QUERY_ONLY_SYNONYM_LUCENE_INDEX);
                metaDataBuilder.addIndex(SIMPLE_DOC, QUERY_ONLY_SYNONYM_LUCENE_COMBINED_SETS_INDEX);
            });
            recordStore.saveRecord(createSimpleDocument(1623L, "synonym is fun", 1));
            assertIndexEntryPrimaryKeys(Collections.emptyList(),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, matchAll("nonsynonym", "is", "fun")), null, ScanProperties.FORWARD_SCAN));
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_COMBINED_SETS_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_COMBINED_SETS_INDEX, matchAll("nonsynonym", "is", "fun")), null, ScanProperties.FORWARD_SCAN));
        }
    }

    @Test
    void proximitySearchWithMultiWordSynonym() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, QUERY_ONLY_SYNONYM_LUCENE_INDEX);

            // Both "hello" and "record" have multi-word synonyms
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello FoundationDB record layer", 1));
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "\"hello record\"~10"), null, ScanProperties.FORWARD_SCAN));
        }
    }

    @Test
    void scanWithNgramIndex() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, NGRAM_LUCENE_INDEX);
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(NGRAM_LUCENE_INDEX, fullTextSearch(NGRAM_LUCENE_INDEX, "hello record layer"), null, ScanProperties.FORWARD_SCAN));
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(NGRAM_LUCENE_INDEX, fullTextSearch(NGRAM_LUCENE_INDEX, "hello"), null, ScanProperties.FORWARD_SCAN));
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(NGRAM_LUCENE_INDEX, fullTextSearch(NGRAM_LUCENE_INDEX, "hel"), null, ScanProperties.FORWARD_SCAN));
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(NGRAM_LUCENE_INDEX, fullTextSearch(NGRAM_LUCENE_INDEX, "ell"), null, ScanProperties.FORWARD_SCAN));
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(NGRAM_LUCENE_INDEX, fullTextSearch(NGRAM_LUCENE_INDEX, "ecord"), null, ScanProperties.FORWARD_SCAN));
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(NGRAM_LUCENE_INDEX, fullTextSearch(NGRAM_LUCENE_INDEX, "hel ord aye"), null, ScanProperties.FORWARD_SCAN));
            assertIndexEntryPrimaryKeys(List.of(1623L),
                    recordStore.scanIndex(NGRAM_LUCENE_INDEX, fullTextSearch(NGRAM_LUCENE_INDEX, "ello record"), null, ScanProperties.FORWARD_SCAN));
            // The term "ella" is not expected
            assertIndexEntryPrimaryKeys(Collections.emptyList(),
                    recordStore.scanIndex(NGRAM_LUCENE_INDEX, fullTextSearch(NGRAM_LUCENE_INDEX, "ella"), null, ScanProperties.FORWARD_SCAN));
        }
    }

    @Test
    void searchForAutoComplete() throws Exception {
        searchForAutoCompleteAndAssert("good", true, false, DEFAULT_AUTO_COMPLETE_TEXT_SIZE_LIMIT);
    }

    @Test
    void searchForAutoCompleteWithoutFieldWithoutTerm() throws Exception {
        assertThrows(RecordCoreArgumentException.class,
                () -> searchForAutoCompleteAndAssert("", true, false, DEFAULT_AUTO_COMPLETE_TEXT_SIZE_LIMIT),
                "Invalid query for auto-complete search");
    }

    @Test
    void searchForAutoCompleteWithPrefix() throws Exception {
        searchForAutoCompleteAndAssert("goo", true, false, DEFAULT_AUTO_COMPLETE_TEXT_SIZE_LIMIT);
    }

    @Test
    void searchForAutoCompleteWithHighlight() throws Exception {
        searchForAutoCompleteAndAssert("good", true, true, DEFAULT_AUTO_COMPLETE_TEXT_SIZE_LIMIT);
    }

    @Test
    void searchForAutoCompleteWithoutHittingSizeLimitation() throws Exception {
        searchForAutoCompleteWithTextSizeLimit(DEFAULT_AUTO_COMPLETE_TEXT_SIZE_LIMIT, true);
    }

    @Test
    void searchForAutoCompleteWithHittingSizeLimitation() throws Exception {
        searchForAutoCompleteWithTextSizeLimit(10, false);
    }

    /**
     * To verify the suggestion lookup can work correctly if the suggester is never built and no entries exist in the directory.
     */
    @Test
    void searchForAutoCompleteWithLoadingNoRecords() throws Exception {
        final RecordLayerPropertyStorage.Builder storageBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_AUTO_COMPLETE_TEXT_SIZE_UPPER_LIMIT, DEFAULT_AUTO_COMPLETE_TEXT_SIZE_LIMIT);
        try (FDBRecordContext context = openContext(storageBuilder)) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_WITH_AUTO_COMPLETE);
            });

            assertIndexEntryPrimaryKeys(Collections.emptyList(),
                    recordStore.scanIndex(SIMPLE_TEXT_WITH_AUTO_COMPLETE, autoComplete(SIMPLE_TEXT_WITH_AUTO_COMPLETE, "hello", false), null, ScanProperties.FORWARD_SCAN));
            assertEquals(0, context.getTimer().getCount(LuceneEvents.Counts.LUCENE_SCAN_MATCHED_AUTO_COMPLETE_SUGGESTIONS));
        }
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
                    autoComplete(COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE, "good", false),
                    null,
                    ScanProperties.FORWARD_SCAN).asList().get();

            results.stream().forEach(i -> assertDocumentPartialRecordFromIndexEntry(recordType, i,
                    (String) i.getKey().get(i.getKeySize() - 1),
                    (String) i.getKey().get(i.getKeySize() - 2), LuceneScanTypes.BY_LUCENE_AUTO_COMPLETE));

            assertEquals(6, context.getTimer().getCounter(LuceneEvents.Counts.LUCENE_SCAN_MATCHED_AUTO_COMPLETE_SUGGESTIONS).getCount());
            assertAutoCompleteEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE),
                    context, "_0.cfs", true);

            // Assert the count of suggestions
            assertEquals(6, results.size());

            // Assert the suggestions' keys
            List<String> suggestions = results.stream().map(i -> (String) i.getKey().get(i.getKeySize() - 1)).collect(Collectors.toList());
            assertThat(suggestions, containsInAnyOrder("good evening", "Good night", "Good morning", "Good afternoon", "I'm good", "That's really good!"));

            // Assert the corresponding field for the suggestions
            List<String> fields = results.stream().map(i -> (String) i.getKey().get(i.getKeySize() - 2)).collect(Collectors.toList());
            assertEquals(ImmutableList.of("text", "text", "text", "text", "text2", "text2"), fields);

            List<Tuple> primaryKeys = results.stream().map(IndexEntry::getPrimaryKey).collect(Collectors.toList());
            assertEquals(ImmutableList.of(Tuple.from(1L, 1623L), Tuple.from(1L, 1624L), Tuple.from(1L, 1625L), Tuple.from(1L, 1626L), Tuple.from(1L, 1627L), Tuple.from(1L, 1628L)), primaryKeys);

            commit(context);
        }
    }

    @Test
    void searchForAutoCompleteWithContinueTyping() throws Exception {
        try (FDBRecordContext context = openContext()) {
            addIndexAndSaveRecordForAutoComplete(context);
            List<IndexEntry> results = recordStore.scanIndex(SIMPLE_TEXT_WITH_AUTO_COMPLETE,
                    autoComplete(SIMPLE_TEXT_WITH_AUTO_COMPLETE, "good mor", false),
                    null,
                    ScanProperties.FORWARD_SCAN).asList().get();

            // Assert only 1 suggestion returned
            assertEquals(1, results.size());

            // Assert the suggestion's key and field
            IndexEntry result = results.get(0);
            assertEquals("Good morning", result.getKey().get(result.getKeySize() - 1));
            assertEquals("text", result.getKey().get(result.getKeySize() - 2));

            assertEquals(1623L, result.getPrimaryKey().get(0));

            assertEquals(1, context.getTimer().getCounter(LuceneEvents.Counts.LUCENE_SCAN_MATCHED_AUTO_COMPLETE_SUGGESTIONS).getCount());
            assertAutoCompleteEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(SIMPLE_TEXT_WITH_AUTO_COMPLETE),
                    context, "_0.cfs", true);

            commit(context);
        }
    }

    @Test
    void searchForAutoCompleteForGroupedRecord() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                TextIndexTestUtils.addRecordTypePrefix(metaDataBuilder);
                metaDataBuilder.addIndex(MAP_DOC, MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE);
            });
            FDBStoredRecord<Message> fdbRecord = recordStore.saveRecord(createMultiEntryMapDoc(1623L, ENGINEER_JOKE, "sampleTextPhrase", WAYLON, "sampleTextSong", 2));
            List<IndexEntry> indexEntries = recordStore.scanIndex(MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE,
                    groupedAutoComplete(MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE, "Vision", "sampleTextPhrase", false),
                    null,
                    ScanProperties.FORWARD_SCAN).asList().get();

            assertEquals(1, indexEntries.size());
            IndexEntry indexEntry = indexEntries.get(0);

            Descriptors.Descriptor recordDescriptor = TestRecordsTextProto.MapDocument.getDescriptor();
            IndexKeyValueToPartialRecord toPartialRecord = LuceneIndexQueryPlan.getToPartialRecord(
                    MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE, fdbRecord.getRecordType(), LuceneScanTypes.BY_LUCENE_AUTO_COMPLETE);
            Message message = toPartialRecord.toRecord(recordDescriptor, indexEntry);

            Descriptors.FieldDescriptor docIdDescriptor = recordDescriptor.findFieldByName("doc_id");
            assertEquals(1623L, message.getField(docIdDescriptor));

            Descriptors.FieldDescriptor entryDescriptor = recordDescriptor.findFieldByName("entry");
            Message entry = (Message)message.getRepeatedField(entryDescriptor, 0);

            Descriptors.FieldDescriptor keyDescriptor = entryDescriptor.getMessageType().findFieldByName("key");
            Descriptors.FieldDescriptor valueDescriptor = entryDescriptor.getMessageType().findFieldByName("value");

            assertEquals("sampleTextPhrase", entry.getField(keyDescriptor));
            assertEquals(ENGINEER_JOKE, entry.getField(valueDescriptor));

            assertEquals(1, context.getTimer().getCounter(LuceneEvents.Counts.LUCENE_SCAN_MATCHED_AUTO_COMPLETE_SUGGESTIONS).getCount());
            assertAutoCompleteEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE).subspace(Tuple.from("sampleTextPhrase")),
                    context, "_0.cfs", true);

            commit(context);
        }
    }

    @Test
    void testAutoCompleteSearchForPhrase() throws Exception {
        try (FDBRecordContext context = openContext()) {
            final Index index = SIMPLE_TEXT_WITH_AUTO_COMPLETE;

            addIndexAndSaveRecordsForAutoCompleteOfPhrase(context, index);

            // All records are matches because they all contain both "united" and "states"
            queryAndAssertAutoCompleteSuggestionsReturned(index, "united states",
                    ImmutableList.of("united states of america",
                            "united states is a country in the continent of america",
                            "united kingdom, france, the states",
                            "states united as a country",
                            "states have been united as a country",
                            "all the states united as a country",
                            "all the states have been united as a country",
                            "welcome to the united states of america",
                            "The countries are united kingdom, france, the states"),
                    false);

            // Only the texts containing "united states" are returned, the last token "states" is queried with term query,
            // same as the other tokens due to the white space following it
            queryAndAssertAutoCompleteSuggestionsReturned(index, "\"united states \"",
                    ImmutableList.of("united states of america",
                            "united states is a country in the continent of america",
                            "welcome to the united states of america"),
                    false);

            // Only the texts containing "united states" are returned, the last token "states" is queried with prefix query
            queryAndAssertAutoCompleteSuggestionsReturned(index, "\"united states\"",
                    ImmutableList.of("united states of america",
                            "united states is a country in the continent of america",
                            "welcome to the united states of america"),
                    false);

            // Only the texts containing "united state" are returned, the last token "state" is queried with prefix query
            queryAndAssertAutoCompleteSuggestionsReturned(index, "\"united state\"",
                    ImmutableList.of("united states of america",
                            "united states is a country in the continent of america",
                            "welcome to the united states of america"),
                    false);

            // Only the texts containing "united states of" are returned, the last token "of" is queried with term query,
            // same as the other tokens due to the white space following it
            queryAndAssertAutoCompleteSuggestionsReturned(index, "\"united states of \"",
                    ImmutableList.of("united states of america",
                            "welcome to the united states of america"),
                    false);

            // Only the texts containing "united states of" are returned, the last token "of" is queried with term query against the NGRAM field
            queryAndAssertAutoCompleteSuggestionsReturned(index, "\"united states of\"",
                    ImmutableList.of("united states of america",
                            "welcome to the united states of america"),
                    false);

            // Only the texts containing "united states o" are returned, the last token "o" is queried with term query against the NGRAM field
            queryAndAssertAutoCompleteSuggestionsReturned(index, "\"united states o\"",
                    ImmutableList.of("united states of america",
                            "welcome to the united states of america"),
                    false);

            commit(context);
        }
    }

    @Test
    void testAutoCompleteSearchWithHighlightForPhrase() throws Exception {
        try (FDBRecordContext context = openContext()) {
            final Index index = SIMPLE_TEXT_WITH_AUTO_COMPLETE;

            addIndexAndSaveRecordsForAutoCompleteOfPhrase(context, index);

            // All records are matches because they all contain both "united" and "states"
            queryAndAssertAutoCompleteSuggestionsReturned(index, "united states",
                    ImmutableList.of("<b>united</b> <b>states</b> of america",
                            "<b>united</b> <b>states</b> is a country in the continent of america",
                            "<b>united</b> kingdom, france, the <b>states</b>",
                            "<b>states</b> <b>united</b> as a country",
                            "<b>states</b> have been <b>united</b> as a country",
                            "all the <b>states</b> <b>united</b> as a country",
                            "all the <b>states</b> have been <b>united</b> as a country",
                            "welcome to the <b>united</b> <b>states</b> of america",
                            "The countries are <b>united</b> kingdom, france, the <b>states</b>"),
                    true);

            // Only the texts containing "united states" are returned, the last token "states" is queried with term query,
            // same as the other tokens due to the white space following it
            queryAndAssertAutoCompleteSuggestionsReturned(index, "\"united states \"",
                    ImmutableList.of("<b>united</b> <b>states</b> of america",
                            "<b>united</b> <b>states</b> is a country in the continent of america",
                            "welcome to the <b>united</b> <b>states</b> of america"),
                    true);

            // Only the texts containing "united states" are returned, the last token "states" is queried with prefix query
            queryAndAssertAutoCompleteSuggestionsReturned(index, "\"united states\"",
                    ImmutableList.of("<b>united</b> <b>states</b> of america",
                            "<b>united</b> <b>states</b> is a country in the continent of america",
                            "welcome to the <b>united</b> <b>states</b> of america"),
                    true);

            // Only the texts containing "united state" are returned, the last token "state" is queried with prefix query
            queryAndAssertAutoCompleteSuggestionsReturned(index, "\"united state\"",
                    ImmutableList.of("<b>united</b> <b>state</b>s of america",
                            "<b>united</b> <b>state</b>s is a country in the continent of america",
                            "welcome to the <b>united</b> <b>state</b>s of america"),
                    true);

            // Only the texts containing "united states of" are returned, the last token "of" is queried with term query,
            // same as the other tokens due to the white space following it
            queryAndAssertAutoCompleteSuggestionsReturned(index, "\"united states of \"",
                    ImmutableList.of("<b>united</b> <b>states</b> <b>of</b> america",
                            "welcome to the <b>united</b> <b>states</b> <b>of</b> america"),
                    true);

            // Only the texts containing "united states of" are returned, the last token "of" is queried with term query against the NGRAM field
            queryAndAssertAutoCompleteSuggestionsReturned(index, "\"united states of\"",
                    ImmutableList.of("<b>united</b> <b>states</b> <b>of</b> america",
                            "welcome to the <b>united</b> <b>states</b> <b>of</b> america"),
                    true);

            // Only the texts containing "united states o" are returned, the last token "o" is queried with term query against the NGRAM field
            queryAndAssertAutoCompleteSuggestionsReturned(index, "\"united states o\"",
                    ImmutableList.of("<b>united</b> <b>states</b> <b>o</b>f america",
                            "welcome to the <b>united</b> <b>states</b> <b>o</b>f america"),
                    true);

            commit(context);
        }
    }

    @Test
    void testAutoCompleteSearchMultipleResultsSingleDocument() {
        try (FDBRecordContext context = openContext()) {
            final Index index = COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE;
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(COMPLEX_DOC, index);
            });

            TestRecordsTextProto.ComplexDocument doc = TestRecordsTextProto.ComplexDocument.newBuilder()
                    .setDocId(1597L)
                    // Romeo and Juliet, Act II, Scene II.
                    .setText("Good night! Good night! Parting is such sweet sorrow")
                    .setText2("That I shall say good night till it be morrow")
                    .build();
            recordStore.saveRecord(doc);

            List<IndexEntry> entries = recordStore.scanIndex(index, autoComplete(index, "good night ", false), null, ScanProperties.FORWARD_SCAN)
                    .asList()
                    .join();
            assertThat(entries, hasSize(2));

            // 2 matches are from the same record
            assertEquals(List.of(Tuple.from(null, 1597L), Tuple.from(null, 1597L)),
                    entries.stream().map(IndexEntry::getPrimaryKey).collect(Collectors.toList()));

            List<Tuple> fieldAndText = entries.stream()
                    .map(entry -> TupleHelpers.subTuple(entry.getKey(), 0, 2))
                    .collect(Collectors.toList());
            assertThat(fieldAndText, containsInAnyOrder(Tuple.from("text", doc.getText()), Tuple.from("text2", doc.getText2())));

            commit(context);
        }
    }

    @Test
    void testAutoCompleteSearchForPhraseWithoutFreqsAndPositions() {
        try (FDBRecordContext context = openContext()) {
            final Index index = SIMPLE_TEXT_WITH_AUTO_COMPLETE_NO_FREQS_POSITIONS;

            addIndexAndSaveRecordsForAutoCompleteOfPhrase(context, index);

            // Phrase search is not supported if positions are not indexed
            assertThrows(ExecutionException.class,
                    () -> queryAndAssertAutoCompleteSuggestionsReturned(index, "\"united states \"",
                            ImmutableList.of(), false));

            commit(context);
        }
    }

    @Test
    void searchForSpellCheck() throws Exception {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SPELLCHECK_INDEX);
            long docId = 1623L;
            for (String word : List.of("hello", "monitor", "keyboard", "mouse", "trackpad", "cable", "help", "elmo", "elbow", "helps", "helm", "helms", "gulps")) {
                recordStore.saveRecord(createSimpleDocument(docId++, word, 1));
            }

            CompletableFuture<List<IndexEntry>> resultsI = recordStore.scanIndex(SPELLCHECK_INDEX,
                    spellCheck(SPELLCHECK_INDEX, "keyboad"),
                    null,
                    ScanProperties.FORWARD_SCAN).asList();
            List<IndexEntry> results = resultsI.get();

            assertEquals(1, results.size());
            IndexEntry result = results.get(0);
            assertEquals("keyboard", result.getKey().getString(1));
            assertEquals("text", result.getKey().getString(0));
            assertEquals(0.85714287F, result.getValue().get(0));

            List<IndexEntry> results2 = recordStore.scanIndex(SPELLCHECK_INDEX,
                    spellCheck(SPELLCHECK_INDEX, "text:keyboad"),
                    null,
                    ScanProperties.FORWARD_SCAN).asList().get();
            assertEquals(1, results.size());
            IndexEntry result2 = results.get(0);
            assertEquals("keyboard", result.getKey().get(1));
            assertEquals("text", result.getKey().get(0));
            assertEquals(0.85714287F, result.getValue().get(0));

            commit(context);
        }
    }

    @Test
    void searchForSpellcheckForGroupedRecord() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(MAP_DOC, MAP_ON_VALUE_INDEX);
            });
            FDBStoredRecord<Message> fdbRecord = recordStore.saveRecord(createMultiEntryMapDoc(1623L, ENGINEER_JOKE, "sampleTextPhrase", WAYLON, "sampleTextSong", 2));
            List<IndexEntry> indexEntries = recordStore.scanIndex(MAP_ON_VALUE_INDEX,
                    groupedSpellCheck(MAP_ON_VALUE_INDEX, "Visin", "sampleTextPhrase"),
                    null,
                    ScanProperties.FORWARD_SCAN).asList().get();

            assertEquals(1, indexEntries.size());
            IndexEntry indexEntry = indexEntries.get(0);
            assertEquals(0.8F, indexEntry.getValue().get(0));

            Descriptors.Descriptor recordDescriptor = TestRecordsTextProto.MapDocument.getDescriptor();
            IndexKeyValueToPartialRecord toPartialRecord = LuceneIndexQueryPlan.getToPartialRecord(
                    MAP_ON_VALUE_INDEX, fdbRecord.getRecordType(), LuceneScanTypes.BY_LUCENE_SPELL_CHECK);
            Message message = toPartialRecord.toRecord(recordDescriptor, indexEntry);

            Descriptors.FieldDescriptor entryDescriptor = recordDescriptor.findFieldByName("entry");
            Message entry = (Message) message.getRepeatedField(entryDescriptor, 0);

            Descriptors.FieldDescriptor keyDescriptor = entryDescriptor.getMessageType().findFieldByName("key");
            Descriptors.FieldDescriptor valueDescriptor = entryDescriptor.getMessageType().findFieldByName("value");

            //TODO: This seems like the wrong field string to return. I'm not sure what to do here
            assertEquals("sampleTextPhrase", entry.getField(keyDescriptor));
            assertEquals("vision", entry.getField(valueDescriptor));

            // assertEquals(1, context.getTimer().getCounter(LuceneEvents.Counts.LUCENE_SCAN_MATCHED_AUTO_COMPLETE_SUGGESTIONS).getCount());
            // assertAutoCompleteEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(MAP_ON_VALUE_INDEX_WITH_void).subspace(Tuple.from("sampleTextPhrase")), context, "_0.cfs", true);

            commit(context);
        }
    }

    private void spellCheckHelper(final Index index, @Nonnull String query, List<Pair<String, String>> expectedSuggestions) throws ExecutionException, InterruptedException {
        List<IndexEntry> suggestions = recordStore.scanIndex(index,
                spellCheck(index, query),
                null,
                ScanProperties.FORWARD_SCAN).asList().get();

        assertEquals(expectedSuggestions.size(), suggestions.size());
        for (int i = 0 ; i < expectedSuggestions.size(); ++i) {
            assertThat(suggestions.get(i).getKey().get(1), equalTo(expectedSuggestions.get(i).getKey()));
            assertThat(suggestions.get(i).getKey().get(0), equalTo(expectedSuggestions.get(i).getValue()));
        }
    }

    @Test
    void spellCheckMultipleMatches() throws Exception {
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

            final List<Pair<String, String>> emptyList = List.of();
            assertThrows(RecordCoreException.class,
                    () -> spellCheckHelper(SPELLCHECK_INDEX, "wrongField:helo", emptyList),
                    "Invalid field name in Lucene index query");
        }
    }

    @Test
    void spellCheckComplexDocument() throws Exception {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, SPELLCHECK_INDEX_COMPLEX);
            long docId = 1623L;
            List<String> text = List.of("beaver", "leopard", "hello", "help", "helm", "boat", "road", "fowl", "foot", "tare", "tire");
            List<String> text2 = List.of("beavers", "lizards", "hell", "helps", "helms", "boot", "read", "fowl", "fool", "tire", "tire");
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
            // Same, but this time, getRight() should be text2 because tire was more frequent in text2 than text
            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "tbre", List.of(
                    Pair.of("tire", "text2"),
                    Pair.of("tare", "text")));
        }
    }

    @Test
    void testDeleteWhereSimple() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                TextIndexTestUtils.addRecordTypePrefix(metaDataBuilder);
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
                metaDataBuilder.getRecordType(SIMPLE_DOC)
                        .setPrimaryKey(concat(recordType(), field("text")));
            });

            TestRecordsTextProto.SimpleDocument simple = TestRecordsTextProto.SimpleDocument.newBuilder()
                    .setDocId(1066L)
                    .setText("foo bar")
                    .build();
            recordStore.saveRecord(simple);
            Query.InvalidExpressionException err = assertThrows(Query.InvalidExpressionException.class,
                    () -> recordStore.deleteRecordsWhere(SIMPLE_DOC, Query.field("text").equalsValue("foo bar")));
            assertThat(err.getMessage(), containsString(String.format("deleteRecordsWhere not supported by index %s", SIMPLE_TEXT_SUFFIXES.getName())));

            FDBStoredRecord<Message> storedRecord = recordStore.loadRecord(Tuple.from(recordStore.getRecordMetaData().getRecordType(SIMPLE_DOC).getRecordTypeKey(), "foo bar"));
            assertNotNull(storedRecord);
            assertEquals(simple, TestRecordsTextProto.SimpleDocument.newBuilder().mergeFrom(storedRecord.getRecord()).build());

            commit(context);
        }
    }

    @Test
    void testDeleteWhereComplexGrouped() {
        final RecordMetaDataHook hook = metaDataBuilder -> {
            TextIndexTestUtils.addRecordTypePrefix(metaDataBuilder);
            metaDataBuilder.addIndex(COMPLEX_DOC, COMPLEX_MULTIPLE_GROUPED);
        };

        final TestRecordsTextProto.ComplexDocument zeroGroupDoc = TestRecordsTextProto.ComplexDocument.newBuilder()
                .setGroup(0)
                .setDocId(1623L)
                .setText(TextSamples.ROMEO_AND_JULIET_PROLOGUE)
                .setText2(TextSamples.ANGSTROM)
                .build();
        final TestRecordsTextProto.ComplexDocument oneGroupDoc = TestRecordsTextProto.ComplexDocument.newBuilder()
                .setGroup(1)
                .setDocId(1623L)
                .setText(TextSamples.ROMEO_AND_JULIET_PROLOGUE)
                .setText2(TextSamples.ANGSTROM)
                .build();

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            recordStore.saveRecord(zeroGroupDoc);
            recordStore.saveRecord(oneGroupDoc);
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);

            RecordQuery recordQuery = RecordQuery.newBuilder()
                    .setRecordType(COMPLEX_DOC)
                    .setFilter(Query.and(
                            Query.field("group").equalsParameter("group_value"),
                            new LuceneQueryComponent("text:\"continuance\" AND text2:\"named\"", List.of("text", "text2"))
                    ))
                    .build();
            LucenePlanner planner = new LucenePlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState(), PlannableIndexTypes.DEFAULT, recordStore.getTimer());
            RecordQueryPlan plan = planner.plan(recordQuery);
            assertThat(plan, indexScan(allOf(
                    indexName(COMPLEX_MULTIPLE_GROUPED.getName()),
                    scanParams(allOf(
                            group(hasTupleString("[EQUALS $group_value]")),
                            query(hasToString("text:\"continuance\" AND text2:\"named\"")))))));

            assertEquals(Collections.singletonList(zeroGroupDoc),
                    plan.execute(recordStore, EvaluationContext.forBinding("group_value", zeroGroupDoc.getGroup()))
                            .map(FDBQueriedRecord::getRecord)
                            .map(rec -> TestRecordsTextProto.ComplexDocument.newBuilder().mergeFrom(rec).build())
                            .asList()
                            .join());
            assertEquals(Collections.singletonList(oneGroupDoc),
                    plan.execute(recordStore, EvaluationContext.forBinding("group_value", oneGroupDoc.getGroup()))
                            .map(FDBQueriedRecord::getRecord)
                            .map(rec -> TestRecordsTextProto.ComplexDocument.newBuilder().mergeFrom(rec).build())
                            .asList()
                            .join());

            // Issue a delete where to delete the zero group
            recordStore.deleteRecordsWhere(COMPLEX_DOC, Query.field("group").equalsValue(zeroGroupDoc.getGroup()));

            assertEquals(Collections.emptyList(),
                    plan.execute(recordStore, EvaluationContext.forBinding("group_value", zeroGroupDoc.getGroup()))
                            .map(FDBQueriedRecord::getRecord)
                            .map(rec -> TestRecordsTextProto.ComplexDocument.newBuilder().mergeFrom(rec).build())
                            .asList()
                            .join());
            assertEquals(Collections.singletonList(oneGroupDoc),
                    plan.execute(recordStore, EvaluationContext.forBinding("group_value", oneGroupDoc.getGroup()))
                            .map(FDBQueriedRecord::getRecord)
                            .map(rec -> TestRecordsTextProto.ComplexDocument.newBuilder().mergeFrom(rec).build())
                            .asList()
                            .join());
        }
    }

    @Test
    void testDeleteWhereAutoComplete() {
        final RecordMetaDataHook hook = metaDataBuilder -> {
            TextIndexTestUtils.addRecordTypePrefix(metaDataBuilder);
            metaDataBuilder.addIndex(COMPLEX_DOC, COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE);
        };
        final int maxGroup = 10;
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            for (int group = 0; group < maxGroup; group++) {
                for (long docId = 0L; docId < 10L; docId++) {
                    TestRecordsTextProto.ComplexDocument doc = TestRecordsTextProto.ComplexDocument.newBuilder()
                            .setGroup(group)
                            .setDocId(docId)
                            .setText(String.format("hello there %d", group))
                            .setText2(TextSamples.TELUGU)
                            .build();
                    recordStore.saveRecord(doc);
                }
            }
            commit(context);
        }
        // Re-initialize the builder so the LUCENE_INDEX_COMPRESSION_ENABLED prop is not added twice
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            for (int group = 0; group < maxGroup; group++) {
                List<IndexEntry> autoCompleted = recordStore.scanIndex(COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE, groupedAutoComplete(COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE, "hello", group, false), null, ScanProperties.FORWARD_SCAN)
                        .asList()
                        .join();
                assertThat(autoCompleted, hasSize(10));
                int docId = 0;
                for (IndexEntry entry : autoCompleted) {
                    Tuple key = entry.getKey();
                    assertEquals(Tuple.from("text", String.format("hello there %d", group)), TupleHelpers.subTuple(key, key.size() - 2, key.size()));
                    Tuple primaryKey = entry.getPrimaryKey();
                    // The 1st element is the key for the record type
                    assertEquals((long) group, primaryKey.get(1));
                    assertEquals((long) docId, primaryKey.get(2));
                    docId++;
                }
            }

            final int groupToDelete = maxGroup / 2;
            recordStore.deleteRecordsWhere(COMPLEX_DOC, Query.field("group").equalsValue(groupToDelete));

            for (int group = 0; group < maxGroup; group++) {
                List<IndexEntry> autoCompleted = recordStore.scanIndex(COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE, groupedAutoComplete(COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE, "hello", group, false), null, ScanProperties.FORWARD_SCAN)
                        .asList()
                        .join();
                if (group == groupToDelete) {
                    assertThat(autoCompleted, empty());
                } else {
                    assertThat(autoCompleted, hasSize(10));
                    int docId = 0;
                    for (IndexEntry entry : autoCompleted) {
                        Tuple key = entry.getKey();
                        assertEquals(Tuple.from("text", String.format("hello there %d", group)), TupleHelpers.subTuple(key, key.size() - 2, key.size()));
                        Tuple primaryKey = entry.getPrimaryKey();
                        // The 1st element is the key for the record type
                        assertEquals((long) group, primaryKey.get(1));
                        assertEquals((long) docId, primaryKey.get(2));
                        docId++;
                    }
                }
            }
        }
    }

    @Test
    void analyzerChooserTest() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, ANALYZER_CHOOSER_TEST_LUCENE_INDEX);

            // Synonym analyzer is chosen due to the keyword "synonym" from the text
            recordStore.saveRecord(createSimpleDocument(1623L, "synonym food", 1));
            assertEquals(1, recordStore.scanIndex(ANALYZER_CHOOSER_TEST_LUCENE_INDEX, fullTextSearch(ANALYZER_CHOOSER_TEST_LUCENE_INDEX, "nutrient"), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            assertEquals(0, recordStore.scanIndex(ANALYZER_CHOOSER_TEST_LUCENE_INDEX, fullTextSearch(ANALYZER_CHOOSER_TEST_LUCENE_INDEX, "foo"), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());

            // Ngram analyzer is chosen due to no keyword "synonym" from the text
            recordStore.saveRecord(createSimpleDocument(1624L, "ngram motivation", 1));
            assertEquals(0, recordStore.scanIndex(ANALYZER_CHOOSER_TEST_LUCENE_INDEX, fullTextSearch(ANALYZER_CHOOSER_TEST_LUCENE_INDEX, "need"), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            assertEquals(1, recordStore.scanIndex(ANALYZER_CHOOSER_TEST_LUCENE_INDEX, fullTextSearch(ANALYZER_CHOOSER_TEST_LUCENE_INDEX, "motivatio"), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
        }
    }

    @Test
    void basicLuceneCursorTest() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            // Save 20 records
            for (int i = 0; i < 20; i++) {
                recordStore.saveRecord(createSimpleDocument(1600L + i, "testing text" + i, 1));
            }
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "\"testing\""), null, ScanProperties.FORWARD_SCAN);

            List<IndexEntry> entries = indexEntries.asList().join();
            assertEquals(20, entries.size());
            assertEquals(20, context.getTimer().getCounter(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
        }
    }

    @Test
    void luceneCursorTestWithMultiplePages() throws Exception {
        // Configure page size as 10
        final RecordLayerPropertyStorage.Builder storageBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_CURSOR_PAGE_SIZE, 10);
        try (FDBRecordContext context = openContext(storageBuilder)) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            // Save 20 records
            for (int i = 0; i < 20; i++) {
                recordStore.saveRecord(createSimpleDocument(1600L + i, "testing text" + i, 1));
            }

            ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .build());
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "\"testing\""), null, scanProperties);

            List<IndexEntry> entries = indexEntries.asList().join();
            assertEquals(20, entries.size());
            assertEquals(20, context.getTimer().getCounter(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, indexEntries.onNext().get().getNoNextReason());
        }
    }

    @Test
    void luceneCursorTestWith3rdPage() throws Exception {
        // Configure page size as 10
        final RecordLayerPropertyStorage.Builder storageBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_CURSOR_PAGE_SIZE, 10);
        try (FDBRecordContext context = openContext(storageBuilder)) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            // Save 21 records
            for (int i = 0; i < 21; i++) {
                recordStore.saveRecord(createSimpleDocument(1600L + i, "testing text" + i, 1));
            }

            ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .build());
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "\"testing\""), null, scanProperties);

            List<IndexEntry> entries = indexEntries.asList().join();
            assertEquals(21, entries.size());
            assertEquals(21, context.getTimer().getCounter(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, indexEntries.onNext().get().getNoNextReason());
        }
    }

    @Test
    void luceneCursorTestWithMultiplePagesWithSkip() throws Exception {
        // Configure page size as 10
        final RecordLayerPropertyStorage.Builder storageBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_CURSOR_PAGE_SIZE, 10);
        try (FDBRecordContext context = openContext(storageBuilder)) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            // Save 31 records
            for (int i = 0; i < 31; i++) {
                recordStore.saveRecord(createSimpleDocument(1600L + i, "testing text" + i, 1));
            }

            ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .setSkip(12)
                    .build());
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "\"testing\""), null, scanProperties);

            List<IndexEntry> entries = indexEntries.asList().join();
            assertEquals(19, entries.size());
            assertEquals(19, context.getTimer().getCounter(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, indexEntries.onNext().get().getNoNextReason());
        }
    }

    @Test
    void luceneCursorTestWithLimit() throws Exception {
        // Configure page size as 10
        final RecordLayerPropertyStorage.Builder storageBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_CURSOR_PAGE_SIZE, 10);
        try (FDBRecordContext context = openContext(storageBuilder)) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            // Save 21 records
            for (int i = 0; i < 21; i++) {
                recordStore.saveRecord(createSimpleDocument(1600L + i, "testing text" + i, 1));
            }

            // Scan with limit = 10
            ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .setReturnedRowLimit(8)
                    .build());
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "\"testing\""), null, scanProperties);

            // Get 8 results and continuation
            List<IndexEntry> entries = indexEntries.asList().join();
            assertEquals(8, entries.size());
            assertEquals(8, context.getTimer().getCounter(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            RecordCursorResult<IndexEntry> lastResult = indexEntries.onNext().get();
            assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, lastResult.getNoNextReason());

            indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "\"testing\""), lastResult.getContinuation().toBytes(), scanProperties);

            // Get 8 results and continuation
            entries = indexEntries.asList().join();
            assertEquals(8, entries.size());
            assertEquals(16, context.getTimer().getCounter(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            lastResult = indexEntries.onNext().get();
            assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, lastResult.getNoNextReason());

            indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "\"testing\""), lastResult.getContinuation().toBytes(), scanProperties);

            // Get 3 results
            entries = indexEntries.asList().join();
            assertEquals(5, entries.size());
            assertEquals(21, context.getTimer().getCounter(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, indexEntries.onNext().get().getNoNextReason());
        }
    }

    @Test
    void luceneCursorTestWithLimitAndSkip() throws Exception {
        // Configure page size as 10
        final RecordLayerPropertyStorage.Builder storageBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_CURSOR_PAGE_SIZE, 10);
        try (FDBRecordContext context = openContext(storageBuilder)) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            // Save 21 records
            for (int i = 0; i < 21; i++) {
                recordStore.saveRecord(createSimpleDocument(1600L + i, "testing text" + i, 1));
            }

            // Scan with limit = 8 and skip = 2
            ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .setReturnedRowLimit(8)
                    .setSkip(2)
                    .build());
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "\"testing\""), null, scanProperties);

            // Get 8 results and continuation
            List<IndexEntry> entries = indexEntries.asList().join();
            assertEquals(8, entries.size());
            assertEquals(8, context.getTimer().getCounter(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            RecordCursorResult<IndexEntry> lastResult = indexEntries.onNext().get();
            assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, lastResult.getNoNextReason());

            // Scan with limit = 8, no skip
            scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .setReturnedRowLimit(8)
                    .build());
            indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "\"testing\""), lastResult.getContinuation().toBytes(), scanProperties);

            // Get 8 results and continuation
            entries = indexEntries.asList().join();
            assertEquals(8, entries.size());
            assertEquals(16, context.getTimer().getCounter(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            lastResult = indexEntries.onNext().get();
            assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, lastResult.getNoNextReason());

            indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "\"testing\""), lastResult.getContinuation().toBytes(), scanProperties);

            // Get 3 results
            entries = indexEntries.asList().join();
            assertEquals(3, entries.size());
            assertEquals(19, context.getTimer().getCounter(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, indexEntries.onNext().get().getNoNextReason());
        }
    }

    @Test
    void luceneCursorTestAllMatchesSkipped() throws Exception {
        // Configure page size as 10
        final RecordLayerPropertyStorage.Builder storageBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_CURSOR_PAGE_SIZE, 10);
        try (FDBRecordContext context = openContext(storageBuilder)) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            // Save 6 records
            for (int i = 0; i < 6; i++) {
                recordStore.saveRecord(createSimpleDocument(1600L + i, "testing text" + i, 1));
            }
            // Scan with skip = 15
            ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .setSkip(15)
                    .build());
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "\"testing\""), null, scanProperties);

            // No matches are found and source is exhausted
            RecordCursorResult<IndexEntry> next = indexEntries.onNext().get();
            assertFalse(next.hasNext());
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, next.getNoNextReason());
            assertNull(context.getTimer().getCounter(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY));
        }
    }

    public static String[] generateRandomWords(int numberOfWords) {
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

    private static void assertAutoCompleteEntriesAndSegmentInfoStoredInCompoundFile(@Nonnull Subspace subspace, @Nonnull FDBRecordContext context, @Nonnull String segment, boolean cleanFiles) {
        assertEntriesAndSegmentInfoStoredInCompoundFile(subspace, context, segment, cleanFiles);
    }

    private static void assertEntriesAndSegmentInfoStoredInCompoundFile(@Nonnull Subspace subspace, @Nonnull FDBRecordContext context, @Nonnull String segment, boolean cleanFiles) {
        try (final FDBDirectory directory = new FDBDirectory(subspace, context)) {
            final FDBLuceneFileReference reference = directory.getFDBLuceneFileReference(segment);
            assertNotNull(reference);
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

    private void searchForAutoCompleteAndAssert(String query, boolean matches, boolean highlight, int textSizeLimit) throws Exception {
        final RecordLayerPropertyStorage.Builder storageBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_AUTO_COMPLETE_TEXT_SIZE_UPPER_LIMIT, textSizeLimit);
        try (FDBRecordContext context = openContext(storageBuilder)) {
            final RecordType recordType = addIndexAndSaveRecordForAutoComplete(context);
            final Index index = SIMPLE_TEXT_WITH_AUTO_COMPLETE;
            List<IndexEntry> results = recordStore.scanIndex(index,
                    autoComplete(index, query, highlight),
                    null,
                    ScanProperties.FORWARD_SCAN).asList().get();

            if (!matches) {
                // Assert no suggestions
                assertTrue(results.isEmpty());
                assertEquals(0, context.getTimer().getCount(LuceneEvents.Counts.LUCENE_SCAN_MATCHED_AUTO_COMPLETE_SUGGESTIONS));
                return;
            }

            // Assert the count of suggestions
            assertEquals(6, results.size());

            // Assert the suggestions' keys
            List<String> suggestions = results.stream().map(i -> i.getKey().getString(i.getKeySize() - 1)).collect(Collectors.toList());
            if (highlight) {
                assertEquals(ImmutableList.of("<b>Good</b> morning", "<b>Good</b> afternoon", "<b>good</b> evening", "<b>Good</b> night", "That's really <b>good</b>!", "I'm <b>good</b>"), suggestions);
            } else {
                assertEquals(ImmutableList.of("Good morning", "Good afternoon", "good evening", "Good night", "That's really good!", "I'm good"), suggestions);
            }

            // Assert the corresponding field for the suggestions
            List<String> fields = results.stream().map(i -> i.getKey().getString(i.getKeySize() - 2)).collect(Collectors.toList());
            assertEquals(ImmutableList.of("text", "text", "text", "text", "text", "text"), fields);

            results.stream().forEach(i -> assertDocumentPartialRecordFromIndexEntry(recordType, i,
                    (String) i.getKey().get(i.getKeySize() - 1),
                    (String) i.getKey().get(i.getKeySize() - 2), LuceneScanTypes.BY_LUCENE_AUTO_COMPLETE));

            assertEquals(6, context.getTimer().getCount(LuceneEvents.Counts.LUCENE_SCAN_MATCHED_AUTO_COMPLETE_SUGGESTIONS));
            assertAutoCompleteEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(SIMPLE_TEXT_WITH_AUTO_COMPLETE),
                    context, "_0.cfs", true);

            commit(context);
        }
    }

    void searchForAutoCompleteWithTextSizeLimit(int limit, boolean matches) throws Exception {
        final RecordLayerPropertyStorage.Builder storageBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_AUTO_COMPLETE_TEXT_SIZE_UPPER_LIMIT, limit);
        try (FDBRecordContext context = openContext(storageBuilder)) {
            final RecordType recordType = addIndexAndSaveRecordForAutoComplete(context);
            List<IndexEntry> results = recordStore.scanIndex(SIMPLE_TEXT_WITH_AUTO_COMPLETE,
                    autoComplete(SIMPLE_TEXT_WITH_AUTO_COMPLETE, "software engineer", false),
                    null,
                    ScanProperties.FORWARD_SCAN).asList().get();

            if (!matches) {
                // Assert no suggestions
                assertTrue(results.isEmpty());
                return;
            }

            // Assert the count of suggestions
            assertEquals(1, results.size());

            // Assert the suggestions' keys
            List<String> suggestions = results.stream().map(i -> (String) i.getKey().get(i.getKeySize() - 1)).collect(Collectors.toList());
            assertEquals(ImmutableList.of(ENGINEER_JOKE), suggestions);

            // Assert the corresponding field for the suggestions
            List<String> fields = results.stream().map(i -> (String) i.getKey().get(i.getKeySize() - 2)).collect(Collectors.toList());
            assertEquals(ImmutableList.of("text"), fields);

            results.stream().forEach(i -> assertDocumentPartialRecordFromIndexEntry(recordType, i,
                    (String) i.getKey().get(i.getKeySize() - 1),
                    (String) i.getKey().get(i.getKeySize() - 2), LuceneScanTypes.BY_LUCENE_AUTO_COMPLETE));

            assertEquals(1, context.getTimer().getCounter(LuceneEvents.Counts.LUCENE_SCAN_MATCHED_AUTO_COMPLETE_SUGGESTIONS).getCount());
            assertAutoCompleteEntriesAndSegmentInfoStoredInCompoundFile(recordStore.indexSubspace(SIMPLE_TEXT_WITH_AUTO_COMPLETE),
                    context, "_0.cfs", true);
        }
    }

    private RecordType addIndexAndSaveRecordForAutoComplete(@Nonnull FDBRecordContext context) {
        openRecordStore(context, metaDataBuilder -> {
            metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
            metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_WITH_AUTO_COMPLETE);
        });

        // Write 8 texts and 6 of them contain the key "good"
        recordStore.saveRecord(createSimpleDocument(1623L, "Good morning", 1));
        recordStore.saveRecord(createSimpleDocument(1624L, "Good afternoon", 1));
        recordStore.saveRecord(createSimpleDocument(1625L, "good evening", 1));
        recordStore.saveRecord(createSimpleDocument(1626L, "Good night", 1));
        recordStore.saveRecord(createSimpleDocument(1627L, "That's really good!", 1));
        recordStore.saveRecord(createSimpleDocument(1628L, "I'm good", 1));
        recordStore.saveRecord(createSimpleDocument(1629L, "Hello Record Layer", 1));
        recordStore.saveRecord(createSimpleDocument(1630L, "Hello FoundationDB!", 1));
        return recordStore.saveRecord(createSimpleDocument(1631L, ENGINEER_JOKE, 1)).getRecordType();
    }

    private void addIndexAndSaveRecordsForAutoCompleteOfPhrase(@Nonnull FDBRecordContext context, @Nonnull Index index) {
        openRecordStore(context, metaDataBuilder -> {
            metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
            metaDataBuilder.addIndex(SIMPLE_DOC, index);
        });

        recordStore.saveRecord(createSimpleDocument(1623L, "united states of america", 1));
        recordStore.saveRecord(createSimpleDocument(1624L, "welcome to the united states of america", 1));
        recordStore.saveRecord(createSimpleDocument(1625L, "united kingdom, france, the states", 1));
        recordStore.saveRecord(createSimpleDocument(1626L, "The countries are united kingdom, france, the states", 1));
        recordStore.saveRecord(createSimpleDocument(1627L, "states united as a country", 1));
        recordStore.saveRecord(createSimpleDocument(1628L, "all the states united as a country", 1));
        recordStore.saveRecord(createSimpleDocument(1629L, "states have been united as a country", 1));
        recordStore.saveRecord(createSimpleDocument(1630L, "all the states have been united as a country", 1));
        recordStore.saveRecord(createSimpleDocument(1631L, "united states is a country in the continent of america", 1));
    }

    private void queryAndAssertAutoCompleteSuggestionsReturned(@Nonnull Index index, @Nonnull String searchKey, @Nonnull List<String> expectedSuggestions, boolean highlight) throws Exception {
        List<IndexEntry> results = recordStore.scanIndex(index,
                autoComplete(index, searchKey, highlight),
                null,
                ScanProperties.FORWARD_SCAN).asList().get();

        assertEquals(expectedSuggestions.size(), results.size());
        List<String> suggestions = results.stream().map(i -> i.getKey().getString(i.getKeySize() - 1)).collect(Collectors.toList());
        assertThat(suggestions, containsInAnyOrder(expectedSuggestions.stream().map(Matchers::equalTo).collect(Collectors.toList())));
    }

    private void assertDocumentPartialRecordFromIndexEntry(@Nonnull RecordType recordType, @Nonnull IndexEntry indexEntry,
                                                           @Nonnull String expectedSuggestion, @Nonnull String fieldName,
                                                           @Nonnull IndexScanType scanType) {
        Descriptors.Descriptor recordDescriptor = recordType.getDescriptor();

        Message message = LuceneIndexQueryPlan.getToPartialRecord(indexEntry.getIndex(), recordType, scanType).toRecord(recordDescriptor, indexEntry);
        Descriptors.FieldDescriptor textDescriptor = recordDescriptor.findFieldByName(fieldName);
        assertEquals(expectedSuggestion, message.getField(textDescriptor));
    }

    private void rebuildIndexMetaData(final FDBRecordContext context, final String document, final Index index) {
        openRecordStore(context, metaDataBuilder -> {
            metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
            metaDataBuilder.addIndex(document, index);
        });
    }

    private void assertIndexEntryPrimaryKeys(List<Long> primaryKeys, RecordCursor<IndexEntry> cursor) {
        List<IndexEntry> indexEntries = cursor.asList().join();
        assertEquals(primaryKeys.stream().map(Tuple::from).collect(Collectors.toList()),
                indexEntries.stream().map(IndexEntry::getPrimaryKey).collect(Collectors.toList()));
    }

    private void assertIndexEntryPrimaryKeyTuples(List<Tuple> primaryKeys, RecordCursor<IndexEntry> cursor) {
        List<IndexEntry> indexEntries = cursor.asList().join();
        assertEquals(primaryKeys,
                indexEntries.stream().map(IndexEntry::getPrimaryKey).collect(Collectors.toList()));
    }

    /**
     * A testing analyzer factory to verify the logic for {@link AnalyzerChooser}.
     */
    @AutoService(LuceneAnalyzerFactory.class)
    public static class TestAnalyzerFactory implements LuceneAnalyzerFactory {
        private static final String ANALYZER_FACTORY_NAME = "TEST_ANALYZER";

        @Override
        @Nonnull
        public String getName() {
            return ANALYZER_FACTORY_NAME;
        }

        @Override
        @Nonnull
        public LuceneAnalyzerType getType() {
            return LuceneAnalyzerType.FULL_TEXT;
        }

        @Override
        @Nonnull
        public AnalyzerChooser getIndexAnalyzerChooser(@Nonnull Index index) {
            return new TestAnalyzerChooser();
        }
    }

    private static class TestAnalyzerChooser implements AnalyzerChooser {
        @Override
        @Nonnull
        public LuceneAnalyzerWrapper chooseAnalyzer(@Nonnull List<String> texts) {
            if (texts.stream().filter(t -> t.contains("synonym")).findAny().isPresent()) {
                return new LuceneAnalyzerWrapper("TEST_SYNONYM",
                        new SynonymAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET, EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig.CONFIG_NAME));
            } else {
                return new LuceneAnalyzerWrapper("TEST_NGRAM",
                        new NgramAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET, 3, 30, false));
            }
        }
    }
}
