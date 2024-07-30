/*
 * LuceneIndexTest.java
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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.TestRecordsTextProto.ComplexDocument;
import com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.IndexedType;
import com.apple.foundationdb.record.lucene.codec.LuceneOptimizedPostingsFormat;
import com.apple.foundationdb.record.lucene.directory.AgilityContext;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.lucene.directory.FDBLuceneFileReference;
import com.apple.foundationdb.record.lucene.ngram.NgramAnalyzer;
import com.apple.foundationdb.record.lucene.synonym.EnglishSynonymMapConfig;
import com.apple.foundationdb.record.lucene.synonym.SynonymAnalyzer;
import com.apple.foundationdb.record.lucene.synonym.SynonymMapConfig;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.JoinedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.common.text.AllSuffixesTextTokenizer;
import com.apple.foundationdb.record.provider.common.text.TextSamples;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Comparisons.Type;
import com.apple.foundationdb.record.query.expressions.Field;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.RandomizedTestUtils;
import com.apple.test.Tags;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.Comparators;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Lock;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.lucene.LuceneIndexOptions.INDEX_PARTITION_BY_FIELD_NAME;
import static com.apple.foundationdb.record.lucene.LuceneIndexOptions.INDEX_PARTITION_HIGH_WATERMARK;
import static com.apple.foundationdb.record.lucene.LuceneIndexOptions.INDEX_PARTITION_LOW_WATERMARK;
import static com.apple.foundationdb.record.lucene.LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.ANALYZER_CHOOSER_TEST_LUCENE_INDEX_KEY;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX_KEY;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.AUTO_COMPLETE_SIMPLE_LUCENE_INDEX_KEY;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.COMBINED_SYNONYM_SETS;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.COMPLEX_GROUPED_WITH_PRIMARY_KEY_SEGMENT_INDEX_KEY;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.COMPLEX_MULTIPLE_GROUPED_KEY;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.COMPLEX_MULTIPLE_TEXT_INDEXES_KEY;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE_KEY;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE_STORED_FIELDS;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE_KEY;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE_STORED_FIELDS;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.EMAIL_CJK_SYM_TEXT_WITH_AUTO_COMPLETE_KEY;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.JOINED_COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE_STORED_FIELDS;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.JOINED_MAP_ON_VALUE_INDEX_STORED_FIELDS;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.JOINED_SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.MANY_FIELDS_INDEX_KEY;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.MAP_ON_VALUE_INDEX_KEY;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.MAP_ON_VALUE_INDEX_STORED_FIELDS;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE_EXCLUDED_FIELDS_KEY;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE_KEY;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.NGRAM_LUCENE_INDEX;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.QUERY_ONLY_SYNONYM_LUCENE_COMBINED_SETS_INDEX_KEY;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.QUERY_ONLY_SYNONYM_LUCENE_INDEX_KEY;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.SIMPLE_TEXT_SUFFIXES;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.SIMPLE_TEXT_SUFFIXES_KEY;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.SIMPLE_TEXT_SUFFIXES_WITH_PRIMARY_KEY_SEGMENT_INDEX_KEY;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.SIMPLE_TEXT_WITH_AUTO_COMPLETE_KEY;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.SIMPLE_TEXT_WITH_AUTO_COMPLETE_NO_FREQS_POSITIONS_KEY;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.SPELLCHECK_INDEX_COMPLEX_KEY;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.SPELLCHECK_INDEX_KEY;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.TEXT_AND_BOOLEAN_INDEX_KEY;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.TEXT_AND_NUMBER_INDEX;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.TEXT_AND_NUMBER_INDEX_KEY;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.createComplexDocument;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.createComplexMapDocument;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.createSimpleDocument;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.keys;
import static com.apple.foundationdb.record.lucene.LucenePartitioner.PARTITION_META_SUBSPACE;
import static com.apple.foundationdb.record.lucene.LucenePlanMatchers.group;
import static com.apple.foundationdb.record.lucene.LucenePlanMatchers.query;
import static com.apple.foundationdb.record.lucene.LucenePlanMatchers.scanParams;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.metadata.Key.Expressions.recordType;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.COMPLEX_DOC;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.MANY_FIELDS_DOC;
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
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@code LUCENE} type indexes.
 */
@SuppressWarnings({"resource", "SameParameterValue"})
@Tag(Tags.RequiresFDB)
public class LuceneIndexTest extends FDBRecordStoreTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneIndexTest.class);
    private static final String LUCENE_INDEX_MAP_PARAMS = "com.apple.foundationdb.record.lucene.LuceneIndexTestUtils#luceneIndexMapParams";

    private Tuple createComplexRecordJoinedToSimple(int group, long docIdSimple, long docIdComplex, String text, String text2, boolean isSeen, long timestamp, Integer score) {
        return createComplexRecordJoinedToSimple(group, docIdSimple, docIdComplex, text, text2, isSeen, timestamp, score, null);
    }

    private Tuple createComplexRecordJoinedToSimple(int group, long docIdSimple, long docIdComplex, String text, String text2, boolean isSeen, long timestamp, Integer score, FDBRecordStoreBase.RecordExistenceCheck existenceCheck) {
        TestRecordsTextProto.SimpleDocument simpleDocument = text != null ? createSimpleDocument(docIdSimple, text, group) : createSimpleDocument(docIdSimple, group);
        ComplexDocument complexDocument = createComplexDocument(docIdComplex, "", text2, group, score, isSeen, timestamp);
        Tuple syntheticRecordTypeKey = recordStore.getRecordMetaData()
                .getSyntheticRecordType("luceneSyntheticComplexJoinedToSimple")
                .getRecordTypeKeyTuple();
        if (existenceCheck != null) {
            Tuple tp = recordStore.saveRecord(simpleDocument, existenceCheck).getPrimaryKey();
            return Tuple.from(syntheticRecordTypeKey.getItems().get(0),
                    recordStore.saveRecord(complexDocument).getPrimaryKey().getItems(),
                    tp.getItems());
        } else {
            return Tuple.from(syntheticRecordTypeKey.getItems().get(0),
                    recordStore.saveRecord(complexDocument).getPrimaryKey().getItems(),
                    recordStore.saveRecord(simpleDocument).getPrimaryKey().getItems());
        }
    }

    private Tuple createComplexRecordJoinedToManyFields(int group, long docIdMany, long docIdComplex, String text, String text2, boolean isSeen, long timestamp, int score) {
        TestRecordsTextProto.ManyFieldsDocument manyFieldsDocument = TestRecordsTextProto.ManyFieldsDocument.newBuilder()
                .setDocId(docIdMany)
                .setText0(text)
                .setText3(text)
                .setText1(text2)
                .setText4(text2)
                .setBool0(isSeen)
                .setLong0(group)
                .build();
        ComplexDocument complexDocument = createComplexDocument(docIdComplex, "", "", group, score, isSeen, timestamp);
        Tuple syntheticRecordTypeKey = recordStore.getRecordMetaData()
                .getSyntheticRecordType("luceneSyntheticComplexJoinedToManyFields")
                .getRecordTypeKeyTuple();
        return Tuple.from(syntheticRecordTypeKey.getItems().get(0),
                recordStore.saveRecord(complexDocument).getPrimaryKey().getItems(),
                recordStore.saveRecord(manyFieldsDocument).getPrimaryKey().getItems());
    }

    private Tuple createComplexRecordJoinedToMap(int group, long docIdMap, long docIdComplex, String text, String text2, String text3, String text4,
                                                 boolean isSeen, long timestamp, int score) {
        TestRecordsTextProto.MapDocument mapDocument = createComplexMapDocument(docIdMap, text, text2, group);
        ComplexDocument complexDocument = createComplexDocument(docIdComplex, "", "", group, score, isSeen, timestamp);
        Tuple syntheticRecordTypeKey = recordStore.getRecordMetaData()
                .getSyntheticRecordType("luceneSyntheticComplexJoinedToMap")
                .getRecordTypeKeyTuple();
        return Tuple.from(syntheticRecordTypeKey.getItems().get(0),
                recordStore.saveRecord(complexDocument).getPrimaryKey().getItems(),
                recordStore.saveRecord(mapDocument).getPrimaryKey().getItems());
    }

    private void metaDataHookSyntheticRecordComplexJoinedToSimple(RecordMetaDataBuilder metaDataBuilder, Index... indexes) {
        final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType("luceneSyntheticComplexJoinedToSimple");
        joined.addConstituent("complex", "ComplexDocument");
        joined.addConstituent("simple", "SimpleDocument");
        joined.addJoin("simple", field("group"), "complex", field("group"));
        for (Index index : indexes) {
            metaDataBuilder.addIndex(joined, index);
        }
    }

    private void metaDataHookSyntheticRecordComplexJoinedToManyFields(RecordMetaDataBuilder metaDataBuilder, Index index) {
        final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType("luceneSyntheticComplexJoinedToManyFields");
        joined.addConstituent("complex", "ComplexDocument");
        joined.addConstituent("many", "ManyFieldsDocument");
        joined.addJoin("many", field("long0"), "complex", field("group"));
        metaDataBuilder.addIndex(joined, index);
    }

    private void metaDataHookSyntheticRecordComplexJoinedToMap(RecordMetaDataBuilder metaDataBuilder, Index index) {
        final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType("luceneSyntheticComplexJoinedToMap");
        joined.addConstituent("complex", "ComplexDocument");
        joined.addConstituent("map", "MapDocument");
        joined.addJoin("map", field("group"), "complex", field("group"));
        metaDataBuilder.addIndex(joined, index);
    }


    protected static final Index COMPLEX_PARTITIONED = complexPartitionedIndex(Map.of(
            IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME,
            INDEX_PARTITION_BY_FIELD_NAME, "timestamp",
            INDEX_PARTITION_HIGH_WATERMARK, "10"));

    @Nonnull
    static Index complexPartitionedIndex(final Map<String, String> options) {
        return new Index("Complex$partitioned",
                concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
                        function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp"))).groupBy(field("group")),
                LuceneIndexTypes.LUCENE,
                options);
    }

    protected static final Index COMPLEX_PARTITIONED_NOGROUP = complexPartitionedIndexNoGroup(Map.of(
            IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME,
            INDEX_PARTITION_BY_FIELD_NAME, "timestamp",
            INDEX_PARTITION_HIGH_WATERMARK, "10"));

    @Nonnull
    private static Index complexPartitionedIndexNoGroup(final Map<String, String> options) {
        return new Index("Complex$partitioned_noGroup",
                concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
                       function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp"))),
                LuceneIndexTypes.LUCENE,
                options);
    }

    private static final Index ANALYZER_CHOOSER_TEST_LUCENE_INDEX = new Index("analyzer_chooser_test_index", function(LuceneFunctionNames.LUCENE_TEXT, field("text")), LuceneIndexTypes.LUCENE,
            ImmutableMap.of(
                    LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION, LuceneIndexTestUtils.TestAnalyzerFactory.ANALYZER_FACTORY_NAME));

    private static final Index MULTIPLE_ANALYZER_LUCENE_INDEX = new Index("Complex$multiple_analyzer_autocomplete",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION, SynonymAnalyzer.QueryOnlySynonymAnalyzerFactory.ANALYZER_FACTORY_NAME,
                    LuceneIndexOptions.LUCENE_ANALYZER_NAME_PER_FIELD_OPTION, "text2:" + NgramAnalyzer.NgramAnalyzerFactory.ANALYZER_FACTORY_NAME));

    private static final Index JOINED_INDEX = getJoinedIndex(Map.of(
            INDEX_PARTITION_BY_FIELD_NAME, "complex.timestamp",
            INDEX_PARTITION_HIGH_WATERMARK, "10"));

    @Nonnull
    private static Index getJoinedIndex(final Map<String, String> options) {
        return new Index("joinNestedConcat",
                concat(
                        field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("is_seen"))),
                        field("simple").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text"))),
                        field("complex").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp")))
                ).groupBy(field("complex").nest("group")), LuceneIndexTypes.LUCENE,
                options);
    }

    private static final Index JOINED_INDEX_NOGROUP = getJoinedIndexNoGroup(Map.of(
            INDEX_PARTITION_BY_FIELD_NAME, "complex.timestamp",
            INDEX_PARTITION_HIGH_WATERMARK, "10"));

    @Nonnull
    private static Index getJoinedIndexNoGroup(final Map<String, String> options) {
        return new Index("joinNestedConcat",
                concat(
                        field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("is_seen"))),
                        field("simple").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text"))),
                        field("complex").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp")))
                ), LuceneIndexTypes.LUCENE, options);
    }

    protected static final String ENGINEER_JOKE = "A software engineer, a hardware engineer, and a departmental manager were driving down a steep mountain road when suddenly the brakes on their car failed. The car careened out of control down the road, bouncing off the crash barriers, ground to a halt scraping along the mountainside. The occupants were stuck halfway down a mountain in a car with no brakes. What were they to do?" +
            "'I know,' said the departmental manager. 'Let's have a meeting, propose a Vision, formulate a Mission Statement, define some Goals, and by a process of Continuous Improvement find a solution to the Critical Problems, and we can be on our way.'" +
            "'No, no,' said the hardware engineer. 'That will take far too long, and that method has never worked before. In no time at all, I can strip down the car's braking system, isolate the fault, fix it, and we can be on our way.'" +
            "'Wait, said the software engineer. 'Before we do anything, I think we should push the car back up the road and see if it happens again.'";

    protected static final String WAYLON = "There's always one more way to do things and that's your way, and you have a right to try it at least once.";
    private long timestamp60DaysAgo;
    private long timestamp30DaysAgo;
    private long timestamp29DaysAgo;
    private long yesterday;

    private static Index getMapOnValueIndexWithOption(@Nonnull String name, @Nonnull ImmutableMap<String, String> options) {
        return new Index(
                name,
                new GroupingKeyExpression(field("entry", KeyExpression.FanType.FanOut).nest(concat(keys)), 3),
                LuceneIndexTypes.LUCENE,
                options);
    }

    protected static final Index MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE_2 =
            new Index(
                    "Map_with_auto_complete$entry-value",
                    new GroupingKeyExpression(field("entry", KeyExpression.FanType.FanOut).nest(concat(keys)), 1),
                    LuceneIndexTypes.LUCENE,
                    ImmutableMap.of());

    protected void openRecordStore(FDBRecordContext context, FDBRecordStoreTestBase.RecordMetaDataHook hook) {
        openRecordStore(context, hook, "group");
    }

    protected void openRecordStore(FDBRecordContext context, FDBRecordStoreTestBase.RecordMetaDataHook hook, String groupField) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsTextProto.getDescriptor());
        metaDataBuilder.getRecordType(COMPLEX_DOC).setPrimaryKey(concatenateFields(groupField, "doc_id"));
        hook.apply(metaDataBuilder);
        recordStore = getStoreBuilder(context, metaDataBuilder.getRecordMetaData()).createOrOpen();

        PlannableIndexTypes indexTypes = new PlannableIndexTypes(
                Sets.newHashSet(IndexTypes.VALUE, IndexTypes.VERSION),
                Sets.newHashSet(IndexTypes.RANK, IndexTypes.TIME_WINDOW_LEADERBOARD),
                Sets.newHashSet(IndexTypes.TEXT),
                Sets.newHashSet(LuceneIndexTypes.LUCENE)
        );
        planner = new LucenePlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState(), indexTypes, recordStore.getTimer());
        planner.setConfiguration(planner.getConfiguration()
                .asBuilder()
                .setPlanOtherAttemptWholeFilter(false)
                .build());
        recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(true);
    }

    @Override
    protected RecordLayerPropertyStorage.Builder addDefaultProps(final RecordLayerPropertyStorage.Builder props) {
        return super.addDefaultProps(props)
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_COMPRESSION_ENABLED, true);
    }

    private LuceneScanBounds fullTextSearch(Index index, String search) {
        return LuceneIndexTestUtils.fullTextSearch(recordStore, index, search, false);
    }

    private LuceneScanBounds specificFieldSearch(Index index, String search, String field) {
        LuceneScanParameters scan = new LuceneScanQueryParameters(
                ScanComparisons.EMPTY,
                new LuceneQuerySearchClause(LuceneQueryType.QUERY, field, search, false));
        return scan.bind(recordStore, index, EvaluationContext.EMPTY);
    }

    private LuceneScanBounds groupedTextSearch(Index index, String search, Object group) {
        return groupedSortedTextSearch(index, search, null, group);
    }

    private LuceneScanBounds groupedSortedTextSearch(Index index, String search, Sort sort, Object group) {
        return LuceneIndexTestValidator.groupedSortedTextSearch(recordStore, index, search, sort, group);
    }

    @Nonnull
    @SuppressWarnings("unused")
    private LuceneScanBounds groupedAutoCompleteBounds(@Nonnull final Index index, @Nonnull final String search,
                                                       @Nonnull final Object group, @Nonnull final Iterable<String> fields) {
        LuceneScanParameters scan = groupedAutoCompleteScanParams(search, group, fields);
        return scan.bind(recordStore, index, EvaluationContext.EMPTY);
    }

    @Nonnull
    protected static LuceneScanParameters groupedAutoCompleteScanParams(@Nonnull final String search,
                                                                        @Nonnull final Object group,
                                                                        @Nonnull final Iterable<String> fields) {
        return new LuceneScanQueryParameters(
                Verify.verifyNotNull(ScanComparisons.from(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, group))),
                new LuceneAutoCompleteQueryClause(search, false, fields));
    }

    @Nonnull
    private LuceneScanBounds autoCompleteBounds(@Nonnull final Index index, @Nonnull final String search,
                                                @Nonnull final Iterable<String> fields) {
        LuceneScanParameters scan = autoCompleteScanParams(search, fields);
        return scan.bind(recordStore, index, EvaluationContext.EMPTY);
    }

    @Nonnull
    private LuceneScanParameters autoCompleteScanParams(@Nonnull final String search,
                                                        @Nonnull final Iterable<String> fields) {
        return new LuceneScanQueryParameters(
                Verify.verifyNotNull(ScanComparisons.EMPTY),
                new LuceneAutoCompleteQueryClause(search, false, fields));
    }

    private LuceneScanBounds spellCheck(Index index, String search) {
        LuceneScanParameters scan = new LuceneScanSpellCheckParameters(
                ScanComparisons.EMPTY,
                search, false);
        return scan.bind(recordStore, index, EvaluationContext.EMPTY);
    }

    private LuceneScanBounds groupedSpellCheck(Index index, String search, Object group) {
        LuceneScanParameters scan = new LuceneScanSpellCheckParameters(
                Verify.verifyNotNull(ScanComparisons.from(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, group))),
                search, false);
        return scan.bind(recordStore, index, EvaluationContext.EMPTY);
    }

    @Nonnull
    private StoreTimer.Counter getCounter(@Nonnull final FDBRecordContext recordContext, @Nonnull final StoreTimer.Event event) {
        return Verify.verifyNotNull(Verify.verifyNotNull(recordContext.getTimer()).getCounter(event));
    }

    // ==================== PARTITIONED INDEX TESTS =====================

    @Test
    void basicGroupedPartitionedTest() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_PARTITIONED);
            recordStore.saveRecord(createComplexDocument(6666L, ENGINEER_JOKE, 1, Instant.now().toEpochMilli()));
            recordStore.saveRecord(createComplexDocument(7777L, ENGINEER_JOKE, 2, Instant.now().toEpochMilli()));
            recordStore.saveRecord(createComplexDocument(8888L, WAYLON, 2, Instant.now().plus(1, ChronoUnit.DAYS).toEpochMilli()));
            recordStore.saveRecord(createComplexDocument(9999L, "hello world!", 1, Instant.now().plus(2, ChronoUnit.DAYS).toEpochMilli()));

            // should find only one match for ENGINEER_JOKE (the other match is in the other group)
            assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(2, 7777L)),
                    recordStore.scanIndex(COMPLEX_PARTITIONED, groupedTextSearch(COMPLEX_PARTITIONED, "text:propose", 2), null, ScanProperties.FORWARD_SCAN));
            assertEquals(1, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

            // should not find any match for WAYLON in this group (it's in the other group)
            assertIndexEntryPrimaryKeyTuples(Set.of(),
                    recordStore.scanIndex(COMPLEX_PARTITIONED, groupedTextSearch(COMPLEX_PARTITIONED, "text:things", 1), null, ScanProperties.FORWARD_SCAN));
            assertEquals(1, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

            // now delete ENGINEER_JOKE from group 1, and verify
            recordStore.deleteRecord(Tuple.from(1, 6666L));
            assertIndexEntryPrimaryKeyTuples(Set.of(),
                    recordStore.scanIndex(COMPLEX_PARTITIONED, groupedTextSearch(COMPLEX_PARTITIONED, "text:propose", 1), null, ScanProperties.FORWARD_SCAN));

            final Subspace partition1Subspace = recordStore.indexSubspace(COMPLEX_PARTITIONED).subspace(Tuple.from(1, LucenePartitioner.PARTITION_DATA_SUBSPACE).add(0));
            final Subspace partition2Subspace = recordStore.indexSubspace(COMPLEX_PARTITIONED).subspace(Tuple.from(2, LucenePartitioner.PARTITION_DATA_SUBSPACE).add(0));

            validateSegmentAndIndexIntegrity(COMPLEX_PARTITIONED, partition1Subspace, context, "_0.cfs");
            validateSegmentAndIndexIntegrity(COMPLEX_PARTITIONED, partition2Subspace, context, "_0.cfs");
        }
    }

    @Test
    void basicNonGroupedPartitionedTest() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_PARTITIONED_NOGROUP);
            recordStore.saveRecord(createComplexDocument(6666L, ENGINEER_JOKE, 1, Instant.now().toEpochMilli()));
            recordStore.saveRecord(createComplexDocument(7777L, ENGINEER_JOKE, 2, Instant.now().toEpochMilli()));
            recordStore.saveRecord(createComplexDocument(8888L, WAYLON, 2, Instant.now().plus(1, ChronoUnit.DAYS).toEpochMilli()));
            recordStore.saveRecord(createComplexDocument(9999L, "hello world!", 1, Instant.now().plus(2, ChronoUnit.DAYS).toEpochMilli()));

            // should find matches for ENGINEER_JOKE
            assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(2, 7777L), Tuple.from(1, 6666L)),
                    recordStore.scanIndex(COMPLEX_PARTITIONED_NOGROUP, fullTextSearch(COMPLEX_PARTITIONED_NOGROUP, "text:propose"), null, ScanProperties.FORWARD_SCAN));
            assertEquals(2, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

            // now delete ENGINEER_JOKE and verify
            recordStore.deleteRecord(Tuple.from(1, 6666L));
            assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(2, 7777L)),
                    recordStore.scanIndex(COMPLEX_PARTITIONED_NOGROUP, fullTextSearch(COMPLEX_PARTITIONED_NOGROUP, "text:propose"), null, ScanProperties.FORWARD_SCAN));

            final Subspace partition1Subspace = recordStore.indexSubspace(COMPLEX_PARTITIONED_NOGROUP).subspace(Tuple.from(LucenePartitioner.PARTITION_DATA_SUBSPACE).add(0));

            validateSegmentAndIndexIntegrity(COMPLEX_PARTITIONED_NOGROUP, partition1Subspace, context, "_0.cfs");
        }
    }

    @Test
    void testBlockCacheRemove() {
        // set cache size to very small, so items will not fit
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_BLOCK_CACHE_MAXIMUM_SIZE, 2)
                .build();
        try (FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_PARTITIONED_NOGROUP);
            recordStore.saveRecord(createComplexDocument(6666L, ENGINEER_JOKE, 1, Instant.now().toEpochMilli()));
            recordStore.saveRecord(createComplexDocument(7777L, ENGINEER_JOKE, 2, Instant.now().toEpochMilli()));
            recordStore.saveRecord(createComplexDocument(8888L, WAYLON, 2, Instant.now().plus(1, ChronoUnit.DAYS).toEpochMilli()));
            recordStore.saveRecord(createComplexDocument(9999L, "hello world!", 1, Instant.now().plus(2, ChronoUnit.DAYS).toEpochMilli()));
            context.commit();

            final FDBStoreTimer timer = context.getTimer();
            assertTrue(timer.getCount(LuceneEvents.Counts.LUCENE_BLOCK_CACHE_REMOVE) > 0);
        }
    }

    @Test
    void testBlockCacheNotRemove() {
        // set cache size to large enough, so all blocks fit
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_BLOCK_CACHE_MAXIMUM_SIZE, 1000)
                .build();
        try (FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_PARTITIONED_NOGROUP);
            recordStore.saveRecord(createComplexDocument(6666L, ENGINEER_JOKE, 1, Instant.now().toEpochMilli()));
            recordStore.saveRecord(createComplexDocument(7777L, ENGINEER_JOKE, 2, Instant.now().toEpochMilli()));
            recordStore.saveRecord(createComplexDocument(8888L, WAYLON, 2, Instant.now().plus(1, ChronoUnit.DAYS).toEpochMilli()));
            recordStore.saveRecord(createComplexDocument(9999L, "hello world!", 1, Instant.now().plus(2, ChronoUnit.DAYS).toEpochMilli()));
            context.commit();

            final FDBStoreTimer timer = context.getTimer();
            assertEquals(timer.getCount(LuceneEvents.Counts.LUCENE_BLOCK_CACHE_REMOVE), 0);
            assertTrue(timer.getCount(LuceneEvents.Waits.WAIT_LUCENE_GET_DATA_BLOCK) > 0);
        }
    }

    static Stream<Pair<Index, Tuple>> dualGroupModeIndexProvider() {
        return Stream.of(Pair.of(COMPLEX_PARTITIONED, Tuple.from(1L)), Pair.of(COMPLEX_PARTITIONED_NOGROUP, Tuple.from()));
    }

    @ParameterizedTest
    @MethodSource(value = {"dualGroupModeIndexProvider"})
    void repartitionGroupedTest(Pair<Index, Tuple> indexAndGroupingKey) throws IOException {
        Index index = indexAndGroupingKey.getLeft();
        Tuple groupingKey = indexAndGroupingKey.getRight();
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 6)
                .build();

        final int totalDocCount = 20;
        Consumer<FDBRecordContext> schemaSetup = context -> rebuildIndexMetaData(context, COMPLEX_DOC, index);
        long docGroupFieldValue = groupingKey.isEmpty() ? 0L : groupingKey.getLong(0);

        // create/save documents
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            long start = Instant.now().toEpochMilli();
            for (int i = 0; i < totalDocCount; i++) {
                recordStore.saveRecord(createComplexDocument(1000L + i, ENGINEER_JOKE, docGroupFieldValue, start + i * 100));
            }
            commit(context);
        }

        // initially, all documents are saved into one partition
        List<LucenePartitionInfoProto.LucenePartitionInfo> partitionInfos = getPartitionMeta(index,
                groupingKey, contextProps, schemaSetup);
        assertEquals(1, partitionInfos.size());
        assertEquals(totalDocCount, partitionInfos.get(0).getCount());

        // run re-partitioning
        explicitMergeIndex(index, contextProps, schemaSetup);
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            assertEquals(1, getCounter(context, LuceneEvents.Counts.LUCENE_REPARTITION_CALLS).getCount());
        }
        partitionInfos = getPartitionMeta(index,
                groupingKey, contextProps, schemaSetup);
        // It should first move 6 from the most-recent to a new, older partition, then move 6 again into a partition
        // in between the two
        assertEquals(List.of(6, 6, 8),
                partitionInfos.stream()
                        .sorted(Comparator.comparing(partitionInfo -> Tuple.fromBytes(partitionInfo.getFrom().toByteArray())))
                        .map(LucenePartitionInfoProto.LucenePartitionInfo::getCount)
                        .collect(Collectors.toList()));
        assertEquals(List.of(1, 2, 0),
                partitionInfos.stream()
                        .sorted(Comparator.comparing(partitionInfo -> Tuple.fromBytes(partitionInfo.getFrom().toByteArray())))
                        .map(LucenePartitionInfoProto.LucenePartitionInfo::getId)
                        .collect(Collectors.toList()));

        // run re-partitioning again, nothing should happend, as the first rebalance should have done everything
        explicitMergeIndex(index, contextProps, schemaSetup);
        assertEquals(partitionInfos, getPartitionMeta(index, groupingKey, contextProps, schemaSetup));

        // partition metadata validated above. now validate that the documents have indeed been
        // moved.
        // expected: docs 1000-1005 -> partition 1 (oldest)
        //                1006-1011 -> partition 2 (middle)
        //                1012-1019 -> partition 0 (newest)
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            // one from the first + one from the second calls to explicitMergeIndex()
            assertEquals(2, getCounter(context, LuceneEvents.Counts.LUCENE_REPARTITION_CALLS).getCount());

            validateDocsInPartition(index, 0, groupingKey, makeKeyTuples(docGroupFieldValue, 1012, 1019), "text:propose");
            validateDocsInPartition(index, 2, groupingKey, makeKeyTuples(docGroupFieldValue, 1006, 1011), "text:propose");
            validateDocsInPartition(index, 1, groupingKey, makeKeyTuples(docGroupFieldValue, 1000, 1005), "text:propose");
        }
    }

    private void validateDocsInPartition(Index index, int partitionId, Tuple groupingKey,
                                         Set<Tuple> expectedPrimaryKeys, final String universalSearch) throws IOException {
        LuceneIndexTestValidator.validateDocsInPartition(recordStore, index, partitionId, groupingKey, expectedPrimaryKeys, universalSearch);
    }

    private Map<Integer, Integer> getSegmentCounts(Index index,
                                                   Tuple groupingKey,
                                                   RecordLayerPropertyStorage contextProps,
                                                   Consumer<FDBRecordContext> schemaSetup) {
        final List<LucenePartitionInfoProto.LucenePartitionInfo> partitionMeta = getPartitionMeta(index, groupingKey, contextProps, schemaSetup);
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            return partitionMeta.stream()
                    .collect(Collectors.toMap(
                            LucenePartitionInfoProto.LucenePartitionInfo::getId,
                            partitionInfo -> Assertions.assertDoesNotThrow(() ->
                                    getIndexReader(index, partitionInfo.getId(), groupingKey).getContext().leaves().size())
                    ));
        }
    }

    private IndexReader getIndexReader(final Index index, final int partitionId, final Tuple groupingKey) throws IOException {
        return LuceneIndexTestValidator.getIndexReader(recordStore, index, groupingKey, partitionId);
    }

    public static Stream<Arguments> repartitionAndMerge() {
        return Stream.of(2, 3).flatMap(repartitionCount ->
                Stream.of(2).flatMap(mergeSegmentsPerTier ->
                        Stream.of(
                                Arguments.of(COMPLEX_PARTITIONED, Tuple.from(1), repartitionCount, mergeSegmentsPerTier),
                                Arguments.of(COMPLEX_PARTITIONED_NOGROUP, Tuple.from(), repartitionCount, mergeSegmentsPerTier)
                        )));
    }

    @Tag(Tags.Slow)
    @ParameterizedTest
    @MethodSource()
    void repartitionAndMerge(Index index, Tuple groupingKey, int repartitionCount, int mergeSegmentsPerTier) throws IOException {
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, repartitionCount)
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, (double)mergeSegmentsPerTier)
                .build();

        Consumer<FDBRecordContext> schemaSetup = context -> rebuildIndexMetaData(context, COMPLEX_DOC, index);
        long docGroupFieldValue = groupingKey.isEmpty() ? 0L : groupingKey.getLong(0);

        int transactionCount = 100;
        int docsPerTransaction = 2;
        // create/save documents
        long id = 0;
        List<Long> allIds = new ArrayList<>();
        for (int i = 0; i < transactionCount; i++) {
            try (FDBRecordContext context = openContext(contextProps)) {
                schemaSetup.accept(context);
                recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(false);
                long start = Instant.now().toEpochMilli();
                for (int j = 0; j < docsPerTransaction; j++) {
                    id++;
                    recordStore.saveRecord(createComplexDocument(id, ENGINEER_JOKE, docGroupFieldValue, start + id));
                    allIds.add(id);
                }
                commit(context);
            }
        }

        // we haven't done any merges yet, or repartitioning, so each transaction should be one new segment
        assertEquals(Map.of(0, transactionCount),
                getSegmentCounts(index, groupingKey, contextProps, schemaSetup));

        timer.reset();
        explicitMergeIndex(index, contextProps, schemaSetup);
        final Map<Integer, Integer> segmentCounts = getSegmentCounts(index, groupingKey, contextProps, schemaSetup);
        final int partitionSize = repartitionCount == 3 ? 9 : 10;
        final int partitionCount;
        if (repartitionCount == 3) {
            partitionCount = allIds.size() / partitionSize + 1;
            assertThat(segmentCounts, Matchers.aMapWithSize(partitionCount));
            assertEquals(IntStream.range(0, partitionCount).boxed()
                            .collect(Collectors.toMap(Function.identity(), partitionId -> partitionId == partitionCount - 1 ? 1 : 2)),
                    segmentCounts);
        } else {
            partitionCount = allIds.size() / partitionSize;
            assertThat(segmentCounts, Matchers.aMapWithSize(partitionCount));
            assertEquals(IntStream.range(0, partitionCount).boxed()
                            .collect(Collectors.toMap(Function.identity(), partitionId -> 2)),
                    segmentCounts);
        }

        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            validateDocsInPartition(index, 0, groupingKey,
                    allIds.stream()
                            .skip(repartitionCount == 3 ? 192 : 190)
                            .map(idLong -> Tuple.from(docGroupFieldValue, idLong))
                            .collect(Collectors.toSet()),
                    "text:propose");
            for (int i = 1; i < 20; i++) {
                // 0 should have the newest
                // everyone else should increase
                validateDocsInPartition(index, i, groupingKey,
                        allIds.stream().skip((i - 1) * partitionSize)
                                .limit(partitionSize)
                                .map(idLong -> Tuple.from(docGroupFieldValue, idLong))
                                .collect(Collectors.toSet()),
                        "text:propose");
            }
            List<LucenePartitionInfoProto.LucenePartitionInfo> partitionInfos = getPartitionMeta(index,
                    groupingKey, contextProps, schemaSetup);
            assertEquals(partitionCount, partitionInfos.size());
        }
    }

    @Test
    void repartitionSyntheticGroupedTest() throws IOException {
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 6)
                .build();

        final int totalDocCount = 20;
        Consumer<FDBRecordContext> schemaSetup = context -> openRecordStore(context, LuceneIndexTest::joinedPartitionedLuceneIndexMetadataHook);

        List<Tuple> ids = new ArrayList<>();
        // create/save documents
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            long start = Instant.now().toEpochMilli();
            for (int i = 0; i < totalDocCount; i++) {
                TestRecordsTextProto.ComplexDocument complexDocument = TestRecordsTextProto.ComplexDocument.newBuilder()
                        .setGroup(42)
                        .setDocId(1000L + i)
                        .setIsSeen(true)
                        .setHeader(ComplexDocument.Header.newBuilder().setHeaderId(1000L - i))
                        .setTimestamp(start + i * 100)
                        .build();
                TestRecordsTextProto.SimpleDocument simpleDocument = TestRecordsTextProto.SimpleDocument.newBuilder()
                        .setGroup(42)
                        .setDocId(1000L - i)
                        .setText("Four score and seven years ago our fathers brought forth propose")
                        .build();
                final Tuple syntheticRecordTypeKey = recordStore.getRecordMetaData()
                        .getSyntheticRecordType("luceneJoinedPartitionedIdx")
                        .getRecordTypeKeyTuple();
                ids.add(Tuple.from(syntheticRecordTypeKey.getItems().get(0),
                        recordStore.saveRecord(complexDocument).getPrimaryKey().getItems(),
                        recordStore.saveRecord(simpleDocument).getPrimaryKey().getItems()));
            }
            commit(context);
        }

        // initially, all documents are saved into one partition
        final Tuple groupingKey = Tuple.from(42);
        List<LucenePartitionInfoProto.LucenePartitionInfo> partitionInfos = getPartitionMeta(JOINED_INDEX,
                groupingKey, contextProps, schemaSetup);
        assertEquals(1, partitionInfos.size());
        assertEquals(totalDocCount, partitionInfos.get(0).getCount());

        // run re-partitioning
        explicitMergeIndex(JOINED_INDEX, contextProps, schemaSetup);
        partitionInfos = getPartitionMeta(JOINED_INDEX, groupingKey, contextProps, schemaSetup);
        // now there should be 2 partitions: partition 0 with (totalDocCount - 6) docs, and partition 1 with 6 docs

        assertEquals(List.of(6, 6, 8),
                partitionInfos.stream()
                        .sorted(Comparator.comparing(partitionInfo -> Tuple.fromBytes(partitionInfo.getFrom().toByteArray())))
                        .map(LucenePartitionInfoProto.LucenePartitionInfo::getCount)
                        .collect(Collectors.toList()));
        assertEquals(List.of(1, 2, 0),
                partitionInfos.stream()
                        .sorted(Comparator.comparing(partitionInfo -> Tuple.fromBytes(partitionInfo.getFrom().toByteArray())))
                        .map(LucenePartitionInfoProto.LucenePartitionInfo::getId)
                        .collect(Collectors.toList()));

        // re-running shouldn't do anything
        explicitMergeIndex(JOINED_INDEX, contextProps, schemaSetup);
        assertEquals(partitionInfos, getPartitionMeta(JOINED_INDEX, groupingKey, contextProps, schemaSetup));

        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);

            validateDocsInPartition(JOINED_INDEX, 0, groupingKey,
                    Set.copyOf(ids.subList(12, totalDocCount)), "simple_text:propose");
            validateDocsInPartition(JOINED_INDEX, 2, groupingKey,
                    Set.copyOf(ids.subList(6, 12)), "simple_text:propose");
            validateDocsInPartition(JOINED_INDEX, 1, groupingKey,
                    Set.copyOf(ids.subList(0, 6)), "simple_text:propose");
        }
    }

    @ParameterizedTest
    @MethodSource(value = {"dualGroupModeIndexProvider"})
    void optimizedPartitionInsertionTest(Pair<Index, Tuple> indexAndGroupingKey) throws IOException {
        Index index = indexAndGroupingKey.getLeft();
        Tuple groupingKey = indexAndGroupingKey.getRight();
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 6)
                .build();

        final int totalDocCount = 10; // configured index's highwater mark
        Consumer<FDBRecordContext> schemaSetup = context -> rebuildIndexMetaData(context, COMPLEX_DOC, index);
        long docGroupFieldValue = groupingKey.isEmpty() ? 0L : groupingKey.getLong(0);

        // create/save documents
        long start = Instant.now().toEpochMilli();
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            for (int i = 0; i < totalDocCount; i++) {
                recordStore.saveRecord(createComplexDocument(1000L + i, ENGINEER_JOKE, docGroupFieldValue, start + i * 100));
            }
            commit(context);
        }

        // partition 0 should be at capacity now
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            validateDocsInPartition(index, 0, groupingKey, makeKeyTuples(docGroupFieldValue, 1000, 1009), "text:propose");
        }

        // now add 20 documents older than the oldest document in partition 0
        // they should go into partitions 1 and 2
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            for (int i = 0; i < 20; i++) {
                recordStore.saveRecord(createComplexDocument(1000L + totalDocCount + i, ENGINEER_JOKE, docGroupFieldValue, start - i - 1));
            }
            validateDocsInPartition(index, 1, groupingKey, makeKeyTuples(docGroupFieldValue, 1010, 1019), "text:propose");
            validateDocsInPartition(index, 2, groupingKey, makeKeyTuples(docGroupFieldValue, 1020, 1029), "text:propose");
        }
    }

    private void explicitMergeIndex(Index index, RecordLayerPropertyStorage contextProps, Consumer<FDBRecordContext> schemaSetup) {
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setRecordStore(recordStore)
                    .setIndex(index)
                    .setTimer(timer)
                    .build()) {
                indexBuilder.mergeIndex();
            }
        }
    }

    private List<LucenePartitionInfoProto.LucenePartitionInfo> getPartitionMeta(Index index,
                                                                                Tuple groupingKey,
                                                                                RecordLayerPropertyStorage contextProps,
                                                                                Consumer<FDBRecordContext> schemaSetup) {
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            LuceneIndexMaintainer indexMaintainer = (LuceneIndexMaintainer)recordStore.getIndexMaintainer(index);
            return indexMaintainer.getPartitioner().getAllPartitionMetaInfo(groupingKey).join();
        }
    }

    private static void joinedPartitionedLuceneIndexMetadataHook(@Nonnull final RecordMetaDataBuilder metaDataBuilder) {
        metaDataBuilder.addIndex(joinedMetadataHook(metaDataBuilder), JOINED_INDEX);
    }

    private static void joinedPartitionedUngroupedLuceneIndexMetadataHook(@Nonnull final RecordMetaDataBuilder metaDataBuilder) {
        metaDataBuilder.addIndex(joinedMetadataHook(metaDataBuilder), JOINED_INDEX_NOGROUP);
    }

    private static JoinedRecordTypeBuilder joinedMetadataHook(@Nonnull final RecordMetaDataBuilder metaDataBuilder) {
        //set up the joined index
        final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType("luceneJoinedPartitionedIdx");
        joined.addConstituent("complex", "ComplexDocument");
        joined.addConstituent("simple", "SimpleDocument");
        joined.addJoin("simple", field("doc_id"), "complex", field("header").nest("header_id"));
        return joined;

    }

    @Test
    void partitionedJoinedIndexTest() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, LuceneIndexTest::joinedPartitionedUngroupedLuceneIndexMetadataHook);

            TestRecordsTextProto.ComplexDocument complexDocument = TestRecordsTextProto.ComplexDocument.newBuilder()
                    .setGroup(42)
                    .setDocId(5)
                    .setIsSeen(true)
                    .setHeader(ComplexDocument.Header.newBuilder().setHeaderId(143))
                    .setTimestamp(System.currentTimeMillis())
                    .build();
            TestRecordsTextProto.SimpleDocument simpleDocument = TestRecordsTextProto.SimpleDocument.newBuilder()
                    .setDocId(143)
                    .setGroup(42)
                    .setText("Four score and seven years ago our fathers brought forth")
                    .build();
            recordStore.saveRecord(complexDocument);
            recordStore.saveRecord(simpleDocument);

            String luceneSearch = "simple_text: \"fathers\"";

            QueryComponent filter = new LuceneQueryComponent(luceneSearch, List.of("simple", "complex"));
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("luceneJoinedPartitionedIdx")
                    .setFilter(filter)
                    .setRequiredResults(List.of(Key.Expressions.field("simple").nest("text")))
                    .build();
            final RecordQueryPlan plan = planner.plan(query);
            final List<?> results = plan.execute(recordStore).asList().join();
            assertNotNull(results);
            assertEquals(1, results.size());
        }
    }

    private Pair<Index, Consumer<FDBRecordContext>> setupIndex(Map<String, String> options, boolean isGrouped, boolean isSynthetic) {
        Index index;
        Consumer<FDBRecordContext> schemaSetup;
        if (isGrouped) {
            if (isSynthetic) {
                index = getJoinedIndex(options);
                schemaSetup = context -> openRecordStore(context, metaDataBuilder -> metaDataBuilder.addIndex(joinedMetadataHook(metaDataBuilder), index));
            } else {
                index = complexPartitionedIndex(options);
                schemaSetup = context -> rebuildIndexMetaData(context, COMPLEX_DOC, index);
            }
        } else {
            if (isSynthetic) {
                index = getJoinedIndexNoGroup(options);
                schemaSetup = context -> openRecordStore(context, metaDataBuilder -> metaDataBuilder.addIndex(joinedMetadataHook(metaDataBuilder), index));
            } else {
                index = complexPartitionedIndexNoGroup(options);
                schemaSetup = context -> rebuildIndexMetaData(context, COMPLEX_DOC, index);
            }
        }
        return Pair.of(index, schemaSetup);
    }

    private enum SortType {
        ASCENDING,
        DESCENDING,
        UNSORTED,
    }

    static Stream<Arguments> continuationDuringRepartitioningTest() {
        return Stream.of(true, false)
                .flatMap(grouped -> Stream.of(true, false)
                        .flatMap(uniqueTimestamps -> Stream.of(true, false)
                                .flatMap(synthetic -> Arrays.stream(SortType.values())
                                        .map(sortType -> Arguments.of(grouped, synthetic, uniqueTimestamps, sortType)))));
    }

    @ParameterizedTest(name = "isGrouped: {0}, isSynthetic: {1}, with unique timestamps: {2}, sort type: {3}")
    @MethodSource
    void continuationDuringRepartitioningTest(boolean isGrouped,
                                              boolean isSynthetic,
                                              boolean uniqueTimestamps,
                                              SortType sortType) throws IOException, ExecutionException, InterruptedException {

        final Map<String, String> options = Map.of(
                INDEX_PARTITION_BY_FIELD_NAME, isSynthetic ? "complex.timestamp" : "timestamp",
                INDEX_PARTITION_HIGH_WATERMARK, String.valueOf(10));
        Pair<Index, Consumer<FDBRecordContext>> indexConsumerPair = setupIndex(options, isGrouped, isSynthetic);
        final Index index = indexConsumerPair.getLeft();
        Consumer<FDBRecordContext> schemaSetup = indexConsumerPair.getRight();

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 6)
                .build();

        final int group = isGrouped ? 1 : 0;
        final Tuple groupTuple = isGrouped ? Tuple.from(group) : Tuple.from();
        final long start = Instant.now().toEpochMilli();
        final String luceneSearch = isSynthetic ? "simple_text:forth" : "text:about";

        final int docCount = 25;
        List<Tuple> primaryKeys = new ArrayList<>();
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            for (int i = 0; i < docCount; i++) {
                ComplexDocument complexDocument = ComplexDocument.newBuilder()
                        .setGroup(group)
                        .setDocId(1000L + i)
                        .setIsSeen(true)
                        .setText("A word about what I want to say")
                        .setTimestamp(uniqueTimestamps ? start + i * 100 : start)
                        .setHeader(ComplexDocument.Header.newBuilder().setHeaderId(1000L - i))
                        .build();
                final Tuple primaryKey;
                if (isSynthetic) {
                    TestRecordsTextProto.SimpleDocument simpleDocument = TestRecordsTextProto.SimpleDocument.newBuilder()
                            .setGroup(group)
                            .setDocId(1000L - i)
                            .setText("Four score and seven years ago our fathers brought forth")
                            .build();
                    final Tuple syntheticRecordTypeKey = recordStore.getRecordMetaData()
                            .getSyntheticRecordType("luceneJoinedPartitionedIdx")
                            .getRecordTypeKeyTuple();
                    primaryKey = Tuple.from(syntheticRecordTypeKey.getItems().get(0),
                            recordStore.saveRecord(complexDocument).getPrimaryKey().getItems(),
                            recordStore.saveRecord(simpleDocument).getPrimaryKey().getItems());
                } else {
                    primaryKey = recordStore.saveRecord(complexDocument).getPrimaryKey();
                }
                primaryKeys.add(primaryKey);
            }
            commit(context);
        }

        // initially, all documents are saved into one partition
        List<LucenePartitionInfoProto.LucenePartitionInfo> partitionInfos = getPartitionMeta(index,
                groupTuple, contextProps, schemaSetup);
        assertEquals(1, partitionInfos.size());
        assertEquals(docCount, partitionInfos.get(0).getCount());

        byte[] continuation;
        final Sort sort;
        if (sortType == SortType.UNSORTED) {
            sort = null;
        } else {
            sort = new Sort(new SortField(isSynthetic ? "complex_timestamp" : "timestamp", SortField.Type.LONG, sortType == SortType.DESCENDING));
        }

        LuceneScanQueryParameters scan = new LuceneScanQueryParameters(
                isGrouped ? Verify.verifyNotNull(ScanComparisons.from(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, group))) : ScanComparisons.EMPTY,
                new LuceneQueryMultiFieldSearchClause(LuceneQueryType.QUERY, luceneSearch, false),
                sort,
                null,
                null,
                null);
        LuceneScanQuery scanQuery = scan.bind(recordStore, index, EvaluationContext.EMPTY);

        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);

            RecordCursor<IndexEntry> indexEntryCursor = recordStore.scanIndex(index, scanQuery, null, ExecuteProperties.newBuilder().setReturnedRowLimit(15).build().asScanProperties(false));

            // Get 15 results and continuation
            List<IndexEntry> entries = indexEntryCursor.asList().join();
            assertEquals(15, entries.size());
            RecordCursorResult<IndexEntry> lastResult = indexEntryCursor.onNext().get();
            assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, lastResult.getNoNextReason());
            LuceneContinuationProto.LuceneIndexContinuation parsed = LuceneContinuationProto.LuceneIndexContinuation.parseFrom(lastResult.getContinuation().toBytes());
            // we stopped in partition 0
            assertEquals(0, parsed.getPartitionId());
            final Set<Tuple> expectedKeys;
            if (sortType == SortType.ASCENDING || sortType == SortType.UNSORTED) {
                expectedKeys = Set.copyOf(primaryKeys.subList(0, 15));
            } else {
                expectedKeys = Set.copyOf(primaryKeys.subList(10, 25));
            }

            assertEquals(expectedKeys, entries.stream().map(IndexEntry::getPrimaryKey).collect(Collectors.toSet()));
            continuation = lastResult.getContinuation().toBytes();
        }

        // run re-partitioning
        explicitMergeIndex(index, contextProps, schemaSetup);
        // now there should be 4 partitions:
        //  partition 0: with docs 18 - 24
        //  partition 3: with docs 12 - 17
        //  partition 2: with docs  6 - 11
        //  partition 1: with docs  0 - 5
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            // default cap (1000), so rebalancing done in one pass
            assertEquals(1, getCounter(context, LuceneEvents.Counts.LUCENE_REPARTITION_CALLS).getCount());
            validateDocsInPartition(index, 0, groupTuple, Set.copyOf(primaryKeys.subList(18, 25)), luceneSearch);
            validateDocsInPartition(index, 3, groupTuple, Set.copyOf(primaryKeys.subList(12, 18)), luceneSearch);
            validateDocsInPartition(index, 2, groupTuple, Set.copyOf(primaryKeys.subList(6, 12)), luceneSearch);
            validateDocsInPartition(index, 1, groupTuple, Set.copyOf(primaryKeys.subList(0, 6)), luceneSearch);
        }

        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);

            RecordCursor<IndexEntry> indexEntryCursor = recordStore.scanIndex(index, scanQuery, continuation, ExecuteProperties.newBuilder().build().asScanProperties(false));

            // so now we should get the remaining docs
            List<IndexEntry> entries = indexEntryCursor.asList().join();
            final Set<Tuple> expectedKeys;
            final RecordCursorResult<IndexEntry> lastResult = indexEntryCursor.onNext().get();
            final int expectedCount;
            if (sortType == SortType.ASCENDING) {
                expectedKeys = Set.copyOf(primaryKeys.subList(15, 25));
                expectedCount = 10;
            } else if (sortType == SortType.DESCENDING) {
                expectedKeys = Set.copyOf(primaryKeys.subList(0, 10));
                expectedCount = 10;
            } else {
                expectedKeys = Set.copyOf(primaryKeys.subList(0, 18));
                expectedCount = 18;
            }
            assertEquals(expectedCount, entries.size());
            assertEquals(expectedKeys, entries.stream().map(IndexEntry::getPrimaryKey).collect(Collectors.toSet()));
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, lastResult.getNoNextReason());
        }
    }

    Pair<int[], Integer> calculateAndValidateRepartitioningExpectations(
            List<LucenePartitionInfoProto.LucenePartitionInfo> partitionInfos,
            int docCount,
            int highWaterMark,
            int docCountPerTxn,
            int maxCountPerRebalanceCall,
            int actualRepartitionCallCount) {

        // if highWaterMark < docCountPerTxn, it will be the actual count of docs moved per txn
        int countActuallyMovedPerTxn = Math.min(docCountPerTxn, highWaterMark);

        // created partitions' max capacity is the highest multiple of countActuallyMovedPerTxn that is <= highWaterMark
        int maxCreatedPartitionCapacity = (int)Math.floor((double)highWaterMark / countActuallyMovedPerTxn) * countActuallyMovedPerTxn;

        // calculate the expected number of partitions after repartitioning
        int partitionCount = docCount / maxCreatedPartitionCapacity;
        if (maxCreatedPartitionCapacity + docCount % maxCreatedPartitionCapacity > highWaterMark) {
            partitionCount++;
        }

        final int[] docDistribution;
        if (partitionCount == 1) {
            docDistribution = new int[] {docCount};
        } else {
            // calculate document distribution
            // the original and last partitions may have irregular count docs in them,
            // and any other new partitions will have maxCreatedPartitionCapacity docs
            docDistribution = new int[partitionCount];
            // fill the "middle" partitions with maxCreatedPartitionCapacity value
            if (partitionCount > 2) {
                Arrays.fill(docDistribution, 1, docDistribution.length - 1, maxCreatedPartitionCapacity);
            }
            // calculate count of docs in the original (first) partition and the last created partition
            int remainder = docCount - (docDistribution.length - 2) * maxCreatedPartitionCapacity;
            int lastCount = ((int)Math.ceil((remainder - highWaterMark) / (double)countActuallyMovedPerTxn)) * countActuallyMovedPerTxn;
            docDistribution[0] = remainder - lastCount;
            docDistribution[docDistribution.length - 1] = lastCount;
        }
        // maxIterations--per LuceneIndexMaintainer.rebalancePartitions()
        int maxIterations = Math.max(1, maxCountPerRebalanceCall / docCountPerTxn);

        // max docs moved per call to LuceneIndexMaintainer.rebalancePartitions()
        int docsMovedPerCall = maxIterations * countActuallyMovedPerTxn;

        // total moved docs during the entire repartition run
        int totalMovedDocs = docCount - docDistribution[0];

        // calculate the expected repartition call count
        int expectedRepartitionCallCount = (int)Math.ceil(totalMovedDocs / (double)docsMovedPerCall);
        // did we do a (no-op) call to rebalancePartitions() at the end?
        if (totalMovedDocs % docsMovedPerCall == 0) {
            expectedRepartitionCallCount++;
        }

        // if actualRepartitionCallCount == -1, we skip validation (e.g. when called from multi-group tests)
        assertTrue(actualRepartitionCallCount == -1 || expectedRepartitionCallCount == actualRepartitionCallCount);
        assertEquals(partitionInfos.size(), partitionCount);
        assertEquals(
                partitionInfos.stream()
                        .sorted(Comparator.comparing(LucenePartitionInfoProto.LucenePartitionInfo::getId))
                        .map(LucenePartitionInfoProto.LucenePartitionInfo::getCount)
                        .collect(Collectors.toList()),
                Arrays.stream(docDistribution).boxed().collect(Collectors.toList()));

        return Pair.of(docDistribution, expectedRepartitionCallCount);
    }

    static Stream<Arguments> capDocCountMovedDuringRepartitioningMultigroupTest() {
        Random r = ThreadLocalRandom.current();
        return Stream.of(
                // basic 2-group setup
                Arguments.of(10, 4, 6, new int[] {51, 32}),
                // random 3-group setup
                Arguments.of(
                        r.nextInt(30) + 5, // highwater mark 5 - 30
                        r.nextInt(8) + 2, // doc count per txn 2 - 10
                        r.nextInt(16) + 1, // max count per rebalance call 1 - 16
                        new int[] {r.nextInt(30) + 20, r.nextInt(30) + 20, r.nextInt(30) + 20}
                ),
                // random 1-group setup (with strict repartition call count validation)
                Arguments.of(
                        r.nextInt(30) + 5, // highwater mark 5 - 30
                        r.nextInt(8) + 2, // doc count per txn 2 - 10
                        r.nextInt(16) + 1, // max count per rebalance call 1 - 16
                        new int[] { r.nextInt(80) + 20 } // doc count 20 - 100
                ),
                // 1-group, no document move needed in repartitioning
                Arguments.of(20, 5, 5, new int[] { 20 }),
                // 1-group, document move needed in repartitioning
                Arguments.of( 9, 5, 4, new int[] { 20 })
        );
    }

    @ParameterizedTest
    @MethodSource
    void capDocCountMovedDuringRepartitioningMultigroupTest(int highWaterMark,
                                                            int docCountPerTxn,
                                                            int maxCountPerRepartitionCall,
                                                            int... docCounts) throws IOException {
        final Map<String, String> options = Map.of(
                INDEX_PARTITION_BY_FIELD_NAME, "timestamp",
                INDEX_PARTITION_HIGH_WATERMARK, String.valueOf(highWaterMark));
        Pair<Index, Consumer<FDBRecordContext>> indexConsumerPair = setupIndex(options, true, false);
        final Index index = indexConsumerPair.getLeft();
        Consumer<FDBRecordContext> schemaSetup = indexConsumerPair.getRight();

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, docCountPerTxn)
                .addProp(LuceneRecordContextProperties.LUCENE_MAX_DOCUMENTS_TO_MOVE_DURING_REPARTITIONING, maxCountPerRepartitionCall)
                .build();

        class GroupSpec {
            final int value;
            final int docCount;
            final Tuple groupTuple;
            
            GroupSpec(int value, int docCount) {
                this.value = value;
                this.docCount = docCount;
                groupTuple = Tuple.from(value);
            }
        }

        final GroupSpec[] groupSpecs = new GroupSpec[docCounts.length];

        for (int i = 0; i < docCounts.length; i++) {
            groupSpecs[i] = new GroupSpec(i + 1, docCounts[i]);
        }

        final long start = Instant.now().toEpochMilli();
        final String luceneSearch = "text:about";

        Map<Integer, List<Tuple>> primaryKeys = new HashMap<>();
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            for (int k = 0; k < groupSpecs.length; k++) {
                for (int i = 0; i < groupSpecs[k].docCount; i++) {
                    ComplexDocument cd = ComplexDocument.newBuilder()
                            .setGroup(groupSpecs[k].value)
                            .setDocId(1000L * (k + 1) + i)
                            .setIsSeen(true)
                            .setText("A word about what I want to say")
                            .setTimestamp(start + i * 100L + k * 100000L)
                            .setHeader(ComplexDocument.Header.newBuilder().setHeaderId(1000L * (k + 1) - i ))
                            .build();
                    final Tuple primaryKey;
                    primaryKey = recordStore.saveRecord(cd).getPrimaryKey();
                    primaryKeys.computeIfAbsent(groupSpecs[k].value, v -> new ArrayList<>()).add(primaryKey);
                }
            }

            commit(context);
        }

        // initially, all documents are saved into one partition
        for (final GroupSpec groupSpec : groupSpecs) {
            List<LucenePartitionInfoProto.LucenePartitionInfo> partitionInfos = getPartitionMeta(index,
                    groupSpec.groupTuple, contextProps, schemaSetup);
            assertEquals(1, partitionInfos.size());
            assertEquals(groupSpec.docCount, partitionInfos.get(0).getCount());
        }

        // run re-partitioning
        explicitMergeIndex(index, contextProps, schemaSetup);

        // get partitions for all groups, post re-partitioning
        Map<Integer, List<LucenePartitionInfoProto.LucenePartitionInfo>> partitionInfos = new HashMap<>();
        for (GroupSpec groupSpec : groupSpecs) {
            partitionInfos.put(groupSpec.value, getPartitionMeta(index, groupSpec.groupTuple, contextProps, schemaSetup));
        }

        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);

            int actualRepartitionCallCount = getCounter(context, LuceneEvents.Counts.LUCENE_REPARTITION_CALLS).getCount();
            int totalExpectedRepartitionCallCount = 0;

            for (final GroupSpec groupSpec : groupSpecs) {
                List<LucenePartitionInfoProto.LucenePartitionInfo> groupPartitionInfos = partitionInfos.get(groupSpec.value);

                Pair<int[], Integer> spreadAndCallCount = calculateAndValidateRepartitioningExpectations(
                        groupPartitionInfos,
                        groupSpec.docCount,
                        highWaterMark,
                        docCountPerTxn,
                        maxCountPerRepartitionCall,
                        -1);
                totalExpectedRepartitionCallCount += spreadAndCallCount.getRight();
                int[] docDistribution = spreadAndCallCount.getLeft();
                int edge = groupSpec.docCount - docDistribution[0];

                // validate content of partition 0 (original)
                validateDocsInPartition(index, 0, groupSpec.groupTuple, Set.copyOf(primaryKeys.get(groupSpec.value).subList(edge, groupSpec.docCount)), luceneSearch);

                // validate content of rest of partitions
                for (int i = docDistribution.length - 1; i > 0; i--) {
                    validateDocsInPartition(index, i, groupSpec.groupTuple, Set.copyOf(primaryKeys.get(groupSpec.value).subList(edge - docDistribution[i], edge)), luceneSearch);
                    edge = edge - docDistribution[i];
                }
            }

            if (groupSpecs.length > 1) {
                // with multiple groups, it's more complex to determine the exact count of repartition calls, however
                // the combined total should be within "number of groups" of the total of each group's repartition calls had they
                // been repartitioned independently.
                assertTrue(Math.abs(totalExpectedRepartitionCallCount - actualRepartitionCallCount) <= groupSpecs.length);
            } else {
                // 1 group, perform strict validation
                assertEquals(totalExpectedRepartitionCallCount, actualRepartitionCallCount);
            }
        }
    }


    LuceneScanQuery buildLuceneScanQuery(Index index,
                                         boolean isSynthetic,
                                         Comparisons.Type comparisonType,
                                         SortType sortType,
                                         long predicateComparand,
                                         String luceneSearch) {
        Map<Comparisons.Type, BiFunction<Field, Object, QueryComponent>> comparisonToQueryFunction = Map.of(
                Type.GREATER_THAN, Field::greaterThan,
                Type.GREATER_THAN_OR_EQUALS, Field::greaterThanOrEquals,
                Type.LESS_THAN, Field::lessThan,
                Type.LESS_THAN_OR_EQUALS, Field::lessThanOrEquals,
                Type.EQUALS, Field::equalsValue,
                Type.NOT_EQUALS, Field::notEquals
        );

        String partitionFieldName = "timestamp";
        final RecordQuery recordQuery;
        List<QueryComponent> queryComponents = new ArrayList<>();
        if (isSynthetic) {
            queryComponents.add(Query.field("complex").matches(Query.field("group").equalsParameter("group_value")));
            if (comparisonType != Type.NOT_EQUALS) {
                queryComponents.add(Query.field("complex").matches(comparisonToQueryFunction.get(comparisonType).apply(Query.field(partitionFieldName), predicateComparand)));
            }
            if (luceneSearch != null) {
                queryComponents.add(new LuceneQueryComponent(luceneSearch, List.of("simple_text")));
            }
            QueryComponent filter;
            if (queryComponents.size() > 1) {
                filter = Query.and(queryComponents);
            } else {
                filter = queryComponents.get(0);
            }
            recordQuery = RecordQuery.newBuilder()
                    .setRecordType("luceneJoinedPartitionedIdx")
                    .setFilter(filter)
                    .setSort(field("complex").nest("timestamp"), sortType != SortType.ASCENDING)
                    .build();
        } else {
            queryComponents.add(Query.field("group").equalsParameter("group_value"));
            if (comparisonType != Type.NOT_EQUALS) {
                queryComponents.add(comparisonToQueryFunction.get(comparisonType).apply(Query.field(partitionFieldName), predicateComparand));
            }
            if (luceneSearch != null) {
                queryComponents.add(new LuceneQueryComponent(luceneSearch, List.of("text")));
            }
            QueryComponent filter;
            if (queryComponents.size() > 1) {
                filter = Query.and(queryComponents);
            } else {
                filter = queryComponents.get(0);
            }
            recordQuery = RecordQuery.newBuilder()
                    .setRecordType(COMPLEX_DOC)
                    .setFilter(filter)
                    .setSort(field(partitionFieldName), sortType != SortType.ASCENDING)
                    .build();
        }

        LucenePlanner planner = new LucenePlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState(), PlannableIndexTypes.DEFAULT, recordStore.getTimer());
        RecordQueryPlan plan = planner.plan(recordQuery);
        assertTrue(plan instanceof LuceneIndexQueryPlan);
        LuceneIndexQueryPlan luceneIndexQueryPlan = (LuceneIndexQueryPlan) plan;
        LuceneScanParameters scanParameters = (LuceneScanParameters)luceneIndexQueryPlan.getScanParameters();
        return (LuceneScanQuery)scanParameters.bind(recordStore, index, EvaluationContext.forBinding("group_value", 1));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void partitionFieldPredicateDetectionTest(boolean isSynthetic) {
        final Map<String, String> options = Map.of(
                INDEX_PARTITION_BY_FIELD_NAME, isSynthetic ? "complex.timestamp" : "timestamp",
                INDEX_PARTITION_HIGH_WATERMARK, String.valueOf(8));
        Pair<Index, Consumer<FDBRecordContext>> indexConsumerPair = setupIndex(options, true, isSynthetic);
        final Index index = indexConsumerPair.getLeft();
        Consumer<FDBRecordContext> schemaSetup = indexConsumerPair.getRight();

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 8)
                .build();
        final String luceneSearch = isSynthetic ? "simple_text:about" : "text:about";

        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            LucenePartitioner partitioner = getIndexMaintainer(index).getPartitioner();

            for (Comparisons.Type comparisonType : List.of(Type.NOT_EQUALS, Type.LESS_THAN, Type.LESS_THAN_OR_EQUALS, Type.GREATER_THAN, Type.LESS_THAN_OR_EQUALS, Type.EQUALS)) {
                for (SortType sortType : EnumSet.allOf(SortType.class)) {
                    LuceneScanQuery luceneScanQuery = buildLuceneScanQuery(
                            index,
                            isSynthetic,
                            comparisonType,
                            sortType,
                            15L,
                            luceneSearch);

                    LuceneComparisonQuery luceneComparisonQuery = partitioner.checkQueryForPartitionFieldPredicate(luceneScanQuery);
                    final LuceneComparisonQuery expectedLuceneComparisonQuery;
                    if (comparisonType == Type.NOT_EQUALS) {
                        expectedLuceneComparisonQuery = null;
                    } else {
                        expectedLuceneComparisonQuery = new LuceneComparisonQuery(
                                toRangeQuery(Objects.requireNonNull(partitioner.getPartitionFieldNameInLucene()), comparisonType, 15L),
                                partitioner.getPartitionFieldNameInLucene(),
                                comparisonType,
                                15L);
                    }
                    assertEquals(expectedLuceneComparisonQuery, luceneComparisonQuery);
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void partitionFieldPredicateNotDetectedTest(boolean isSynthetic) {
        final Map<String, String> options = Map.of(
                INDEX_PARTITION_BY_FIELD_NAME, isSynthetic ? "complex.timestamp" : "timestamp",
                INDEX_PARTITION_HIGH_WATERMARK, String.valueOf(8));
        Pair<Index, Consumer<FDBRecordContext>> indexConsumerPair = setupIndex(options, true, isSynthetic);
        final Index index = indexConsumerPair.getLeft();
        Consumer<FDBRecordContext> schemaSetup = indexConsumerPair.getRight();

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 8)
                .build();
        final String luceneSearch = isSynthetic ? "simple_text:about" : "text:about";
        final String luceneSearch2 = isSynthetic ? "simple_text:mary" : "text:mary";
        final String textFieldName = isSynthetic ? "simple_text" : "text";
        final String partitionFieldName = isSynthetic ? "complex_timestamp" : "timestamp";

        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            LucenePartitioner partitioner = getIndexMaintainer(index).getPartitioner();

            // test some "negative" use cases, asserting we won't mistakenly detect a partition field predicate and use
            // it to optimize the query
            QueryComponent textSearchPredicate = new LuceneQueryComponent(luceneSearch, List.of(textFieldName));
            QueryComponent secondTextSearchPredicate = new LuceneQueryComponent(luceneSearch2, List.of(textFieldName));
            QueryComponent luceneDslPredicate = new LuceneQueryComponent(luceneSearch + " " + partitionFieldName + ":[1 TO 10]", List.of(textFieldName, partitionFieldName));
            final QueryComponent partitionFieldPredicate;
            final QueryComponent secondPartitionFieldPredicate;
            final QueryComponent groupPredicate;
            if (isSynthetic) {
                partitionFieldPredicate = Query.field("complex").matches(Query.field("timestamp").greaterThan(15L));
                secondPartitionFieldPredicate = Query.field("complex").matches(Query.field("timestamp").greaterThan(55L));
                groupPredicate = Query.field("complex").matches(Query.field("group").equalsParameter("group_value"));
            } else {
                partitionFieldPredicate = Query.field(partitionFieldName).greaterThan(15L);
                secondPartitionFieldPredicate = Query.field(partitionFieldName).greaterThan(55L);
                groupPredicate = Query.field("group").equalsParameter("group_value");
            }
            Map<QueryComponent, QueryPlanningExpectation> filterToOutcomeMap = Map.of(
                    // full disjunction
                    Query.or(groupPredicate, textSearchPredicate, partitionFieldPredicate), new QueryPlanningExpectation(
                            QueryPlanningExpectation.DetectionStatus.NON_LUCENE_PLAN,
                            QueryPlanningExpectation.DetectionStatus.EXCEPTION_THROWN),
                    // group AND (text search OR partition field predicate)
                    Query.and(groupPredicate, Query.or(textSearchPredicate, partitionFieldPredicate)), new QueryPlanningExpectation(
                            QueryPlanningExpectation.DetectionStatus.NON_LUCENE_PLAN,
                            QueryPlanningExpectation.DetectionStatus.NON_LUCENE_PLAN),
                    // conjunction, and multiple partition group predicates
                    Query.and(groupPredicate, textSearchPredicate, partitionFieldPredicate, secondPartitionFieldPredicate), new QueryPlanningExpectation(
                            QueryPlanningExpectation.DetectionStatus.PREDICATE_NOT_SELECTED,
                            QueryPlanningExpectation.DetectionStatus.PREDICATE_NOT_SELECTED),
                    // group AND (text search 1 OR (text search 2 AND partition field))
                    Query.and(groupPredicate, Query.or(textSearchPredicate, Query.and(secondTextSearchPredicate, partitionFieldPredicate))), new QueryPlanningExpectation(
                            QueryPlanningExpectation.DetectionStatus.NON_LUCENE_PLAN,
                            QueryPlanningExpectation.DetectionStatus.NON_LUCENE_PLAN),
                    // no group predicate
                    Query.and(textSearchPredicate, partitionFieldPredicate), new QueryPlanningExpectation(
                            QueryPlanningExpectation.DetectionStatus.NON_LUCENE_PLAN,
                            QueryPlanningExpectation.DetectionStatus.EXCEPTION_THROWN),
                    // Lucene DSL query (both search and timestamp expressed in single, lucene-single, string)
                    Query.and(groupPredicate, luceneDslPredicate), new QueryPlanningExpectation(
                            QueryPlanningExpectation.DetectionStatus.PREDICATE_NOT_SELECTED,
                            QueryPlanningExpectation.DetectionStatus.PREDICATE_NOT_SELECTED)
            );

            for (Map.Entry<QueryComponent, QueryPlanningExpectation> entry : filterToOutcomeMap.entrySet()) {
                QueryComponent filter = entry.getKey();
                QueryPlanningExpectation expectation = entry.getValue();

                RecordQuery recordQuery = RecordQuery.newBuilder()
                        .setRecordType(isSynthetic ? "luceneJoinedPartitionedIdx" : COMPLEX_DOC)
                        .setFilter(filter)
                        .build();

                if (isSynthetic && expectation.forSynthetic == QueryPlanningExpectation.DetectionStatus.EXCEPTION_THROWN ||
                        !isSynthetic && expectation.forSimple == QueryPlanningExpectation.DetectionStatus.EXCEPTION_THROWN) {
                    assertThrows(RecordCoreException.class, () -> planner.plan(recordQuery));
                } else {
                    RecordQueryPlan plan = planner.plan(recordQuery);
                    if (isSynthetic && expectation.forSynthetic == QueryPlanningExpectation.DetectionStatus.NON_LUCENE_PLAN ||
                            !isSynthetic && expectation.forSimple == QueryPlanningExpectation.DetectionStatus.NON_LUCENE_PLAN) {
                        assertFalse(plan instanceof LuceneIndexQueryPlan);
                    } else {
                        LuceneIndexQueryPlan luceneIndexQueryPlan = (LuceneIndexQueryPlan)plan;
                        LuceneScanParameters scanParameters = (LuceneScanParameters)luceneIndexQueryPlan.getScanParameters();
                        LuceneScanQuery luceneScanQuery = (LuceneScanQuery)scanParameters.bind(recordStore, index, EvaluationContext.forBinding("group_value", 1));
                        LuceneComparisonQuery detectedPredicate = partitioner.checkQueryForPartitionFieldPredicate(luceneScanQuery);
                        assertEquals(detectedPredicate != null, expectation == QueryPlanningExpectation.SELECTED);
                    }
                }
            }
        }
    }

    static class QueryPlanningExpectation {
        static final QueryPlanningExpectation SELECTED = new QueryPlanningExpectation(
                DetectionStatus.PREDICATE_SELECTED,
                DetectionStatus.PREDICATE_SELECTED);

        enum DetectionStatus {
            NON_LUCENE_PLAN,
            EXCEPTION_THROWN,
            PREDICATE_SELECTED,
            PREDICATE_NOT_SELECTED
        }

        DetectionStatus forSynthetic;
        DetectionStatus forSimple;

        QueryPlanningExpectation(DetectionStatus forSimple,
                                 DetectionStatus forSynthetic) {
            this.forSynthetic = forSynthetic;
            this.forSimple = forSimple;
        }
    }

    private org.apache.lucene.search.Query toRangeQuery(String fieldName, Type comparisonType, Long comparand) {
        switch (comparisonType) {
            case EQUALS:
                return LongPoint.newExactQuery(fieldName, comparand);
            case LESS_THAN:
                return LongPoint.newRangeQuery(fieldName, Long.MIN_VALUE, comparand - 1);
            case LESS_THAN_OR_EQUALS:
                return LongPoint.newRangeQuery(fieldName, Long.MIN_VALUE, comparand);
            case GREATER_THAN:
                return LongPoint.newRangeQuery(fieldName, comparand + 1, Long.MAX_VALUE);
            case GREATER_THAN_OR_EQUALS:
                return LongPoint.newRangeQuery(fieldName, comparand, Long.MAX_VALUE);
            default:
                throw new IllegalArgumentException("unsupported comparison type: " + comparisonType);
        }
    }

    static Stream<Arguments> functionalPartitionFieldPredicateTest() {
        return Stream.concat(
                Stream.of( 23045978L,
                        98432L,
                        -439208L,
                        -547118062778370833L,
                        -8561053686039912077L).map(Arguments::of),
                RandomizedTestUtils.randomArguments(random -> Arguments.of(random.nextLong())));
    }

    @ParameterizedTest
    @MethodSource
    void functionalPartitionFieldPredicateTest(long seed) {
        Random random = new Random(seed);
        boolean isSynthetic = false;

        final Map<String, String> options = Map.of(
                INDEX_PARTITION_BY_FIELD_NAME, isSynthetic ? "complex.timestamp" : "timestamp",
                INDEX_PARTITION_HIGH_WATERMARK, String.valueOf(3));
        Pair<Index, Consumer<FDBRecordContext>> indexConsumerPair = setupIndex(options, true, isSynthetic);
        final Index index = indexConsumerPair.getLeft();
        Consumer<FDBRecordContext> schemaSetup = indexConsumerPair.getRight();

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 8)
                .build();
        final String luceneSearch = isSynthetic ? "simple_text:about" : "text:about";
        Map<Tuple, Long> primaryKeys = new HashMap<>();
        int docCount = 30;

        for (SortType sortType : EnumSet.allOf(SortType.class)) {
            for (Comparisons.Type comparisonType : List.of(Type.EQUALS, Type.LESS_THAN, Type.LESS_THAN_OR_EQUALS, Type.GREATER_THAN_OR_EQUALS, Type.GREATER_THAN)) {
                try (FDBRecordContext context = openContext(contextProps)) {
                    schemaSetup.accept(context);
                    for (int i = 0; i < docCount; i++) {
                        long timestamp = (long) random.nextInt(10) + 2; // 2 - 11
                        long docId = 1000L + i;
                        ComplexDocument cd = ComplexDocument.newBuilder()
                                .setGroup(1)
                                .setDocId(docId)
                                .setIsSeen(true)
                                .setText("A word about what I want to say")
                                .setTimestamp(timestamp)
                                .setHeader(ComplexDocument.Header.newBuilder().setHeaderId(1000L + i))
                                .build();
                        final Tuple primaryKey;
                        primaryKey = recordStore.saveRecord(cd).getPrimaryKey();
                        primaryKeys.put(primaryKey, timestamp);

                        for (long queriedValue = 1L; queriedValue <= 12L; queriedValue++) {
                            LOGGER.debug("i={}, queriedValue={}, comparisonType={}, sortType={}", i, queriedValue, comparisonType, sortType);
                            LuceneScanQuery luceneScanQuery = buildLuceneScanQuery(index, isSynthetic, comparisonType, sortType, queriedValue, luceneSearch);
                            try (RecordCursor<IndexEntry> indexEntryCursor = recordStore.scanIndex(index, luceneScanQuery, null, ExecuteProperties.newBuilder().setReturnedRowLimit(Integer.MAX_VALUE).build().asScanProperties(false))) {

                                Stream<Tuple> actualKeys = indexEntryCursor.asList().join()
                                        .stream().map(IndexEntry::getPrimaryKey);
                                List<Tuple> expectedKeys = queryLocal(primaryKeys, comparisonType, queriedValue, sortType);
                                if (sortType == SortType.UNSORTED) {
                                    actualKeys = actualKeys.sorted();
                                    expectedKeys.sort(Comparator.naturalOrder());
                                }
                                assertEquals(expectedKeys, actualKeys.collect(Collectors.toList()),
                                        () -> {
                                            LuceneIndexMaintainer indexMaintainer = (LuceneIndexMaintainer)recordStore.getIndexMaintainer(index);
                                            return primaryKeys.entrySet().stream().sorted(Map.Entry.comparingByValue())
                                                    .map(entry -> entry.getValue() + " " + entry.getKey())
                                                    .collect(Collectors.joining(", ", "All data: [", "]\n")) +
                                                    indexMaintainer.getPartitioner().getAllPartitionMetaInfo(Tuple.from(1)).join()
                                                            .stream()
                                                            .sorted(Comparator.comparing(partitionInfo -> Tuple.fromBytes(partitionInfo.getFrom().toByteArray())))
                                                            .map(partitionInfo -> partitionInfo.getId() + " (" + partitionInfo.getCount() + "): [" +
                                                                    Tuple.fromBytes(partitionInfo.getFrom().toByteArray()) + "," +
                                                                    Tuple.fromBytes(partitionInfo.getTo().toByteArray()) + "]")
                                                            .collect(Collectors.joining(", ", "Partitions: [", "]"));
                                        });
                            }
                        }
                    }
                    commit(context);
                }
            }
        }
    }

    private List<Tuple> queryLocal(Map<Tuple, Long> dataset, Comparisons.Type comparisonType, long comparand, SortType sortType) {
        List<Map.Entry<Tuple, Long>> hits = dataset.entrySet()
                .stream()
                .filter(entry -> {
                    long value = entry.getValue();
                    switch (comparisonType) {
                        case EQUALS:
                            return value == comparand;
                        case LESS_THAN:
                            return value < comparand;
                        case LESS_THAN_OR_EQUALS:
                            return value <= comparand;
                        case GREATER_THAN:
                            return value > comparand;
                        case GREATER_THAN_OR_EQUALS:
                            return value >= comparand;
                        default:
                            return false;
                    }
                })
                .collect(Collectors.toList());

        if (sortType == SortType.ASCENDING) {
            hits.sort(Comparator.comparing(Map.Entry<Tuple, Long>::getValue).thenComparing(Map.Entry::getKey));
        } else {
            hits.sort(Comparator.comparing(Map.Entry<Tuple, Long>::getValue).thenComparing(Map.Entry::getKey).reversed());
        }
        return hits.stream().map(Map.Entry::getKey).collect(Collectors.toList());
    }

    static Stream<Arguments> integratedConsolidationTest() {
        return Stream.of(true, false).flatMap(isSynthetic ->
                Stream.of(
                        // arguments: isSynthetic, high, low, repartitionCount, total docs created, counts to delete from partitions

                        // -> start with 3 partitions, 8 docs each,
                        Arguments.of(isSynthetic, 8, 5, 2, 24, new int[] {4, 4, 0}), // delete 4 from oldest, 4 from middle
                        // -> start with 3 partitions, 8 docs each,
                        Arguments.of(isSynthetic, 8, 5, 2, 24, new int[] {2, 4, 2}), // delete 2 from oldest, 4 from middle, 2 from newest
                        // -> start with 2 partitions, 6 and 8 docs respectively,
                        Arguments.of(isSynthetic, 8, 5, 2, 14, new int[] {2, 4}), // delete 2 from oldest, 4 from newest
                        // -> start with 2 partitions, 5 and 8 docs respectively
                        Arguments.of(isSynthetic, 8, 5, 2, 13, new int[] {1, 1}) // delete 1 from oldest, 1 from newest
                ));
    }

    @ParameterizedTest
    @MethodSource
    void integratedConsolidationTest(boolean isSynthetic,
                                     int highWatermark,
                                     int lowWatermark,
                                     int repartitionDocCount,
                                     int docCount,
                                     int... docCountsToDelete) throws IOException {
        final Map<String, String> options = Map.of(
                INDEX_PARTITION_BY_FIELD_NAME, isSynthetic ? "complex.timestamp" : "timestamp",
                PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED, "true",
                INDEX_PARTITION_LOW_WATERMARK, String.valueOf(lowWatermark),
                INDEX_PARTITION_HIGH_WATERMARK, String.valueOf(highWatermark));
        Pair<Index, Consumer<FDBRecordContext>> indexConsumerPair = setupIndex(options, true, isSynthetic);
        final Index index = indexConsumerPair.getLeft();
        Consumer<FDBRecordContext> schemaSetup = indexConsumerPair.getRight();

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, repartitionDocCount)
                .build();

        final long start = Instant.now().toEpochMilli();
        final String textFieldValue = "A word about what I want to say";
        final String luceneSearch = isSynthetic ? "simple_text:about" : "text:about";
        final Tuple groupTuple = Tuple.from(1L);

        Map<Tuple, Tuple> primaryKeyToTimestamp = new HashMap<>();
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(false);
            for (int i = 0; i < docCount; i++) {
                long timestamp = start + i * 100000L;
                long complexDocId = 1000L + i;
                long simpleDocId = 1000L - i;
                final Tuple primaryKey;
                ComplexDocument complexDocument = ComplexDocument.newBuilder()
                        .setGroup(1L)
                        .setDocId(complexDocId)
                        .setIsSeen(true)
                        .setText(textFieldValue)
                        .setTimestamp(timestamp)
                        .setHeader(ComplexDocument.Header.newBuilder().setHeaderId(1000L - i))
                        .build();
                if (isSynthetic) {
                    TestRecordsTextProto.SimpleDocument simpleDocument = TestRecordsTextProto.SimpleDocument.newBuilder()
                            .setGroup(1L)
                            .setDocId(simpleDocId)
                            .setText(textFieldValue)
                            .build();
                    final Tuple syntheticRecordTypeKey = recordStore.getRecordMetaData()
                            .getSyntheticRecordType("luceneJoinedPartitionedIdx")
                            .getRecordTypeKeyTuple();
                    primaryKey = Tuple.from(syntheticRecordTypeKey.getItems().get(0),
                            recordStore.saveRecord(complexDocument).getPrimaryKey().getItems(),
                            recordStore.saveRecord(simpleDocument).getPrimaryKey().getItems());
                } else {
                    primaryKey = recordStore.saveRecord(complexDocument).getPrimaryKey();
                }
                primaryKeyToTimestamp.put(primaryKey, Tuple.from(timestamp));
            }
            commit(context);
        }

        // initially, all documents are saved into one partition
        List<LucenePartitionInfoProto.LucenePartitionInfo> partitionInfos = getPartitionMeta(index,
                groupTuple, contextProps, schemaSetup);
        assertEquals(1, partitionInfos.size());
        assertEquals(docCount, partitionInfos.get(0).getCount());

        // run re-partitioning
        explicitMergeIndex(index, contextProps, schemaSetup);

        // get partitions for all groups, post re-partitioning
        partitionInfos = getPartitionMeta(index, groupTuple, contextProps, schemaSetup);

        LuceneIndexTestValidator luceneIndexTestValidator =
                new LuceneIndexTestValidator(() -> openContext(contextProps), context -> {
                    schemaSetup.accept(context);
                    return recordStore;
                });
        luceneIndexTestValidator.validate(Objects.requireNonNull(index), Map.of(groupTuple, primaryKeyToTimestamp), luceneSearch, false);

        // next delete some docs
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(false);

            Collections.reverse(partitionInfos);
            List<Tuple> sortedPrimaryKeys = primaryKeyToTimestamp.keySet().stream().sorted().collect(Collectors.toList());
            int docOffset = 0;
            for (int i = 0; i < docCountsToDelete.length; i++) {
                int howManyToDelete = docCountsToDelete[i];
                for (int j = 0; j < howManyToDelete; j++) {
                    Tuple primaryKeyToDelete = sortedPrimaryKeys.get(docOffset + j);
                    if (isSynthetic) {
                        Tuple complexDocKey = Tuple.fromList((List<?>)primaryKeyToDelete.get(1));
                        Tuple simpleDocKey = Tuple.fromList((List<?>)primaryKeyToDelete.get(2));
                        recordStore.deleteRecord(complexDocKey);
                        recordStore.deleteRecord(simpleDocKey);
                    } else {
                        recordStore.deleteRecord(primaryKeyToDelete);
                    }
                    primaryKeyToTimestamp.remove(primaryKeyToDelete);
                }
                docOffset += partitionInfos.get(i).getCount();
            }
            context.commit();
        }
        // run re-partitioning
        explicitMergeIndex(index, contextProps, schemaSetup);

        // validate that after merge, partitions have been adjusted as expected
        luceneIndexTestValidator.validate(Objects.requireNonNull(index), Map.of(groupTuple, primaryKeyToTimestamp), luceneSearch, false);
    }

    static Stream<Arguments> simplePartitionConsolidationTest() {
        // arguments:
        // - low watermark
        // - high watermark
        // - repartition doc count
        // - the first int array is the count of docs in each partition, starting with
        //   the oldest partition -> newest
        // - the second int array is the resulting count of docs in each partition
        //   after merge, in the same order

        return Stream.of(
                // consolidate two low partitions into one
                Arguments.of(2, 4, 3, new int[] {1, 1}, new int[] {2}),
                // consolidate a partition into its previous neighbor
                Arguments.of(2, 4, 3, new int[] {2, 2, 1}, new int[] {2, 3}),
                // consolidate a partition falling between two partitions with capacity into its previous neighbor
                Arguments.of(3, 7, 3, new int[] {5, 2, 5}, new int[] {7, 5}),
                // consolidate a partition falling between two partitions which individually don't have enough
                // capacity, but together they do
                Arguments.of(3, 7, 3, new int[] {6, 2, 6}, new int[] {7, 7}),
                // cannot consolidate a partition that has no neighbors with capacity
                Arguments.of(3, 7, 3, new int[] {6, 2, 7}, new int[] {6, 2, 7}),
                // cannot consolidate a partition that has no neighbors with capacity
                Arguments.of(3, 7, 3, new int[] {7, 2, 6}, new int[] {7, 2, 6}),
                // cannot consolidate partitions that have no neighbors with capacity
                Arguments.of(3, 7, 3, new int[] {6, 2, 7, 3}, new int[] {6, 2, 7, 3}),
                Arguments.of(4, 7, 3, new int[] {6, 3, 4, 5, 5, 5, 5}, new int[] {6, 7, 5, 5, 5, 5}),
                // consolidate one partition that has a neighbor with capacity, while another
                // that doesn't, won't be consolidated
                Arguments.of(3, 7, 3, new int[] {6, 2, 6, 1}, new int[] {6, 2, 7}),
                // splitting one partition, removes the need to consolidate a previous low partition
                Arguments.of(3, 7, 3, new int[] {6, 1, 8}, new int[] {6, 4, 5}),
                // consolidating two neighboring partitions into their left and right neighbors
                Arguments.of(3, 7, 3, new int[] {6, 1, 1, 6}, new int[] {7, 7}),
                // two low partitions merge into one
                Arguments.of(5, 7, 3, new int[] {3, 4}, new int[] {7}),
                // multiple-stage split
                Arguments.of(3, 7, 3, new int[] { 15 }, new int[] {6, 3, 6}),
                // move more than 2x the repartition count
                Arguments.of(3, 6, 2, new int[] { 13, 2 }, new int[] {6, 2, 5, 2}),
                // ensure it won't move 3 from second partition to first
                Arguments.of(10, 20, 3, new int[] {15, 8, 20}, new int[] {15, 8, 20})
        );
    }

    @ParameterizedTest
    @MethodSource
    void simplePartitionConsolidationTest(int lowWatermark,
                                          int highWatermark,
                                          int repartitionCount,
                                          int[] initialPartitionCounts,
                                          int[] expectedPartitionCounts) throws IOException {
        boolean isSynthetic = false;

        Map<String, String> options = Map.of(
                INDEX_PARTITION_BY_FIELD_NAME, "timestamp",
                INDEX_PARTITION_HIGH_WATERMARK, String.valueOf(highWatermark),
                INDEX_PARTITION_LOW_WATERMARK, String.valueOf(lowWatermark)
                );
        Pair<Index, Consumer<FDBRecordContext>> indexConsumerPair = setupIndex(options, true, isSynthetic);
        Index index = indexConsumerPair.getLeft();
        Consumer<FDBRecordContext> schemaSetup = indexConsumerPair.getRight();

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, repartitionCount)
                .build();

        final String luceneSearch = "text:vision";
        Map<Tuple, Map<Tuple, Tuple>> createdKeys;
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(false);
            createdKeys = createPartitionsAndComplexDocs(index, initialPartitionCounts);
            context.commit();
        }

        List<LucenePartitionInfoProto.LucenePartitionInfo> partitionInfos = getPartitionMeta(index,
                Tuple.from(1), contextProps, schemaSetup);

        // getPartitionMeta() returns partitions sorted by `from` in descending order.
        // the rest of this test assumes they're sorted in ascending order, so we reverse.
        Collections.reverse(partitionInfos);

        // assert sane initial setup
        assertEquals(initialPartitionCounts.length, partitionInfos.size());
        assertArrayEquals(initialPartitionCounts, partitionInfos.stream().mapToInt(LucenePartitionInfoProto.LucenePartitionInfo::getCount).toArray());

        explicitMergeIndex(index, contextProps, schemaSetup);

        partitionInfos = getPartitionMeta(index, Tuple.from(1), contextProps, schemaSetup);
        partitionInfos.sort(Comparator.comparing(pInfo -> Tuple.fromBytes(pInfo.getFrom().toByteArray())));

        // assert correct partition setup after repartitioning
        assertEquals(expectedPartitionCounts.length, partitionInfos.size());
        assertArrayEquals(expectedPartitionCounts, partitionInfos.stream().mapToInt(LucenePartitionInfoProto.LucenePartitionInfo::getCount).toArray());

        LuceneIndexTestValidator luceneIndexTestValidator =
                new LuceneIndexTestValidator(() -> openContext(contextProps), context -> {
                    schemaSetup.accept(context);
                    return recordStore;
                });
        luceneIndexTestValidator.validate(Objects.requireNonNull(index), createdKeys, luceneSearch, false);
    }

    @Test
    void simpleCrossPartitionQuery() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_PARTITIONED);
            setTimestamps();
            createDualPartitionsWithComplexDocs(1);

            // query will return results from both partitions
            assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(1, 0L), Tuple.from(1, 1000L)),
                    recordStore.scanIndex(COMPLEX_PARTITIONED, groupedTextSearch(COMPLEX_PARTITIONED, "text:propose", 1), null, ScanProperties.FORWARD_SCAN));
            assertEquals(2, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

            commit(context);
        }
    }

    static Stream<Arguments> findStartingPartitionTest() {
        return Stream.concat(
                Stream.of(true, false).map(isSynthetic -> Arguments.of(isSynthetic, 1714058544895L)),
                RandomizedTestUtils.randomArguments(random -> Arguments.of(random.nextBoolean(), random.nextLong())));
    }

    @ParameterizedTest
    @MethodSource
    void findStartingPartitionTest(boolean isSynthetic, long startTime) {

        final Map<String, String> options = Map.of(
                INDEX_PARTITION_BY_FIELD_NAME, isSynthetic ? "complex.timestamp" : "timestamp",
                INDEX_PARTITION_HIGH_WATERMARK, String.valueOf(8));
        Pair<Index, Consumer<FDBRecordContext>> indexConsumerPair = setupIndex(options, true, isSynthetic);
        final Index index = indexConsumerPair.getLeft();
        Consumer<FDBRecordContext> schemaSetup = indexConsumerPair.getRight();

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 8)
                .build();
        final String luceneSearch = isSynthetic ? "simple_text:about" : "text:about";

        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            LucenePartitioner partitioner = getIndexMaintainer(index).getPartitioner();
            Tuple groupKey = Tuple.from(1L);
            
            // timestamps present in partition keys
            long time0 = Math.abs(startTime);
            long time1 = time0 + 1000;
            long time2 = time0 + 2000;
            
            // timestamp not in partition keys, but within a partition
            long time1_2 = time1 + 500; // between time1 and time2

            // timestamps outside the bounds of the partitions
            long timeTooOld = time0 - 500;
            long timeTooNew = time2 + 500;

            Map<Long, String> timesForLogging = Map.of(
                    time0, "time0",
                    time1, "time1",
                    time2, "time2",
                    time1_2, "time1_2",
                    timeTooOld, "timeTooOld",
                    timeTooNew, "timeTooNew");

            //
            // p0: from: t0+1200 to: t0+1300
            // p1: from: t0+1301 to: t1+1400
            // p2: from: t1+1410 to: t2+1500
            // p3: from: t2+1510 to: t2+1600
            // create partition metadata directly rather than depending on repartitioning to ensure that the partitions
            // have predetermined boundaries.
            createPartitionMetadata(index, groupKey, 0, time0, time0, Tuple.from(1, 1300), Tuple.from(1, 1400));
            createPartitionMetadata(index, groupKey, 1, time0, time1, Tuple.from(1, 1500), Tuple.from(1, 1100));
            createPartitionMetadata(index, groupKey, 2, time1, time2 - 200, Tuple.from(1, 1700), Tuple.from(1, 1000));
            createPartitionMetadata(index, groupKey, 3, time2, time2, Tuple.from(1, 900), Tuple.from(1, 910));

            Map<Long, Map<Comparisons.Type, Map<SortType, Integer>>> startingPartitionExpectation = new HashMap<>();

            // =====================================
            // Expectations for time0
            // =====================================
            Map<Comparisons.Type, Map<SortType, Integer>> expectationsForTime0 = Map.of(
                    Type.GREATER_THAN, Map.of(SortType.ASCENDING, 1, SortType.DESCENDING, 3, SortType.UNSORTED, 3),
                    Type.GREATER_THAN_OR_EQUALS, Map.of(SortType.ASCENDING, 0, SortType.DESCENDING, 3, SortType.UNSORTED, 3),
                    Type.LESS_THAN, Map.of(SortType.ASCENDING, -1, SortType.DESCENDING, -1, SortType.UNSORTED, -1),
                    Type.LESS_THAN_OR_EQUALS, Map.of(SortType.ASCENDING, 0, SortType.DESCENDING, 1, SortType.UNSORTED, 1),
                    Type.EQUALS, Map.of(SortType.ASCENDING, 0, SortType.DESCENDING, 1, SortType.UNSORTED, 1));
            startingPartitionExpectation.put(time0, expectationsForTime0);

            // =====================================
            // Expectations for time1
            // =====================================
            Map<Comparisons.Type, Map<SortType, Integer>> expectationsForTime1 = Map.of(
                    Type.GREATER_THAN, Map.of(SortType.ASCENDING, 2, SortType.DESCENDING, 3, SortType.UNSORTED, 3),
                    Type.GREATER_THAN_OR_EQUALS, Map.of(SortType.ASCENDING, 1, SortType.DESCENDING, 3, SortType.UNSORTED, 3),
                    Type.LESS_THAN, Map.of(SortType.ASCENDING, 0, SortType.DESCENDING, 1, SortType.UNSORTED, 1),
                    Type.LESS_THAN_OR_EQUALS, Map.of(SortType.ASCENDING, 0, SortType.DESCENDING, 2, SortType.UNSORTED, 2),
                    Type.EQUALS, Map.of(SortType.ASCENDING, 1, SortType.DESCENDING, 2, SortType.UNSORTED, 2));
            startingPartitionExpectation.put(time1, expectationsForTime1);

            // =====================================
            // Expectations for time2
            // =====================================
            Map<Comparisons.Type, Map<SortType, Integer>> expectationsForTime2 = Map.of(
                    Type.GREATER_THAN, Map.of(SortType.ASCENDING, 3, SortType.DESCENDING, 3, SortType.UNSORTED, 3),
                    Type.GREATER_THAN_OR_EQUALS, Map.of(SortType.ASCENDING, 3, SortType.DESCENDING, 3, SortType.UNSORTED, 3),
                    Type.LESS_THAN, Map.of(SortType.ASCENDING, 0, SortType.DESCENDING, 2, SortType.UNSORTED, 2),
                    Type.LESS_THAN_OR_EQUALS, Map.of(SortType.ASCENDING, 0, SortType.DESCENDING, 3, SortType.UNSORTED, 3),
                    Type.EQUALS, Map.of(SortType.ASCENDING, 3, SortType.DESCENDING, 3, SortType.UNSORTED, 3));
            startingPartitionExpectation.put(time2, expectationsForTime2);

            // =====================================
            // Expectations for time1_2
            // =====================================
            Map<Comparisons.Type, Map<SortType, Integer>> expectationsForTime1_2 = Map.of(
                    Type.GREATER_THAN, Map.of(SortType.ASCENDING, 2, SortType.DESCENDING, 3, SortType.UNSORTED, 3),
                    Type.GREATER_THAN_OR_EQUALS, Map.of(SortType.ASCENDING, 2, SortType.DESCENDING, 3, SortType.UNSORTED, 3),
                    Type.LESS_THAN, Map.of(SortType.ASCENDING, 0, SortType.DESCENDING, 2, SortType.UNSORTED, 2),
                    Type.LESS_THAN_OR_EQUALS, Map.of(SortType.ASCENDING, 0, SortType.DESCENDING, 2, SortType.UNSORTED, 2),
                    Type.EQUALS, Map.of(SortType.ASCENDING, 2, SortType.DESCENDING, 2, SortType.UNSORTED, 2));
            startingPartitionExpectation.put(time1_2, expectationsForTime1_2);

            // =====================================
            // Expectations for timeTooOld
            // =====================================
            Map<Comparisons.Type, Map<SortType, Integer>> expectationsForTimeTooOld = Map.of(
                    Type.GREATER_THAN, Map.of(SortType.ASCENDING, 0, SortType.DESCENDING, 3, SortType.UNSORTED, 3),
                    Type.GREATER_THAN_OR_EQUALS, Map.of(SortType.ASCENDING, 0, SortType.DESCENDING, 3, SortType.UNSORTED, 3),
                    Type.LESS_THAN, Map.of(SortType.ASCENDING, -1, SortType.DESCENDING, -1, SortType.UNSORTED, -1),
                    Type.LESS_THAN_OR_EQUALS, Map.of(SortType.ASCENDING, -1, SortType.DESCENDING, -1, SortType.UNSORTED, -1),
                    Type.EQUALS, Map.of(SortType.ASCENDING, -1, SortType.DESCENDING, -1, SortType.UNSORTED, -1));
            startingPartitionExpectation.put(timeTooOld, expectationsForTimeTooOld);

            // =====================================
            // Expectations for timeTooNew
            // =====================================
            Map<Comparisons.Type, Map<SortType, Integer>> expectationsForTimeTooNew = Map.of(
                    Type.GREATER_THAN, Map.of(SortType.ASCENDING, -1, SortType.DESCENDING, -1, SortType.UNSORTED, -1),
                    Type.GREATER_THAN_OR_EQUALS, Map.of(SortType.ASCENDING, -1, SortType.DESCENDING, -1, SortType.UNSORTED, -1),
                    Type.LESS_THAN, Map.of(SortType.ASCENDING, 0, SortType.DESCENDING, 3, SortType.UNSORTED, 3),
                    Type.LESS_THAN_OR_EQUALS, Map.of(SortType.ASCENDING, 0, SortType.DESCENDING, 3, SortType.UNSORTED, 3),
                    Type.EQUALS, Map.of(SortType.ASCENDING, -1, SortType.DESCENDING, -1, SortType.UNSORTED, -1));
            startingPartitionExpectation.put(timeTooNew, expectationsForTimeTooNew);

            // check all combinations against expectations
            for (Long predicateComparand : List.of(time0, time1, time2, time1_2, timeTooOld, timeTooNew)) {
                for (Comparisons.Type comparisonType : List.of(Type.NOT_EQUALS, Type.GREATER_THAN, Type.GREATER_THAN_OR_EQUALS, Type.LESS_THAN, Type.LESS_THAN_OR_EQUALS, Type.EQUALS)) {
                    for (SortType sortType : EnumSet.allOf(SortType.class)) {
                        LOGGER.debug("comparison: {} sort: {} time: {}", comparisonType, sortType, timesForLogging.get(predicateComparand));
                        LuceneScanQuery luceneScanQuery = buildLuceneScanQuery(index, isSynthetic, comparisonType, sortType, predicateComparand, luceneSearch);
                        LucenePartitionInfoProto.LucenePartitionInfo selectedPartitionInfo = partitioner.selectQueryPartition(groupKey, luceneScanQuery).startPartition;
                        if (comparisonType == Type.NOT_EQUALS) {
                            // comparisonType == NOT_EQUALS means no partition field predicate was in the query,
                            // in which case we start from oldest partition when ASCENDING, and latest partition
                            // otherwise
                            assertNotNull(selectedPartitionInfo);
                            assertTrue((sortType == SortType.ASCENDING && selectedPartitionInfo.getId() == 0)
                                    || selectedPartitionInfo.getId() == 3);
                        } else {
                            assertEquals(startingPartitionExpectation.get(predicateComparand).get(comparisonType).get(sortType), selectedPartitionInfo == null ? -1 : selectedPartitionInfo.getId());
                        }
                    }
                }
            }
        }
    }

    /**
     * test LIMIT spanning partitions.
     */
    @Test
    void testPartitionedLimit() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_PARTITIONED);
            setTimestamps();
            createDualPartitionsWithComplexDocs(15);

            // get 20 records (15 from partition 1, and 5 from partition 0)
            assertIndexEntryPrimaryKeyTuples(makeKeyTuples(1L, 1000, 1014, 0, 4), recordStore.scanIndex(COMPLEX_PARTITIONED, groupedTextSearch(COMPLEX_PARTITIONED, "text:propose", 1), null, ExecuteProperties.newBuilder().setReturnedRowLimit(20).build().asScanProperties(false)));

            validatePartitionSegmentIntegrity(COMPLEX_PARTITIONED, context, 1, 0, 1);
        }
    }

    /**
     * test SKIP spanning partitions.
     */
    @Test
    void testPartitionedSkip() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_PARTITIONED);
            setTimestamps();
            createDualPartitionsWithComplexDocs(30);

            // 25 messages from partition 0 (all partition 1 skipped)
            assertIndexEntryPrimaryKeyTuples(makeKeyTuples(1L, 5, 29), recordStore.scanIndex(COMPLEX_PARTITIONED, groupedTextSearch(COMPLEX_PARTITIONED, "text:propose", 1), null, ExecuteProperties.newBuilder().setSkip(35).build().asScanProperties(false)));

            validatePartitionSegmentIntegrity(COMPLEX_PARTITIONED, context, 1, 0, 1);
        }
    }

    /**
     * test skip with limit spanning partitions.
     */
    @Test
    void testPartitionedSkipWithLimit() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_PARTITIONED);
            setTimestamps();
            createDualPartitionsWithComplexDocs(30);

            // 20 messages from partition 0 (all partition 1 skipped)
            assertIndexEntryPrimaryKeyTuples(makeKeyTuples(1L, 5, 24), recordStore.scanIndex(COMPLEX_PARTITIONED, groupedTextSearch(COMPLEX_PARTITIONED, "text:propose", 1), null, ExecuteProperties.newBuilder().setReturnedRowLimit(20).setSkip(35).build().asScanProperties(false)));

            validatePartitionSegmentIntegrity(COMPLEX_PARTITIONED, context, 1, 0, 1);
        }
    }

    /**
     * test limit with continuation spanning partitions.
     */
    @Test
    void testPartitionedLimitWithContinuation() throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_PARTITIONED);
            setTimestamps();
            createDualPartitionsWithComplexDocs(10);

            RecordCursor<IndexEntry> indexEntryCursor = recordStore.scanIndex(COMPLEX_PARTITIONED, groupedTextSearch(COMPLEX_PARTITIONED, "text:propose", 1), null, ExecuteProperties.newBuilder().setReturnedRowLimit(5).build().asScanProperties(false));

            // Get 5 results and continuation
            List<IndexEntry> entries = indexEntryCursor.asList().join();
            assertEquals(5, entries.size());
            assertEquals(5, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            RecordCursorResult<IndexEntry> lastResult = indexEntryCursor.onNext().get();
            assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, lastResult.getNoNextReason());
            LuceneContinuationProto.LuceneIndexContinuation parsed = LuceneContinuationProto.LuceneIndexContinuation.parseFrom(lastResult.getContinuation().toBytes());
            // we stopped in partition 1
            assertEquals(1, parsed.getPartitionId());

            RecordCursor<IndexEntry> indexEntryCursor2 = recordStore.scanIndex(COMPLEX_PARTITIONED, groupedTextSearch(COMPLEX_PARTITIONED, "text:propose", 1), lastResult.getContinuation().toBytes(), ExecuteProperties.newBuilder().setReturnedRowLimit(10).build().asScanProperties(false));
            // Get 10 results and continuation
            List<IndexEntry> entries2 = indexEntryCursor2.asList().join();
            assertEquals(10, entries2.size());
            RecordCursorResult<IndexEntry> lastResult2 = indexEntryCursor2.onNext().get();
            assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, lastResult.getNoNextReason());
            LuceneContinuationProto.LuceneIndexContinuation parsed2 = LuceneContinuationProto.LuceneIndexContinuation.parseFrom(lastResult2.getContinuation().toBytes());
            // we stopped in partition 0
            assertEquals(0, parsed2.getPartitionId());

            // assert we got the right docs
            assertEquals(makeKeyTuples(1L, 1005, 1009, 0, 4), entries2.stream().map(IndexEntry::getPrimaryKey).collect(Collectors.toSet()));

            validatePartitionSegmentIntegrity(COMPLEX_PARTITIONED, context, 1, 0, 1);
        }
    }

    /**
     * test cross partition limit query with multiple scans.
     */
    @Test
    void testPartitionedLimitNeedsMultipleScans() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_PARTITIONED);
            setTimestamps();
            createDualPartitionsWithComplexDocs(300);

            assertEquals(451, recordStore.scanIndex(COMPLEX_PARTITIONED, groupedTextSearch(COMPLEX_PARTITIONED, "text:propose", 1), null, ExecuteProperties.newBuilder().setReturnedRowLimit(451).build().asScanProperties(false))
                    .getCount().join());
            assertEquals(3, getCounter(context, LuceneEvents.Events.LUCENE_INDEX_SCAN).getCount());
            assertEquals(451, getCounter(context, LuceneEvents.Counts.LUCENE_SCAN_MATCHED_DOCUMENTS).getCount());

            validatePartitionSegmentIntegrity(COMPLEX_PARTITIONED, context, 1, 0, 1);
        }
    }

    /**
     * test cross partition skip over max page size.
     */
    @Test
    void testPartitionedSkipOverMaxPageSize() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_PARTITIONED);
            setTimestamps();
            createDualPartitionsWithComplexDocs(150);
            assertEquals(99, recordStore.scanIndex(COMPLEX_PARTITIONED, groupedTextSearch(COMPLEX_PARTITIONED, "text:propose", 1), null, ExecuteProperties.newBuilder().setReturnedRowLimit(351).setSkip(201).build().asScanProperties(false))
                    .getCount().join());
            assertEquals(3, getCounter(context, LuceneEvents.Events.LUCENE_INDEX_SCAN).getCount());
            assertEquals(300, getCounter(context, LuceneEvents.Counts.LUCENE_SCAN_MATCHED_DOCUMENTS).getCount());

            validatePartitionSegmentIntegrity(COMPLEX_PARTITIONED, context, 1, 0, 1);
        }
    }

    @Test
    void testPartitionedSorted() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_PARTITIONED);
            setTimestamps();
            createDualPartitionsWithComplexDocs(10);

            LuceneScanQuery scanQuery = (LuceneScanQuery)groupedSortedTextSearch(COMPLEX_PARTITIONED, "text:propose", new Sort(new SortField("timestamp", SortField.Type.LONG, true)), 1);
            RecordCursor<IndexEntry> cursor = recordStore.scanIndex(COMPLEX_PARTITIONED, scanQuery, null, ExecuteProperties.newBuilder().setReturnedRowLimit(15).build().asScanProperties(false));
            List<IndexEntry> entries = cursor.asList().join();
            assertEquals(15, entries.size());
            assertEquals(15, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

            List<Long> timestamps = entries.stream().map(a -> ((FieldDoc)((LuceneRecordCursor.ScoreDocIndexEntry)a).getScoreDoc()).fields[0]).map(Long.class::cast).collect(Collectors.toList());
            Comparator<Long> comparator = Long::compareTo;
            assertTrue(Comparators.isInOrder(timestamps, comparator.reversed()));

            validatePartitionSegmentIntegrity(COMPLEX_PARTITIONED, context, 1, 0, 1);
        }
    }

    private Set<Tuple> makeKeyTuples(long group, int... ranges) {
        int[] rangeList = Arrays.stream(ranges).toArray();
        if (rangeList.length == 0 || rangeList.length % 2 == 1) {
            throw new IllegalArgumentException("specify ranges as pairs of (from, to)");
        }
        Set<Tuple> tuples = new HashSet<>();
        for (int i = 0; i < rangeList.length - 1; i += 2) {
            for (int j = rangeList[i]; j <= rangeList[i + 1]; j++) {
                tuples.add(Tuple.from(group, j));
            }
        }
        return tuples;
    }

    private void setTimestamps() {
        timestamp60DaysAgo = Instant.now().minus(60, ChronoUnit.DAYS).toEpochMilli();
        timestamp30DaysAgo = Instant.now().minus(30, ChronoUnit.DAYS).toEpochMilli();
        timestamp29DaysAgo = Instant.now().minus(29, ChronoUnit.DAYS).toEpochMilli();
        yesterday = Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli();
    }

    Map<Tuple, Map<Tuple, Tuple>> createPartitionsAndComplexDocs(Index index, int[] docCounts) {
        // make partition ids not sequential relative to their from/to ranges
        List<Integer> partitionIds = IntStream.rangeClosed(0, docCounts.length - 1).boxed().collect(Collectors.toList());
        Collections.shuffle(partitionIds);

        Map<Tuple, Map<Tuple, Tuple>> keys = new HashMap<>();
        Tuple groupingKey = Tuple.from(1L);
        keys.put(groupingKey, new HashMap<>());
        long startTime = 1000;
        for (int i = 0; i < docCounts.length; i++) {
            long from = startTime * (i + 1);
            long to = from + 999;

            createPartitionMetadata(index, groupingKey, partitionIds.get(i), from, to);

            for (int j = 0; j < docCounts[i]; j++) {
                long timestamp = ThreadLocalRandom.current().nextLong(from, to + 1);
                keys.get(groupingKey).put(recordStore.saveRecord(createComplexDocument(i * 100L + j, ENGINEER_JOKE, 1, timestamp)).getPrimaryKey(), Tuple.from(timestamp));
            }
        }
        return keys;
    }

    void createDualPartitionsWithComplexDocs(int docCount) {
        createDualPartitionsWithComplexDocs(COMPLEX_PARTITIONED, docCount);
    }

    void createDualPartitionsWithComplexDocs(Index index, int docCount) {
        // two partitions: 0 and 1. 0 has older messages, 1 has newer.
        // doc keys in 0 start at (1, 0), doc keys in 1 start at (1, 1000)
        createPartitionMetadata(index, Tuple.from(1L), 0, timestamp60DaysAgo, timestamp30DaysAgo);
        createPartitionMetadata(index, Tuple.from(1L), 1, timestamp29DaysAgo, yesterday);
        for (int i = 0; i < docCount; i++) {
            recordStore.saveRecord(createComplexDocument(i, ENGINEER_JOKE, 1, ThreadLocalRandom.current().nextLong(timestamp60DaysAgo, timestamp30DaysAgo + 1)));
        }
        for (int i = 0; i < docCount; i++) {
            recordStore.saveRecord(createComplexDocument(1000L + i, ENGINEER_JOKE, 1, ThreadLocalRandom.current().nextLong(timestamp29DaysAgo, yesterday + 1)));
        }
    }

    void createPartitionMetadata(Index index, Tuple groupKey, int partitionId, long fromTimestamp, long toTimestamp, Tuple fromPrimaryKey, Tuple toPrimaryKey) {
        Tuple from = Tuple.from(fromTimestamp).add(fromPrimaryKey);
        Tuple to = Tuple.from(toTimestamp).add(toPrimaryKey);
        LucenePartitionInfoProto.LucenePartitionInfo partitionInfo = LucenePartitionInfoProto.LucenePartitionInfo.newBuilder()
                .setCount(0)
                .setFrom(ByteString.copyFrom(from.pack()))
                .setTo(ByteString.copyFrom(to.pack()))
                .setId(partitionId)
                .build();

        byte[] primaryKey = recordStore.indexSubspace(index).pack(groupKey.add(PARTITION_META_SUBSPACE).addAll(from));
        recordStore.getContext().ensureActive().set(primaryKey, partitionInfo.toByteArray());
    }

    void createPartitionMetadata(Index index, Tuple groupKey, int partitionId, long fromTimestamp, long toTimestamp) {
        Tuple from = Tuple.from(fromTimestamp).add(Tuple.from(1, 0));
        Tuple to = Tuple.from(toTimestamp).add(Tuple.from(1, Long.MAX_VALUE));
        LucenePartitionInfoProto.LucenePartitionInfo partitionInfo = LucenePartitionInfoProto.LucenePartitionInfo.newBuilder()
                .setCount(0)
                .setFrom(ByteString.copyFrom(from.pack()))
                .setTo(ByteString.copyFrom(to.pack()))
                .setId(partitionId)
                .build();

        byte[] primaryKey = recordStore.indexSubspace(index).pack(groupKey.add(PARTITION_META_SUBSPACE).addAll(from));
        recordStore.getContext().ensureActive().set(primaryKey, partitionInfo.toByteArray());
    }

    void validatePartitionSegmentIntegrity(Index index, FDBRecordContext context, long group, int... partitionIds) {
        for (int partitionId : partitionIds) {
            final Subspace subspace = recordStore.indexSubspace(COMPLEX_PARTITIONED).subspace(Tuple.from(group, LucenePartitioner.PARTITION_DATA_SUBSPACE).add(partitionId));
            validateSegmentAndIndexIntegrity(index, subspace, context, "_0.cfs");
        }
    }

    // ==================== NON-PARTITIONED TESTS =====================
    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void simpleInsertAndSearch(IndexedType indexedType) {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                Tuple primaryKey = createComplexRecordJoinedToSimple(2, 1623L, 1623L, ENGINEER_JOKE, "", true, System.currentTimeMillis(), 0);
                createComplexRecordJoinedToSimple(1, 1547L, 1547L, WAYLON, "", true, System.currentTimeMillis(), 0);
                timer.reset();
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:\"propose a Vision\""), null, ScanProperties.FORWARD_SCAN));
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
                recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"propose a Vision\""), null, ScanProperties.FORWARD_SCAN));
            }

            assertEquals(1, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index), context, "_0.cfs");
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void largeMetadataTest(LuceneIndexTestUtils.IndexedType indexedType) {
        // Test a document with many fields, where the field metadata is larger than a data block

        final Index index = indexedType.getIndex(MANY_FIELDS_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToManyFields(metaDataBuilder, index));
                Tuple primaryKey = createComplexRecordJoinedToManyFields(1, 1623L, 1623L, "propose a Vision", "", true, System.currentTimeMillis(), 0);
                createComplexRecordJoinedToManyFields(2, 1547L, 1547L, "different smoochies", "", false, System.currentTimeMillis(), 0);
                timer.reset();
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "many_text0:Vision AND many_bool0: true"), null, ScanProperties.FORWARD_SCAN));
                // is 7 reasonable / expected ?
            } else {
                rebuildIndexMetaData(context, MANY_FIELDS_DOC, index);
                recordStore.saveRecord(LuceneIndexTestUtils.createManyFieldsDocument(1623L, "propose a Vision", 1L, true));
                recordStore.saveRecord(LuceneIndexTestUtils.createManyFieldsDocument(1547L, "different smoochies", 2L, false));

                assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(1623L)),
                        recordStore.scanIndex(index, fullTextSearch(index, "text0:Vision AND bool0: true"), null, ScanProperties.FORWARD_SCAN));
            }

            assertEquals(1, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index), context, "_0.cfs");
        }
    }

    /**
     * Make sure the text search for individual fields is not confused when there are multiple fields in the
     * fieldsFormat schema.
     * Fields are overlapping (0 and 1).
     */
    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void differentFieldSearch(IndexedType indexedType) {
        final Index index = indexedType.getIndex(MANY_FIELDS_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToManyFields(metaDataBuilder, index));
                Tuple primaryKey = createComplexRecordJoinedToManyFields(1, 11L, 11L, "matching text for field 0 pineapple", "non matching text for field 1 orange", true, System.currentTimeMillis(), 0);
                Tuple primaryKey2 = createComplexRecordJoinedToManyFields(2, 387L, 387L, "non matching text for field 0 orange", "matching text for field 1 pineapple", false, System.currentTimeMillis(), 0);
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "many_text0:pineapple"), null, ScanProperties.FORWARD_SCAN));
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey2),
                        recordStore.scanIndex(index, fullTextSearch(index, "many_text1:pineapple"), null, ScanProperties.FORWARD_SCAN));
            } else {
                rebuildIndexMetaData(context, MANY_FIELDS_DOC, index);
                TestRecordsTextProto.ManyFieldsDocument doc1 = TestRecordsTextProto.ManyFieldsDocument.newBuilder()
                        .setDocId(11L)
                        .setText0("matching text for field 0 pineapple")
                        .setText1("non matching text for field 1 orange")
                        .build();
                TestRecordsTextProto.ManyFieldsDocument doc2 = TestRecordsTextProto.ManyFieldsDocument.newBuilder()
                        .setDocId(387L)
                        .setText0("non matching text for field 0 orange")
                        .setText1("matching text for field 1 pineapple")
                        .build();
                recordStore.saveRecord(doc1);
                recordStore.saveRecord(doc2);

                // Make sure the text search for individual fields
                assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(11)),
                        recordStore.scanIndex(index, fullTextSearch(index, "text0:pineapple"), null, ScanProperties.FORWARD_SCAN));
                assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(387)),
                        recordStore.scanIndex(index, fullTextSearch(index, "text1:pineapple"), null, ScanProperties.FORWARD_SCAN));
            }
        }
    }

    /**
     * Make sure the text search for individual fields is not confused when there are multiple fields in the
     * fieldsFormat schema.
     * This test has no overlap in the fields (0/1 vs 3/4).
     */
    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void differentFieldSearchNoOverlap(IndexedType indexedType) {
        final Index index = indexedType.getIndex(MANY_FIELDS_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToManyFields(metaDataBuilder, index));
                Tuple primaryKey = createComplexRecordJoinedToManyFields(1, 11L, 11L, "matching text for field 0 pineapple", "non matching text for field 1 orange", true, System.currentTimeMillis(), 0);
                Tuple primaryKey2 = createComplexRecordJoinedToManyFields(2, 387L, 387L, "non matching text for field 3 orange", "matching text for field 4 pineapple", false, System.currentTimeMillis(), 0);
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "many_text0:pineapple"), null, ScanProperties.FORWARD_SCAN));
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey2),
                        recordStore.scanIndex(index, fullTextSearch(index, "many_text4:pineapple"), null, ScanProperties.FORWARD_SCAN));
            } else {
                rebuildIndexMetaData(context, MANY_FIELDS_DOC, index);
                TestRecordsTextProto.ManyFieldsDocument doc1 = TestRecordsTextProto.ManyFieldsDocument.newBuilder()
                        .setDocId(11L)
                        .setText0("matching text for field 0 pineapple")
                        .setText1("non matching text for field 1 orange")
                        .build();
                TestRecordsTextProto.ManyFieldsDocument doc2 = TestRecordsTextProto.ManyFieldsDocument.newBuilder()
                        .setDocId(387L)
                        .setText3("non matching text for field 3 orange")
                        .setText4("matching text for field 4 pineapple")
                        .build();
                recordStore.saveRecord(doc1);
                recordStore.saveRecord(doc2);

                // Make sure the text search for individual fields
                assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(11)),
                        recordStore.scanIndex(index, fullTextSearch(index, "text0:pineapple"), null, ScanProperties.FORWARD_SCAN));
                assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(387)),
                        recordStore.scanIndex(index, fullTextSearch(index, "text4:pineapple"), null, ScanProperties.FORWARD_SCAN));
            }
        }
    }

    private static Stream<Arguments> specialCharacterParams() {
        return LuceneIndexTestUtils.luceneIndexMapParams().flatMap(
                indexedType -> Stream.of("Ω", "ç").map(
                        param2 -> Arguments.of(indexedType, param2)));
    }

    @Nonnull
    private String specialCharacterText(String specialCharacter) {
        return "Do we match special characters like " + specialCharacter + ", even when its mashed together like " + specialCharacter + "noSpaces?";
    }

    @ParameterizedTest
    @MethodSource({"specialCharacterParams"})
    void insertAndSearchWithSpecialCharacters(IndexedType indexedType, String specialCharacter) {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                Tuple primaryKey = createComplexRecordJoinedToSimple(2, 1623L, 1623L, specialCharacterText(specialCharacter), "", true, System.currentTimeMillis(), 0);
                createComplexRecordJoinedToSimple(1, 1547L, 1547L, specialCharacterText(" "), "", true, System.currentTimeMillis(), 0);
                timer.reset();
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, specialCharacter), null, ScanProperties.FORWARD_SCAN));
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                //one document when the chars, one without
                recordStore.saveRecord(createSimpleDocument(1623L, specialCharacterText(specialCharacter), 2));
                recordStore.saveRecord(createSimpleDocument(1547L, specialCharacterText(" "), 1));
                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, fullTextSearch(index, specialCharacter), null, ScanProperties.FORWARD_SCAN));
            }

            assertEquals(1, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index), context, "_0.cfs");
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void searchTextQueryWithBooleanEquals(IndexedType indexedType) {
        /*
         * Check that a point query on a number type and a text match together return the correct result
         */
        final Index index = indexedType.getIndex(TEXT_AND_BOOLEAN_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                Tuple primaryKey = createComplexRecordJoinedToSimple(2, 1623L, 1623L, ENGINEER_JOKE, "propose a Vision", true, System.currentTimeMillis(), 0);
                createComplexRecordJoinedToSimple(1, 1547L, 1547L, ENGINEER_JOKE, "different smoochies", false, System.currentTimeMillis(), 0);
                timer.reset();
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"propose a Vision\" AND complex_is_seen: true"), null, ScanProperties.FORWARD_SCAN));
            } else {
                rebuildIndexMetaData(context, COMPLEX_DOC, index);
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, ENGINEER_JOKE, "propose a Vision", 2, true));
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1547L, ENGINEER_JOKE, "different smoochies", 2, false));

                assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(2, 1623L)),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"propose a Vision\" AND is_seen: true"), null, ScanProperties.FORWARD_SCAN));
            }

            assertEquals(1, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index), context, "_0.cfs");
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void searchTextQueryWithBooleanNotEquals(IndexedType indexedType) {
        /*
         * Check that a point query on a number type and a text match together return the correct result
         */
        final Index index = indexedType.getIndex(TEXT_AND_BOOLEAN_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                createComplexRecordJoinedToSimple(2, 1623L, 1623L, ENGINEER_JOKE, "propose a Vision", true, System.currentTimeMillis(), 0);
                Tuple primaryKey = createComplexRecordJoinedToSimple(1, 1547L, 1547L, ENGINEER_JOKE, "different smoochies", false, System.currentTimeMillis(), 0);
                timer.reset();
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"propose a Vision\" AND NOT complex_is_seen: true"), null, ScanProperties.FORWARD_SCAN));
            } else {
                rebuildIndexMetaData(context, COMPLEX_DOC, index);
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, ENGINEER_JOKE, "propose a Vision", 2, true));
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1547L, ENGINEER_JOKE, "different smoochies", 2, false));

                assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(2, 1547L)),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"propose a Vision\" AND NOT is_seen: true"), null, ScanProperties.FORWARD_SCAN));
            }

            assertEquals(1, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index), context, "_0.cfs");
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void searchTextQueryWithBooleanRange(IndexedType indexedType) {
        /*
         * Check that a point query on a number type and a text match together return the correct result
         */
        final Index index = indexedType.getIndex(TEXT_AND_BOOLEAN_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                Tuple primaryKey = createComplexRecordJoinedToSimple(2, 1623L, 1623L, ENGINEER_JOKE, "propose a Vision", true, System.currentTimeMillis(), 0);
                Tuple primaryKey2 = createComplexRecordJoinedToSimple(1, 1547L, 1547L, ENGINEER_JOKE, "different smoochies", false, System.currentTimeMillis(), 0);
                timer.reset();
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey, primaryKey2),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"propose a Vision\" AND complex_is_seen: [false TO true]"), null, ScanProperties.FORWARD_SCAN));

            } else {
                rebuildIndexMetaData(context, COMPLEX_DOC, index);
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, ENGINEER_JOKE, "propose a Vision", 2, true));
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1547L, ENGINEER_JOKE, "different smoochies", 2, false));

                assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(2, 1623L), Tuple.from(2, 1547L)),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"propose a Vision\" AND is_seen: [false TO true]"), null, ScanProperties.FORWARD_SCAN));
            }

            assertEquals(2, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index), context, "_0.cfs");
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void searchTextQueryWithBooleanBoth(IndexedType indexedType) {
        /*
         * Check that a point query on a number type and a text match together return the correct result
         */
        final Index index = indexedType.getIndex(TEXT_AND_BOOLEAN_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                createComplexRecordJoinedToSimple(2, 1623L, 1623L, ENGINEER_JOKE, "propose a Vision", true, System.currentTimeMillis(), 0);
                createComplexRecordJoinedToSimple(1, 1547L, 1547L, ENGINEER_JOKE, "different smoochies", false, System.currentTimeMillis(), 0);
                timer.reset();
                assertIndexEntryPrimaryKeyTuples(Set.of(),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"propose a Vision\" AND complex_is_seen: true AND complex_is_seen: false"), null, ScanProperties.FORWARD_SCAN));
            } else {
                rebuildIndexMetaData(context, COMPLEX_DOC, index);
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, ENGINEER_JOKE, "propose a Vision", 2, true));
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1547L, ENGINEER_JOKE, "different smoochies", 2, false));

                assertIndexEntryPrimaryKeyTuples(Set.of(),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"propose a Vision\" AND is_seen: true AND is_seen: false"), null, ScanProperties.FORWARD_SCAN));
            }

            assertNull(Verify.verifyNotNull(context.getTimer()).getCounter(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY));
            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index), context, "_0.cfs");
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void searchTextQueryWithBooleanEither(IndexedType indexedType) {
        /*
         * Check that a point query on a number type and a text match together return the correct result
         */
        final Index index = indexedType.getIndex(TEXT_AND_BOOLEAN_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                Tuple primaryKey = createComplexRecordJoinedToSimple(2, 1623L, 1623L, ENGINEER_JOKE, "propose a Vision", true, System.currentTimeMillis(), 0);
                Tuple primaryKey2 = createComplexRecordJoinedToSimple(1, 1547L, 1547L, ENGINEER_JOKE, "different smoochies", false, System.currentTimeMillis(), 0);
                timer.reset();
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey, primaryKey2),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"propose a Vision\" AND (complex_is_seen: true OR complex_is_seen: false)"), null, ScanProperties.FORWARD_SCAN));
            } else {

                rebuildIndexMetaData(context, COMPLEX_DOC, index);
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, ENGINEER_JOKE, "propose a Vision", 2, true));
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1547L, ENGINEER_JOKE, "different smoochies", 2, false));

                assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(2, 1623L), Tuple.from(2, 1547L)),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"propose a Vision\" AND (is_seen: true OR is_seen: false)"), null, ScanProperties.FORWARD_SCAN));
            }

            assertEquals(2, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index), context, "_0.cfs");
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void searchTextQueryWithNumberEquals(IndexedType indexedType) {
        /*
         * Check that a point query on a number type and a text match together return the correct result
         */
        final Index index = indexedType.getIndex(TEXT_AND_NUMBER_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                final Tuple primaryKey = createComplexRecordJoinedToSimple(2, 1623L, 1623L, ENGINEER_JOKE, "", true, System.currentTimeMillis(), 2);
                createComplexRecordJoinedToSimple(1, 1547L, 1547L, ENGINEER_JOKE, "", false, System.currentTimeMillis(), 1);
                createComplexRecordJoinedToSimple(3, 1548L, 1548L, ENGINEER_JOKE, "", false, System.currentTimeMillis(), null);
                timer.reset();
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"propose a Vision\" AND complex_score:2"), null, ScanProperties.FORWARD_SCAN));
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
                recordStore.saveRecord(createSimpleDocument(1547L, ENGINEER_JOKE, 1));
                recordStore.saveRecord(createSimpleDocument(1548L, ENGINEER_JOKE, null));

                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"propose a Vision\" AND group:2"), null, ScanProperties.FORWARD_SCAN));
            }

            assertEquals(1, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index), context, "_0.cfs");
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void searchTextWithEmailPrefix(IndexedType indexedType) {
        /*
         * Check that a prefix query with an email in it will return the email
         */
        final Index index = indexedType.getIndex(TEXT_AND_NUMBER_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                Tuple primaryKey = createComplexRecordJoinedToSimple(1, 1241L, 1623L, "{to: aburritoofjoy@tacos.com, from: tacosareevil@badfoodtakes.net}", "", true, System.currentTimeMillis(), 1);
                createComplexRecordJoinedToSimple(2, 1342L, 1547L, "{to: aburritoofjoy@tacos.com, from: tacosareevil@badfoodtakes.net}", "", false, System.currentTimeMillis(), 2);
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "aburritoofjoy@tacos.com* AND complex_score: 1"), null, ScanProperties.FORWARD_SCAN));
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                recordStore.saveRecord(createSimpleDocument(1241L, "{to: aburritoofjoy@tacos.com, from: tacosareevil@badfoodtakes.net}", 1));
                recordStore.saveRecord(createSimpleDocument(1342L, "{to: aburritoofjoy@tacos.com, from: tacosareevil@badfoodtakes.net}", 2));

                assertIndexEntryPrimaryKeys(Set.of(1241L),
                        recordStore.scanIndex(index, fullTextSearch(index, "aburritoofjoy@tacos.com* AND group: 1"), null, ScanProperties.FORWARD_SCAN));
            }
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void searchTextQueryWithNumberRange(IndexedType indexedType) {
        /*
         * Check that a range query on a number type and a text match together return the correct result
         */
        final Index index = indexedType.getIndex(TEXT_AND_NUMBER_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                Tuple primaryKey = createComplexRecordJoinedToSimple(2, 1623L, 1623L, ENGINEER_JOKE, "", true, System.currentTimeMillis(), 2);
                createComplexRecordJoinedToSimple(1, 1547L, 1547L, ENGINEER_JOKE, "", false, System.currentTimeMillis(), 1);
                timer.reset();
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"propose a Vision\" AND complex_score:[2 TO 4]"), null, ScanProperties.FORWARD_SCAN));
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
                recordStore.saveRecord(createSimpleDocument(1547L, ENGINEER_JOKE, 1));

                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"propose a Vision\" AND group:[2 TO 4]"), null, ScanProperties.FORWARD_SCAN));
            }

            assertEquals(1, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index), context, "_0.cfs");
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void searchTextWithNumberRangeInfinite(IndexedType indexedType) {
        /*
         * Check that a range query returns empty if you feed it a range that is logically empty (i.e. (Long.MAX_VALUE,...)
         */
        final Index index = indexedType.getIndex(TEXT_AND_NUMBER_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                createComplexRecordJoinedToSimple(2, 1623L, 1623L, ENGINEER_JOKE, "", true, System.currentTimeMillis(), 2);
                createComplexRecordJoinedToSimple(1, 1547L, 1547L, ENGINEER_JOKE, "", false, System.currentTimeMillis(), 1);
                //positive infinity
                assertIndexEntryPrimaryKeys(Set.of(),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"propose a Vision\" AND complex_score:{" + Long.MAX_VALUE + " TO " + Long.MAX_VALUE + "]"), null, ScanProperties.FORWARD_SCAN));


                //negative infinite
                assertIndexEntryPrimaryKeys(Set.of(),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"propose a Vision\" AND complex_score:[" + Long.MIN_VALUE + " TO " + Long.MIN_VALUE + "}"), null, ScanProperties.FORWARD_SCAN));
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, TEXT_AND_NUMBER_INDEX);
                recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
                recordStore.saveRecord(createSimpleDocument(1547L, ENGINEER_JOKE, 1));

                //positive infinity
                assertIndexEntryPrimaryKeys(Set.of(),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"propose a Vision\" AND group:{" + Long.MAX_VALUE + " TO " + Long.MAX_VALUE + "]"), null, ScanProperties.FORWARD_SCAN));


                //negative infinite
                assertIndexEntryPrimaryKeys(Set.of(),
                        recordStore.scanIndex(index, fullTextSearch(TEXT_AND_NUMBER_INDEX, "\"propose a Vision\" AND group:[" + Long.MIN_VALUE + " TO " + Long.MIN_VALUE + "}"), null, ScanProperties.FORWARD_SCAN));
            }

            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index), context, "_0.cfs");
        }
    }


    private static Stream<Arguments> bitsetParams() {
        return LuceneIndexTestUtils.luceneIndexMapParams().flatMap(
                indexedType -> Stream.of(
                        Arguments.of(0b0, List.of(1623L, 1547L),
                                Set.of(Tuple.from(-1, List.of(28, 1623), List.of(1623)),
                                        Tuple.from(-1, List.of(26, 1547), List.of(1547)))),
                        Arguments.of(0b100, List.of(1623L),
                                Set.of(Tuple.from(-1, List.of(28, 1623), List.of(1623)))),
                        Arguments.of(0b10, List.of(1547L),
                                Set.of(Tuple.from(-1, List.of(26, 1547), List.of(1547)))),
                        Arguments.of(0b1000, List.of(1623L, 1547L),
                                Set.of(Tuple.from(-1, List.of(28, 1623), List.of(1623)),
                                        Tuple.from(-1, List.of(26, 1547), List.of(1547)))),
                        Arguments.of(0b110, List.of(), Set.of()),
                        Arguments.of(0b11000, List.of(1623L, 1547L),
                                Set.of(Tuple.from(-1, List.of(28, 1623), List.of(1623)),
                                        Tuple.from(-1, List.of(26, 1547), List.of(1547)))),
                        Arguments.of(0b1100, List.of(1623L),
                                Set.of(Tuple.from(-1, List.of(28, 1623), List.of(1623))))
                        ).map(param2 -> Arguments.of(indexedType, param2.get()[0], param2.get()[1], param2.get()[2])));
    }

    @ParameterizedTest
    @MethodSource("bitsetParams")
    void bitset(IndexedType indexedType, int mask, List<Long> expectedResult, Set<Tuple> syntheticExpectedResult) {
        /*
         * Check that a bitset_contains query returns the right result given a certain mask
         */
        final Index index = indexedType.getIndex(TEXT_AND_NUMBER_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                createComplexRecordJoinedToSimple(0b11100, 1623L, 1623L, ENGINEER_JOKE, "", true, System.currentTimeMillis(), 0b11100);
                createComplexRecordJoinedToSimple(0b11010, 1547L, 1547L, ENGINEER_JOKE, "", false, System.currentTimeMillis(), 0b11010);

                assertIndexEntryPrimaryKeyTuples(
                        syntheticExpectedResult,
                        recordStore.scanIndex(
                                index,
                                fullTextSearch(index, "\"propose a Vision\" AND complex_group:BITSET_CONTAINS(" + mask + ")"),
                                null,
                                ScanProperties.FORWARD_SCAN));

                assertIndexEntryPrimaryKeyTuples(
                        syntheticExpectedResult,
                        recordStore.scanIndex(
                                index,
                                fullTextSearch(index, "complex_group:BITSET_CONTAINS(" + mask + ")"),
                                null,
                                ScanProperties.FORWARD_SCAN));
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 0b11100));
                recordStore.saveRecord(createSimpleDocument(1547L, ENGINEER_JOKE, 0b11010));

                assertIndexEntryPrimaryKeys(
                        expectedResult,
                        recordStore.scanIndex(
                                index,
                                fullTextSearch(index, "\"propose a Vision\" AND group:BITSET_CONTAINS(" + mask + ")"),
                                null,
                                ScanProperties.FORWARD_SCAN));

                assertIndexEntryPrimaryKeys(
                        expectedResult,
                        recordStore.scanIndex(
                                index,
                                fullTextSearch(index, "group:BITSET_CONTAINS(" + mask + ")"),
                                null,
                                ScanProperties.FORWARD_SCAN));
            }
        }
    }

    private static Stream<Arguments> bitsetOrParams() {
        return LuceneIndexTestUtils.luceneIndexMapParams().flatMap(
                indexedType -> Stream.of(
                        Arguments.of(0b0, 0b0, List.of(1623L, 1547L),
                                Set.of(Tuple.from(-1, List.of(28, 1623), List.of(1623)),
                                        Tuple.from(-1, List.of(26, 1547), List.of(1547)))),
                        Arguments.of(0b100, 0b1, List.of(1623L),
                                Set.of(Tuple.from(-1, List.of(28, 1623), List.of(1623)))),
                        Arguments.of(0b10, 0b1, List.of(1547L),
                                Set.of(Tuple.from(-1, List.of(26, 1547), List.of(1547)))),
                        Arguments.of(0b1000, 0b1000, List.of(1623L, 1547L),
                                Set.of(Tuple.from(-1, List.of(28, 1623), List.of(1623)),
                                        Tuple.from(-1, List.of(26, 1547), List.of(1547)))),
                        Arguments.of(0b100, 0b10, List.of(1623L, 1547L),
                                Set.of(Tuple.from(-1, List.of(28, 1623), List.of(1623)),
                                        Tuple.from(-1, List.of(26, 1547), List.of(1547)))),
                        Arguments.of(0b10000, 0b1000, List.of(1623L, 1547L),
                                Set.of(Tuple.from(-1, List.of(28, 1623), List.of(1623)),
                                        Tuple.from(-1, List.of(26, 1547), List.of(1547)))),
                        Arguments.of(0b1000, 0b100, List.of(1623L, 1547L),
                                Set.of(Tuple.from(-1, List.of(28, 1623), List.of(1623)),
                                        Tuple.from(-1, List.of(26, 1547), List.of(1547))))
                ).map(param2 -> Arguments.of(indexedType, param2.get()[0], param2.get()[1], param2.get()[2], param2.get()[3])));
    }

    @ParameterizedTest
    @MethodSource("bitsetOrParams")
    void bitsetOr(IndexedType indexedType, int mask1, int mask2, List<Long> expectedResult, Set<Tuple> syntheticExpectedResult) {
        /*
         * Check that a bitset_contains query returns the right result given a certain mask
         */
        final Index index = indexedType.getIndex(TEXT_AND_NUMBER_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                createComplexRecordJoinedToSimple(0b11100, 1623L, 1623L, ENGINEER_JOKE, "", true, System.currentTimeMillis(), 0b11100);
                createComplexRecordJoinedToSimple(0b11010, 1547L, 1547L, ENGINEER_JOKE, "", false, System.currentTimeMillis(), 0b11010);

                assertIndexEntryPrimaryKeyTuples(
                        syntheticExpectedResult,
                        recordStore.scanIndex(
                                index,
                                fullTextSearch(index, "complex_group:BITSET_CONTAINS(" + mask1 + ") OR complex_group:BITSET_CONTAINS(" + mask2 + ")"),
                                null,
                                ScanProperties.FORWARD_SCAN));
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 0b11100));
                recordStore.saveRecord(createSimpleDocument(1547L, ENGINEER_JOKE, 0b11010));

                assertIndexEntryPrimaryKeys(
                        expectedResult,
                        recordStore.scanIndex(
                                index,
                                fullTextSearch(index, "group:BITSET_CONTAINS(" + mask1 + ") OR group:BITSET_CONTAINS(" + mask2 + ")"),
                                null,
                                ScanProperties.FORWARD_SCAN));
            }
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void simpleEmptyIndex(IndexedType indexedType) {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
            }
            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(index, fullTextSearch(index, "something"), null, ScanProperties.FORWARD_SCAN)) {
                assertEquals(RecordCursorResult.exhausted(), cursor.getNext());
            }
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void simpleEmptyAutoComplete(IndexedType indexedType) {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_WITH_AUTO_COMPLETE_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
            }
            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(index,
                    autoCompleteBounds(index, "something", ImmutableSet.of("text")),
                    null, ScanProperties.FORWARD_SCAN)) {
                assertEquals(RecordCursorResult.exhausted(), cursor.getNext());
            }
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void simpleInsertAndSearchNumFDBFetches(IndexedType indexedType) {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                Tuple primaryKey = createComplexRecordJoinedToSimple(2, 1623L, 1623L, ENGINEER_JOKE, "", true, System.currentTimeMillis(), 0);
                createComplexRecordJoinedToSimple(1, 1547L, 1547L, WAYLON, "", false, System.currentTimeMillis(), 0);
                timer.reset();
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "Vision"), null, ScanProperties.FORWARD_SCAN));

            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
                recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, fullTextSearch(index, "Vision"), null, ScanProperties.FORWARD_SCAN));
            }

            assertEquals(1, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index), context, "_0.cfs");
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void testContinuation(IndexedType indexedType) {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_KEY);
        try (FDBRecordContext context = openContext()) {
            LuceneContinuationProto.LuceneIndexContinuation continuation = LuceneContinuationProto.LuceneIndexContinuation.newBuilder()
                    .setDoc(1)
                    .setScore(0.21973526F)
                    .setShard(0)
                    .build();
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                createComplexRecordJoinedToSimple(2, 1623L, 1623L, ENGINEER_JOKE, "", true, System.currentTimeMillis(), 0);
                createComplexRecordJoinedToSimple(3, 1624L, 1624L, ENGINEER_JOKE, "", true, System.currentTimeMillis(), 0);
                Tuple primaryKey3 = createComplexRecordJoinedToSimple(4, 1625L, 1625L, ENGINEER_JOKE, "", true, System.currentTimeMillis(), 0);
                Tuple primaryKey4 = createComplexRecordJoinedToSimple(5, 1626L, 1626L, ENGINEER_JOKE, "", true, System.currentTimeMillis(), 0);
                createComplexRecordJoinedToSimple(1, 1547L, 1547L, WAYLON, "", false, System.currentTimeMillis(), 0);
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey3, primaryKey4),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:Vision"), continuation.toByteArray(), ScanProperties.FORWARD_SCAN));

            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
                recordStore.saveRecord(createSimpleDocument(1624L, ENGINEER_JOKE, 2));
                recordStore.saveRecord(createSimpleDocument(1625L, ENGINEER_JOKE, 2));
                recordStore.saveRecord(createSimpleDocument(1626L, ENGINEER_JOKE, 2));
                recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
                assertIndexEntryPrimaryKeys(Set.of(1625L, 1626L),
                        recordStore.scanIndex(index, fullTextSearch(index, "Vision"), continuation.toByteArray(), ScanProperties.FORWARD_SCAN));
            }

            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index), context, "_0.cfs");
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void testNullValue(IndexedType indexedType) {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                Tuple primaryKey = createComplexRecordJoinedToSimple(2, 1623L, 1623L, null, "", true, System.currentTimeMillis(), 0);
                createComplexRecordJoinedToSimple(3, 1632L, 1632L, ENGINEER_JOKE, "", true, System.currentTimeMillis(), 0);
                createComplexRecordJoinedToSimple(1, 1547L, 1547L, WAYLON, "", false, System.currentTimeMillis(), 0);
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "*:* AND NOT simple_text:[* TO *]"), null, ScanProperties.FORWARD_SCAN));

            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                recordStore.saveRecord(createSimpleDocument(1623L, 2));
                recordStore.saveRecord(createSimpleDocument(1632L, ENGINEER_JOKE, 2));
                recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 2));
                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, fullTextSearch(index, "*:* AND NOT text:[* TO *]"), null, ScanProperties.FORWARD_SCAN));
            }

            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index), context, "_0.cfs");
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void testLimit(IndexedType indexedType) {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                for (int i = 0; i < 200; i++) {
                    createComplexRecordJoinedToSimple(2 + i, 1623L + i, 1623L + i, ENGINEER_JOKE, "", true, System.currentTimeMillis(), 0);
                }
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                for (int i = 0; i < 200; i++) {
                    recordStore.saveRecord(createSimpleDocument(1623L + i, ENGINEER_JOKE, 2));
                }
            }

            final String searchString = indexedType.isSynthetic() ? "simple_text:Vision" : "Vision";
            assertEquals(50, recordStore.scanIndex(index, fullTextSearch(index, searchString), null, ExecuteProperties.newBuilder().setReturnedRowLimit(50).build().asScanProperties(false))
                    .getCount().join());
            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index), context, "_0.cfs");
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void testSkip(IndexedType indexedType) {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                for (int i = 0; i < 50; i++) {
                    createComplexRecordJoinedToSimple(2 + i, 1623L + i, 1623L + i, ENGINEER_JOKE, "", true, System.currentTimeMillis(), 0);
                }
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                for (int i = 0; i < 50; i++) {
                    recordStore.saveRecord(createSimpleDocument(1623L + i, ENGINEER_JOKE, 2));
                }
            }

            final String searchString = indexedType.isSynthetic() ? "simple_text:Vision" : "Vision";
            assertEquals(40, recordStore.scanIndex(index, fullTextSearch(index, searchString), null, ExecuteProperties.newBuilder().setSkip(10).build().asScanProperties(false))
                    .getCount().join());
            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index), context, "_0.cfs");
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void testSkipWithLimit(IndexedType indexedType) {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                for (int i = 0; i < 50; i++) {
                    createComplexRecordJoinedToSimple(2 + i, 1623L + i, 1623L + i, ENGINEER_JOKE, "", true, System.currentTimeMillis(), 0);
                }
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
                for (int i = 0; i < 50; i++) {
                    recordStore.saveRecord(createSimpleDocument(1623L + i, ENGINEER_JOKE, 2));
                }
            }

            final String searchString = indexedType.isSynthetic() ? "simple_text:Vision" : "Vision";
            assertEquals(40, recordStore.scanIndex(index, fullTextSearch(index, searchString), null, ExecuteProperties.newBuilder().setReturnedRowLimit(50).setSkip(10).build().asScanProperties(false))
                    .getCount().join());
            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index), context, "_0.cfs");
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void testLimitWithContinuation(IndexedType indexedType) {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_KEY);
        try (FDBRecordContext context = openContext()) {
            LuceneContinuationProto.LuceneIndexContinuation continuation = LuceneContinuationProto.LuceneIndexContinuation.newBuilder()
                    .setDoc(151)
                    .setScore(0.0019047183F)
                    .setShard(0)
                    .build();
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                for (int i = 0; i < 200; i++) {
                    createComplexRecordJoinedToSimple(2 + i, 1623L + i, 1623L + i, ENGINEER_JOKE, "", true, System.currentTimeMillis(), 0);
                }
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                for (int i = 0; i < 200; i++) {
                    recordStore.saveRecord(createSimpleDocument(1623L + i, ENGINEER_JOKE, 2));
                }
            }

            final String searchString = indexedType.isSynthetic() ? "simple_text:Vision" : "Vision";
            assertEquals(48, recordStore.scanIndex(index, fullTextSearch(index, searchString), continuation.toByteArray(), ExecuteProperties.newBuilder().setReturnedRowLimit(50).build().asScanProperties(false))
                    .getCount().join());
            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index), context, "_0.cfs");
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void testLimitNeedsMultipleScans(IndexedType indexedType) {
        final RecordLayerPropertyStorage.Builder storageBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_CURSOR_PAGE_SIZE, 201);

        final Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_KEY);
        try (FDBRecordContext context = openContext(storageBuilder)) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                for (int i = 0; i < 800; i++) {
                    createComplexRecordJoinedToSimple(2 + i, 1623L + i, 1623L + i, ENGINEER_JOKE, "", true, System.currentTimeMillis(), 0);
                }
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                for (int i = 0; i < 800; i++) {
                    recordStore.saveRecord(createSimpleDocument(1623L + i, ENGINEER_JOKE, 2));
                }
            }

            final String searchString = indexedType.isSynthetic() ? "simple_text:Vision" : "Vision";
            assertEquals(251, recordStore.scanIndex(index, fullTextSearch(index, searchString), null, ExecuteProperties.newBuilder().setReturnedRowLimit(251).build().asScanProperties(false))
                    .getCount().join());
            assertEquals(2, getCounter(context, LuceneEvents.Events.LUCENE_INDEX_SCAN).getCount()); // cursor page size is 201, so we need 2 scans
            assertEquals(251, getCounter(context, LuceneEvents.Counts.LUCENE_SCAN_MATCHED_DOCUMENTS).getCount());
            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index), context, "_0.cfs");
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void testSkipOverMultipleScans(IndexedType indexedType) {
        final RecordLayerPropertyStorage.Builder storageBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_CURSOR_PAGE_SIZE, 201);

        final Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_KEY);
        try (FDBRecordContext context = openContext(storageBuilder)) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                for (int i = 0; i < 251; i++) {
                    createComplexRecordJoinedToSimple(2 + i, 1623L + i, 1623L + i, ENGINEER_JOKE, "", true, System.currentTimeMillis(), 0);
                }
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                for (int i = 0; i < 251; i++) {
                    recordStore.saveRecord(createSimpleDocument(1623L + i, ENGINEER_JOKE, 2));
                }
            }

            final String searchString = indexedType.isSynthetic() ? "simple_text:Vision" : "Vision";
            assertEquals(50, recordStore.scanIndex(index, fullTextSearch(index, searchString), null, ExecuteProperties.newBuilder().setReturnedRowLimit(251).setSkip(201).build().asScanProperties(false))
                    .getCount().join());
            assertEquals(2, getCounter(context, LuceneEvents.Events.LUCENE_INDEX_SCAN).getCount()); // cursor page size is 201, so we need 2 scans
            assertEquals(251, getCounter(context, LuceneEvents.Counts.LUCENE_SCAN_MATCHED_DOCUMENTS).getCount());

            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index), context, "_0.cfs");
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void testNestedFieldSearch(IndexedType indexedType) {
        final Index index = indexedType.getIndex(MAP_ON_VALUE_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToMap(metaDataBuilder, index));
                Tuple primaryKey = createComplexRecordJoinedToMap(2, 1623L, 1623L, ENGINEER_JOKE, "sampleTextSong", "", "", true, System.currentTimeMillis(), 0);
                createComplexRecordJoinedToMap(3, 1547L, 1547L, WAYLON, "sampleTextPhrase", "", "", true, System.currentTimeMillis(), 0);
                timer.reset();
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, groupedTextSearch(index, "map_entry_value:Vision", "sampleTextSong"), null, ScanProperties.FORWARD_SCAN));
                assertIndexEntryPrimaryKeyTuples(Set.of(),
                        recordStore.scanIndex(index, groupedTextSearch(index, "map_entry_value:random", "sampleTextSong"), null, ScanProperties.FORWARD_SCAN));
                assertIndexEntryPrimaryKeyTuples(Set.of(),
                        recordStore.scanIndex(index, groupedTextSearch(index, "map_entry_value:Vision", "sampleTextBook"), null, ScanProperties.FORWARD_SCAN));
            } else {
                rebuildIndexMetaData(context, MAP_DOC, index);
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexMapDocument(1623L, ENGINEER_JOKE, "sampleTextSong", 2));
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexMapDocument(1547L, WAYLON, "sampleTextPhrase", 1));
                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, groupedTextSearch(index, "entry_value:Vision", "sampleTextSong"), null, ScanProperties.FORWARD_SCAN));
                assertIndexEntryPrimaryKeys(Set.of(),
                        recordStore.scanIndex(index, groupedTextSearch(index, "entry_value:random", "sampleTextSong"), null, ScanProperties.FORWARD_SCAN));
                assertIndexEntryPrimaryKeys(Set.of(),
                        recordStore.scanIndex(index, groupedTextSearch(index, "entry_value:Vision", "sampleTextBook"), null, ScanProperties.FORWARD_SCAN));
            }

            assertEquals(1, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index).subspace(Tuple.from("sampleTextSong")), context, "_0.cfs");
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void testGroupedRecordSearch(IndexedType indexedType) {
        final Index index = indexedType.getIndex(MAP_ON_VALUE_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToMap(metaDataBuilder, index));
                Tuple primaryKey = createComplexRecordJoinedToMap(2, 1623L, 1623L, ENGINEER_JOKE, "sampleTextPhrase", "", "sampleTextSong", true, System.currentTimeMillis(), 0);
                timer.reset();
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, groupedTextSearch(index, "map_entry_value:Vision", "sampleTextPhrase"), null, ScanProperties.FORWARD_SCAN));
                assertIndexEntryPrimaryKeyTuples(Set.of(),
                        recordStore.scanIndex(index, groupedTextSearch(index, "map_entry_value:random", "sampleTextSong"), null, ScanProperties.FORWARD_SCAN));
                assertIndexEntryPrimaryKeyTuples(Set.of(),
                        recordStore.scanIndex(index, groupedTextSearch(index, "map_entry_value:Vision", "sampleTextBook"), null, ScanProperties.FORWARD_SCAN));
            } else {
                rebuildIndexMetaData(context, MAP_DOC, index);
                recordStore.saveRecord(LuceneIndexTestUtils.createMultiEntryMapDoc(1623L, ENGINEER_JOKE, "sampleTextPhrase", WAYLON, "sampleTextSong", 2));
                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, groupedTextSearch(index, "entry_value:Vision", "sampleTextPhrase"), null, ScanProperties.FORWARD_SCAN));
                assertIndexEntryPrimaryKeys(Set.of(),
                        recordStore.scanIndex(index, groupedTextSearch(index, "entry_value:random", "sampleTextSong"), null, ScanProperties.FORWARD_SCAN));
                assertIndexEntryPrimaryKeys(Set.of(),
                        recordStore.scanIndex(index, groupedTextSearch(index, "entry_value:Vision", "sampleTextBook"), null, ScanProperties.FORWARD_SCAN));
            }

            assertEquals(1, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index).subspace(Tuple.from("sampleTextPhrase")), context, "_0.cfs");
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void testMultipleFieldSearch(IndexedType indexedType) {
        final Index index = indexedType.getIndex(COMPLEX_MULTIPLE_TEXT_INDEXES_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                Tuple primaryKey = createComplexRecordJoinedToSimple(2, 1623L, 1623L, ENGINEER_JOKE, "john_leach@apple.com", true, System.currentTimeMillis(), 0);
                createComplexRecordJoinedToSimple(3, 1547L, 1547L, WAYLON, "hering@gmail.com", true, System.currentTimeMillis(), 0);
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:\"Vision\" AND complex_text2:\"john_leach@apple.com\""), null, ScanProperties.FORWARD_SCAN));
            } else {
                rebuildIndexMetaData(context, COMPLEX_DOC, index);
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, ENGINEER_JOKE, "john_leach@apple.com", 2));
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1547L, WAYLON, "hering@gmail.com", 3));
                assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(2L, 1623L)),
                        recordStore.scanIndex(index, fullTextSearch(index, "text:\"Vision\" AND text2:\"john_leach@apple.com\""), null, ScanProperties.FORWARD_SCAN));
            }

            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index), context, "_0.cfs");
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void testFuzzySearchWithDefaultEdit2(IndexedType indexedType) {
        final Index index = indexedType.getIndex(COMPLEX_MULTIPLE_TEXT_INDEXES_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                Tuple primaryKey = createComplexRecordJoinedToSimple(2, 1623L, 1623L, ENGINEER_JOKE, "john_leach@apple.com", true, System.currentTimeMillis(), 0);
                createComplexRecordJoinedToSimple(3, 1547L, 1547L, WAYLON, "hering@gmail.com", true, System.currentTimeMillis(), 0);
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:\"Vision\" AND complex_text2:jonleach@apple.com~"), null, ScanProperties.FORWARD_SCAN));

            } else {
                rebuildIndexMetaData(context, COMPLEX_DOC, index);
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, ENGINEER_JOKE, "john_leach@apple.com", 2));
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1547L, WAYLON, "hering@gmail.com", 2));
                assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(2L, 1623L)),
                        recordStore.scanIndex(index, fullTextSearch(index, "text:\"Vision\" AND text2:jonleach@apple.com~"), null, ScanProperties.FORWARD_SCAN));
            }

            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index), context, "_0.cfs");
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void simpleInsertDeleteAndSearch(IndexedType indexedType) {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                Tuple primaryKey1 = createComplexRecordJoinedToSimple(2, 1623L, 1623L, ENGINEER_JOKE, "", true, System.currentTimeMillis(), 0);
                Tuple primaryKey2 = createComplexRecordJoinedToSimple(3, 1624L, 1624L, ENGINEER_JOKE, "", true, System.currentTimeMillis(), 0);
                createComplexRecordJoinedToSimple(1, 1547L, 1547L, WAYLON, "", false, System.currentTimeMillis(), 0);
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey1, primaryKey2),
                        recordStore.scanIndex(index, fullTextSearch(index, "Vision"), null, ScanProperties.FORWARD_SCAN));
                assertTrue(recordStore.deleteRecord(Tuple.from(3, 1624L)));
                assertTrue(recordStore.deleteRecord(Tuple.from(1624L)));
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey1),
                        recordStore.scanIndex(index, fullTextSearch(index, "Vision"), null, ScanProperties.FORWARD_SCAN));

            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
                recordStore.saveRecord(createSimpleDocument(1624L, ENGINEER_JOKE, 2));
                recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 2));
                assertIndexEntryPrimaryKeys(Set.of(1623L, 1624L),
                        recordStore.scanIndex(index, fullTextSearch(index, "Vision"), null, ScanProperties.FORWARD_SCAN));
                assertTrue(recordStore.deleteRecord(Tuple.from(1624L)));
                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, fullTextSearch(index, "Vision"), null, ScanProperties.FORWARD_SCAN));
            }

            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index), context, "_0.cfs");
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void simpleInsertAndSearchSingleTransaction(IndexedType indexedType) {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_KEY);
        LuceneOptimizedPostingsFormat.setAllowCheckDataIntegrity(false);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                // Save one record and try and search for it
                for (int docId = 0; docId < 100; docId++) {
                    createComplexRecordJoinedToSimple(100 + docId, docId, docId, ENGINEER_JOKE, "", true, System.currentTimeMillis(), 0);
                    assertEquals(docId + 1, recordStore.scanIndex(index, fullTextSearch(index, "formulate"), null, ScanProperties.FORWARD_SCAN)
                            .getCount().join());
                }
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                // Save one record and try and search for it
                for (int docId = 0; docId < 100; docId++) {
                    recordStore.saveRecord(createSimpleDocument(docId, ENGINEER_JOKE, 2));
                    assertEquals(docId + 1, recordStore.scanIndex(index, fullTextSearch(index, "formulate"), null, ScanProperties.FORWARD_SCAN)
                            .getCount().join());
                }
            }

            commit(context);
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void testCommit(IndexedType indexedType) {
        Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_KEY);
        Tuple primaryKey1 = null;
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                primaryKey1 = createComplexRecordJoinedToSimple(2, 1623L, 1623L, ENGINEER_JOKE, "", true, System.currentTimeMillis(), 0);
                createComplexRecordJoinedToSimple(1, 1547L, 1547L, WAYLON, "", false, System.currentTimeMillis(), 0);
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey1),
                        recordStore.scanIndex(index, fullTextSearch(index, "Vision"), null, ScanProperties.FORWARD_SCAN));

            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
                recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, fullTextSearch(index, "Vision"), null, ScanProperties.FORWARD_SCAN));

            }
            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index), context, "_0.cfs");
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey1),
                        recordStore.scanIndex(index, fullTextSearch(index, "Vision"), null, ScanProperties.FORWARD_SCAN));

            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, fullTextSearch(index, "Vision"), null, ScanProperties.FORWARD_SCAN));
            }
            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index), context, "_0.cfs");
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void testRollback(IndexedType indexedType) {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_KEY);
        Tuple primaryKey1;
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                primaryKey1 = createComplexRecordJoinedToSimple(2, 1623L, 1623L, ENGINEER_JOKE, "", true, System.currentTimeMillis(), 0);
                createComplexRecordJoinedToSimple(1, 1547L, 1547L, WAYLON, "", false, System.currentTimeMillis(), 0);
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey1),
                        recordStore.scanIndex(index, fullTextSearch(index, "Vision"), null, ScanProperties.FORWARD_SCAN));

            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
                recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, fullTextSearch(index, "Vision"), null, ScanProperties.FORWARD_SCAN));
            }

            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index), context, "_0.cfs");
            context.ensureActive().cancel();
        }
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                createComplexRecordJoinedToSimple(1, 1547L, 1547L, WAYLON, "", false, System.currentTimeMillis(), 0);
                assertIndexEntryPrimaryKeyTuples(Set.of(),
                        recordStore.scanIndex(index, fullTextSearch(index, "Vision"), null, ScanProperties.FORWARD_SCAN));
                primaryKey1 = createComplexRecordJoinedToSimple(2, 1623L, 1623L, ENGINEER_JOKE, "", true, System.currentTimeMillis(), 0);
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey1),
                        recordStore.scanIndex(index, fullTextSearch(index, "Vision"), null, ScanProperties.FORWARD_SCAN));

            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
                assertIndexEntryPrimaryKeys(Collections.emptySet(),
                        recordStore.scanIndex(index, fullTextSearch(index, "Vision"), null, ScanProperties.FORWARD_SCAN));
                recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, fullTextSearch(index, "Vision"), null, ScanProperties.FORWARD_SCAN));
            }

            validateSegmentAndIndexIntegrity(index, recordStore.indexSubspace(index), context, "_0.cfs");
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void testDataLoad(IndexedType indexedType) {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_KEY);
        FDBRecordContext context = openContext();
        if (indexedType.isSynthetic()) {
            for (int i = 0; i < 1000; i++) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                String[] randomWords = LuceneIndexTestUtils.generateRandomWords(500);
                createComplexRecordJoinedToSimple(2000 + i, i, i, randomWords[1], "", false, System.currentTimeMillis(), 0);
                if ((i + 1) % 10 == 0) {
                    commit(context);
                    context = openContext();
                    validateIndexIntegrity(index, recordStore.indexSubspace(index), context, null, null);
                }
            }

        } else {
            for (int i = 0; i < 2000; i++) {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                String[] randomWords = LuceneIndexTestUtils.generateRandomWords(500);
                final TestRecordsTextProto.SimpleDocument dylan = TestRecordsTextProto.SimpleDocument.newBuilder()
                        .setDocId(i)
                        .setText(randomWords[1])
                        .setGroup(2)
                        .build();
                recordStore.saveRecord(dylan);
                if ((i + 1) % 50 == 0) {
                    commit(context);
                    context = openContext();
                    validateIndexIntegrity(index, recordStore.indexSubspace(index), context, null, null);
                }
            }
        }
        context.close();
    }

    private static Stream<Arguments> primaryKeySegmentIndexEnabledParams() {
        return LuceneIndexTestUtils.luceneIndexMapParams().flatMap(
                indexedType -> Stream.of(true, false).map(
                        primaryKeySegmentIndexEnabled -> Arguments.of(indexedType, primaryKeySegmentIndexEnabled)));
    }

    @ParameterizedTest
    @MethodSource("primaryKeySegmentIndexEnabledParams")
    void testSimpleUpdate(IndexedType indexedType, boolean primaryKeySegmentIndexEnabled) throws IOException {
        final Index index = indexedType.getIndex(primaryKeySegmentIndexEnabled ? SIMPLE_TEXT_SUFFIXES_WITH_PRIMARY_KEY_SEGMENT_INDEX_KEY : SIMPLE_TEXT_SUFFIXES_KEY);
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, 3.0)
                .build();
        if (indexedType.isSynthetic()) {
            Map<Long, Tuple> primaryKeys = new HashMap<>();
            for (int i = 0; i < 20; i++) {
                try (FDBRecordContext context = openContext(contextProps)) {
                    openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                    var existenceCheck = i < 5
                                         ? FDBRecordStoreBase.RecordExistenceCheck.ERROR_IF_EXISTS
                                         : FDBRecordStoreBase.RecordExistenceCheck.ERROR_IF_NOT_EXISTS;
                    Tuple primaryKey = createComplexRecordJoinedToSimple(3000 + i, 1000L + i % 5, 1000L + i % 5, numbersText(i + 1), "", false, System.currentTimeMillis(), 0, existenceCheck);
                    primaryKeys.put((Long) ((List) primaryKey.getItems().get(2)).get(0), primaryKey);
                    context.commit();
                }
            }

            if (primaryKeySegmentIndexEnabled) {
                assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY), equalTo(0));
                assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY), greaterThan(10));
                assertThat(timer.getCount(LuceneEvents.Counts.LUCENE_MERGE_SEGMENTS), equalTo(0));
            } else {
                assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY), greaterThan(10));
                assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY), equalTo(0));
                assertThat(timer.getCount(LuceneEvents.Counts.LUCENE_MERGE_SEGMENTS), equalTo(0));
            }

            try (FDBRecordContext context = openContext()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(-1, Tuple.from(3017, 1002), Tuple.from(1002))), // 18
                        recordStore.scanIndex(index, fullTextSearch(index, "three"), null, ScanProperties.FORWARD_SCAN));
                assertIndexEntryPrimaryKeyTuples(Set.of(
                                                Tuple.from(-1, Tuple.from(3015, 1000), Tuple.from(1000)),
                                                Tuple.from(-1, Tuple.from(3019, 1004), Tuple.from(1004))),  // 16,20
                        recordStore.scanIndex(index, fullTextSearch(index, "four"), null, ScanProperties.FORWARD_SCAN));
                assertIndexEntryPrimaryKeyTuples(Set.of(),
                        recordStore.scanIndex(index, fullTextSearch(index, "seven"), null, ScanProperties.FORWARD_SCAN));

                if (primaryKeySegmentIndexEnabled) {
                    // TODO: Is there a more stable way to check this?
                    final LucenePrimaryKeySegmentIndex primaryKeySegmentIndex = getDirectory(index, Tuple.from())
                            .getPrimaryKeySegmentIndex();
                    assertEquals(new ArrayList<>(Arrays.asList(
                                    new ArrayList<>(Arrays.asList(-1L, new ArrayList<>(Arrays.asList(3015L, 1000L)), new ArrayList<>(Arrays.asList(1000L)), "_q", 2)),
                                    new ArrayList<>(Arrays.asList(-1L, new ArrayList<>(Arrays.asList(3016L, 1001L)), new ArrayList<>(Arrays.asList(1001L)), "_q", 0)),
                                    new ArrayList<>(Arrays.asList(-1L, new ArrayList<>(Arrays.asList(3017L, 1002L)), new ArrayList<>(Arrays.asList(1002L)), "_o", 0)),
                                    new ArrayList<>(Arrays.asList(-1L, new ArrayList<>(Arrays.asList(3018L, 1003L)), new ArrayList<>(Arrays.asList(1003L)), "_q", 1)),
                                    new ArrayList<>(Arrays.asList(-1L, new ArrayList<>(Arrays.asList(3019L, 1004L)), new ArrayList<>(Arrays.asList(1004L)), "_r", 0)))
                            ),
                            primaryKeySegmentIndex.readAllEntries());
                }
                LuceneIndexTestValidator.validatePrimaryKeySegmentIndex(recordStore, index,
                        Tuple.from(), null, new HashSet<>(primaryKeys.values()), false);
            }
        } else {
            Set<Tuple> primaryKeys = new HashSet<>();
            for (int i = 0; i < 20; i++) {
                try (FDBRecordContext context = openContext(contextProps)) {
                    rebuildIndexMetaData(context, SIMPLE_DOC, index);
                    var existenceCheck = i < 5
                                         ? FDBRecordStoreBase.RecordExistenceCheck.ERROR_IF_EXISTS
                                         : FDBRecordStoreBase.RecordExistenceCheck.ERROR_IF_NOT_EXISTS;
                    final TestRecordsTextProto.SimpleDocument record = createSimpleDocument(1000L + i % 5, numbersText(i + 1), null);
                    primaryKeys.add(recordStore.saveRecord(record, existenceCheck).getPrimaryKey());
                    context.commit();
                }
            }

            if (primaryKeySegmentIndexEnabled) {
                assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY), equalTo(0));
                assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY), greaterThan(10));
                assertThat(timer.getCount(LuceneEvents.Counts.LUCENE_MERGE_SEGMENTS), equalTo(0));
            } else {
                assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY), greaterThan(10));
                assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY), equalTo(0));
                assertThat(timer.getCount(LuceneEvents.Counts.LUCENE_MERGE_SEGMENTS), equalTo(0));
            }

            try (FDBRecordContext context = openContext()) {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                assertIndexEntryPrimaryKeys(Set.of(1002L), // 18
                        recordStore.scanIndex(index, fullTextSearch(index, "three"), null, ScanProperties.FORWARD_SCAN));
                assertIndexEntryPrimaryKeys(Set.of(1000L, 1004L),  // 16,20
                        recordStore.scanIndex(index, fullTextSearch(index, "four"), null, ScanProperties.FORWARD_SCAN));
                assertIndexEntryPrimaryKeys(Set.of(),
                        recordStore.scanIndex(index, fullTextSearch(index, "seven"), null, ScanProperties.FORWARD_SCAN));

                if (primaryKeySegmentIndexEnabled) {
                    // TODO: Is there a more stable way to check this?
                    final LucenePrimaryKeySegmentIndex primaryKeySegmentIndex = getDirectory(index, Tuple.from())
                            .getPrimaryKeySegmentIndex();
                    assertEquals(List.of(
                                    List.of(1000L, "_q", 2),
                                    List.of(1001L, "_q", 0),
                                    List.of(1002L, "_o", 0),
                                    List.of(1003L, "_q", 1),
                                    List.of(1004L, "_r", 0)
                            ),
                            primaryKeySegmentIndex.readAllEntries());
                }
                LuceneIndexTestValidator.validatePrimaryKeySegmentIndex(recordStore, index,
                        Tuple.from(), null, primaryKeys, false);
            }
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void simpleDeleteSegmentIndex(IndexedType indexedType) {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_WITH_PRIMARY_KEY_SEGMENT_INDEX_KEY);
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, 3.0)
                .build();
        Tuple primaryKey1 = null;
        Tuple primaryKey2 = null;
        Tuple primaryKey3 = null;
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                primaryKey1 = createComplexRecordJoinedToSimple(2, 1623L, 1623L, ENGINEER_JOKE, "", false, System.currentTimeMillis(), 0);
                primaryKey2 = createComplexRecordJoinedToSimple(3, 1624L, 1624L, ENGINEER_JOKE, "", false, System.currentTimeMillis(), 0);
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
                recordStore.saveRecord(createSimpleDocument(1624L, ENGINEER_JOKE, 2));
            }
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                primaryKey3 = createComplexRecordJoinedToSimple(4, 1547L, 1547L, WAYLON, "", false, System.currentTimeMillis(), 0);
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 2));
            }
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey1, primaryKey2),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:Vision"), null, ScanProperties.FORWARD_SCAN));
                assertTrue(recordStore.deleteRecord(Tuple.from(3, 1624L)));
                assertTrue(recordStore.deleteRecord(Tuple.from(1624L)));
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey1),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:Vision"), null, ScanProperties.FORWARD_SCAN));
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                assertIndexEntryPrimaryKeys(Set.of(1623L, 1624L),
                        recordStore.scanIndex(index, fullTextSearch(index, "Vision"), null, ScanProperties.FORWARD_SCAN));
                assertTrue(recordStore.deleteRecord(Tuple.from(1624L)));
                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, fullTextSearch(index, "Vision"), null, ScanProperties.FORWARD_SCAN));
            }
            assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY), equalTo(0));
            assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY), equalTo(1));
        }
        timer.reset();
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey1, primaryKey2, primaryKey3),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:way"), null, ScanProperties.FORWARD_SCAN));
                assertTrue(recordStore.deleteRecord(Tuple.from(4, 1547L)));
                assertTrue(recordStore.deleteRecord(Tuple.from(1547L)));
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey1, primaryKey2),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:way"), null, ScanProperties.FORWARD_SCAN));
                primaryKey3 = createComplexRecordJoinedToSimple(4, 1547L, 1547L, ENGINEER_JOKE, "", false, System.currentTimeMillis(), 0);
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey1, primaryKey2, primaryKey3),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:Vision"), null, ScanProperties.FORWARD_SCAN));
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                assertIndexEntryPrimaryKeys(Set.of(1623L, 1624L, 1547L),
                        recordStore.scanIndex(index, fullTextSearch(index, "way"), null, ScanProperties.FORWARD_SCAN));
                assertTrue(recordStore.deleteRecord(Tuple.from(1547L)));
                assertIndexEntryPrimaryKeys(Set.of(1623L, 1624L),
                        recordStore.scanIndex(index, fullTextSearch(index, "way"), null, ScanProperties.FORWARD_SCAN));
                recordStore.saveRecord(createSimpleDocument(1547L, ENGINEER_JOKE, 2));
                assertIndexEntryPrimaryKeys(Set.of(1623L, 1624L, 1547L),
                        recordStore.scanIndex(index, fullTextSearch(index, "Vision"), null, ScanProperties.FORWARD_SCAN));
            }
            assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY), equalTo(0));
            assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY), equalTo(1));
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void fullDeleteSegmentIndex(IndexedType indexedType) throws Exception {
        fullDeleteHelper(indexMaintainer -> {
            final LucenePrimaryKeySegmentIndex primaryKeySegmentIndex1 = indexMaintainer
                    .getDirectory(Tuple.from(), null)
                    .getPrimaryKeySegmentIndex();
            assertEquals(List.of(), primaryKeySegmentIndex1.readAllEntries());
        }, indexMaintainer -> {
            final LucenePrimaryKeySegmentIndex primaryKeySegmentIndex = indexMaintainer
                    .getDirectory(Tuple.from(), null)
                    .getPrimaryKeySegmentIndex();
            assertNotEquals(List.of(), primaryKeySegmentIndex.readAllEntries());
        },
                indexedType);
    }

    @Nonnull
    private Index fullDeleteHelper(final TestHelpers.DangerousConsumer<LuceneIndexMaintainer> assertEmpty,
                                   final TestHelpers.DangerousConsumer<LuceneIndexMaintainer> assertNotEmpty,
                                   final IndexedType indexedType) throws Exception {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_WITH_PRIMARY_KEY_SEGMENT_INDEX_KEY);
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, 3.0)
                .build();
        try (FDBRecordContext context = openContext(contextProps)) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
            }
            assertTrue(recordStore.getIndexDeferredMaintenanceControl().shouldAutoMergeDuringCommit());
            assertEmpty.accept(getIndexMaintainer(index));
        }
        Set<Tuple> primaryKeys = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            try (FDBRecordContext context = openContext(contextProps)) {
                if (indexedType.isSynthetic()) {
                    openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                    primaryKeys.add(createComplexRecordJoinedToSimple(2000 + i, 1000 + i, 1000 + i, ENGINEER_JOKE, "", false, System.currentTimeMillis(), 0));
                    primaryKeys.add(createComplexRecordJoinedToSimple(2010 + i, 1010 + i, 1010 + i, WAYLON, "", false, System.currentTimeMillis(), 0));
                } else {
                    rebuildIndexMetaData(context, SIMPLE_DOC, index);
                    primaryKeys.add(recordStore.saveRecord(createSimpleDocument(1000 + i, ENGINEER_JOKE, 2)).getPrimaryKey());
                    primaryKeys.add(recordStore.saveRecord(createSimpleDocument(1010 + i, WAYLON, 2)).getPrimaryKey());
                }
                context.commit();
            }
        }
        try (FDBRecordContext context = openContext(contextProps)) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
            }
            assertTrue(recordStore.getIndexDeferredMaintenanceControl().shouldAutoMergeDuringCommit());
            assertNotEmpty.accept(getIndexMaintainer(index));
            LuceneIndexTestValidator.validatePrimaryKeySegmentIndex(recordStore, index,
                    Tuple.from(), null, primaryKeys, false);
        }
        for (int i = 0; i < 4; i++) {
            try (FDBRecordContext context = openContext(contextProps)) {
                if (indexedType.isSynthetic()) {
                    openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                    assertTrue(recordStore.getIndexDeferredMaintenanceControl().shouldAutoMergeDuringCommit());
                    for (int j = 0; j < 5; j++) {
                        final int docId = 1000 + i * 5 + j;
                        final int group = 2000 + i * 5 + j;
                        recordStore.deleteRecord(Tuple.from(group, docId));
                        recordStore.deleteRecord(Tuple.from(docId));
                        primaryKeys.remove(Tuple.from(-1,
                                new ArrayList<>(Arrays.asList(group, docId)),
                                new ArrayList<>(Arrays.asList(docId))));
                    }
                } else {
                    rebuildIndexMetaData(context, SIMPLE_DOC, index);
                    assertTrue(recordStore.getIndexDeferredMaintenanceControl().shouldAutoMergeDuringCommit());
                    for (int j = 0; j < 5; j++) {
                        final int docId = 1000 + i * 5 + j;
                        recordStore.deleteRecord(Tuple.from(docId));
                        primaryKeys.remove(Tuple.from(docId));
                    }
                }
                context.commit();
            }

            try (FDBRecordContext context = openContext(contextProps)) {
                if (indexedType.isSynthetic()) {
                    openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                } else {
                    rebuildIndexMetaData(context, SIMPLE_DOC, index);
                }
                LuceneIndexTestValidator.validatePrimaryKeySegmentIndex(recordStore, index,
                        Tuple.from(), null, primaryKeys, false);
            }
        }
        // without this Lucene might not cleanup the files for the segments that have no live documents in them
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setRecordStore(recordStore)
                .setIndex(index)
                .build()) {
            indexBuilder.mergeIndex();
        }

        try (FDBRecordContext context = openContext(contextProps)) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
            }
            assertEmpty.accept(getIndexMaintainer(index));
            LuceneIndexTestValidator.validatePrimaryKeySegmentIndex(recordStore, index,
                    Tuple.from(), null, Set.of(), false);
        }
        return index;
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void fullDeleteFieldInfos(IndexedType indexedType) throws Exception {
        // if we delete all the documents, the FieldInfos should be cleared out
        fullDeleteHelper(indexMaintainer -> {
            FDBDirectory fdbDirectory = indexMaintainer.getDirectory(Tuple.from(), null);
            final var allFieldInfos = fdbDirectory.getFieldInfosStorage().getAllFieldInfos();
            assertEquals(Map.of(), allFieldInfos,
                    () -> String.join(", ", indexMaintainer.getDirectory(Tuple.from(), null).listAll()));
        }, indexMaintainer -> {
            FDBDirectory fdbDirectory = indexMaintainer.getDirectory(Tuple.from(), null);
            final var allFieldInfos = fdbDirectory.getFieldInfosStorage().getAllFieldInfos();
            assertNotEquals(Map.of(), allFieldInfos,
                    () -> String.join(", ", indexMaintainer.getDirectory(Tuple.from(), null).listAll()));
        },
                indexedType);
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void checkFileCountAfterMerge(IndexedType indexedType) {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_WITH_PRIMARY_KEY_SEGMENT_INDEX_KEY);
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, 3.0)
                .build();
        int lastFileCount = -1;
        boolean mergeHappened = false;
        for (int i = 0; i < 10; i++) {
            try (FDBRecordContext context = openContext(contextProps)) {
                if (indexedType.isSynthetic()) {
                    openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                    createComplexRecordJoinedToSimple(2000 + i, 1000 + i, 1000 + i, ENGINEER_JOKE, "", false, System.currentTimeMillis(), 0);
                    createComplexRecordJoinedToSimple(2010 + i, 1010 + i, 1010 + i, WAYLON, "", false, System.currentTimeMillis(), 0);
                } else {
                    rebuildIndexMetaData(context, SIMPLE_DOC, index);
                    recordStore.saveRecord(createSimpleDocument(1000 + i, ENGINEER_JOKE, 2));
                    recordStore.saveRecord(createSimpleDocument(1010 + i, WAYLON, 2));
                }
                context.commit();
            }

            try (FDBRecordContext context = openContext(contextProps)) {
                if (indexedType.isSynthetic()) {
                    openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                } else {
                    rebuildIndexMetaData(context, SIMPLE_DOC, index);
                }
                validateIndexIntegrity(index, recordStore.indexSubspace(index), context, null, null);
                final int fileCount = getDirectory(index, Tuple.from()).listAll().length;
                if (fileCount < lastFileCount) {
                    mergeHappened = true;
                }
                lastFileCount = fileCount;
                context.commit();
            }
        }
        assertTrue(mergeHappened);
    }

    private static Stream<Arguments> autoMergeParams() {
        return LuceneIndexTestUtils.luceneIndexMapParams().flatMap(
                indexedType -> Stream.of(true, false).map(
                        autoMerge -> Arguments.of(indexedType, autoMerge)));
    }

    @ParameterizedTest
    @MethodSource("autoMergeParams")
    void testMultipleUpdateSegments(IndexedType indexedType, boolean autoMerge) {
        final Index index = indexedType.getIndex(COMPLEX_GROUPED_WITH_PRIMARY_KEY_SEGMENT_INDEX_KEY);
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, 3.0)
                .build();
        try (FDBRecordContext context = openContext(contextProps)) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                for (int i = 0; i < 20; i++) {
                    createComplexRecordJoinedToSimple(1000 + i, i, i, "", "", false, System.currentTimeMillis(), 0);
                }
            } else {
                rebuildIndexMetaData(context, COMPLEX_DOC, index);
                for (int i = 0; i < 20; i++) {
                    recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(i, "", "", 0));
                }
            }
            context.commit();
        }
        final long segmentCountBefore;
        try (FDBRecordContext context = openContext(contextProps)) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                segmentCountBefore = getSegmentCount(index, Tuple.from(-1, Tuple.from(1000, 0), Tuple.from(0)));
            } else {
                rebuildIndexMetaData(context, COMPLEX_DOC, index);
                segmentCountBefore = getSegmentCount(index, Tuple.from(0));
            }
        }
        try (FDBRecordContext context = openContext(contextProps)) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(autoMerge);
                for (int i = 0; i < 10; i++) {
                    createComplexRecordJoinedToSimple(2000 + i, i, i, "", "", false, System.currentTimeMillis(), 0);
                }
            } else {
                rebuildIndexMetaData(context, COMPLEX_DOC, index);
                recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(autoMerge);
                for (int i = 0; i < 10; i++) {
                    recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(i, numbersText(i), "", 0));
                }
            }
            context.commit();
        }
        assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY), equalTo(0));
        assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY), equalTo(10));
        assertThat(timer.getCount(LuceneEvents.Events.LUCENE_FIND_MERGES), lessThan(5));
        try (FDBRecordContext context = openContext(contextProps)) {
            long segmentCountAfter;
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                segmentCountAfter = getSegmentCount(index, Tuple.from(0));
            } else {
                rebuildIndexMetaData(context, COMPLEX_DOC, index);
                segmentCountAfter = getSegmentCount(index, Tuple.from(-1, Tuple.from(1000, 0), Tuple.from(0)));
            }
            assertThat(segmentCountAfter - segmentCountBefore, lessThan(5L));
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void testGroupedMultipleUpdate(IndexedType indexedType) {
        final Index index = indexedType.getIndex(COMPLEX_GROUPED_WITH_PRIMARY_KEY_SEGMENT_INDEX_KEY);
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, 3.0)
                .build();
        for (int g = 0; g < 3; g++) {
            for (int i = 0; i < 5; i++) {
                try (FDBRecordContext context = openContext(contextProps)) {
                    if (indexedType.isSynthetic()) {
                        openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                        for (int j = 0; j < 10; j++) {
                            int n = i * 10 + j + 1;
                            createComplexRecordJoinedToSimple(1000 * (g + 1) + n, 100 * g + n, 100 * g + n, "", numbersText(n), false, System.currentTimeMillis(), g);
                        }
                    } else {
                        rebuildIndexMetaData(context, COMPLEX_DOC, index);
                        for (int j = 0; j < 10; j++) {
                            int n = i * 10 + j + 1;
                            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(n, numbersText(n), "", g));
                        }
                    }
                    context.commit();
                }
            }
        }
        try (FDBRecordContext context = openContext(contextProps)) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                assertEquals(IntStream.rangeClosed(1, 7).mapToObj(i -> new ArrayList<>(Arrays.asList(i * 7L))).collect(Collectors.toSet()),
                        new HashSet<>(recordStore.scanIndex(index, groupedTextSearch(index, "complex_text2:seven", 0), null, ScanProperties.FORWARD_SCAN)
                                .map(x -> x.getPrimaryKey().getItems().get(2)).asList().join()));
            } else {
                rebuildIndexMetaData(context, COMPLEX_DOC, index);
                assertEquals(IntStream.rangeClosed(1, 7).mapToObj(i -> Tuple.from(0, i * 7)).collect(Collectors.toSet()),
                        new HashSet<>(recordStore.scanIndex(index, groupedTextSearch(index, "text:seven", 0L), null, ScanProperties.FORWARD_SCAN)
                                .map(IndexEntry::getPrimaryKey).asList().join()));
            }
        }
        try (FDBRecordContext context = openContext(contextProps)) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(false);
                createComplexRecordJoinedToSimple(4000, 49, 49, "", "not here", false, System.currentTimeMillis(), 0);
                createComplexRecordJoinedToSimple(4001, 35, 35, "", "nor here either", false, System.currentTimeMillis(), 0);
            } else {
                rebuildIndexMetaData(context, COMPLEX_DOC, index);
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(49, "not here", "", 0));
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(35, "nor here either", "", 0));
            }
            context.commit();
        }
        try (FDBRecordContext context = openContext(contextProps)) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                assertEquals(Stream.of(1, 2, 3, 4, 6).map(i -> new ArrayList<>(Arrays.asList(i * 7L))).collect(Collectors.toSet()),
                        new HashSet<>(recordStore.scanIndex(index, groupedTextSearch(index, "complex_text2:seven", 0L), null, ScanProperties.FORWARD_SCAN)
                                .map(x -> x.getPrimaryKey().getItems().get(2)).asList().join()));
                assertEquals(Stream.of(5, 7).map(i -> new ArrayList<>(Arrays.asList(i * 7L))).collect(Collectors.toSet()),
                        new HashSet<>(recordStore.scanIndex(index, groupedTextSearch(index, "complex_text2:here", 0L), null, ScanProperties.FORWARD_SCAN)
                                .map(x -> x.getPrimaryKey().getItems().get(2)).asList().join()));
            } else {
                rebuildIndexMetaData(context, COMPLEX_DOC, index);
                assertEquals(Stream.of(1, 2, 3, 4, 6).map(i -> Tuple.from(0, i * 7)).collect(Collectors.toSet()),
                        new HashSet<>(recordStore.scanIndex(index, groupedTextSearch(index, "text:seven", 0L), null, ScanProperties.FORWARD_SCAN)
                                .map(IndexEntry::getPrimaryKey).asList().join()));
                assertEquals(Stream.of(5, 7).map(i -> Tuple.from(0, i * 7)).collect(Collectors.toSet()),
                        new HashSet<>(recordStore.scanIndex(index, groupedTextSearch(index, "text:here", 0L), null, ScanProperties.FORWARD_SCAN)
                                .map(IndexEntry::getPrimaryKey).asList().join()));
            }
        }
        assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY), equalTo(0));
        assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY), equalTo(2));
    }

    private String numbersText(int i) {
        final String[] nums = {"zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"};
        return IntStream.range(1, nums.length)
                .filter(n -> i % n == 0)
                .mapToObj(n -> nums[n])
                .collect(Collectors.joining(" "));
    }

    private String matchAll(String luceneField, String... words) {
        return luceneField + ":(" +
                Arrays.stream(words).map(word -> "+\"" + word + "\"").collect(Collectors.joining(" AND ")) +
                ")";
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void scanWithQueryOnlySynonymIndex(IndexedType indexedType) {
        final Index index = indexedType.getIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                Tuple primaryKey = createComplexRecordJoinedToSimple(1, 1623L, 1623L, "Hello record layer", "", false, System.currentTimeMillis(), 0);
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "hullo record layer"), null, ScanProperties.FORWARD_SCAN));
                primaryKey = createComplexRecordJoinedToSimple(1, 1623L, 1623L, "Hello recor layer", "", false, System.currentTimeMillis(), 0);
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "hullo record layer"), null, ScanProperties.FORWARD_SCAN));
                // "hullo" is synonym of "hello"
                primaryKey = createComplexRecordJoinedToSimple(1, 1623L, 1623L, "Hello record layer", "", false, System.currentTimeMillis(), 0);
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, matchAll("simple_text", "hullo", "record", "layer")), null, ScanProperties.FORWARD_SCAN));
                // it doesn't match due to the "recor"
                primaryKey = createComplexRecordJoinedToSimple(1, 1623L, 1623L, "Hello record layer", "", false, System.currentTimeMillis(), 0);
                assertIndexEntryPrimaryKeyTuples(Collections.emptySet(),
                        recordStore.scanIndex(index, fullTextSearch(index, matchAll("simple_text", "hullo", "recor", "layer")), null, ScanProperties.FORWARD_SCAN));
                // "hullo" is synonym of "hello", and "show" is synonym of "record"
                primaryKey = createComplexRecordJoinedToSimple(1, 1623L, 1623L, "Hello record layer", "", false, System.currentTimeMillis(), 0);
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, matchAll("simple_text", "hullo", "show", "layer")), null, ScanProperties.FORWARD_SCAN));
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, fullTextSearch(index, "hullo record layer"), null, ScanProperties.FORWARD_SCAN));
                recordStore.saveRecord(createSimpleDocument(1623L, "Hello recor layer", 1));
                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, fullTextSearch(index, "hullo record layer"), null, ScanProperties.FORWARD_SCAN));
                // "hullo" is synonym of "hello"
                recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, fullTextSearch(index, matchAll("text", "hullo", "record", "layer")), null, ScanProperties.FORWARD_SCAN));
                // it doesn't match due to the "recor"
                recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
                assertIndexEntryPrimaryKeys(Collections.emptySet(),
                        recordStore.scanIndex(index, fullTextSearch(index, matchAll("text", "hullo", "recor", "layer")), null, ScanProperties.FORWARD_SCAN));
                // "hullo" is synonym of "hello", and "show" is synonym of "record"
                recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, fullTextSearch(index, matchAll("text", "hullo", "show", "layer")), null, ScanProperties.FORWARD_SCAN));
            }
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void scanWithAuthoritativeSynonymOnlyIndex(IndexedType indexedType) {
        final Index index = indexedType.getIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                createComplexRecordJoinedToSimple(1, 1623L, 1623L, "Hello record layer", "", false, System.currentTimeMillis(), 0);
                assertEquals(1, recordStore.scanIndex(index, fullTextSearch(index, "hullo record layer"), null, ScanProperties.FORWARD_SCAN)
                        .getCount().join());
                createComplexRecordJoinedToSimple(1, 1623L, 1623L, "Hello recor layer", "", false, System.currentTimeMillis(), 0);
                assertEquals(1, recordStore.scanIndex(index, fullTextSearch(index, "hullo record layer"), null, ScanProperties.FORWARD_SCAN)
                        .getCount().join());
                // "hullo" is synonym of "hello"
                createComplexRecordJoinedToSimple(1, 1623L, 1623L, "Hello record layer", "", false, System.currentTimeMillis(), 0);
                assertEquals(1, recordStore.scanIndex(index,
                                fullTextSearch(index, matchAll("simple_text", "hullo", "record", "layer")), null, ScanProperties.FORWARD_SCAN)
                        .getCount().join());
                // it doesn't match due to the "recor"
                recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
                assertEquals(0, recordStore.scanIndex(index,
                                fullTextSearch(index, matchAll("simple_text", "hullo", "recor", "layer")), null, ScanProperties.FORWARD_SCAN)
                        .getCount().join());
                // "hullo" is synonym of "hello", and "show" is synonym of "record"
                recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
                assertEquals(1, recordStore.scanIndex(index,
                                fullTextSearch(index, matchAll("simple_text", "hullo", "show", "layer")), null, ScanProperties.FORWARD_SCAN)
                        .getCount().join());
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
                assertEquals(1, recordStore.scanIndex(index, fullTextSearch(index, "hullo record layer"), null, ScanProperties.FORWARD_SCAN)
                        .getCount().join());
                recordStore.saveRecord(createSimpleDocument(1623L, "Hello recor layer", 1));
                assertEquals(1, recordStore.scanIndex(index, fullTextSearch(index, "hullo record layer"), null, ScanProperties.FORWARD_SCAN)
                        .getCount().join());
                // "hullo" is synonym of "hello"
                recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
                assertEquals(1, recordStore.scanIndex(index,
                                fullTextSearch(index, matchAll("text", "hullo", "record", "layer")), null, ScanProperties.FORWARD_SCAN)
                        .getCount().join());
                // it doesn't match due to the "recor"
                recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
                assertEquals(0, recordStore.scanIndex(index,
                                fullTextSearch(index, matchAll("text", "hullo", "recor", "layer")), null, ScanProperties.FORWARD_SCAN)
                        .getCount().join());
                // "hullo" is synonym of "hello", and "show" is synonym of "record"
                recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
                assertEquals(1, recordStore.scanIndex(index,
                                fullTextSearch(index, matchAll("text", "hullo", "show", "layer")), null, ScanProperties.FORWARD_SCAN)
                        .getCount().join());
            }
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void phraseSearchBasedOnQueryOnlySynonymIndex(IndexedType indexedType) {
        final Index index = indexedType.getIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                // Save a document to verify synonym search based on the group {'motivation','motive','need'}
                Tuple primaryKey = createComplexRecordJoinedToSimple(1, 1623L, 1623L, "I think you need to search with Lucene index", "", false, System.currentTimeMillis(), 0);
                // Search for original phrase
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:\"you need to\""), null, ScanProperties.FORWARD_SCAN));
                // Search for phrase with changed order of tokens, no match is expected
                assertIndexEntryPrimaryKeyTuples(Collections.emptySet(),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:\"need you to\""), null, ScanProperties.FORWARD_SCAN));
                // Search for phrase with "motivation" as synonym of "need"
                // "Motivation" is the authoritative term of the group {'motivation','motive','need'}
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:\"you motivation to\""), null, ScanProperties.FORWARD_SCAN));
                // Search for phrase with "motivation" with changed order of tokens, no match is expected
                assertIndexEntryPrimaryKeyTuples(Collections.emptySet(),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:\"motivation you to\""), null, ScanProperties.FORWARD_SCAN));
                // Search for phrase with "motive" as synonym of "need"
                // "Motive" is not the authoritative term of the group {'motivation','motive','need'}
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:\"you motive to\""), null, ScanProperties.FORWARD_SCAN));
                // Search for phrase with "motive" with changed order of tokens, no match is expected
                assertIndexEntryPrimaryKeyTuples(Collections.emptySet(),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:\"motive you to\""), null, ScanProperties.FORWARD_SCAN));
                // Term query rather than phrase query, so match is expected although the order is changed
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:motivation simple_text:you simple_text:to"), null, ScanProperties.FORWARD_SCAN));

                // Save a document to verify synonym search based on the group {'departure','going','going away','leaving'}
                // This group contains multi-word term "going away", and also the single-word term "going"
                primaryKey = createComplexRecordJoinedToSimple(2, 1624L, 1624L, "He is leaving for New York next week", "", false, System.currentTimeMillis(), 0);
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:\"is departure for\""), null, ScanProperties.FORWARD_SCAN));

                // Search for phrase with "going away"
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:\"is going away for\""), null, ScanProperties.FORWARD_SCAN));

                //Search for phrase with "going"
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:\"is going for\""), null, ScanProperties.FORWARD_SCAN));

                // Search for phrase with only "away", no match is expected
                assertIndexEntryPrimaryKeyTuples(Collections.emptySet(),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:\"is away for\""), null, ScanProperties.FORWARD_SCAN));
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                // Save a document to verify synonym search based on the group {'motivation','motive','need'}
                recordStore.saveRecord(createSimpleDocument(1623L, "I think you need to search with Lucene index", 1));
                // Search for original phrase
                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"you need to\""), null, ScanProperties.FORWARD_SCAN));
                // Search for phrase with changed order of tokens, no match is expected
                assertIndexEntryPrimaryKeys(Collections.emptySet(),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"need you to\""), null, ScanProperties.FORWARD_SCAN));
                // Search for phrase with "motivation" as synonym of "need"
                // "Motivation" is the authoritative term of the group {'motivation','motive','need'}
                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"you motivation to\""), null, ScanProperties.FORWARD_SCAN));
                // Search for phrase with "motivation" with changed order of tokens, no match is expected
                assertIndexEntryPrimaryKeys(Collections.emptySet(),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"motivation you to\""), null, ScanProperties.FORWARD_SCAN));
                // Search for phrase with "motive" as synonym of "need"
                // "Motive" is not the authoritative term of the group {'motivation','motive','need'}
                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"you motive to\""), null, ScanProperties.FORWARD_SCAN));
                // Search for phrase with "motive" with changed order of tokens, no match is expected
                assertIndexEntryPrimaryKeys(Collections.emptySet(),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"motive you to\""), null, ScanProperties.FORWARD_SCAN));
                // Term query rather than phrase query, so match is expected although the order is changed
                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, fullTextSearch(index, "motivation you to"), null, ScanProperties.FORWARD_SCAN));

                // Save a document to verify synonym search based on the group {'departure','going','going away','leaving'}
                // This group contains multi-word term "going away", and also the single-word term "going"
                recordStore.saveRecord(createSimpleDocument(1624L, "He is leaving for New York next week", 1));
                assertIndexEntryPrimaryKeys(Set.of(1624L),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"is departure for\""), null, ScanProperties.FORWARD_SCAN));

                // Search for phrase with "going away"
                assertIndexEntryPrimaryKeys(Set.of(1624L),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"is going away for\""), null, ScanProperties.FORWARD_SCAN));

                //Search for phrase with "going"
                assertIndexEntryPrimaryKeys(Set.of(1624L),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"is going for\""), null, ScanProperties.FORWARD_SCAN));

                // Search for phrase with only "away", no match is expected
                assertIndexEntryPrimaryKeys(Collections.emptySet(),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"is away for\""), null, ScanProperties.FORWARD_SCAN));
            }
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void phraseSearchBasedOnAuthoritativeSynonymOnlyIndex(IndexedType indexedType) {
        final Index index = indexedType.getIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                // Save a document to verify synonym search based on the group {'motivation','motive','need'}
                Tuple primaryKey = createComplexRecordJoinedToSimple(1, 1623L, 1623L, "I think you need to search with Lucene index", "", false, System.currentTimeMillis(), 0);
                // Search for original phrase
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:\"you need to\""), null, ScanProperties.FORWARD_SCAN));
                // Search for phrase with changed order of tokens, no match is expected
                assertIndexEntryPrimaryKeyTuples(Collections.emptySet(),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:\"need you to\""), null, ScanProperties.FORWARD_SCAN));
                // Search for phrase with "motivation" as synonym of "need"
                // "Motivation" is the authoritative term of the group {'motivation','motive','need'}
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:\"you motivation to\""), null, ScanProperties.FORWARD_SCAN));
                // Search for phrase with "motivation" with changed order of tokens, no match is expected
                assertIndexEntryPrimaryKeyTuples(Collections.emptySet(),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:\"motivation you to\""), null, ScanProperties.FORWARD_SCAN));
                // Search for phrase with "motive" as synonym of "need"
                // "Motive" is not the authoritative term of the group {'motivation','motive','need'}
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:\"you motive to\""), null, ScanProperties.FORWARD_SCAN));
                // Search for phrase with "motive" with changed order of tokens, no match is expected
                assertIndexEntryPrimaryKeyTuples(Collections.emptySet(),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:\"motive you to\""), null, ScanProperties.FORWARD_SCAN));
                // Term query rather than phrase query, so match is expected although the order is changed
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:motivation simple_text:you simple_text:to"), null, ScanProperties.FORWARD_SCAN));

                // Save a document to verify synonym search based on the group {'departure','going','going away','leaving'}
                // This group contains multi-word term "going away", and also the single-word term "going"
                primaryKey = createComplexRecordJoinedToSimple(2, 1624L, 1624L, "He is leaving for New York next week", "", false, System.currentTimeMillis(), 0);
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:\"is departure for\""), null, ScanProperties.FORWARD_SCAN));

                // Search for phrase with "going away"
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:\"is going away for\""), null, ScanProperties.FORWARD_SCAN));

                //Search for phrase with "going"
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:\"is going for\""), null, ScanProperties.FORWARD_SCAN));

                // Search for phrase with only "away", the correct behavior is to return no match. But match is still hit due to the poor handling of positional data for multi-word synonym by this analyzer
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:\"is away for\""), null, ScanProperties.FORWARD_SCAN));
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                recordStore.saveRecord(createSimpleDocument(1623L, "I think you need to search with Lucene index", 1));
                // Search for original phrase
                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"you need to\""), null, ScanProperties.FORWARD_SCAN));
                // Search for phrase with changed order of tokens, no match is expected
                assertIndexEntryPrimaryKeys(Collections.emptySet(),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"need you to\""), null, ScanProperties.FORWARD_SCAN));
                // Search for phrase with "motivation" as synonym of "need"
                // "Motivation" is the authoritative term of the group {'motivation','motive','need'}
                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"you motivation to\""), null, ScanProperties.FORWARD_SCAN));
                // Search for phrase with "motivation" with changed order of tokens, no match is expected
                assertIndexEntryPrimaryKeys(Collections.emptySet(),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"motivation you to\""), null, ScanProperties.FORWARD_SCAN));
                // Search for phrase with "motive" as synonym of "need"
                // "Motive" is not the authoritative term of the group {'motivation','motive','need'}
                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"you motive to\""), null, ScanProperties.FORWARD_SCAN));
                // Search for phrase with "motive" with changed order of tokens, no match is expected
                assertIndexEntryPrimaryKeys(Collections.emptySet(),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"motive you to\""), null, ScanProperties.FORWARD_SCAN));
                // Term query rather than phrase query, so match is expected although the order is changed
                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, fullTextSearch(index, "motivation you to"), null, ScanProperties.FORWARD_SCAN));

                // Save a document to verify synonym search based on the group {'departure','going','going away','leaving'}
                // This group contains multi-word term "going away", and also the single-word term "going"
                recordStore.saveRecord(createSimpleDocument(1624L, "He is leaving for New York next week", 1));
                assertIndexEntryPrimaryKeys(Set.of(1624L),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"is departure for\""), null, ScanProperties.FORWARD_SCAN));

                // Search for phrase with "going away"
                assertIndexEntryPrimaryKeys(Set.of(1624L),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"is going away for\""), null, ScanProperties.FORWARD_SCAN));

                //Search for phrase with "going"
                assertIndexEntryPrimaryKeys(Set.of(1624L),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"is going for\""), null, ScanProperties.FORWARD_SCAN));

                // Search for phrase with only "away", the correct behavior is to return no match. But match is still hit due to the poor handling of positional data for multi-word synonym by this analyzer
                assertIndexEntryPrimaryKeys(Set.of(1624L),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"is away for\""), null, ScanProperties.FORWARD_SCAN));
            }
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

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void scanWithCombinedSetsSynonymIndex(IndexedType indexedType) {
        // The COMBINED_SYNONYM_SETS adds this extra line to our synonym set:
        // 'synonym', 'nonsynonym'
        final Index index1 = indexedType.getIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX_KEY);
        final Index index2 = indexedType.getIndex(QUERY_ONLY_SYNONYM_LUCENE_COMBINED_SETS_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index1, index2));
                Tuple primaryKey = createComplexRecordJoinedToSimple(1, 1623L, 1623L, "synonym is fun", "", false, System.currentTimeMillis(), 0);
                assertIndexEntryPrimaryKeyTuples(Collections.emptySet(),
                        recordStore.scanIndex(index1, fullTextSearch(index1, matchAll("simple_text", "nonsynonym", "is", "fun")), null, ScanProperties.FORWARD_SCAN));
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index2, fullTextSearch(index2, matchAll("simple_text", "nonsynonym", "is", "fun")), null, ScanProperties.FORWARD_SCAN));
            } else {
                openRecordStore(context, metaDataBuilder -> {
                    metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                    metaDataBuilder.addIndex(SIMPLE_DOC, index1);
                    metaDataBuilder.addIndex(SIMPLE_DOC, index2);
                });
                recordStore.saveRecord(createSimpleDocument(1623L, "synonym is fun", 1));
                assertIndexEntryPrimaryKeys(Collections.emptySet(),
                        recordStore.scanIndex(index1, fullTextSearch(index1, matchAll("text", "nonsynonym", "is", "fun")), null, ScanProperties.FORWARD_SCAN));
                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index2, fullTextSearch(index2, matchAll("text", "nonsynonym", "is", "fun")), null, ScanProperties.FORWARD_SCAN));
            }
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void proximitySearchOnMultiFieldWithMultiWordSynonym(IndexedType indexedType) {
        final Index index = indexedType.getIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                // Both "hello" and "record" have multi-word synonyms
                Tuple primaryKey = createComplexRecordJoinedToSimple(1, 1623L, 1623L, "Hello FoundationDB record layer", "", false, System.currentTimeMillis(), 0);
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, fullTextSearch(index, "simple_text:\"hello record\"~10"), null, ScanProperties.FORWARD_SCAN));

            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);

                // Both "hello" and "record" have multi-word synonyms
                recordStore.saveRecord(createSimpleDocument(1623L, "Hello FoundationDB record layer", 1));
                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, fullTextSearch(index, "\"hello record\"~10"), null, ScanProperties.FORWARD_SCAN));
            }
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void proximitySearchOnSpecificFieldWithMultiWordSynonym(IndexedType indexedType) {
        final Index index = indexedType.getIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                // Both "hello" and "record" have multi-word synonyms
                Tuple primaryKey = createComplexRecordJoinedToSimple(1, 1623L, 1623L, "Hello FoundationDB record layer", "", false, System.currentTimeMillis(), 0);
                assertIndexEntryPrimaryKeyTuples(Set.of(primaryKey),
                        recordStore.scanIndex(index, specificFieldSearch(index, "simple_text:\"hello record\"~10", "text"), null, ScanProperties.FORWARD_SCAN));
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);

                // Both "hello" and "record" have multi-word synonyms
                recordStore.saveRecord(createSimpleDocument(1623L, "Hello FoundationDB record layer", 1));
                assertIndexEntryPrimaryKeys(Set.of(1623L),
                        recordStore.scanIndex(index, specificFieldSearch(index, "\"hello record\"~10", "text"), null, ScanProperties.FORWARD_SCAN));
            }
        }
    }

    @Test
    void scanWithNgramIndex() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, NGRAM_LUCENE_INDEX);
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(NGRAM_LUCENE_INDEX, fullTextSearch(NGRAM_LUCENE_INDEX, "hello record layer"), null, ScanProperties.FORWARD_SCAN));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(NGRAM_LUCENE_INDEX, fullTextSearch(NGRAM_LUCENE_INDEX, "hello"), null, ScanProperties.FORWARD_SCAN));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(NGRAM_LUCENE_INDEX, fullTextSearch(NGRAM_LUCENE_INDEX, "hel"), null, ScanProperties.FORWARD_SCAN));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(NGRAM_LUCENE_INDEX, fullTextSearch(NGRAM_LUCENE_INDEX, "ell"), null, ScanProperties.FORWARD_SCAN));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(NGRAM_LUCENE_INDEX, fullTextSearch(NGRAM_LUCENE_INDEX, "ecord"), null, ScanProperties.FORWARD_SCAN));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(NGRAM_LUCENE_INDEX, fullTextSearch(NGRAM_LUCENE_INDEX, "hel ord aye"), null, ScanProperties.FORWARD_SCAN));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(NGRAM_LUCENE_INDEX, fullTextSearch(NGRAM_LUCENE_INDEX, "ello record"), null, ScanProperties.FORWARD_SCAN));
            // The term "ella" is not expected
            assertIndexEntryPrimaryKeys(Collections.emptySet(),
                    recordStore.scanIndex(NGRAM_LUCENE_INDEX, fullTextSearch(NGRAM_LUCENE_INDEX, "ella"), null, ScanProperties.FORWARD_SCAN));
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void searchForAutoComplete(IndexedType indexedType) throws Exception {
        searchForAutoCompleteAndAssert(indexedType, "good", true, false, indexedType.getPlanHashes().get(0));
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void searchForAutoCompleteWithPrefix(IndexedType indexedType) throws Exception {
        searchForAutoCompleteAndAssert(indexedType, "goo", true, false, indexedType.getPlanHashes().get(1));
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void searchForAutoCompleteWithHighlight(IndexedType indexedType) throws Exception {
        searchForAutoCompleteAndAssert(indexedType, "good", true, true, indexedType.getPlanHashes().get(0));
    }

    /**
     * To verify the suggestion lookup can work correctly if the suggester is never built and no entries exist in the
     * directory.
     */
    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void searchForAutoCompleteWithLoadingNoRecords(IndexedType indexedType) {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_WITH_AUTO_COMPLETE_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                assertIndexEntryPrimaryKeys(Collections.emptySet(),
                        recordStore.scanIndex(index, autoCompleteBounds(index, "hello", ImmutableSet.of("simple_text")), null, ScanProperties.FORWARD_SCAN));
            } else {
                openRecordStore(context, metaDataBuilder -> {
                    metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                    metaDataBuilder.addIndex(SIMPLE_DOC, index);
                });
                assertIndexEntryPrimaryKeys(Collections.emptySet(),
                        recordStore.scanIndex(index, autoCompleteBounds(index, "hello", ImmutableSet.of("text")), null, ScanProperties.FORWARD_SCAN));
            }

            assertEquals(0, Verify.verifyNotNull(context.getTimer()).getCount(LuceneEvents.Counts.LUCENE_SCAN_MATCHED_AUTO_COMPLETE_SUGGESTIONS));
        }
    }

    @SuppressWarnings("UnstableApiUsage")
    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void searchForAutoCompleteAcrossMultipleFields(IndexedType indexedType) throws Exception {
        final Index index = indexedType.getIndex(COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                createComplexRecordJoinedToSimple(1, 1623L, 1623L, "Good morning", "", false, System.currentTimeMillis(), 1);
                createComplexRecordJoinedToSimple(2, 1624L, 1624L, "Good afternoon", "", false, System.currentTimeMillis(), 1);
                createComplexRecordJoinedToSimple(3, 1625L, 1625L, "good evening", "", false, System.currentTimeMillis(), 1);
                createComplexRecordJoinedToSimple(4, 1626L, 1626L, "Good night", "", false, System.currentTimeMillis(), 1);
                createComplexRecordJoinedToSimple(5, 1627L, 1627L, "", "That's really good!", false, System.currentTimeMillis(), 1);
                createComplexRecordJoinedToSimple(6, 1628L, 1628L, "Good day", "I'm good", false, System.currentTimeMillis(), 1);
                createComplexRecordJoinedToSimple(7, 1629L, 1629L, "", "Hello Record Layer", false, System.currentTimeMillis(), 1);
                createComplexRecordJoinedToSimple(8, 1630L, 1630L, "", "Hello FoundationDB!", false, System.currentTimeMillis(), 1);
            } else {
                openRecordStore(context, metaDataBuilder -> {
                    metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                    metaDataBuilder.addIndex(COMPLEX_DOC, index);
                });

                // Write 8 texts and 6 of them contain the key "good"
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, "Good morning", "", 1));
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1624L, "Good afternoon", "", 1));
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1625L, "good evening", "", 1));
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1626L, "Good night", "", 1));
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1627L, "", "That's really good!", 1));
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1628L, "Good day", "I'm good", 1));
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1629L, "", "Hello Record Layer", 1));
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1630L, "", "Hello FoundationDB!", 1));
            }

            final RecordQueryPlan luceneIndexPlan =
                    LuceneIndexQueryPlan.of(index.getName(),
                            autoCompleteScanParams("good", indexedType.isSynthetic() ? ImmutableSet.of("simple_text", "complex_text2") : ImmutableSet.of("text", "text2")),
                            indexedType.isSynthetic()
                                ? RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.SYNTHETIC_CONSTITUENTS
                                : RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                            false,
                            null,
                            indexedType.isSynthetic()
                                ? JOINED_COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE_STORED_FIELDS
                                : COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE_STORED_FIELDS);
            assertEquals(indexedType.getPlanHashes().get(2), luceneIndexPlan.planHash(PlanHashable.CURRENT_LEGACY));

            final List<FDBQueriedRecord<Message>> results =
                    recordStore.executeQuery(luceneIndexPlan, null, ExecuteProperties.SERIAL_EXECUTE)
                            .asList().get();

            final ImmutableList<Pair<String, String>> expectedResults = ImmutableList.of(
                    Pair.of("Good day", "I'm good"),
                    Pair.of("Good morning", ""),
                    Pair.of("Good afternoon", ""),
                    Pair.of("good evening", ""),
                    Pair.of("Good night", ""),
                    Pair.of("", "That's really good!")
            );
            Assertions.assertEquals(expectedResults.size(), results.size());

            Assertions.assertTrue(Streams.zip(expectedResults.stream(), results.stream(),
                    (expectedResult, result) -> {
                        final Message record = Verify.verifyNotNull(result.getRecord());
                        if (indexedType.isSynthetic()) {
                            DynamicMessage dm = (DynamicMessage) record.getField(record.getDescriptorForType().findFieldByName("simple"));
                            final Descriptors.Descriptor descriptor = dm.getDescriptorForType();
                            final Descriptors.FieldDescriptor textDescriptor = descriptor.findFieldByName("text");
                            assertEquals(expectedResult.getLeft(), dm.getField(textDescriptor));
                            DynamicMessage dm2 = (DynamicMessage) record.getField(record.getDescriptorForType().findFieldByName("complex"));
                            final Descriptors.Descriptor descriptor2 = dm2.getDescriptorForType();
                            final Descriptors.FieldDescriptor textDescriptor2 = descriptor2.findFieldByName("text2");
                            assertEquals(expectedResult.getRight(), dm2.getField(textDescriptor2));
                        } else {
                            final Descriptors.Descriptor descriptor = record.getDescriptorForType();
                            final Descriptors.FieldDescriptor textDescriptor = descriptor.findFieldByName("text");
                            assertEquals(expectedResult.getLeft(), record.getField(textDescriptor));
                            final Descriptors.FieldDescriptor text2Descriptor = descriptor.findFieldByName("text2");
                            assertEquals(expectedResult.getRight(), record.getField(text2Descriptor));
                        }
                        return true;
                    }).allMatch(r -> r));

            Subspace subspace = recordStore.indexSubspace(index);
            validateSegmentAndIndexIntegrity(index, subspace, context, "_0.cfs");

            List<Tuple> expectedPks = indexedType.isSynthetic()
                          ? ImmutableList.of(
                                    Tuple.from(-1, Tuple.from(6, 1628), Tuple.from(1628)),
                                    Tuple.from(-1, Tuple.from(1, 1623), Tuple.from(1623)),
                                    Tuple.from(-1, Tuple.from(2, 1624), Tuple.from(1624)),
                                    Tuple.from(-1, Tuple.from(3, 1625), Tuple.from(1625)),
                                    Tuple.from(-1, Tuple.from(4, 1626), Tuple.from(1626)),
                                    Tuple.from(-1, Tuple.from(5, 1627), Tuple.from(1627)))
                          : ImmutableList.of(Tuple.from(1L, 1628L), Tuple.from(1L, 1623L), Tuple.from(1L, 1624L), Tuple.from(1L, 1625L), Tuple.from(1L, 1626L), Tuple.from(1L, 1627L));
            List<Tuple> primaryKeys = results.stream()
                    .map(FDBQueriedRecord::getIndexEntry)
                    .map(Verify::verifyNotNull)
                    .map(IndexEntry::getPrimaryKey).collect(Collectors.toList());
            assertEquals(expectedPks, primaryKeys);

            commit(context);
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void searchForAutoCompleteWithContinueTyping(IndexedType indexedType) throws Exception {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_WITH_AUTO_COMPLETE_KEY);
        try (FDBRecordContext context = openContext()) {
            // Write 8 texts and 6 of them contain the key "good"
            addIndexAndSaveRecordForAutoComplete(context, index, indexedType.isSynthetic(), autoCompletes);

            final RecordQueryPlan luceneIndexPlan =
                    LuceneIndexQueryPlan.of(index.getName(),
                            autoCompleteScanParams("good mor", ImmutableSet.of(indexedType.isSynthetic() ? "simple_text" : "text")),
                            indexedType.isSynthetic()
                                ? RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.SYNTHETIC_CONSTITUENTS
                                : RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                            false,
                            null,
                            ImmutableList.of(indexedType.isSynthetic()
                                ? JOINED_SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD
                                : SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD));
            assertEquals(indexedType.getPlanHashes().get(3), luceneIndexPlan.planHash(PlanHashable.CURRENT_LEGACY));
            final List<FDBQueriedRecord<Message>> results =
                    recordStore.executeQuery(luceneIndexPlan, null, ExecuteProperties.SERIAL_EXECUTE)
                            .asList().get();

            // Assert only 1 suggestion returned
            assertEquals(1, results.size());

            // Assert the suggestion's key and field
            final FDBQueriedRecord<Message> result = Iterables.getOnlyElement(results);
            Message record = result.getRecord();
            if (indexedType.isSynthetic()) {
                record = (DynamicMessage)record.getField(record.getDescriptorForType().findFieldByName("simple"));
            }
            final Descriptors.Descriptor descriptor = record.getDescriptorForType();
            final Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("text");
            final String field = Verify.verifyNotNull((String)record.getField(fieldDescriptor));
            assertEquals("Good morning", field);
            IndexEntry entry = result.getIndexEntry();
            Assertions.assertNotNull(entry);
            assertEquals(1623L, indexedType.isSynthetic()
                                ? ((ArrayList) entry.getPrimaryKey().get(2)).get(0)
                                : entry.getPrimaryKey().get(0));

            assertAutoCompleteEntriesAndSegmentInfoStoredInCompoundFile(index, recordStore.indexSubspace(index),
                    context, "_0.cfs");

            commit(context);
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void searchForAutoCompleteForGroupedRecord(IndexedType indexedType) throws Exception {
        final Index index = indexedType.getIndex(MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToMap(metaDataBuilder, index));
                createComplexRecordJoinedToMap(2, 1623L, 1623L, ENGINEER_JOKE, "sampleTextPhrase", WAYLON, "sampleTextSong", true, System.currentTimeMillis(), 0);
            } else {
                openRecordStore(context, metaDataBuilder -> {
                    metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                    TextIndexTestUtils.addRecordTypePrefix(metaDataBuilder);
                    metaDataBuilder.addIndex(MAP_DOC, index);
                });
                recordStore.saveRecord(LuceneIndexTestUtils.createMultiEntryMapDoc(1623L, ENGINEER_JOKE, "sampleTextPhrase", WAYLON, "sampleTextSong", 2));
            }

            final RecordQueryPlan luceneIndexPlan =
                    LuceneIndexQueryPlan.of(index.getName(),
                            groupedAutoCompleteScanParams("Vision", "sampleTextPhrase", ImmutableSet.of(indexedType.isSynthetic() ? "map_entry_value" : "entry_value")),
                            indexedType.isSynthetic()
                                ? RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.SYNTHETIC_CONSTITUENTS
                                : RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                            false,
                            null,
                            indexedType.isSynthetic()
                                ? JOINED_MAP_ON_VALUE_INDEX_STORED_FIELDS
                                : MAP_ON_VALUE_INDEX_STORED_FIELDS);
            assertEquals(indexedType.getPlanHashes().get(4), luceneIndexPlan.planHash(PlanHashable.CURRENT_LEGACY));
            final List<FDBQueriedRecord<Message>> results =
                    recordStore.executeQuery(luceneIndexPlan, null, ExecuteProperties.SERIAL_EXECUTE)
                            .asList().get();

            assertEquals(1, results.size());
            final var resultRecord = results.get(0);
            final IndexEntry indexEntry = resultRecord.getIndexEntry();
            assertNotNull(indexEntry);
            Message message = resultRecord.getRecord();
            if (indexedType.isSynthetic()) {
                message = (DynamicMessage) message.getField(message.getDescriptorForType().findFieldByName("map"));
            }

            Descriptors.Descriptor recordDescriptor = TestRecordsTextProto.MapDocument.getDescriptor();

            Descriptors.FieldDescriptor docIdDescriptor = recordDescriptor.findFieldByName("doc_id");
            assertEquals(1623L, message.getField(docIdDescriptor));

            Descriptors.FieldDescriptor entryDescriptor = recordDescriptor.findFieldByName("entry");
            Message entry = (Message)message.getRepeatedField(entryDescriptor, 0);

            Descriptors.FieldDescriptor keyDescriptor = entryDescriptor.getMessageType().findFieldByName("key");
            Descriptors.FieldDescriptor valueDescriptor = entryDescriptor.getMessageType().findFieldByName("value");

            assertEquals("sampleTextPhrase", entry.getField(keyDescriptor));
            assertEquals(ENGINEER_JOKE, entry.getField(valueDescriptor));

            assertAutoCompleteEntriesAndSegmentInfoStoredInCompoundFile(index, recordStore.indexSubspace(index).subspace(Tuple.from("sampleTextPhrase")),
                    context, "_0.cfs");

            commit(context);
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void searchForAutoCompleteExcludedFieldsForGroupedRecord(IndexedType indexedType) throws Exception {
        final Index index = indexedType.getIndex(MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE_EXCLUDED_FIELDS_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToMap(metaDataBuilder, index));
                createComplexRecordJoinedToMap(2, 1623L, 1623L, ENGINEER_JOKE, "sampleTextPhrase", WAYLON, "sampleTextSong", true, System.currentTimeMillis(), 0);
            } else {
                openRecordStore(context, metaDataBuilder -> {
                    metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                    TextIndexTestUtils.addRecordTypePrefix(metaDataBuilder);
                    metaDataBuilder.addIndex(MAP_DOC, index);
                });
                recordStore.saveRecord(LuceneIndexTestUtils.createMultiEntryMapDoc(1623L, ENGINEER_JOKE, "sampleTextPhrase", WAYLON, "sampleTextSong", 2));
            }

            final RecordQueryPlan luceneIndexPlan =
                    LuceneIndexQueryPlan.of(index.getName(),
                            groupedAutoCompleteScanParams("Vision", "sampleTextPhrase", ImmutableSet.of(indexedType.isSynthetic() ? "map_entry_second_value" : "entry_second_value")),
                            indexedType.isSynthetic()
                                ? RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.SYNTHETIC_CONSTITUENTS
                                : RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                            false,
                            null,
                            indexedType.isSynthetic()
                                ? JOINED_MAP_ON_VALUE_INDEX_STORED_FIELDS
                                : MAP_ON_VALUE_INDEX_STORED_FIELDS);
            assertEquals(indexedType.getPlanHashes().get(5), luceneIndexPlan.planHash(PlanHashable.CURRENT_LEGACY));
            final List<FDBQueriedRecord<Message>> results =
                    recordStore.executeQuery(luceneIndexPlan, null, ExecuteProperties.SERIAL_EXECUTE)
                            .asList().get();

            assertEquals(0, results.size());
            commit(context);
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void testAutoCompleteSearchForPhrase(IndexedType indexedType) throws Exception {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_WITH_AUTO_COMPLETE_KEY);
        try (FDBRecordContext context = openContext()) {
            final List<KeyExpression> storedFields = ImmutableList.of(indexedType.isSynthetic() ? JOINED_SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD : SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD);
            addIndexAndSaveRecordForAutoComplete(context, index, indexedType.isSynthetic(), autoCompletePhrases);

            // All records are matches because they all contain both "united" and "states"
            queryAndAssertAutoCompleteSuggestionsReturned(index,
                    indexedType.isSynthetic(),
                    indexedType.isSynthetic() ? "simple" : null,
                    storedFields,
                    "text",
                    "united states",
                    ImmutableList.of("united states of america",
                            "united states is a country in the continent of america",
                            "united kingdom, france, the states",
                            "states united as a country",
                            "states have been united as a country",
                            "all the states united as a country",
                            "all the states have been united as a country",
                            "welcome to the united states of america",
                            "The countries are united kingdom, france, the states"));

            // Only the texts containing "united states" are returned, the last token "states" is queried with term query,
            // same as the other tokens due to the white space following it
            queryAndAssertAutoCompleteSuggestionsReturned(index,
                    indexedType.isSynthetic(),
                    indexedType.isSynthetic() ? "simple" : null,
                    storedFields,
                    "text",
                    "\"united states \"",
                    ImmutableList.of("united states of america",
                            "united states is a country in the continent of america",
                            "welcome to the united states of america"));

            // Only the texts containing "united states" are returned, the last token "states" is queried with prefix query
            queryAndAssertAutoCompleteSuggestionsReturned(index,
                    indexedType.isSynthetic(),
                    indexedType.isSynthetic() ? "simple" : null,
                    storedFields,
                    "text",
                    "\"united states\"",
                    ImmutableList.of("united states of america",
                            "united states is a country in the continent of america",
                            "welcome to the united states of america"));

            // Only the texts containing "united state" are returned, the last token "state" is queried with prefix query
            queryAndAssertAutoCompleteSuggestionsReturned(index,
                    indexedType.isSynthetic(),
                    indexedType.isSynthetic() ? "simple" : null,
                    storedFields,
                    "text",
                    "\"united state\"",
                    ImmutableList.of("united states of america",
                            "united states is a country in the continent of america",
                            "welcome to the united states of america"));
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void autoCompletePhraseSearchIncludingStopWords(IndexedType indexedType) throws Exception {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_WITH_AUTO_COMPLETE_KEY);
        try (FDBRecordContext context = openContext()) {
            final List<KeyExpression> storedFields = ImmutableList.of(indexedType.isSynthetic() ? JOINED_SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD : SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD);
            addIndexAndSaveRecordForAutoComplete(context, index, indexedType.isSynthetic(), autoCompletePhrases);

            queryAndAssertAutoCompleteSuggestionsReturned(index,
                    indexedType.isSynthetic(),
                    indexedType.isSynthetic() ? "simple" : null,
                    storedFields,
                    "text",
                    "\"united states of ameri\"",
                    ImmutableList.of("united states of america",
                            "welcome to the united states of america"));

            queryAndAssertAutoCompleteSuggestionsReturned(index,
                    indexedType.isSynthetic(),
                    indexedType.isSynthetic() ? "simple" : null,
                    storedFields,
                    "text",
                    "\"united states of \"",
                    ImmutableList.of("united states of america",
                            "welcome to the united states of america",
                            "united states is a country in the continent of america"));

            // Stop-words are interchangeable, so client would have to enforce "exact" stop-word suggestion if required
            queryAndAssertAutoCompleteSuggestionsReturned(index,
                    indexedType.isSynthetic(),
                    indexedType.isSynthetic() ? "simple" : null,
                    storedFields,
                    "text",
                    "\"united states of\"",
                    ImmutableList.of("united states of america",
                            "welcome to the united states of america",
                            "united states is a country in the continent of america"));

            // Prefix match on stop-words is not supported and is hard to do. Should be a rare corner case.
            // The user would have to type the entire stop-word
            queryAndAssertAutoCompleteSuggestionsReturned(index,
                    indexedType.isSynthetic(),
                    indexedType.isSynthetic() ? "simple" : null,
                    storedFields,
                    "text",
                    "\"united states o\"",
                    ImmutableList.of());

            commit(context);
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void autoCompletePhraseSearchWithLeadingStopWords(IndexedType indexedType) throws Exception {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_WITH_AUTO_COMPLETE_KEY);
        try (FDBRecordContext context = openContext()) {
            final List<KeyExpression> storedFields = ImmutableList.of(indexedType.isSynthetic() ? JOINED_SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD : SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD);
            addIndexAndSaveRecordForAutoComplete(context, index, indexedType.isSynthetic(), autoCompletePhrases);

            queryAndAssertAutoCompleteSuggestionsReturned(index,
                    indexedType.isSynthetic(),
                    indexedType.isSynthetic() ? "simple" : null,
                    storedFields,
                    "text",
                    "\"of ameri\"",
                    ImmutableList.of("united states of america",
                            "welcome to the united states of america",
                            "united states is a country in the continent of america"));

            queryAndAssertAutoCompleteSuggestionsReturned(index,
                    indexedType.isSynthetic(),
                    indexedType.isSynthetic() ? "simple" : null,
                    storedFields,
                    "text",
                    "\"and of ameri\"",
                    ImmutableList.of("united states of america",
                            "welcome to the united states of america",
                            "united states is a country in the continent of america"));

            commit(context);
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void testAutoCompleteSearchMultipleResultsSingleDocument(IndexedType indexedType) throws Exception {
        final Index index = indexedType.getIndex(COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                createComplexRecordJoinedToSimple(2, 1597L, 1597L,
                        "Good night! Good night! Parting is such sweet sorrow",
                        "That I shall say good night till it be morrow",
                        true, System.currentTimeMillis(), 0);
            } else {
                openRecordStore(context, metaDataBuilder -> {
                    metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                    metaDataBuilder.addIndex(COMPLEX_DOC, index);
                });

                ComplexDocument doc = ComplexDocument.newBuilder()
                        .setDocId(1597L)
                        // Romeo and Juliet, Act II, Scene II.
                        .setText("Good night! Good night! Parting is such sweet sorrow")
                        .setText2("That I shall say good night till it be morrow")
                        .build();
                recordStore.saveRecord(doc);
            }

            final RecordQueryPlan luceneIndexPlan =
                    LuceneIndexQueryPlan.of(index.getName(),
                            autoCompleteScanParams("good night", ImmutableSet.of(indexedType.isSynthetic() ? "simple_text" : "text")),
                            indexedType.isSynthetic()
                                ? RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.SYNTHETIC_CONSTITUENTS
                                : RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                            false,
                            null,
                            indexedType.isSynthetic()
                                ? JOINED_COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE_STORED_FIELDS
                                : COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE_STORED_FIELDS);
            assertEquals(indexedType.getPlanHashes().get(6), luceneIndexPlan.planHash(PlanHashable.CURRENT_LEGACY));
            final List<FDBQueriedRecord<Message>> results =
                    recordStore.executeQuery(luceneIndexPlan, null, ExecuteProperties.SERIAL_EXECUTE)
                            .asList().get();

            assertThat(results, hasSize(1));
            final FDBQueriedRecord<Message> result = Iterables.getOnlyElement(results);
            if (indexedType.isSynthetic()) {
                assertEquals(Tuple.from(-1, Tuple.from(2, 1597L), Tuple.from(1597L)), Verify.verifyNotNull(result.getIndexEntry()).getPrimaryKey());
            } else {
                assertEquals(Tuple.from(null, 1597L), Verify.verifyNotNull(result.getIndexEntry()).getPrimaryKey());
            }
            commit(context);
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void testAutoCompleteSearchForPhraseWithoutFreqsAndPositions(IndexedType indexedType) {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_WITH_AUTO_COMPLETE_NO_FREQS_POSITIONS_KEY);
        try (FDBRecordContext context = openContext()) {
            final List<KeyExpression> storedFields = ImmutableList.of(index.getRootExpression());
            addIndexAndSaveRecordForAutoComplete(context, index, indexedType.isSynthetic(), autoCompletePhrases);

            // Phrase search is not supported if positions are not indexed
            assertThrows(ExecutionException.class,
                    () -> queryAndAssertAutoCompleteSuggestionsReturned(index,
                            indexedType.isSynthetic(),
                            indexedType.isSynthetic() ? "simple" : null,
                            storedFields,
                            "text",
                            "\"united states \"",
                            ImmutableList.of()));

            commit(context);
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void autoCompletePhraseSearchWithMixedCases(IndexedType indexedType) throws Exception {
        final Index index = indexedType.getIndex(EMAIL_CJK_SYM_TEXT_WITH_AUTO_COMPLETE_KEY);
        try (FDBRecordContext context = openContext()) {
            final List<KeyExpression> storedFields = ImmutableList.of(indexedType.isSynthetic() ? JOINED_SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD : SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD);
            addIndexAndSaveRecordForAutoComplete(context, index, indexedType.isSynthetic(), capitalizedAutoCaseCompletePhrases);

            BiFunction<String, List<String>, Object> test = (query, expected) -> {
                try {
                    queryAndAssertAutoCompleteSuggestionsReturned(index,
                            indexedType.isSynthetic(),
                            indexedType.isSynthetic() ? "simple" : null,
                            storedFields,
                            "text",
                            query,
                            expected);
                } catch (Exception ex) {
                    Assertions.fail(ex);
                }
                return null;
            };

            test.apply("\"STateS of AMeri\"",
                    ImmutableList.of("United States of America",
                    "welcome to the United States of America"));
            test.apply("\"THE oF ameri\"",
                    ImmutableList.of("United States of America",
                            "welcome to the United States of America",
                            "United States is a country in the continent of America"));
            test.apply("\"THE\"",
                    ImmutableList.of("There is a country called Armenia"));

            commit(context);
        }
    }

    protected static final List<String> autoCompleteCJKPhrases = List.of(
            "世上无难事",
            "世上无English word",
            "世の中に難しいことなんてない",
            "シマス風ト雷ヲ",
            "シマス風ト 시험@김치오랜",
            "평가했다",
            "세상에 어려운 일은 없다",
            "用户@例子.广告",
            "おいしい@すし.あど なんてない",
            "オイシイ@スシ.アド なんてない",
            "시험@김치오랜.광고",
            "asb@icloud.com 김치오랜"
    );

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void autoCompleteCjkPhraseSearch(IndexedType indexedType) throws Exception {
        final Index index = indexedType.getIndex(EMAIL_CJK_SYM_TEXT_WITH_AUTO_COMPLETE_KEY);
        final boolean isSynthetic = indexedType.isSynthetic();
        try (FDBRecordContext context = openContext()) {
            final List<KeyExpression> storedFields = ImmutableList.of(isSynthetic ? JOINED_SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD : SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD);
            addIndexAndSaveRecordForAutoComplete(context, index, isSynthetic, autoCompleteCJKPhrases);

            BiFunction<String, List<String>, Object> test = (query, expected) -> {
                try {
                    queryAndAssertAutoCompleteSuggestionsReturned(index,
                            indexedType.isSynthetic(),
                            indexedType.isSynthetic() ? "simple" : null,
                            storedFields,
                            "text",
                            query,
                            expected);
                } catch (Exception ex) {
                    Assertions.fail(ex);
                }
                return null;
            };

            test.apply("\"世上无\"",
                    ImmutableList.of("世上无难事", "世上无English word"));
            test.apply("\"世\"",
                    ImmutableList.of("世上无难事", "世上无English word", "世の中に難しいことなんてない"));
            test.apply("\"に難しい\"",
                    ImmutableList.of("世の中に難しいことなんてない"));
            test.apply("\"シマス\"",
                    ImmutableList.of("シマス風ト雷ヲ", "シマス風ト 시험@김치오랜"));
            test.apply("\"평가\"",
                    ImmutableList.of("평가했다"));
            test.apply("\"상에 어\"",
                    ImmutableList.of("세상에 어려운 일은 없다"));

            commit(context);
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void autoCompleteCjkEmailSearch(IndexedType indexedType) throws Exception {
        final Index index = indexedType.getIndex(EMAIL_CJK_SYM_TEXT_WITH_AUTO_COMPLETE_KEY);
        final boolean isSynthetic = indexedType.isSynthetic();
        try (FDBRecordContext context = openContext()) {
            final List<KeyExpression> storedFields = ImmutableList.of(isSynthetic ? JOINED_SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD : SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD);
            addIndexAndSaveRecordForAutoComplete(context, index, isSynthetic, autoCompleteCJKPhrases);

            BiFunction<String, List<String>, Object> test = (query, expected) -> {
                try {
                    queryAndAssertAutoCompleteSuggestionsReturned(index,
                            indexedType.isSynthetic(),
                            indexedType.isSynthetic() ? "simple" : null,
                            storedFields,
                            "text",
                            query,
                            expected);
                } catch (Exception ex) {
                    Assertions.fail(ex);
                }
                return null;
            };

            test.apply("\"用户\"",
                    ImmutableList.of("用户@例子.广告"));
            test.apply("\"用户\\@\"",
                    ImmutableList.of("用户@例子.广告"));
            test.apply("\"い\\@すし\"",
                    ImmutableList.of("おいしい@すし.あど なんてない"));
            test.apply("\"シイ\\@スシ.アド\"",
                    ImmutableList.of("オイシイ@スシ.アド なんてない"));
            test.apply("\"시험@김\"",
                    ImmutableList.of("시험@김치오랜.광고", "シマス風ト 시험@김치오랜"));

            commit(context);
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void autoCompleteMultiPhraseLuceneQuery(IndexedType indexedType) throws Exception {
        final Index index = indexedType.getIndex(EMAIL_CJK_SYM_TEXT_WITH_AUTO_COMPLETE_KEY);
        final boolean isSynthetic = indexedType.isSynthetic();
        try (FDBRecordContext context = openContext()) {
            final List<KeyExpression> storedFields = ImmutableList.of(isSynthetic ? JOINED_SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD : SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD);
            addIndexAndSaveRecordForAutoComplete(context, index, isSynthetic, autoCompleteCJKPhrases);

            BiFunction<String, List<String>, Object> test = (query, expected) -> {
                try {
                    queryAndAssertAutoCompleteSuggestionsReturned(index,
                            indexedType.isSynthetic(),
                            indexedType.isSynthetic() ? "simple" : null,
                            storedFields,
                            "text",
                            query,
                            expected);
                } catch (Exception ex) {
                    Assertions.fail(ex);
                }
                return null;
            };

            test.apply("\"い\\@すし.あど なんあど\"",
                    ImmutableList.of());
            test.apply("\"い\\@すし.あど なん\"",
                    ImmutableList.of("おいしい@すし.あど なんてない"));
            test.apply("\"シイ\\@スシ.アド な\"",
                    ImmutableList.of("オイシイ@スシ.アド なんてない"));
            test.apply("\"asb@icloud.com 김치오\"",
                    ImmutableList.of("asb@icloud.com 김치오랜"));

            commit(context);
        }
    }

    private static final List<String> spellcheckWords = List.of("hello", "monitor", "keyboard", "mouse", "trackpad", "cable", "help", "elmo", "elbow", "helps", "helm", "helms", "gulps");

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void searchForSpellCheck(IndexedType indexedType) throws Exception {
        final Index index = indexedType.getIndex(SPELLCHECK_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                long docId = 1623L;
                int i = 1;
                for (String word : spellcheckWords) {
                    createComplexRecordJoinedToSimple(i++, docId, docId++, word, "", false, System.currentTimeMillis(), 1);
                }
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                long docId = 1623L;
                for (String word : spellcheckWords) {
                    recordStore.saveRecord(createSimpleDocument(docId++, word, 1));
                }
            }

            CompletableFuture<List<IndexEntry>> resultsI = recordStore.scanIndex(index,
                    spellCheck(index, "keyboad"),
                    null,
                    ScanProperties.FORWARD_SCAN).asList();
            List<IndexEntry> results = resultsI.get();

            assertEquals(1, results.size());
            IndexEntry result = results.get(0);
            assertEquals("keyboard", result.getKey().getString(1));
            assertEquals(indexedType.isSynthetic() ? "simple_text" : "text", result.getKey().getString(0));
            assertEquals(0.85714287F, result.getValue().get(0));

            List<IndexEntry> results2 = recordStore.scanIndex(index,
                    spellCheck(index, indexedType.isSynthetic() ? "simple_text:keyboad" : "text:keyboad"),
                    null,
                    ScanProperties.FORWARD_SCAN).asList().get();
            assertEquals(1, results2.size());
            IndexEntry result2 = results2.get(0);
            assertEquals("keyboard", result2.getKey().get(1));
            assertEquals(indexedType.isSynthetic() ? "simple_text" : "text", result2.getKey().get(0));
            assertEquals(0.85714287F, result2.getValue().get(0));

            commit(context);
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void searchForSpellcheckForGroupedRecord(IndexedType indexedType) throws Exception {
        final Index index = indexedType.getIndex(MAP_ON_VALUE_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            RecordType recordType;
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToMap(metaDataBuilder, index));
                createComplexRecordJoinedToMap(1, 1623L, 1623L, ENGINEER_JOKE, "sampleTextPhrase", WAYLON, "sampleTextSong", true, System.currentTimeMillis(), 1);
                recordType = recordStore.getRecordMetaData()
                        .getSyntheticRecordType("luceneSyntheticComplexJoinedToMap");
            } else {
                openRecordStore(context, metaDataBuilder -> {
                    metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                    metaDataBuilder.addIndex(MAP_DOC, index);
                });
                recordType = recordStore.saveRecord(LuceneIndexTestUtils.createMultiEntryMapDoc(1623L, ENGINEER_JOKE, "sampleTextPhrase", WAYLON, "sampleTextSong", 2)).getRecordType();
            }
            List<IndexEntry> indexEntries = recordStore.scanIndex(index,
                    groupedSpellCheck(index, "Visin", "sampleTextPhrase"),
                    null,
                    ScanProperties.FORWARD_SCAN).asList().get();

            assertEquals(1, indexEntries.size());
            IndexEntry indexEntry = indexEntries.get(0);
            assertEquals(0.8F, indexEntry.getValue().get(0));

            Descriptors.Descriptor recordDescriptor = recordType.getDescriptor();
            IndexKeyValueToPartialRecord toPartialRecord = LuceneIndexKeyValueToPartialRecordUtils.getToPartialRecord(
                    index, recordType, LuceneScanTypes.BY_LUCENE_SPELL_CHECK);
            Message message = toPartialRecord.toRecord(recordDescriptor, indexEntry);
            if (indexedType.isSynthetic()) {
                message = (DynamicMessage)message.getField(message.getDescriptorForType().findFieldByName("map"));
                recordDescriptor = message.getDescriptorForType();
            }

            Descriptors.FieldDescriptor entryDescriptor = recordDescriptor.findFieldByName("entry");
            Message entry = (Message)message.getRepeatedField(entryDescriptor, 0);

            Descriptors.FieldDescriptor keyDescriptor = entryDescriptor.getMessageType().findFieldByName("key");
            Descriptors.FieldDescriptor valueDescriptor = entryDescriptor.getMessageType().findFieldByName("value");

            //TODO: This seems like the wrong field string to return. I'm not sure what to do here
            assertEquals("sampleTextPhrase", entry.getField(keyDescriptor));
            assertEquals("vision", entry.getField(valueDescriptor));

            commit(context);
        }
    }

    private void spellCheckHelper(final Index index, @Nonnull String query, List<Pair<String, String>> expectedSuggestions) throws ExecutionException, InterruptedException {
        List<IndexEntry> suggestions = recordStore.scanIndex(index,
                spellCheck(index, query),
                null,
                ScanProperties.FORWARD_SCAN).asList().get();

        assertEquals(expectedSuggestions.size(), suggestions.size());
        for (int i = 0; i < expectedSuggestions.size(); ++i) {
            assertThat(suggestions.get(i).getKey().get(1), equalTo(expectedSuggestions.get(i).getKey()));
            assertThat(suggestions.get(i).getKey().get(0), equalTo(expectedSuggestions.get(i).getValue()));
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void spellCheckMultipleMatches(IndexedType indexedType) throws Exception {
        Index index = indexedType.getIndex(SPELLCHECK_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                long docId = 1623L;
                int i = 1;
                for (String word : spellcheckWords) {
                    createComplexRecordJoinedToSimple(i++, docId, docId++, word, "", false, System.currentTimeMillis(), 1);
                }
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                long docId = 1623L;
                for (String word : spellcheckWords) {
                    recordStore.saveRecord(createSimpleDocument(docId++, word, 1));
                }
            }

            String field = indexedType.isSynthetic() ? "simple_text" : "text";
            spellCheckHelper(index, "keyboad", List.of(Pair.of("keyboard", field)));
            spellCheckHelper(index, indexedType.isSynthetic() ? "simple_text:keyboad" : "text:keyboad", List.of(Pair.of("keyboard", field)));
            spellCheckHelper(index, "helo", List.of(
                    Pair.of("hello", field),
                    Pair.of("helm", field),
                    Pair.of("help", field),
                    Pair.of("helms", field),
                    Pair.of("helps", field)
            ));
            spellCheckHelper(index, "hello", List.of());
            spellCheckHelper(index, "mous", List.of(Pair.of("mouse", field)));

            final List<Pair<String, String>> emptyList = List.of();
            assertThrows(RecordCoreException.class,
                    () -> spellCheckHelper(index, "wrongField:helo", emptyList),
                    "Invalid field name in Lucene index query");
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void spellCheckComplexDocument(IndexedType indexedType) throws Exception {
        final Index index = indexedType.getIndex(SPELLCHECK_INDEX_COMPLEX_KEY);
        try (FDBRecordContext context = openContext()) {
            List<String> textList = List.of("beaver", "leopard", "hello", "help", "helm", "boat", "road", "foot", "tare", "tire");
            List<String> text2List = List.of("beavers", "lizards", "hell", "helps", "helms", "boot", "read", "fool", "tire", "tire");
            assertThat(text2List, hasSize(textList.size()));
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                long docId = 1623L;
                for (int i = 0; i < textList.size(); ++i) {
                    createComplexRecordJoinedToSimple(i, docId, docId++, textList.get(i), text2List.get(i), false, System.currentTimeMillis(), 1);
                }
            } else {
                rebuildIndexMetaData(context, COMPLEX_DOC, index);
                long docId = 1623L;
                for (int i = 0; i < textList.size(); ++i) {
                    recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(docId++, textList.get(i), text2List.get(i), 1));
                }
            }

            String text = indexedType.isSynthetic() ? "simple_text" : "text";
            String text2 = indexedType.isSynthetic() ? "complex_text2" : "text2";
            spellCheckHelper(index, "baver", List.of(Pair.of("beaver", text), Pair.of("beavers", text2)));
            spellCheckHelper(index, text + ":baver", List.of(Pair.of("beaver", text)));
            spellCheckHelper(index, text2 + ":baver", List.of(Pair.of("beavers", text2)));

            spellCheckHelper(index, "lepard", List.of(Pair.of("leopard", text)));
            spellCheckHelper(index, text + ":lepard", List.of(Pair.of("leopard", text)));
            spellCheckHelper(index, text2 + ":lepard", List.of());

            spellCheckHelper(index, "lizerds", List.of(Pair.of("lizards", text2)));
            spellCheckHelper(index, text + ":lizerds", List.of());
            spellCheckHelper(index, text2 + ":lizerds", List.of(Pair.of("lizards", text2)));

            // Apply the limit of 5 fields so do not return "helms" which has a lower score than the rest
            spellCheckHelper(index, "hela", List.of(
                    Pair.of("hell", text2),
                    Pair.of("helm", text),
                    Pair.of("help", text),
                    Pair.of("hello", text),
                    Pair.of("helms", text2)));

            // Same score and same frequency, this is sorted by field name
            spellCheckHelper(index, "bost", List.of(
                    Pair.of("boat", text),
                    Pair.of("boot", text2)));
            spellCheckHelper(index, "rlad", List.of(
                    Pair.of("read", text2),
                    Pair.of("road", text)));

            // Same score but different frequency, priority to the more frequent item
            spellCheckHelper(index, "foml", List.of(
                    Pair.of("fool", text2),
                    Pair.of("foot", text)));
            // Same, but this time, getRight() should be text2 because tire was more frequent in text2 than text
            spellCheckHelper(index, "tbre", List.of(
                    Pair.of("tire", text2),
                    Pair.of("tare", text)));
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void testDeleteWhereSimple(IndexedType indexedType) throws Exception {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_KEY);
        Tuple primaryKey;
        final TestRecordsTextProto.SimpleDocument simple = TestRecordsTextProto.SimpleDocument.newBuilder()
                .setDocId(1066L)
                .setText("foo bar")
                .setGroup(1)
                .build();

        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> {
                    metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index);
                    TextIndexTestUtils.addRecordTypePrefix(metaDataBuilder);
                    metaDataBuilder.getRecordType(SIMPLE_DOC)
                            .setPrimaryKey(concat(recordType(), field("text")));
                });
                primaryKey = createComplexRecordJoinedToSimple(1, 1066L, 1066L, "foo bar", "bar foo", false, System.currentTimeMillis(), 1, null);
            } else {
                openRecordStore(context, metaDataBuilder -> {
                    TextIndexTestUtils.addRecordTypePrefix(metaDataBuilder);
                    metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                    metaDataBuilder.addIndex(SIMPLE_DOC, index);
                    metaDataBuilder.getRecordType(SIMPLE_DOC)
                            .setPrimaryKey(concat(recordType(), field("text")));
                });
                primaryKey = recordStore.saveRecord(simple).getPrimaryKey();
            }

            final QueryComponent qc = Query.field("text").equalsValue("foo bar");
            Query.InvalidExpressionException err = assertThrows(Query.InvalidExpressionException.class,
                    () -> recordStore.deleteRecordsWhere(SIMPLE_DOC, indexedType.isSynthetic() ? Query.field("simple").matches(qc) : qc));
            assertThat(err.getMessage(), containsString(indexedType.isSynthetic()
                    ? "deleteRecordsWhere not matching primary key " + SIMPLE_DOC
                    : "deleteRecordsWhere not supported by index " + index.getName()));

            FDBStoredRecord<? extends Message> storedRecord = indexedType.isSynthetic()
                                                    ? recordStore.loadSyntheticRecord(primaryKey).get().getConstituent("simple")
                                                    : recordStore.loadRecord(primaryKey);

            assertNotNull(storedRecord);
            assertEquals(simple, TestRecordsTextProto.SimpleDocument.newBuilder().mergeFrom(storedRecord.getRecord()).build());

            commit(context);
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void testDeleteWhereComplexGrouped(IndexedType indexedType) {
        final Index index = indexedType.getIndex(COMPLEX_MULTIPLE_GROUPED_KEY);
        RecordMetaDataHook hook;
        final ComplexDocument zeroGroupDoc = ComplexDocument.newBuilder()
                .setGroup(0)
                .setDocId(1623L)
                .setText(TextSamples.ROMEO_AND_JULIET_PROLOGUE)
                .setText2(TextSamples.ANGSTROM)
                .setScore(0)
                .build();
        final ComplexDocument oneGroupDoc = ComplexDocument.newBuilder()
                .setGroup(1)
                .setDocId(1624L)
                .setText(TextSamples.ROMEO_AND_JULIET_PROLOGUE)
                .setText2(TextSamples.ANGSTROM)
                .setScore(1)
                .build();

        if (indexedType.isSynthetic()) {
            hook = metaDataBuilder -> {
                TextIndexTestUtils.addRecordTypePrefix(metaDataBuilder);
                metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index);
                metaDataBuilder.getRecordType("ComplexDocument")
                        .setPrimaryKey(concat(recordType(), Key.Expressions.concatenateFields("score", "doc_id")));
            };
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context, hook);
                TestRecordsTextProto.SimpleDocument simpleDocument = createSimpleDocument(1623, TextSamples.ROMEO_AND_JULIET_PROLOGUE, 0);
                recordStore.getRecordMetaData()
                        .getSyntheticRecordType("luceneSyntheticComplexJoinedToSimple")
                        .getRecordTypeKeyTuple();
                recordStore.saveRecord(simpleDocument);
                recordStore.saveRecord(zeroGroupDoc);

                simpleDocument = createSimpleDocument(1624, TextSamples.ROMEO_AND_JULIET_PROLOGUE, 1);
                recordStore.getRecordMetaData()
                        .getSyntheticRecordType("luceneSyntheticComplexJoinedToSimple")
                        .getRecordTypeKeyTuple();
                recordStore.saveRecord(simpleDocument);
                recordStore.saveRecord(oneGroupDoc);

                commit(context);
            }

        } else {
            hook = metaDataBuilder -> {
                TextIndexTestUtils.addRecordTypePrefix(metaDataBuilder);
                metaDataBuilder.addIndex(COMPLEX_DOC, index);
            };

            try (FDBRecordContext context = openContext()) {
                openRecordStore(context, hook);
                recordStore.saveRecord(zeroGroupDoc);
                recordStore.saveRecord(oneGroupDoc);
                commit(context);
            }
        }


        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);

            RecordQuery recordQuery =
                    indexedType.isSynthetic() ?
                    RecordQuery.newBuilder()
                    .setRecordType("luceneSyntheticComplexJoinedToSimple")
                    .setFilter(Query.and(
                            Query.field("complex").matches(Query.field("score").equalsParameter("group_value")),
                            new LuceneQueryComponent("simple_text:\"continuance\" AND complex_text2:\"named\"", List.of("simple_text", "complex_text2"))
                    ))
                    .build()
                                              :
                    RecordQuery.newBuilder()
                    .setRecordType(COMPLEX_DOC)
                    .setFilter(Query.and(
                            Query.field("group").equalsParameter("group_value"),
                            new LuceneQueryComponent("text:\"continuance\" AND text2:\"named\"", List.of("text", "text2"))
                    ))
                    .build();

            LucenePlanner planner = new LucenePlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState(), PlannableIndexTypes.DEFAULT, recordStore.getTimer());
            RecordQueryPlan plan = planner.plan(recordQuery);
            assertThat(plan, indexScan(allOf(
                    indexName(index.getName()),
                    scanParams(allOf(
                            group(hasTupleString("[EQUALS $group_value]")),
                            query(hasToString(indexedType.isSynthetic()
                                              ? "simple_text:\"continuance\" AND complex_text2:\"named\""
                                              : "text:\"continuance\" AND text2:\"named\"")))))));

            assertEquals(Collections.singletonList(zeroGroupDoc),
                    plan.execute(recordStore, EvaluationContext.forBinding("group_value", zeroGroupDoc.getGroup()))
                            .map(r -> {
                                if (indexedType.isSynthetic()) {
                                    return r.getConstituent("complex").getRecord();
                                } else {
                                    return r.getRecord();
                                }
                            })
                            .map(rec -> ComplexDocument.newBuilder().mergeFrom(rec).build())
                            .asList()
                            .join());
            assertEquals(Collections.singletonList(oneGroupDoc),
                    plan.execute(recordStore, EvaluationContext.forBinding("group_value", oneGroupDoc.getGroup()))
                            .map(r -> {
                                if (indexedType.isSynthetic()) {
                                    return r.getConstituent("complex").getRecord();
                                } else {
                                    return r.getRecord();
                                }
                            })
                            .map(rec -> ComplexDocument.newBuilder().mergeFrom(rec).build())
                            .asList()
                            .join());

            if (indexedType.isSynthetic()) {
                // delete where currently does not work with the synthetic records defined in this test
                assertThrows(Query.InvalidExpressionException.class,
                        () -> recordStore.deleteRecordsWhere(COMPLEX_DOC, Query.field("score").equalsValue(zeroGroupDoc.getScore())));
                return;
            }

            // Issue a delete where to delete the zero group
            recordStore.deleteRecordsWhere(COMPLEX_DOC, Query.field("group").equalsValue(zeroGroupDoc.getGroup()));

            assertEquals(Collections.emptyList(),
                    plan.execute(recordStore, EvaluationContext.forBinding("group_value", zeroGroupDoc.getGroup()))
                            .map(FDBQueriedRecord::getRecord)
                            .map(rec -> ComplexDocument.newBuilder().mergeFrom(rec).build())
                            .asList()
                            .join());
            assertEquals(Collections.singletonList(oneGroupDoc),
                    plan.execute(recordStore, EvaluationContext.forBinding("group_value", oneGroupDoc.getGroup()))
                            .map(FDBQueriedRecord::getRecord)
                            .map(rec -> ComplexDocument.newBuilder().mergeFrom(rec).build())
                            .asList()
                            .join());
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void testDeleteWhereAutoComplete(IndexedType indexedType) throws Exception {
        final Index index = indexedType.getIndex(COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE_KEY);
        final String groupField = indexedType.isSynthetic() ? "score" : "group";
        final RecordMetaDataHook hook =
                indexedType.isSynthetic()
                ? metaDataBuilder -> {
                    TextIndexTestUtils.addRecordTypePrefix(metaDataBuilder);
                    metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index);
                }
                : metaDataBuilder -> {
                    TextIndexTestUtils.addRecordTypePrefix(metaDataBuilder);
                    metaDataBuilder.addIndex(COMPLEX_DOC, index);
                };
        final int maxGroup = 10;
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook, groupField);
            for (int group = 0; group < maxGroup; group++) {
                for (long docId = 0L; docId < 10L; docId++) {
                    if (indexedType.isSynthetic()) {
                        int newId = group + (int) docId;
                        createComplexRecordJoinedToSimple(newId, newId, newId,
                                TextSamples.TELUGU, "hello there " + group, false, System.currentTimeMillis(), group);
                    } else {
                        ComplexDocument doc = ComplexDocument.newBuilder()
                                .setGroup(group)
                                .setDocId(docId)
                                .setText("hello there " + group)
                                .setText2(TextSamples.TELUGU)
                                .build();
                        recordStore.saveRecord(doc);
                    }
                }
            }
            commit(context);
        }
        // Re-initialize the builder so the LUCENE_INDEX_COMPRESSION_ENABLED prop is not added twice
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook, groupField);
            for (long group = 0; group < maxGroup; group++) {
                final RecordQueryPlan luceneIndexPlan =
                        LuceneIndexQueryPlan.of(index.getName(),
                                groupedAutoCompleteScanParams("hello", group, indexedType.isSynthetic() ? ImmutableList.of("simple_text", "complex_text2") : ImmutableList.of("text", "text2")),
                                indexedType.isSynthetic()
                                    ? RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.SYNTHETIC_CONSTITUENTS
                                    : RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                                false,
                                null,
                                indexedType.isSynthetic()
                                    ? JOINED_COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE_STORED_FIELDS
                                    : COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE_STORED_FIELDS);
                final List<FDBQueriedRecord<Message>> results =
                        recordStore.executeQuery(luceneIndexPlan, null, ExecuteProperties.SERIAL_EXECUTE)
                                .asList().get();
                assertThat(results, hasSize(10));
                int docId = 0;
                for (FDBQueriedRecord<?> result : results) {
                    Message record = result.getRecord();
                    if (indexedType.isSynthetic()) {
                        record = (DynamicMessage)record.getField(record.getDescriptorForType().findFieldByName("complex"));
                    }
                    final Descriptors.Descriptor descriptor = record.getDescriptorForType();
                    final Descriptors.FieldDescriptor textFieldDescriptor = descriptor.findFieldByName(indexedType.isSynthetic() ? "text2" : "text");
                    assertTrue(record.hasField(textFieldDescriptor));
                    final String textField = (String)record.getField(textFieldDescriptor);
                    assertEquals("hello there " + group, textField);

                    final IndexEntry entry = result.getIndexEntry();
                    Assertions.assertNotNull(entry);
                    Tuple primaryKey = entry.getPrimaryKey();
                    // The 1st element is the key for the record type
                    assertEquals(group, indexedType.isSynthetic() ? ((ArrayList)primaryKey.get(1)).get(1) : primaryKey.get(1));
                    assertEquals(indexedType.isSynthetic() ? group + docId : docId, indexedType.isSynthetic() ? ((ArrayList)primaryKey.get(1)).get(2) : primaryKey.get(2));
                    docId++;
                }
            }

            final int groupToDelete = maxGroup / 2;

            if (indexedType.isSynthetic()) {
                // delete where currently does not work with the synthetic records defined in this test
                assertThrows(Query.InvalidExpressionException.class,
                        () -> recordStore.deleteRecordsWhere(COMPLEX_DOC, Query.field("score").equalsValue(groupToDelete)));
                return;
            }

            recordStore.deleteRecordsWhere(COMPLEX_DOC, Query.field("group").equalsValue(groupToDelete));

            for (long group = 0; group < maxGroup; group++) {
                final RecordQueryPlan luceneIndexPlan =
                        LuceneIndexQueryPlan.of(index.getName(),
                                groupedAutoCompleteScanParams("hello", group, ImmutableList.of("text", "text2")),
                                RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                                false,
                                null,
                                COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE_STORED_FIELDS);
                final List<FDBQueriedRecord<Message>> results =
                        recordStore.executeQuery(luceneIndexPlan, null, ExecuteProperties.SERIAL_EXECUTE)
                                .asList().get();

                if (group == groupToDelete) {
                    assertThat(results, empty());
                } else {
                    assertThat(results, hasSize(10));
                    int docId = 0;
                    for (FDBQueriedRecord<?> result : results) {
                        final Message record = result.getRecord();
                        final Descriptors.Descriptor descriptor = record.getDescriptorForType();
                        final Descriptors.FieldDescriptor textFieldDescriptor = descriptor.findFieldByName("text");
                        assertTrue(record.hasField(textFieldDescriptor));
                        final String textField = (String)record.getField(textFieldDescriptor);
                        assertEquals("hello there " + group, textField);
                        final IndexEntry entry = result.getIndexEntry();
                        Assertions.assertNotNull(entry);
                        Tuple primaryKey = entry.getPrimaryKey();
                        // The 1st element is the key for the record type
                        assertEquals(group, primaryKey.get(1));
                        assertEquals((long)docId, primaryKey.get(2));
                        docId++;
                    }
                }
            }
        }
    }

    @Test
    void analyzerPerField() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, MULTIPLE_ANALYZER_LUCENE_INDEX);
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, "Hello, I am working on record layer", "Hello, I am working on FoundationDB", 1));
            // text field uses the default SYNONYM analyzer, so "hullo" should have match
            assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(1L, 1623L)),
                    recordStore.scanIndex(MULTIPLE_ANALYZER_LUCENE_INDEX, fullTextSearch(MULTIPLE_ANALYZER_LUCENE_INDEX, "text:hullo"), null, ScanProperties.FORWARD_SCAN));
            // text2 field has NGRAM analyzer override, so "hullo" should not have match
            assertIndexEntryPrimaryKeyTuples(Collections.emptySet(),
                    recordStore.scanIndex(MULTIPLE_ANALYZER_LUCENE_INDEX, fullTextSearch(MULTIPLE_ANALYZER_LUCENE_INDEX, "text2:hullo"), null, ScanProperties.FORWARD_SCAN));
            // text field uses the default SYNONYM analyzer, so "orkin" should not have match
            assertIndexEntryPrimaryKeyTuples(Collections.emptySet(),
                    recordStore.scanIndex(MULTIPLE_ANALYZER_LUCENE_INDEX, fullTextSearch(MULTIPLE_ANALYZER_LUCENE_INDEX, "text:orkin"), null, ScanProperties.FORWARD_SCAN));
            // text2 field has NGRAM analyzer override, so "orkin" should have match
            assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(1L, 1623L)),
                    recordStore.scanIndex(MULTIPLE_ANALYZER_LUCENE_INDEX, fullTextSearch(MULTIPLE_ANALYZER_LUCENE_INDEX, "text2:orkin"), null, ScanProperties.FORWARD_SCAN));
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void testSimpleAutoComplete(IndexedType indexedType) {
        final Index index = indexedType.getIndex(AUTO_COMPLETE_SIMPLE_LUCENE_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                createComplexRecordJoinedToSimple(1, 1623L, 1623L, "Hello, I am working on record layer", "Hello, I am working on FoundationDB", false, System.currentTimeMillis(), 1);
                // text field has auto-complete enabled, so the auto-complete query for "record layer" should have match
                assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(-1, Tuple.from(1L, 1623L), Tuple.from(1623))),
                        recordStore.scanIndex(index, autoCompleteBounds(index, "record layer", ImmutableSet.of("simple_text")), null, ScanProperties.FORWARD_SCAN));
            } else {
                rebuildIndexMetaData(context, COMPLEX_DOC, index);
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, "Hello, I am working on record layer", "Hello, I am working on FoundationDB", 1));
                // text field has auto-complete enabled, so the auto-complete query for "record layer" should have match
                assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(1L, 1623L)),
                        recordStore.scanIndex(index, autoCompleteBounds(index, "record layer", ImmutableSet.of("text")), null, ScanProperties.FORWARD_SCAN));
            }
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void analyzerChooserTest(IndexedType indexedType) {
        final Index index = indexedType.getIndex(ANALYZER_CHOOSER_TEST_LUCENE_INDEX_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                // Synonym analyzer is chosen due to the keyword "synonym" from the text
                createComplexRecordJoinedToSimple(1, 1623L, 1623L, "synonym food", "", false, System.currentTimeMillis(), 0);
                // Ngram analyzer is chosen due to no keyword "synonym" from the text
                createComplexRecordJoinedToSimple(2, 1624L, 1624L, "ngram motivation", "", false, System.currentTimeMillis(), 1);
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                // Synonym analyzer is chosen due to the keyword "synonym" from the text
                recordStore.saveRecord(createSimpleDocument(1623L, "synonym food", 1));
                // Ngram analyzer is chosen due to no keyword "synonym" from the text
                recordStore.saveRecord(createSimpleDocument(1624L, "ngram motivation", 1));
            }
            assertEquals(1, recordStore.scanIndex(index, fullTextSearch(index, "nutrient"), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            assertEquals(0, recordStore.scanIndex(index, fullTextSearch(index, "foo"), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            assertEquals(0, recordStore.scanIndex(index, fullTextSearch(index, "need"), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            assertEquals(1, recordStore.scanIndex(index, fullTextSearch(index, "motivatio"), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void basicLuceneCursorTest(IndexedType indexedType) {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_KEY);
        try (FDBRecordContext context = openContext()) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                // Save 20 records
                for (int i = 0; i < 20; i++) {
                    createComplexRecordJoinedToSimple(i, 1600L + i, 1600L + i, "testing text" + i, "", false, System.currentTimeMillis(), 1);
                }
                timer.reset();
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                // Save 20 records
                for (int i = 0; i < 20; i++) {
                    recordStore.saveRecord(createSimpleDocument(1600L + i, "testing text" + i, 1));
                }
            }
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(index, fullTextSearch(index, "\"testing\""), null, ScanProperties.FORWARD_SCAN);

            List<IndexEntry> entries = indexEntries.asList().join();
            assertEquals(20, entries.size());
            assertEquals(20, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void luceneCursorTestWithMultiplePages(IndexedType indexedType) throws Exception {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_KEY);
        // Configure page size as 10
        final RecordLayerPropertyStorage.Builder storageBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_CURSOR_PAGE_SIZE, 10);
        try (FDBRecordContext context = openContext(storageBuilder)) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                // Save 20 records
                for (int i = 0; i < 20; i++) {
                    createComplexRecordJoinedToSimple(i, 1600L + i, 1600L + i, "testing text" + i, "", false, System.currentTimeMillis(), 1);
                }
                timer.reset();
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                // Save 20 records
                for (int i = 0; i < 20; i++) {
                    recordStore.saveRecord(createSimpleDocument(1600L + i, "testing text" + i, 1));
                }
            }

            ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .build());
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(index, fullTextSearch(index, "\"testing\""), null, scanProperties);

            List<IndexEntry> entries = indexEntries.asList().join();
            assertEquals(20, entries.size());
            assertEquals(20, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, indexEntries.onNext().get().getNoNextReason());
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void luceneCursorTestWith3rdPage(IndexedType indexedType) throws Exception {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_KEY);
        // Configure page size as 10
        final RecordLayerPropertyStorage.Builder storageBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_CURSOR_PAGE_SIZE, 10);
        try (FDBRecordContext context = openContext(storageBuilder)) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                // Save 21 records
                for (int i = 0; i < 21; i++) {
                    createComplexRecordJoinedToSimple(i, 1600L + i, 1600L + i, "testing text" + i, "", false, System.currentTimeMillis(), 1);
                }
                timer.reset();
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                // Save 21 records
                for (int i = 0; i < 21; i++) {
                    recordStore.saveRecord(createSimpleDocument(1600L + i, "testing text" + i, 1));
                }
            }

            ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .build());
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(index, fullTextSearch(index, "\"testing\""), null, scanProperties);

            List<IndexEntry> entries = indexEntries.asList().join();
            assertEquals(21, entries.size());
            assertEquals(21, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, indexEntries.onNext().get().getNoNextReason());
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void luceneCursorTestWithMultiplePagesWithSkip(IndexedType indexedType) throws Exception {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_KEY);
        // Configure page size as 10
        final RecordLayerPropertyStorage.Builder storageBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_CURSOR_PAGE_SIZE, 10);
        try (FDBRecordContext context = openContext(storageBuilder)) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                // Save 31 records
                for (int i = 0; i < 31; i++) {
                    createComplexRecordJoinedToSimple(i, 1600L + i, 1600L + i, "testing text" + i, "", false, System.currentTimeMillis(), 1);
                }
                timer.reset();
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                // Save 31 records
                for (int i = 0; i < 31; i++) {
                    recordStore.saveRecord(createSimpleDocument(1600L + i, "testing text" + i, 1));
                }
            }

            ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .setSkip(12)
                    .build());
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(index, fullTextSearch(index, "\"testing\""), null, scanProperties);

            List<IndexEntry> entries = indexEntries.asList().join();
            assertEquals(19, entries.size());
            assertEquals(19, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, indexEntries.onNext().get().getNoNextReason());
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void luceneCursorTestWithLimit(IndexedType indexedType) throws Exception {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_KEY);
        // Configure page size as 10
        final RecordLayerPropertyStorage.Builder storageBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_CURSOR_PAGE_SIZE, 10);
        try (FDBRecordContext context = openContext(storageBuilder)) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                // Save 21 records
                for (int i = 0; i < 21; i++) {
                    createComplexRecordJoinedToSimple(i, 1600L + i, 1600L + i, "testing text" + i, "", false, System.currentTimeMillis(), 1);
                }
                timer.reset();
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                // Save 21 records
                for (int i = 0; i < 21; i++) {
                    recordStore.saveRecord(createSimpleDocument(1600L + i, "testing text" + i, 1));
                }
            }

            // Scan with limit = 10
            ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .setReturnedRowLimit(8)
                    .build());
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(index, fullTextSearch(index, "\"testing\""), null, scanProperties);

            // Get 8 results and continuation
            List<IndexEntry> entries = indexEntries.asList().join();
            assertEquals(8, entries.size());
            assertEquals(8, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            RecordCursorResult<IndexEntry> lastResult = indexEntries.onNext().get();
            assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, lastResult.getNoNextReason());

            indexEntries = recordStore.scanIndex(index, fullTextSearch(index, "\"testing\""), lastResult.getContinuation().toBytes(), scanProperties);

            // Get 8 results and continuation
            entries = indexEntries.asList().join();
            assertEquals(8, entries.size());
            assertEquals(16, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            lastResult = indexEntries.onNext().get();
            assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, lastResult.getNoNextReason());

            indexEntries = recordStore.scanIndex(index, fullTextSearch(index, "\"testing\""), lastResult.getContinuation().toBytes(), scanProperties);

            // Get 3 results
            entries = indexEntries.asList().join();
            assertEquals(5, entries.size());
            assertEquals(21, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, indexEntries.onNext().get().getNoNextReason());
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void luceneCursorTestWithLimitAndSkip(IndexedType indexedType) throws Exception {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_KEY);
        // Configure page size as 10
        final RecordLayerPropertyStorage.Builder storageBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_CURSOR_PAGE_SIZE, 10);
        try (FDBRecordContext context = openContext(storageBuilder)) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                // Save 21 records
                for (int i = 0; i < 21; i++) {
                    createComplexRecordJoinedToSimple(i, 1600L + i, 1600L + i, "testing text" + i, "", false, System.currentTimeMillis(), 1);
                }
                timer.reset();
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                // Save 21 records
                for (int i = 0; i < 21; i++) {
                    recordStore.saveRecord(createSimpleDocument(1600L + i, "testing text" + i, 1));
                }
            }

            // Scan with limit = 8 and skip = 2
            ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .setReturnedRowLimit(8)
                    .setSkip(2)
                    .build());
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(index, fullTextSearch(index, "\"testing\""), null, scanProperties);

            // Get 8 results and continuation
            List<IndexEntry> entries = indexEntries.asList().join();
            assertEquals(8, entries.size());
            assertEquals(8, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            RecordCursorResult<IndexEntry> lastResult = indexEntries.onNext().get();
            assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, lastResult.getNoNextReason());

            // Scan with limit = 8, no skip
            scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .setReturnedRowLimit(8)
                    .build());
            indexEntries = recordStore.scanIndex(index, fullTextSearch(index, "\"testing\""), lastResult.getContinuation().toBytes(), scanProperties);

            // Get 8 results and continuation
            entries = indexEntries.asList().join();
            assertEquals(8, entries.size());
            assertEquals(16, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            lastResult = indexEntries.onNext().get();
            assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, lastResult.getNoNextReason());

            indexEntries = recordStore.scanIndex(index, fullTextSearch(index, "\"testing\""), lastResult.getContinuation().toBytes(), scanProperties);

            // Get 3 results
            entries = indexEntries.asList().join();
            assertEquals(3, entries.size());
            assertEquals(19, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, indexEntries.onNext().get().getNoNextReason());
        }
    }

    @ParameterizedTest
    @MethodSource(LUCENE_INDEX_MAP_PARAMS)
    void luceneCursorTestAllMatchesSkipped(IndexedType indexedType) throws Exception {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_SUFFIXES_KEY);
        // Configure page size as 10
        final RecordLayerPropertyStorage.Builder storageBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_CURSOR_PAGE_SIZE, 10);
        try (FDBRecordContext context = openContext(storageBuilder)) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                // Save 6 records
                for (int i = 0; i < 6; i++) {
                    createComplexRecordJoinedToSimple(i, 1600L + i, 1600L + i, "testing text" + i, "", false, System.currentTimeMillis(), 1);
                }
                timer.reset();
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                // Save 6 records
                for (int i = 0; i < 6; i++) {
                    recordStore.saveRecord(createSimpleDocument(1600L + i, "testing text" + i, 1));
                }
            }
            // Scan with skip = 15
            ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .setSkip(15)
                    .build());
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(index, fullTextSearch(index, "\"testing\""), null, scanProperties);

            // No matches are found and source is exhausted
            RecordCursorResult<IndexEntry> next = indexEntries.onNext().get();
            assertFalse(next.hasNext());
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, next.getNoNextReason());
            assertNull(Verify.verifyNotNull(context.getTimer()).getCounter(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY));
        }
    }

    @ParameterizedTest
    @MethodSource("primaryKeySegmentIndexEnabledParams")
    void manySegmentsParallelOpen(IndexedType indexedType, boolean primaryKeySegmentIndexEnabled) {
        final Index index = indexedType.getIndex(primaryKeySegmentIndexEnabled ? SIMPLE_TEXT_SUFFIXES_WITH_PRIMARY_KEY_SEGMENT_INDEX_KEY : SIMPLE_TEXT_SUFFIXES_KEY);
        for (int i = 0; i < 20; i++) {
            final RecordLayerPropertyStorage.Builder insertProps = RecordLayerPropertyStorage.newBuilder()
                    .addProp(LuceneRecordContextProperties.LUCENE_MERGE_MAX_SIZE, 0.001); // Don't merge
            try (FDBRecordContext context = openContext(insertProps)) {
                if (indexedType.isSynthetic()) {
                    openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
                    createComplexRecordJoinedToSimple(i, 1000L + i, 1000L + i, ENGINEER_JOKE, "", false, System.currentTimeMillis(), 1);
                } else {
                    rebuildIndexMetaData(context, SIMPLE_DOC, index);
                    recordStore.saveRecord(createSimpleDocument(1000 + i, ENGINEER_JOKE, 2));
                }
                context.commit();
            }
        }
        final RecordLayerPropertyStorage.Builder scanProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_OPEN_PARALLELISM, 2); // Decrease parallelism when opening segments
        try (FDBRecordContext context = openContext(scanProps)) {
            if (indexedType.isSynthetic()) {
                openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
            } else {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
            }
            assertEquals(20,
                    recordStore.scanIndex(index, fullTextSearch(index, "Vision"), null, ScanProperties.FORWARD_SCAN).getCount().join());
            try (FDBDirectory directory = new FDBDirectory(recordStore.indexSubspace(index), context, index.getOptions())) {
                assertEquals(61, directory.listAll().length);
            }
        }
    }

    private static void assertAutoCompleteEntriesAndSegmentInfoStoredInCompoundFile(Index index, @Nonnull Subspace subspace, @Nonnull FDBRecordContext context, @Nonnull String segment) {
        validateSegmentAndIndexIntegrity(index, subspace, context, segment);
    }

    private static void validateSegmentAndIndexIntegrity(Index index, @Nonnull Subspace subspace, @Nonnull FDBRecordContext context, @Nonnull String segmentFile) {
        try (final FDBDirectory directory = new FDBDirectory(subspace, context, index.getOptions())) {
            final FDBLuceneFileReference reference = directory.getFDBLuceneFileReference(segmentFile);
            assertNotNull(reference);
            // Extract the segment name from the file name
            final String segmentName = segmentFile.substring(0, segmentFile.indexOf("."));
            validateIndexIntegrity(index, subspace, context, directory, segmentName);
        }
    }

    @Nonnull
    private LuceneIndexMaintainer getIndexMaintainer(final Index index) {
        return (LuceneIndexMaintainer)recordStore.getIndexMaintainer(index);
    }

    private FDBDirectory getDirectory(final Index index, final Tuple groupingKey) {
        return getIndexMaintainer(index).getDirectory(groupingKey, null);
    }

    private long getSegmentCount(final Index index, final Tuple groupingKey) {
        final String[] files = getDirectory(index, groupingKey).listAll();
        return Arrays.stream(files).filter(FDBDirectory::isCompoundFile).count();
    }

    private static void validateIndexIntegrity(Index index, @Nonnull Subspace subspace, @Nonnull FDBRecordContext context, @Nullable FDBDirectory fdbDirectory, @Nullable String segmentName) {
        final FDBDirectory directory = fdbDirectory == null ? new FDBDirectory(subspace, context, index.getOptions()) : fdbDirectory;
        String[] allFiles = directory.listAll();
        Set<Long> usedFieldInfos = new HashSet<>();
        final Set<Long> allFieldInfos = assertDoesNotThrow(() -> directory.getFieldInfosStorage().getAllFieldInfos().keySet());
        int segmentCount = 0;
        for (String file : allFiles) {
            final FDBLuceneFileReference fileReference = directory.getFDBLuceneFileReference(file);
            if (FDBDirectory.isEntriesFile(file) || FDBDirectory.isSegmentInfo(file) || FDBDirectory.isFieldInfoFile(file)
                    || file.endsWith(".pky")) {
                assertFalse(fileReference.getContent().isEmpty(), "fileName=" + file);
            } else if (FDBDirectory.isStoredFieldsFile(file) && (segmentName != null)) {
                // It is OK for a stored fields info for the next segment to be created at this point.
                // Make sure it is not around for the verified segment only
                if (file.startsWith(segmentName)) {
                    fail("Found stored fields file that should have been removed");
                }
            } else {
                assertTrue(FDBDirectory.isCompoundFile(file) || file.startsWith(IndexFileNames.SEGMENTS),
                        "fileName=" + file);
                assertTrue(fileReference.getContent().isEmpty(), "fileName=" + file);
            }
            if (FDBDirectory.isSegmentInfo(file)) {
                segmentCount++;
            }
            usedFieldInfos.add(validateFieldInfos(file, fileReference, directory));
        }
        usedFieldInfos.remove(0L);
        assertEquals(allFieldInfos, usedFieldInfos);
        assertThat(allFieldInfos.size(), Matchers.lessThanOrEqualTo(segmentCount));
    }

    private static long validateFieldInfos(final String file, final FDBLuceneFileReference fileReference, final FDBDirectory directory) {
        if (FDBDirectory.isFieldInfoFile(file) || FDBDirectory.isEntriesFile(file)) {
            assertNotEquals(0, fileReference.getFieldInfosId());
            assertNotEquals(ByteString.EMPTY, fileReference.getFieldInfosBitSet());
            final LuceneFieldInfosProto.FieldInfos fieldInfos = assertDoesNotThrow(() -> directory.getFieldInfosStorage().readFieldInfos(fileReference.getFieldInfosId()));
            final BitSet bitSet = BitSet.valueOf(fileReference.getFieldInfosBitSet().toByteArray());
            final Set<Integer> fieldNumbers = fieldInfos.getFieldInfoList().stream()
                    .map(LuceneFieldInfosProto.FieldInfo::getNumber)
                    .collect(Collectors.toSet());
            for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
                // operate on index i here
                if (i == Integer.MAX_VALUE) {
                    break; // or (i+1) would overflow
                }
                assertTrue(fieldNumbers.contains(i));
            }
        } else {
            assertEquals(0, fileReference.getFieldInfosId());
            assertEquals(ByteString.EMPTY, fileReference.getFieldInfosBitSet());
        }
        return fileReference.getFieldInfosId();
    }

    private void searchForAutoCompleteAndAssert(IndexedType indexedType, String query, boolean matches, boolean highlight, int planHash) throws Exception {
        final Index index = indexedType.getIndex(SIMPLE_TEXT_WITH_AUTO_COMPLETE_KEY);
        try (FDBRecordContext context = openContext()) {
            // Write 8 texts and 6 of them contain the key "good"
            addIndexAndSaveRecordForAutoComplete(context, index, indexedType.isSynthetic(), autoCompletes);

            final RecordQueryPlan luceneIndexPlan =
                    LuceneIndexQueryPlan.of(index.getName(),
                            autoCompleteScanParams(query, ImmutableSet.of(indexedType.isSynthetic() ? "simple_text" : "text")),
                            indexedType.isSynthetic()
                                ? RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.SYNTHETIC_CONSTITUENTS
                                : RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                            false,
                            null,
                            ImmutableList.of(indexedType.isSynthetic() ? JOINED_SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD : SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD));
            assertEquals(planHash, luceneIndexPlan.planHash(PlanHashable.CURRENT_LEGACY));
            final List<FDBQueriedRecord<Message>> results =
                    recordStore.executeQuery(luceneIndexPlan, null, ExecuteProperties.SERIAL_EXECUTE)
                            .asList().get();

            final var timer = Verify.verifyNotNull(context.getTimer());
            if (!matches) {
                // Assert no suggestions
                assertTrue(results.isEmpty());
                assertEquals(0, timer.getCount(LuceneEvents.Counts.LUCENE_SCAN_MATCHED_AUTO_COMPLETE_SUGGESTIONS));
                return;
            }

            // Assert the count of suggestions
            assertEquals(6, results.size());

            // Assert the suggestions' keys
            List<String> suggestions = results.stream()
                    .map(FDBQueriedRecord::getRecord)
                    .map(record -> {
                        if (indexedType.isSynthetic()) {
                            record = (DynamicMessage)record.getField(record.getDescriptorForType().findFieldByName("simple"));
                        }
                        final Descriptors.Descriptor descriptor = record.getDescriptorForType();
                        final Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("text");
                        assertTrue(record.hasField(fieldDescriptor));
                        return Verify.verifyNotNull((String) record.getField(fieldDescriptor));
                    }).collect(Collectors.toList());
            if (highlight) {
                assertEquals(ImmutableList.of("Good morning", "Good afternoon", "good evening", "Good night", "That's really good!", "I'm good"), suggestions);
            } else {
                assertEquals(ImmutableList.of("Good morning", "Good afternoon", "good evening", "Good night", "That's really good!", "I'm good"), suggestions);
            }

            assertAutoCompleteEntriesAndSegmentInfoStoredInCompoundFile(index, recordStore.indexSubspace(index),
                    context, "_0.cfs");

            commit(context);
        }
    }

    protected static final List<String> autoCompletes = List.of("Good morning", "Good afternoon", "good evening", "Good night", "That's really good!", "I'm good", "Hello Record Layer", "Hello FoundationDB!", ENGINEER_JOKE);
    protected static final List<String> autoCompletePhrases = List.of(
            "united states of america",
            "welcome to the united states of america",
            "united kingdom, france, the states",
            "The countries are united kingdom, france, the states",
            "states united as a country",
            "all the states united as a country",
            "states have been united as a country",
            "all the states have been united as a country",
            "united states is a country in the continent of america"
    );
    protected static final List<String> capitalizedAutoCaseCompletePhrases = List.of(
            "United States of America",
            "welcome to the United States of America",
            "United Kingdom, France, the States",
            "The countries are United Kingdom, France, the States",
            "States United as a country",
            "all the States United as a country",
            "States have been United as a country",
            "all the States have been united as a country",
            "United States is a country in the continent of America",
            "There is a country called Armenia"
    );

    private void addIndexAndSaveRecordForAutoComplete(@Nonnull FDBRecordContext context, Index index, boolean isSynthetic, List<String> autoCompletes) {
        if (isSynthetic) {
            openRecordStore(context, metaDataBuilder -> metaDataHookSyntheticRecordComplexJoinedToSimple(metaDataBuilder, index));
            for (int i = 0; i < autoCompletes.size(); i++) {
                createComplexRecordJoinedToSimple(1 + i, 1623L + i, 1623L + i, autoCompletes.get(i), "", false, System.currentTimeMillis(), 1);
            }
        } else {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, index);
            });

            for (int i = 0; i < autoCompletes.size(); i++) {
                recordStore.saveRecord(createSimpleDocument(1623L + i, autoCompletes.get(i), 1));
            }
        }
    }

    @SuppressWarnings("UnusedReturnValue")
    private void queryAndAssertAutoCompleteSuggestionsReturned(@Nonnull Index index, boolean isSynthetic, @Nullable String queriedConstituent, @Nonnull List<KeyExpression> storedFields,
                                                               @Nonnull String queriedField,
                                                               @Nonnull String searchKey, @Nonnull List<String> expectedSuggestions) throws Exception {
        final RecordQueryPlan luceneIndexPlan =
                LuceneIndexQueryPlan.of(index.getName(),
                        autoCompleteScanParams(searchKey, ImmutableSet.of(isSynthetic ? queriedConstituent + "_" + queriedField : queriedField)),
                        isSynthetic
                            ? RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.SYNTHETIC_CONSTITUENTS
                            : RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                        false,
                        null,
                        storedFields);
        final List<FDBQueriedRecord<Message>> results =
                recordStore.executeQuery(luceneIndexPlan, null, ExecuteProperties.SERIAL_EXECUTE)
                        .asList().get();

        assertEquals(expectedSuggestions.size(), results.size());
        List<String> suggestions = results.stream()
                .map(FDBQueriedRecord::getRecord)
                .map(record -> {
                    if (isSynthetic) {
                        record = (DynamicMessage)record.getField(record.getDescriptorForType().findFieldByName(queriedConstituent));
                    }
                    final Descriptors.Descriptor descriptor = record.getDescriptorForType();
                    final Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(queriedField);
                    Assertions.assertTrue(record.hasField(fieldDescriptor));
                    return Verify.verifyNotNull((String)record.getField(fieldDescriptor));
                })
                .collect(Collectors.toList());
        assertThat(suggestions, containsInAnyOrder(expectedSuggestions.stream().map(Matchers::equalTo).collect(Collectors.toList())));
    }

    private void rebuildIndexMetaData(final FDBRecordContext context, final String document, final Index index) {
        Pair<FDBRecordStore, QueryPlanner> pair = LuceneIndexTestUtils.rebuildIndexMetaData(context, path, document, index, isUseCascadesPlanner());
        this.recordStore = pair.getLeft();
        this.planner = pair.getRight();
        this.recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(true);
    }

    private void assertIndexEntryPrimaryKeys(Collection<Long> primaryKeys, RecordCursor<IndexEntry> cursor) {
        List<IndexEntry> indexEntries = cursor.asList().join();
        assertEquals(primaryKeys.stream().map(Tuple::from).collect(Collectors.toSet()),
                indexEntries.stream().map(IndexEntry::getPrimaryKey).collect(Collectors.toSet()));
    }

    private void assertIndexEntryPrimaryKeyTuples(Set<Tuple> primaryKeys, RecordCursor<IndexEntry> cursor) {
        List<IndexEntry> indexEntries = cursor.asList().join();
        assertEquals(primaryKeys,
                indexEntries.stream().map(IndexEntry::getPrimaryKey).collect(Collectors.toSet()));
    }

    @Test
    void testConflictWithLock() throws IOException {
        // Recreate the scenario of:
        //  - Lucene file lock is successfully acquired.
        //  - Another transaction fails with conflict while attempting to release the file lock.
        // With a recent code, the lock is cleared during the directory's close.
        Index index = SIMPLE_TEXT_SUFFIXES;
        try (final FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            context.commit();
        }
        Tuple conflictTuple = Tuple.from(100, 100, 100);
        try (final FDBRecordContext context = fdb.openContext()) {
            FDBRecordStore.Builder builder = recordStore.asBuilder()
                    .setContext(context)
                    .setMetaDataProvider(recordStore.getMetaDataProvider());

            final FDBRecordStore store = builder.createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);

            // Obtain lock
            final AgilityContext agile = AgilityContext.agile(context, TimeUnit.SECONDS.toMillis(4), 100_000);
            final FDBDirectory directory = new FDBDirectory(store.indexSubspace(index), index.getOptions(),
                    null, null, true, agile);

            final Lock lock = directory.obtainLock(IndexWriter.WRITE_LOCK_NAME); // will also flush lock

            // Read/write the conflicting value (without committing, yet)
            testConflictWithLockAgileSet(agile, conflictTuple);

            // Cause the conflict
            testConflictWithLockCauseConflict(conflictTuple);

            // finalize - imitating the Lucene flow (close lock, close directory, flush agility-context, then close user-context)
            boolean gotException = false;
            try {
                try {
                    lock.close();
                } catch (RuntimeException ex) {
                    gotException = true;
                }
                try {
                    directory.close();
                } catch (RuntimeException ex) {
                    gotException = true;
                }
                agile.flushAndClose();
                context.commit();
            } catch (RuntimeException ex) {
                gotException = true;
            } finally {
                assertTrue(gotException);
            }
        }

        // ensure that a new lock is possible
        TestRecordsTextProto.SimpleDocument doc3 = createSimpleDocument(1623L, "Salesmen jokes are funnier", 2);
        try (final FDBRecordContext context = fdb.openContext()) {
            FDBRecordStore.Builder builder = recordStore.asBuilder()
                    .setContext(context)
                    .setMetaDataProvider(recordStore.getMetaDataProvider());

            final FDBRecordStore store = builder.createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
            store.saveRecord(doc3);
            context.commit();
        }
    }

    private void testConflictWithLockAgileSet(AgilityContext agile1, Tuple conflictTuple) {
        agile1.accept(aContext -> {
            byte [] conflictKey = path.toSubspace(aContext).pack(conflictTuple);
            final Transaction tr = aContext.ensureActive();
            tr.get(conflictKey).join();
            tr.set(conflictKey, Tuple.from(100, 20).pack());
        });
    }

    private void testConflictWithLockCauseConflict(Tuple conflictTuple) {
        try (final FDBRecordContext context = fdb.openContext()) {
            byte [] conflictKey = path.toSubspace(context).pack(conflictTuple);
            final Transaction tr = context.ensureActive();
            tr.get(conflictKey).join();
            tr.set(conflictKey, Tuple.from(100, 10).pack());
            context.commit();
        }
    }
}

