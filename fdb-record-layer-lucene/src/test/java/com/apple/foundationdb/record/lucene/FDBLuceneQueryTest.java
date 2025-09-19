/*
 * FDBLuceneQueryTest.java
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.lucene.ngram.NgramAnalyzer;
import com.apple.foundationdb.record.lucene.synonym.EnglishSynonymMapConfig;
import com.apple.foundationdb.record.lucene.synonym.SynonymAnalyzer;
import com.apple.foundationdb.record.lucene.synonym.SynonymMapRegistryImpl;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.text.AllSuffixesTextTokenizer;
import com.apple.foundationdb.record.provider.common.text.TextSamples;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.RandomizedTestUtils;
import com.apple.test.SuperSlow;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.Message;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeAll;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.TestHelpers.assertLoadRecord;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.SIMPLE_TEXT_SUFFIXES;
import static com.apple.foundationdb.record.lucene.LucenePlanMatchers.group;
import static com.apple.foundationdb.record.lucene.LucenePlanMatchers.query;
import static com.apple.foundationdb.record.lucene.LucenePlanMatchers.scanParams;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.coveringIndexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.fetch;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.filter;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScanType;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.primaryKeyDistinct;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.scan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.typeFilter;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.unbounded;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.union;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.unorderedUnion;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Sophisticated queries involving full text predicates.
 */
@Tag(Tags.RequiresFDB)
public class FDBLuceneQueryTest extends FDBRecordStoreQueryTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(FDBLuceneQueryTest.class);
    // The fork/join pool parallelism factor (the number of active threads in the pool).
    private static final int PARALLELISM = 8;

    @BeforeAll
    public static void setup() {
        //set up the English Synonym Map so that we don't spend forever setting it up for every test, because this takes a long time
        SynonymMapRegistryImpl.instance().getSynonymMap(EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig.CONFIG_NAME);
    }

    private static final List<TestRecordsTextProto.SimpleDocument> DOCUMENTS = TextIndexTestUtils.toSimpleDocuments(Arrays.asList(
            TextSamples.ANGSTROM,
            TextSamples.AETHELRED,
            TextSamples.PARTIAL_ROMEO_AND_JULIET_PROLOGUE,
            TextSamples.FRENCH,
            TextSamples.ROMEO_AND_JULIET_PROLOGUE,
            TextSamples.ROMEO_AND_JULIET_PROLOGUE_END
    ));

    final List<String> textSamples = Arrays.asList(
            TextSamples.ROMEO_AND_JULIET_PROLOGUE,
            TextSamples.AETHELRED,
            TextSamples.ROMEO_AND_JULIET_PROLOGUE,
            TextSamples.ANGSTROM,
            TextSamples.AETHELRED,
            TextSamples.FRENCH
    );

    private final List<TestRecordsTextProto.MapDocument> mapDocuments = IntStream.range(0, textSamples.size() / 2)
            .mapToObj(i -> TestRecordsTextProto.MapDocument.newBuilder()
                    .setDocId(i)
                    .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("a").setValue(textSamples.get(i * 2)).build())
                    .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("b").setValue(textSamples.get(i * 2 + 1)).build())
                    .setGroup(i % 2)
                    .build()
            )
            .collect(Collectors.toList());


    private final List<TestRecordsTextProto.MapDocument> mapWithFieldDocuments = IntStream.range(0, textSamples.size() / 2)
            .mapToObj(i -> TestRecordsTextProto.MapDocument.newBuilder()
                    .setDocId(i)
                    .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("a").setValue(textSamples.get(i * 2)).build())
                    .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("b").setValue(textSamples.get(i * 2 + 1)).build())
                    .setGroup(i % 2)
                    .build()
            )
            .collect(Collectors.toList());

    private final List<TestRecordsTextProto.ComplexDocument> complexDocuments = IntStream.range(0, textSamples.size())
            .mapToObj(i -> TestRecordsTextProto.ComplexDocument.newBuilder()
                    .setDocId(i)
                    .setGroup(i % 2)
                    .setText(textSamples.get(i))
                    .build()
            )
            .collect(Collectors.toList());

    private static final String MAP_DOC = "MapDocument";

    private static final Index TEXT_AND_GROUP = new Index("text_and_group", concat(
            function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
            function(LuceneFunctionNames.LUCENE_STORED, function(LuceneFunctionNames.LUCENE_SORTED, field("group")))),
            LuceneIndexTypes.LUCENE, ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME));
    private static final List<KeyExpression> keys = List.of(field("key"), function(LuceneFunctionNames.LUCENE_TEXT, field("value")));
    private static final KeyExpression mainExpression = field("entry", KeyExpression.FanType.FanOut).nest(concat(keys));

    private static final Index MAP_AND_FIELD_ON_LUCENE_INDEX = new Index("MapField$values", concat(mainExpression, field("doc_id")), LuceneIndexTypes.LUCENE);
    private static final Index MAP_ON_LUCENE_INDEX = new Index("Map$entry-value", new GroupingKeyExpression(mainExpression, 1), LuceneIndexTypes.LUCENE);

    private static final Index COMPLEX_TEXT_BY_GROUP = new Index("Complex$text_by_group", function(LuceneFunctionNames.LUCENE_TEXT, field("text")).groupBy(field("group")), LuceneIndexTypes.LUCENE);

    private ExecutorService executorService = null;

    @Override
    public void setupPlanner(@Nullable PlannableIndexTypes indexTypes) {
        if (isUseCascadesPlanner()) {
            planner = new CascadesPlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState());
        } else {
            if (indexTypes == null) {
                indexTypes = new PlannableIndexTypes(
                        Sets.newHashSet(IndexTypes.VALUE, IndexTypes.VERSION),
                        Sets.newHashSet(IndexTypes.RANK, IndexTypes.TIME_WINDOW_LEADERBOARD),
                        Sets.newHashSet(IndexTypes.TEXT),
                        Sets.newHashSet(LuceneIndexTypes.LUCENE)
                );
            }
            planner = new LucenePlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState(), indexTypes, recordStore.getTimer());
        }
    }

    @Override
    protected RecordLayerPropertyStorage.Builder addDefaultProps(final RecordLayerPropertyStorage.Builder props) {
        return super.addDefaultProps(props)
                .addProp(LuceneRecordContextProperties.LUCENE_EXECUTOR_SERVICE, (Supplier<ExecutorService>)() -> executorService);
    }

    @SuppressWarnings("SameParameterValue")
    protected void openRecordStoreWithNgramIndex(FDBRecordContext context, boolean edgesOnly, int minSize, int maxSize) {
        final Index ngramIndex = new Index("Complex$text_index", function(LuceneFunctionNames.LUCENE_TEXT, field("text")), LuceneIndexTypes.LUCENE,
                ImmutableMap.of(LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION, NgramAnalyzer.NgramAnalyzerFactory.ANALYZER_FACTORY_NAME,
                        IndexOptions.TEXT_TOKEN_MIN_SIZE, String.valueOf(minSize),
                        IndexOptions.TEXT_TOKEN_MAX_SIZE, String.valueOf(maxSize),
                        LuceneIndexOptions.NGRAM_TOKEN_EDGES_ONLY, String.valueOf(edgesOnly)));
        openRecordStore(context, md -> {
        }, ngramIndex);
    }

    protected void openRecordStoreWithSynonymIndex(FDBRecordContext context) {
        final Index ngramIndex = new Index("Complex$text_index", function(LuceneFunctionNames.LUCENE_TEXT, field("text")), LuceneIndexTypes.LUCENE,
                ImmutableMap.of(
                        LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION, SynonymAnalyzer.QueryOnlySynonymAnalyzerFactory.ANALYZER_FACTORY_NAME,
                        LuceneIndexOptions.TEXT_SYNONYM_SET_NAME_OPTION, EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig.CONFIG_NAME));
        openRecordStore(context, md -> {
        }, ngramIndex);
    }

    protected void openRecordStoreWithGroup(FDBRecordContext context) {
        openRecordStore(context, md -> {
        }, TEXT_AND_GROUP);
    }

    protected void openRecordStoreWithComplex(FDBRecordContext context) {
        openRecordStore(context, md -> md.addIndex(TextIndexTestUtils.COMPLEX_DOC, COMPLEX_TEXT_BY_GROUP), null);
    }

    protected void openRecordStore(FDBRecordContext context) {
        openRecordStore(context, md -> {
        }, SIMPLE_TEXT_SUFFIXES);
    }

    protected void openRecordStore(FDBRecordContext context, RecordMetaDataHook hook, Index simpleDocIndex) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsTextProto.getDescriptor());
        metaDataBuilder.getRecordType(TextIndexTestUtils.COMPLEX_DOC).setPrimaryKey(concatenateFields("group", "doc_id"));
        if (simpleDocIndex != null) {
            metaDataBuilder.removeIndex("SimpleDocument$text");
            metaDataBuilder.addIndex(TextIndexTestUtils.SIMPLE_DOC, simpleDocIndex);
        }
        metaDataBuilder.addIndex(MAP_DOC, MAP_ON_LUCENE_INDEX);
        metaDataBuilder.addIndex(MAP_DOC, MAP_AND_FIELD_ON_LUCENE_INDEX);
        hook.apply(metaDataBuilder);
        recordStore = getStoreBuilder(context, metaDataBuilder.getRecordMetaData())
                .setSerializer(TextIndexTestUtils.COMPRESSING_SERIALIZER)
                .createOrOpen();
        setupPlanner(null);
    }

    private void initializeFlat() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            DOCUMENTS.forEach(recordStore::saveRecord);
            commit(context);
        }
    }

    private void initializeWithGroup() {
        try (FDBRecordContext context = openContext()) {
            openRecordStoreWithGroup(context);
            DOCUMENTS.forEach(recordStore::saveRecord);
            commit(context);
        }
    }

    private void initializeNested() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            mapDocuments.forEach(recordStore::saveRecord);
            commit(context);
        }
    }

    private void initializeNestedWithField() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            mapWithFieldDocuments.forEach(recordStore::saveRecord);
            commit(context);
        }
    }

    private void assertPrimaryKeys(String term, boolean shouldDeferFetch, Set<Long> expectedPrimaryKeys) {
        final QueryComponent filter = new LuceneQueryComponent(term, Lists.newArrayList());
        // Query for full records
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                .setFilter(filter)
                .build();
        setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
        RecordQueryPlan plan = planQuery(query);
        try (RecordCursor<FDBQueriedRecord<Message>> fdbQueriedRecordRecordCursor = recordStore.executeQuery(plan)) {
            Set<Long> primaryKeys = fdbQueriedRecordRecordCursor.map(FDBQueriedRecord::getPrimaryKey)
                    .map(t -> t.getLong(0)).asStream().collect(Collectors.toSet());
            assertEquals(expectedPrimaryKeys, primaryKeys, "Expected term not indexed");
        }
    }

    @ParameterizedTest(name = "testSynonym[shouldDeferFetch={0}]")
    @ValueSource(booleans = {true, false})
    void testSynonym(boolean shouldDeferFetch) {
        try (FDBRecordContext context = openContext()) {
            openRecordStoreWithSynonymIndex(context);
            TestRecordsTextProto.SimpleDocument document = TestRecordsTextProto.SimpleDocument.newBuilder()
                    .setDocId(1L)
                    .setGroup(1)
                    .setText("Good morning Mr Tian")
                    .build();
            recordStore.saveRecord(document);

            assertPrimaryKeys("good", shouldDeferFetch, Set.of(1L));
            assertPrimaryKeys("morning", shouldDeferFetch, Set.of(1L));
            //The synonym analyzer will add in synonym terms to the query, so this should return even though
            //it's not in the text
            assertPrimaryKeys("full", shouldDeferFetch, Set.of(1L));
        }
    }

    @ParameterizedTest(name = "testNgram[shouldDeferFetch={0}]")
    @ValueSource(booleans = {true, false})
    void testNgram(boolean shouldDeferFetch) {
        try (FDBRecordContext context = openContext()) {
            openRecordStoreWithNgramIndex(context, false, 3, 5);
            TestRecordsTextProto.SimpleDocument document = TestRecordsTextProto.SimpleDocument.newBuilder()
                    .setDocId(1L)
                    .setGroup(1)
                    .setText("Good morning Mr Tian")
                    .build();
            recordStore.saveRecord(document);

            assertPrimaryKeys("mo", shouldDeferFetch, Set.of());
            assertPrimaryKeys("mor", shouldDeferFetch, Set.of(1L));
            assertPrimaryKeys("morn", shouldDeferFetch, Set.of(1L));
            assertPrimaryKeys("morni", shouldDeferFetch, Set.of(1L));
            assertPrimaryKeys("ornin", shouldDeferFetch, Set.of(1L));
            assertPrimaryKeys("orning", shouldDeferFetch, Set.of());
            assertPrimaryKeys("mornin", shouldDeferFetch, Set.of());
            assertPrimaryKeys("Good", shouldDeferFetch, Set.of(1L));
            assertPrimaryKeys("good", shouldDeferFetch, Set.of(1L));
            assertPrimaryKeys("Mr", shouldDeferFetch, Set.of(1L));
            assertPrimaryKeys("morning", shouldDeferFetch, Set.of(1L));
            assertPrimaryKeys("Mr T", shouldDeferFetch, Set.of(1L));

        }
    }

    @ParameterizedTest(name = "testNgramEdgesOnly[shouldDeferFetch={0}]")
    @ValueSource(booleans = {true, false})
    void testNgramEdgesOnly(boolean shouldDeferFetch) {
        try (FDBRecordContext context = openContext()) {
            openRecordStoreWithNgramIndex(context, true, 3, 5);
            TestRecordsTextProto.SimpleDocument document = TestRecordsTextProto.SimpleDocument.newBuilder()
                    .setDocId(1L)
                    .setGroup(1)
                    .setText("Good morning Mr Tian")
                    .build();
            recordStore.saveRecord(document);
            assertPrimaryKeys("mo", shouldDeferFetch, Set.of());
            assertPrimaryKeys("mor", shouldDeferFetch, Set.of(1L));
            assertPrimaryKeys("morn", shouldDeferFetch, Set.of(1L));
            assertPrimaryKeys("morni", shouldDeferFetch, Set.of(1L));
            assertPrimaryKeys("ornin", shouldDeferFetch, Set.of());
            assertPrimaryKeys("rning", shouldDeferFetch, Set.of());
        }
    }

    @Test
    void testQueryWithStopWords() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            TestRecordsTextProto.SimpleDocument document1 = TestRecordsTextProto.SimpleDocument.newBuilder()
                    .setDocId(1L)
                    .setGroup(1)
                    .setText("Good morning Mr Tian")
                    .build();
            recordStore.saveRecord(document1);
            TestRecordsTextProto.SimpleDocument document2 = TestRecordsTextProto.SimpleDocument.newBuilder()
                    .setDocId(2L)
                    .setGroup(1)
                    .setText("Good morning the Mr Tian")
                    .build();
            recordStore.saveRecord(document2);
            TestRecordsTextProto.SimpleDocument document3 = TestRecordsTextProto.SimpleDocument.newBuilder()
                    .setDocId(3L)
                    .setGroup(1)
                    .setText("Good morning these Mr Tian")
                    .build();
            recordStore.saveRecord(document3);

            assertPrimaryKeys("text:(+morn* +the*)", false, Set.of(1L, 2L, 3L));
            assertPrimaryKeys("text:(+morn* the*)", false, Set.of(1L, 2L, 3L));
            assertPrimaryKeys("text:(+morn* +the)", false, Set.of(1L, 2L, 3L));
            assertPrimaryKeys("text:(+morn* the)", false, Set.of(1L, 2L, 3L));
        }
    }

    /**
     * Test that lucene doesn't break whene there's unicode in the text.
     * <p>
     *     This test is not intending to test the behavior of querying with unicode, just that having it doesn't break
     *     other queries.
     * </p>
     */
    @Test
    void testQueryWithUnicode() {
        List<StringBuilder> allBuilders = new ArrayList<>();
        final String beginningWord = "banana";
        final String endWord = "catamaran";
        BiConsumer<StringBuilder, String> addWord = (stringBuilder, word) -> stringBuilder.append(" ").append(word).append(" ");
        StringBuilder sb = new StringBuilder();
        addWord.accept(sb, beginningWord);
        allBuilders.add(sb);
        int skipped = 0;
        for (int i = 0; i < Character.MAX_CODE_POINT; i++) {
            if (Character.isSupplementaryCodePoint(i)) {
                sb.append(Character.highSurrogate(i));
                sb.append(Character.lowSurrogate(i));
            } else if (Character.isDefined(i) && Character.getType(i) != Character.PRIVATE_USE
                    && !Character.isSurrogate((char) i)) {
                sb.append((char) i);
            } else {
                skipped++;
            }
            if (sb.length() > 50_000) {
                addWord.accept(sb, endWord);
                sb = new StringBuilder();
                addWord.accept(sb, beginningWord);
                allBuilders.add(sb);
            }
        }
        addWord.accept(allBuilders.get(allBuilders.size() - 1), endWord);
        List<String> allTexts = allBuilders.stream().map(StringBuilder::toString).collect(Collectors.toList());
        LOGGER.debug("Skipped: " + skipped + "/" + Character.MAX_CODE_POINT);
        LOGGER.debug("AllTexts (" + allTexts.size() + "): " + allTexts.stream().map(text -> String.valueOf(text.length())).collect(Collectors.joining(", ")));
        long id = 1L;
        for (final String text : allTexts) {
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context);
                TestRecordsTextProto.SimpleDocument document1 = TestRecordsTextProto.SimpleDocument.newBuilder()
                        .setDocId(id++)
                        .setGroup(1)
                        .setText(text)
                        .build();
                recordStore.saveRecord(document1);
                context.commit();
            }
        }

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            assertPrimaryKeys("text:(" + beginningWord + ")", false,
                    LongStream.range(1, id).boxed().collect(Collectors.toSet()));
            assertPrimaryKeys("text:(" + endWord + ")", false,
                    LongStream.range(1, id).boxed().collect(Collectors.toSet()));
        }

    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void simpleLuceneScans(boolean shouldDeferFetch) throws Exception {
        initializeFlat();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter1 = new LuceneQueryComponent(LuceneQueryType.QUERY,
                    "civil blood makes civil hands unclean", false, Lists.newArrayList(), true,
                    null, null);
            // Query for full records
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(filter1)
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            Matcher<RecordQueryPlan> matcher = indexScan(allOf(indexScan(SIMPLE_TEXT_SUFFIXES.getName()),
                    indexScanType(LuceneScanTypes.BY_LUCENE),
                    scanParams(query(hasToString("MULTI civil blood makes civil hands unclean")))));
            RecordQueryPlan plan = planQuery(query);
            assertThat(plan, matcher);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan)) {
                List<FDBQueriedRecord<Message>> queriedRecordList = cursor.asList().get();
                Set<Long> primaryKeys = queriedRecordList.stream().map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).collect(Collectors.toSet());
                assertEquals(Set.of(2L, 4L), Set.copyOf(primaryKeys));

                Set<String> texts = queriedRecordList.stream().map(FDBQueriedRecord::getStoredRecord)
                        .filter(Objects::nonNull) //shouldn't be necessary, but helps the linter out
                        .map(FDBStoredRecord::getRecord)
                        .map(m -> (String)m.getField(m.getDescriptorForType().findFieldByName("text")))
                        .collect(Collectors.toSet());
                texts.forEach(t -> assertTrue(t.contains("civil blood makes civil hands unclean")));
            }
        }
    }

    @ParameterizedTest(name = "testThenExpressionBeforeFieldExpression[shouldDeferFetch={0}]")
    @BooleanSource
    void testThenExpressionBeforeFieldExpression(boolean shouldDeferFetch) throws Exception {
        initializeNestedWithField();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter1 = new LuceneQueryComponent("*:*", Lists.newArrayList("doc_id"));
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(MAP_DOC)
                    .setFilter(filter1)
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planQuery(query);
            try (RecordCursor<FDBQueriedRecord<Message>> fdbQueriedRecordRecordCursor = recordStore.executeQuery(plan)) {
                RecordCursor<Tuple> map = fdbQueriedRecordRecordCursor.map(FDBQueriedRecord::getPrimaryKey);
                List<Long> primaryKeys = map.map(t -> t.getLong(0)).asList().get();
                assertEquals(Set.of(0L, 1L, 2L), Set.copyOf(primaryKeys));
            }
        }

    }

    @ParameterizedTest(name = "simpleLuceneScansDocId[shouldDeferFetch={0}]")
    @BooleanSource
    void simpleLuceneScansDocId(boolean shouldDeferFetch) throws Exception {
        initializeFlat();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            // Query for full records
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(Query.field("doc_id").equalsValue(1L))
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            Matcher<RecordQueryPlan> matcher = typeFilter(equalTo(Collections.singleton(TextIndexTestUtils.SIMPLE_DOC)), scan(bounds(hasTupleString("[[1],[1]]"))));
            RecordQueryPlan plan = planQuery(query);
            assertThat(plan, matcher);
            try (final RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan)) {
                List<Long> primaryKeys = cursor.map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
                assertEquals(Set.of(1L), Set.copyOf(primaryKeys));
            }
        }
    }

    @ParameterizedTest(name = "delayFetchOnOrOfLuceneScanWithFieldFilter[shouldDeferFetch={0}]")
    @BooleanSource
    void delayFetchOnOrOfLuceneScanWithFieldFilter(boolean shouldDeferFetch) throws Exception {
        initializeFlat();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter1 = new LuceneQueryComponent("civil blood makes civil hands unclean", Lists.newArrayList("text"));
            // Query for full records
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(Query.or(filter1, Query.field("doc_id").lessThan(10000L)))
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planQuery(query);
            Matcher<RecordQueryPlan> matcher = primaryKeyDistinct(
                    unorderedUnion(
                            indexScan(allOf(indexScan(SIMPLE_TEXT_SUFFIXES.getName()),
                                    indexScanType(LuceneScanTypes.BY_LUCENE),
                                    scanParams(query(hasToString("civil blood makes civil hands unclean"))))),
                            typeFilter(equalTo(Collections.singleton(TextIndexTestUtils.SIMPLE_DOC)),
                                    scan(bounds(hasTupleString("([null],[10000])"))))
                    ));
            assertThat(plan, matcher);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan)) {
                List<Long> primaryKeys = cursor.map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
                assertEquals(Set.of(2L, 4L, 0L, 1L, 3L, 5L), Set.copyOf(primaryKeys));
                if (shouldDeferFetch) {
                    assertLoadRecord(5, context);
                } else {
                    assertLoadRecord(6, context);
                }
            }
        }
    }

    @ParameterizedTest(name = "delayFetchOnFilterWithSort[shouldDeferFetch={0}]")
    @BooleanSource
    void delayFetchOnLuceneFilterWithSort(boolean shouldDeferFetch) throws Exception {
        initializeWithGroup();
        try (FDBRecordContext context = openContext()) {
            openRecordStoreWithGroup(context);
            final QueryComponent filter1 = new LuceneQueryComponent("parents", Lists.newArrayList("text"), true);
            // Query for full records
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(filter1)
                    .setSort(field("group"), true)
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planQuery(query);
            try (RecordCursor<FDBQueriedRecord<Message>> recordCursor = recordStore.executeQuery(plan)) {
                List<Long> primaryKeys = recordCursor.map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
                assertEquals(List.of(5L, 2L, 4L), primaryKeys);
                if (shouldDeferFetch) {
                    assertLoadRecord(5, context);
                } else {
                    assertLoadRecord(6, context);
                }
            }
        }
    }

    @ParameterizedTest(name = "delayFetchOnAndOfLuceneAndFieldFilter[shouldDeferFetch={0}]")
    @ValueSource(booleans = {true, false})
    void delayFetchOnAndOfLuceneAndFieldFilter(boolean shouldDeferFetch) throws Exception {
        initializeFlat();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter1 = new LuceneQueryComponent("civil blood makes civil hands unclean", Lists.newArrayList());
            // Query for full records
            QueryComponent filter2 = Query.field("doc_id").equalsValue(2L);
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(Query.and(filter2, filter1))
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planner.plan(query);
            Matcher<RecordQueryPlan> scanMatcher = fetch(filter(filter2, coveringIndexScan(indexScan(allOf(indexScanType(LuceneScanTypes.BY_LUCENE), indexScan(SIMPLE_TEXT_SUFFIXES.getName()),
                    scanParams(query(hasToString("MULTI civil blood makes civil hands unclean"))))))));
            assertThat(plan, scanMatcher);
            try (RecordCursor<FDBQueriedRecord<Message>> primaryKeys = recordStore.executeQuery(plan)) {
                final List<Long> keys = primaryKeys.map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
                assertEquals(Set.of(2L), Set.copyOf(keys));
                if (shouldDeferFetch) {
                    assertLoadRecord(3, context);
                } else {
                    assertLoadRecord(4, context);
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void delayFetchOnOrOfLuceneFiltersGivesUnion(boolean shouldDeferFetch) throws Exception {
        initializeFlat();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter1 = new LuceneQueryComponent(LuceneQueryType.QUERY, "(\"civil blood makes civil hands unclean\")", false, Lists.newArrayList("text"), true,
                    null, null);
            final QueryComponent filter2 = new LuceneQueryComponent(LuceneQueryType.QUERY, "(\"was king from 966 to 1016\")", false, Lists.newArrayList(), true,
                    null, null);
            // Query for full records
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(Query.or(filter1, filter2))
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planQuery(query);
            final Matcher<RecordQueryPlan> scan1 = indexScan(allOf(indexScan(SIMPLE_TEXT_SUFFIXES.getName()),
                    indexScanType(LuceneScanTypes.BY_LUCENE),
                    scanParams(query(hasToString("MULTI (\"civil blood makes civil hands unclean\")")))));
            final Matcher<RecordQueryPlan> scan2 = indexScan(allOf(indexScan(SIMPLE_TEXT_SUFFIXES.getName()),
                    indexScanType(LuceneScanTypes.BY_LUCENE),
                    scanParams(query(hasToString("MULTI (\"was king from 966 to 1016\")")))));
            Matcher<RecordQueryPlan> matcher;
            if (shouldDeferFetch) {
                matcher = fetch(primaryKeyDistinct(unorderedUnion(coveringIndexScan(scan1), coveringIndexScan(scan2))));
            } else {
                matcher = primaryKeyDistinct(unorderedUnion(scan1, scan2));
            }
            assertThat(plan, matcher);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan)) {
                List<FDBQueriedRecord<Message>> queriedRecordList = cursor.asList().get();
                Set<Long> primaryKeys = queriedRecordList.stream()
                        .map(FDBRecord::getPrimaryKey)
                        .map(t -> t.getLong(0))
                        .collect(Collectors.toSet());
                assertEquals(Set.of(1L, 2L, 4L), primaryKeys);
                if (shouldDeferFetch) {
                    assertLoadRecord(5, context);
                } else {
                    assertLoadRecord(6, context);
                }

                Set<String> texts = queriedRecordList.stream()
                        .map(FDBQueriedRecord::getStoredRecord)
                        .filter(Objects::nonNull) //unnecessary, but helps keep the linter happy
                        .map(FDBStoredRecord::getRecord)
                        .map(m -> (String)m.getField(m.getDescriptorForType().findFieldByName("text")))
                        .collect(Collectors.toSet());
                for (String text : texts) {
                    boolean match1 = text.contains("was king from 966 to 1016");
                    boolean match2 = text.contains("civil blood makes civil hands unclean");
                    assertTrue(match1 || match2);
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void delayFetchOnAndOfLuceneFilters(boolean shouldDeferFetch) throws Exception {
        initializeFlat();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter1 = new LuceneQueryComponent(LuceneQueryType.QUERY,
                    "\"the continuance\"", false, Lists.newArrayList(), true,
                    null, null);
            final QueryComponent filter2 = new LuceneQueryComponent(LuceneQueryType.QUERY,
                    "grudge", false, Lists.newArrayList(), true,
                    null, null);
            // Query for full records
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(Query.and(filter1, filter2))
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planQuery(query);
            Matcher<RecordQueryPlan> matcher = indexScan(allOf(indexScanType(LuceneScanTypes.BY_LUCENE),
                    indexName(SIMPLE_TEXT_SUFFIXES.getName()),
                    scanParams(query(hasToString("MULTI \"the continuance\" AND MULTI grudge")))));
            assertThat(plan, matcher);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan)) {
                List<FDBQueriedRecord<Message>> queriedRecordList = cursor.asList().get();
                Set<Long> primaryKeys = queriedRecordList.stream()
                        .map(FDBQueriedRecord::getPrimaryKey)
                        .map(t -> t.getLong(0))
                        .collect(Collectors.toSet());
                assertEquals(Set.of(4L), primaryKeys);
                if (shouldDeferFetch) {
                    assertLoadRecord(3, context);
                } else {
                    assertLoadRecord(4, context);
                }

                Set<String> texts = queriedRecordList.stream()
                        .map(FDBQueriedRecord::getStoredRecord)
                        .filter(Objects::nonNull)
                        .map(FDBStoredRecord::getRecord)
                        .map(m -> (String)m.getField(m.getDescriptorForType().findFieldByName("text")))
                        .collect(Collectors.toSet());
                for (String text : texts) {
                    boolean match1 = text.contains("the continuance");
                    boolean match2 = text.contains("grudge");
                    assertTrue(match1 || match2);
                }
            }
        }
    }

    @ParameterizedTest(name = "delayFetchOnLuceneComplexStringAnd[shouldDeferFetch={0}]")
    @BooleanSource
    void delayFetchOnLuceneComplexStringAnd(boolean shouldDeferFetch) throws Exception {
        initializeFlat();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter1 = new LuceneQueryComponent("(the continuance AND grudge)", Lists.newArrayList());
            // Query for full records
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(filter1)
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planQuery(query);
            Matcher<RecordQueryPlan> matcher = indexScan(allOf(indexScanType(LuceneScanTypes.BY_LUCENE),
                    indexName(SIMPLE_TEXT_SUFFIXES.getName()),
                    scanParams(query(hasToString("MULTI (the continuance AND grudge)")))));
            assertThat(plan, matcher);
            try (RecordCursor<FDBQueriedRecord<Message>> recordCursor = recordStore.executeQuery(plan)) {
                List<Long> primaryKeys = recordCursor
                        .map(FDBQueriedRecord::getPrimaryKey)
                        .map(t -> t.getLong(0))
                        .asList().get();
                assertEquals(Set.of(4L), Set.copyOf(primaryKeys));
                if (shouldDeferFetch) {
                    assertLoadRecord(3, context);
                } else {
                    assertLoadRecord(4, context);
                }
            }
        }
    }

    @ParameterizedTest(name = "delayFetchOnLuceneComplexStringOr[shouldDeferFetch={0}]")
    @BooleanSource
    void delayFetchOnLuceneComplexStringOr(boolean shouldDeferFetch) throws Exception {
        initializeFlat();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter1 = new LuceneQueryComponent("\"the continuance\" OR grudge", Lists.newArrayList());
            // Query for full records
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(filter1)
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planQuery(query);
            Matcher<RecordQueryPlan> matcher = indexScan(allOf(indexScanType(LuceneScanTypes.BY_LUCENE),
                    indexName(SIMPLE_TEXT_SUFFIXES.getName()),
                    scanParams(query(hasToString("MULTI \"the continuance\" OR grudge")))));
            assertThat(plan, matcher);
            try (RecordCursor<FDBQueriedRecord<Message>> recordCursor = recordStore.executeQuery(plan)) {
                List<Long> primaryKeys = recordCursor.map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
                assertEquals(Set.of(4L, 5L, 2L), Set.copyOf(primaryKeys));
                if (shouldDeferFetch) {
                    assertLoadRecord(3, context);
                } else {
                    assertLoadRecord(4, context);
                }
            }
        }
    }

    @ParameterizedTest(name = "misMatchQueryShouldReturnNoResult[shouldDeferFetch={0}]")
    @ValueSource(booleans = {true, false})
    void misMatchQueryShouldReturnNoResult(boolean shouldDeferFetch) throws Exception {
        initializeFlat();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter1 = new LuceneQueryComponent("doesNotExist", Lists.newArrayList("text"), true);
            // Query for full records
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(filter1)
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planQuery(query);
            Matcher<RecordQueryPlan> matcher = indexScan(allOf(indexScan(SIMPLE_TEXT_SUFFIXES.getName()), indexScanType(LuceneScanTypes.BY_LUCENE), scanParams(query(hasToString("MULTI doesNotExist")))));
            assertThat(plan, matcher);
            try (RecordCursor<FDBQueriedRecord<Message>> recordCursor = recordStore.executeQuery(plan)) {
                List<Long> primaryKeys = recordCursor.map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
                assertEquals(Set.of(), Set.copyOf(primaryKeys));
                if (shouldDeferFetch) {
                    assertLoadRecord(3, context);
                } else {
                    assertLoadRecord(4, context);
                }
            }
        }
    }

    private static Stream<Arguments> threadCount() {
        return Stream.concat(
                Stream.of(1, 10).map(Arguments::of),
                RandomizedTestUtils.randomArguments(random ->
                        Arguments.of(random.nextInt(10) + 1)));
    }

    @ParameterizedTest(name = "threadedLuceneScanDoesntBreakPlannerAndSearch-PoolThreadCount={0}")
    @MethodSource("threadCount")
    @SuperSlow
    void threadedLuceneScanDoesntBreakPlannerAndSearch(@Nonnull Integer value) throws Exception {
        final FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        // limit the FJP size to try and force the # segments to exceed the # threads
        factory.setExecutor(new ForkJoinPool(PARALLELISM,
                ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                null, false));
        CountingThreadFactory threadFactory = new CountingThreadFactory();
        executorService = Executors.newFixedThreadPool(value, threadFactory);
        initializeFlat();
        for (int i = 0; i < 200; i++) {
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context);
                String[] randomWords = LuceneIndexTestUtils.generateRandomWords(500);
                final TestRecordsTextProto.SimpleDocument dylan = TestRecordsTextProto.SimpleDocument.newBuilder()
                        .setDocId(i)
                        .setText(randomWords[1])
                        .setGroup(2)
                        .build();
                recordStore.saveRecord(dylan);
                commit(context);
            }
        }
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter1 = new LuceneQueryComponent("*:*", Lists.newArrayList("text"), false);
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(filter1)
                    .build();
            setDeferFetchAfterUnionAndIntersection(false);
            RecordQueryPlan plan = planQuery(query);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan)) {
                @SuppressWarnings("unused") List<Long> ignored = cursor //this is here to make sure that we iterate the cursor
                        .map(FDBQueriedRecord::getPrimaryKey)
                        .map(t -> t.getLong(0))
                        .asList().get();
                assertThat(threadFactory.threadCounts, aMapWithSize(greaterThan(0)));
            }
        }
    }

    static class CountingThreadFactory implements ThreadFactory {
        final Map<String, Integer> threadCounts = new ConcurrentHashMap<>();
        final ThreadFactory delegate = Executors.defaultThreadFactory();

        @Override
        public Thread newThread(@Nonnull Runnable r) {
            return delegate.newThread(() -> {
                threadCounts.merge(Thread.currentThread().getName(), 1, Integer::sum);
                r.run();
            });
        }
    }

    @ParameterizedTest(name = "nestedLuceneAndQuery[shouldDeferFetch={0}]")
    @BooleanSource
    void nestedLuceneAndQuery(boolean shouldDeferFetch) throws Exception {
        initializeNested();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(MAP_DOC)
                    .setFilter(Query.and(
                            new LuceneQueryComponent("entry_value:king", Lists.newArrayList("entry"), false),
                            Query.field("entry").oneOfThem().matches(Query.field("key").equalsValue("a"))))
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planQuery(query);

            try (RecordCursor<FDBQueriedRecord<Message>> recordCursor = recordStore.executeQuery(plan)) {
                List<Long> primaryKeys = recordCursor.map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
                Matcher<RecordQueryPlan> matcher = indexScan(allOf(
                        indexScanType(LuceneScanTypes.BY_LUCENE),
                        indexScan("Map$entry-value"),
                        scanParams(allOf(
                                query(hasToString("entry_value:king")),
                                group(hasTupleString("[[a],[a]]"))))
                ));
                assertThat(plan, matcher);
                assertEquals(Set.of(2L), Set.copyOf(primaryKeys));
            }
        }
    }

    @ParameterizedTest(name = "nestedLuceneFieldQuery[shouldDeferFetch={0}]")
    @BooleanSource
    void nestedLuceneFieldQuery(boolean shouldDeferFetch) throws Exception {
        initializeNested();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(MAP_DOC)
                    .setFilter(new LuceneQueryComponent("entry_value:king", Lists.newArrayList("entry")))
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planQuery(query);
            Matcher<RecordQueryPlan> matcher = indexScan(allOf(
                    indexScanType(LuceneScanTypes.BY_LUCENE),
                    indexScan("MapField$values"),
                    scanParams(query(hasToString("entry_value:king")))
            ));
            assertThat(plan, matcher);
            try (RecordCursor<FDBQueriedRecord<Message>> recordCursor = recordStore.executeQuery(plan)) {
                List<Long> primaryKeys = recordCursor.map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
                assertEquals(Set.of(0L, 2L), Set.copyOf(primaryKeys));
            }
        }
    }

    @ParameterizedTest(name = "nestedOneOfThemQuery[shouldDeferFetch={0}]")
    @BooleanSource
    void nestedOneOfThemQuery(boolean shouldDeferFetch) throws Exception {
        initializeNested();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);

            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(MAP_DOC)
                    .setFilter(Query.field("entry").oneOfThem().matches(Query.field("key").equalsValue("king")))
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planQuery(query);
            Matcher<RecordQueryPlan> matcher = indexScan(allOf(
                    indexScanType(LuceneScanTypes.BY_LUCENE),
                    indexScan("MapField$values"),
                    scanParams(query(hasToString("entry_key:STRING EQUALS king")))));
            if (shouldDeferFetch) {
                matcher = fetch(primaryKeyDistinct(coveringIndexScan(matcher)));
            } else {
                matcher = primaryKeyDistinct(matcher);
            }
            assertThat(plan, matcher);
            try (RecordCursor<FDBQueriedRecord<Message>> recordCursor = recordStore.executeQuery(plan)) {
                List<Long> primaryKeys = recordCursor.map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
                assertEquals(Set.of(), Set.copyOf(primaryKeys));
            }
        }
    }

    @ParameterizedTest(name = "nestedOneOfThemWithAndQuery[shouldDeferFetch={0}]")
    @BooleanSource
    void nestedOneOfThemWithAndQuery(boolean shouldDeferFetch) {
        initializeNested();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter = Query.field("entry").oneOfThem().matches(Query.and(Query.field("key").equalsValue("b"),
                    Query.field("value").text().containsPhrase("civil blood makes civil hands unclean")));
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(MAP_DOC)
                    .setFilter(filter)
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planQuery(query);
            Matcher<RecordQueryPlan> matcher = indexScan(allOf(
                    indexName(MAP_AND_FIELD_ON_LUCENE_INDEX.getName()),
                    indexScanType(LuceneScanTypes.BY_LUCENE),
                    // TODO: This query does not have the proper correlation between the two children. An index that put the key into the name would be needed to tell that.
                    scanParams(query(hasToString("entry_key:STRING EQUALS b AND entry_value:TEXT TEXT_CONTAINS_PHRASE civil blood makes civil hands unclean")))
            ));
            if (shouldDeferFetch) {
                matcher = fetch(primaryKeyDistinct(coveringIndexScan(matcher)));
            } else {
                matcher = primaryKeyDistinct(matcher);
            }
            assertThat(plan, matcher);
        }
    }

    @ParameterizedTest(name = "nestedOneOfThemWithOrQuery[shouldDeferFetch={0}]")
    @BooleanSource
    void nestedOneOfThemWithOrQuery(boolean shouldDeferFetch) throws Exception {
        initializeNested();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter = Query.field("entry").oneOfThem().matches(Query.or(Query.field("key").equalsValue("b"),
                    Query.field("value").text().containsPhrase("civil blood makes civil hands unclean")));
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(MAP_DOC)
                    .setFilter(filter)
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planQuery(query);
            Matcher<RecordQueryPlan> matcher = indexScan(allOf(
                    indexName(MAP_AND_FIELD_ON_LUCENE_INDEX.getName()),
                    indexScanType(LuceneScanTypes.BY_LUCENE),
                    scanParams(query(hasToString("entry_key:STRING EQUALS b OR entry_value:TEXT TEXT_CONTAINS_PHRASE civil blood makes civil hands unclean")))
            ));
            if (shouldDeferFetch) {
                matcher = fetch(primaryKeyDistinct(coveringIndexScan(matcher)));
            } else {
                matcher = primaryKeyDistinct(matcher);
            }
            assertThat(plan, matcher);
            try (RecordCursor<FDBQueriedRecord<Message>> recordCursor = recordStore.executeQuery(plan)) {
                List<Long> primaryKeys = recordCursor.map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
                assertEquals(Set.of(0L, 1L, 2L), Set.copyOf(primaryKeys));
            }
        }

    }

    @ParameterizedTest(name = "longFieldQuery[shouldDeferFetch={0}]")
    @BooleanSource
    void longFieldQuery(boolean shouldDeferFetch) throws Exception {
        initializeNested();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(MAP_DOC)
                    .setFilter(Query.field("doc_id").greaterThan(1L))
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planQuery(query);
            Matcher<RecordQueryPlan> matcher = indexScan(allOf(
                    indexScanType(LuceneScanTypes.BY_LUCENE),
                    indexName(MAP_AND_FIELD_ON_LUCENE_INDEX.getName()),
                    scanParams(query(hasToString("doc_id:LONG GREATER_THAN 1")))
            ));
            assertThat(plan, matcher);
            try (RecordCursor<FDBQueriedRecord<Message>> recordCursor = recordStore.executeQuery(plan)) {
                List<Long> primaryKeys = recordCursor.map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
                assertEquals(Set.of(2L), Set.copyOf(primaryKeys));
            }
        }
    }

    @Test
    void covering() throws Exception {
        initializeWithGroup();
        try (FDBRecordContext context = openContext()) {
            openRecordStoreWithGroup(context);
            final QueryComponent filter1 = new LuceneQueryComponent("parents", Lists.newArrayList("text"), true);
            // Query partial full record
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(filter1)
                    .setRequiredResults(List.of(field("group")))
                    .build();
            RecordQueryPlan plan = planQuery(query);
            Matcher<RecordQueryPlan> matcher = coveringIndexScan(indexScan(allOf(
                    indexScanType(LuceneScanTypes.BY_LUCENE),
                    indexName(TEXT_AND_GROUP.getName()),
                    scanParams(query(hasToString("MULTI parents")))
            )));
            assertThat(plan, matcher);
            try (RecordCursor<FDBQueriedRecord<Message>> recordCursor = recordStore.executeQuery(plan)) {
                List<Pair<Long, Long>> results = recordCursor.map(qr -> {
                    long pk = qr.getPrimaryKey().getLong(0);
                    TestRecordsTextProto.SimpleDocument.Builder builder = TestRecordsTextProto.SimpleDocument.newBuilder();
                    builder.mergeFrom(qr.getRecord());
                    long gr = builder.getGroup();
                    return Pair.of(pk, gr);
                }).asList().get();
                assertEquals(Set.of(Pair.of(2L, 0L), Pair.of(4L, 0L), Pair.of(5L, 1L)), Set.copyOf(results));
                assertLoadRecord(0, context);
            }
        }
    }

    @Test
    void fullGroupScan() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, md -> md.removeIndex(MAP_AND_FIELD_ON_LUCENE_INDEX.getName()), SIMPLE_TEXT_SUFFIXES);
            QueryComponent groupFilter = Query.field("entry").oneOfThem().matches(Query.field("key").equalsValue("a"));
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(MAP_DOC)
                    .setFilter(groupFilter)
                    .build();
            RecordQueryPlan plan = planQuery(query);
            Matcher<RecordQueryPlan> matcher = filter(groupFilter, typeFilter(equalTo(Collections.singleton(TextIndexTestUtils.MAP_DOC)), scan(unbounded())));
            assertThat(plan, matcher);
        }

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, md -> {
                md.removeIndex(MAP_AND_FIELD_ON_LUCENE_INDEX.getName());
                md.removeIndex(MAP_ON_LUCENE_INDEX.getName());
                md.addIndex(MAP_DOC, new Index("GroupedMap", new GroupingKeyExpression(concat(field("group"), mainExpression), 1), LuceneIndexTypes.LUCENE));
            }, SIMPLE_TEXT_SUFFIXES);
            QueryComponent groupFilter = Query.and(
                    Query.field("group").equalsValue(1L),
                    Query.field("entry").oneOfThem().matches(Query.field("key").equalsValue("a")));
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(MAP_DOC)
                    .setFilter(groupFilter)
                    .build();
            RecordQueryPlan plan = planQuery(query);
            Matcher<RecordQueryPlan> matcher = filter(groupFilter, typeFilter(equalTo(Collections.singleton(TextIndexTestUtils.MAP_DOC)), scan(unbounded())));
            assertThat(plan, matcher);
        }
    }

    @Test
    void andNot() throws Exception {
        initializeFlat();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter1 = new LuceneQueryComponent("Verona", Lists.newArrayList("text"), true);
            final QueryComponent filter2 = new LuceneQueryComponent("traffic", Lists.newArrayList("text"), true);
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(Query.and(filter1, Query.not(filter2)))
                    .build();
            RecordQueryPlan plan = planQuery(query);
            Matcher<RecordQueryPlan> matcher = indexScan(allOf(indexScan(SIMPLE_TEXT_SUFFIXES.getName()),
                    indexScanType(LuceneScanTypes.BY_LUCENE),
                    scanParams(query(hasToString("MULTI Verona AND NOT MULTI traffic")))));
            assertThat(plan, matcher);
            assertThat(getLuceneQuery(plan), Matchers.hasToString("+(text:verona) -(text:traffic)"));
            try (RecordCursor<FDBQueriedRecord<Message>> fdbQueriedRecordRecordCursor = recordStore.executeQuery(plan)) {
                RecordCursor<Tuple> map = fdbQueriedRecordRecordCursor.map(FDBQueriedRecord::getPrimaryKey);
                List<Long> primaryKeys = map.map(t -> t.getLong(0)).asList().get();
                assertEquals(Set.of(2L), Set.copyOf(primaryKeys));
            }
        }
    }

    @Test
    void justNot() throws Exception {
        initializeFlat();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter = new LuceneQueryComponent("Verona", Lists.newArrayList("text"), true);
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(Query.not(filter))
                    .build();
            RecordQueryPlan plan = planQuery(query);
            Matcher<RecordQueryPlan> matcher = indexScan(allOf(indexScan(SIMPLE_TEXT_SUFFIXES.getName()),
                    indexScanType(LuceneScanTypes.BY_LUCENE),
                    scanParams(query(hasToString("NOT MULTI Verona")))));
            assertThat(plan, matcher);
            assertThat(getLuceneQuery(plan), Matchers.hasToString("+*:* -(text:verona)"));
            try (RecordCursor<FDBQueriedRecord<Message>> fdbQueriedRecordRecordCursor = recordStore.executeQuery(plan)) {
                RecordCursor<Tuple> map = fdbQueriedRecordRecordCursor.map(FDBQueriedRecord::getPrimaryKey);
                List<Long> primaryKeys = map.map(t -> t.getLong(0)).asList().get();
                assertEquals(Set.of(0L, 1L, 3L, 5L), Set.copyOf(primaryKeys));
            }
        }
    }

    @Test
    void notOr() throws Exception {
        initializeFlat();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter1 = new LuceneQueryComponent("Verona", Lists.newArrayList("text"), true);
            final QueryComponent filter2 = new LuceneQueryComponent("traffic", Lists.newArrayList("text"), true);
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(Query.not(Query.or(filter1, filter2)))
                    .build();
            RecordQueryPlan plan = planQuery(query);
            Matcher<RecordQueryPlan> matcher = indexScan(allOf(indexScan(SIMPLE_TEXT_SUFFIXES.getName()),
                    indexScanType(LuceneScanTypes.BY_LUCENE),
                    scanParams(query(hasToString("NOT MULTI Verona AND NOT MULTI traffic")))));
            assertThat(plan, matcher);
            assertThat(getLuceneQuery(plan), Matchers.hasToString("+*:* -(text:verona) -(text:traffic)"));
            try (RecordCursor<FDBQueriedRecord<Message>> fdbQueriedRecordRecordCursor = recordStore.executeQuery(plan)) {
                RecordCursor<Tuple> map = fdbQueriedRecordRecordCursor.map(FDBQueriedRecord::getPrimaryKey);
                List<Long> primaryKeys = map.map(t -> t.getLong(0)).asList().get();
                assertEquals(Set.of(0L, 1L, 3L), Set.copyOf(primaryKeys));
            }
        }
    }

    private org.apache.lucene.search.Query getLuceneQuery(RecordQueryPlan plan) {
        LuceneIndexQueryPlan indexPlan = (LuceneIndexQueryPlan)plan;
        LuceneScanQuery scan = (LuceneScanQuery)indexPlan.getScanParameters().bind(recordStore, recordStore.getRecordMetaData().getIndex(indexPlan.getIndexName()), EvaluationContext.EMPTY);
        return scan.getQuery();
    }

    @Test
    void sortByPrimaryKey() throws Exception {
        initializeFlat();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter1 = new LuceneQueryComponent("parents", Lists.newArrayList("text"), true);
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(filter1)
                    .setSort(field("doc_id"), true)
                    .build();
            RecordQueryPlan plan = planQuery(query);
            try (RecordCursor<FDBQueriedRecord<Message>> recordCursor = recordStore.executeQuery(plan)) {
                List<Long> primaryKeys = recordCursor.map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
                assertEquals(List.of(5L, 4L, 2L), primaryKeys);
            }
        }
    }

    @Test
    void sortByGroupedPrimaryKey() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStoreWithComplex(context);
            complexDocuments.forEach(recordStore::saveRecord);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openRecordStoreWithComplex(context);
            final QueryComponent filter1 = new LuceneQueryComponent("parents", Lists.newArrayList("text"), true);
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.COMPLEX_DOC)
                    .setFilter(Query.and(Query.field("group").equalsValue(0L), filter1))
                    .setSort(field("doc_id"), true)
                    .build();
            RecordQueryPlan plan = planQuery(query);
            try (RecordCursor<FDBQueriedRecord<Message>> recordCursor = recordStore.executeQuery(plan)) {
                List<Long> primaryKeys = recordCursor.map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(1)).asList().get();
                assertEquals(List.of(2L, 0L), primaryKeys);
            }
        }
    }

    @ParameterizedTest(name = "unionSearches[sorted={0}]")
    @BooleanSource
    void unionSearches(boolean sorted) throws Exception {
        initializeFlat();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter1 = new LuceneQueryComponent("parents", Lists.newArrayList("text"), true);
            final QueryComponent filter2 = new LuceneQueryComponent("king", Lists.newArrayList("text"), true);
            RecordQuery.Builder query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(Query.or(filter1, filter2));
            if (sorted) {
                query.setSort(field("doc_id"));
            }
            RecordQueryPlan plan = planQuery(query.build());
            Matcher<RecordQueryPlan> matcher1 = indexScan(allOf(indexScanType(LuceneScanTypes.BY_LUCENE),
                    indexName(SIMPLE_TEXT_SUFFIXES.getName()),
                    scanParams(query(hasToString("MULTI parents")))));
            Matcher<RecordQueryPlan> matcher2 = indexScan(allOf(indexScanType(LuceneScanTypes.BY_LUCENE),
                    indexName(SIMPLE_TEXT_SUFFIXES.getName()),
                    scanParams(query(hasToString("MULTI king")))));
            if (sorted) {
                assertThat(plan, union(matcher1, matcher2));
            } else {
                assertThat(plan, primaryKeyDistinct(unorderedUnion(matcher1, matcher2)));
            }
            try (RecordCursor<FDBQueriedRecord<Message>> recordCursor = recordStore.executeQuery(plan)) {
                List<Long> primaryKeys = recordCursor.map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
                if (sorted) {
                    assertEquals(List.of(1L, 2L, 4L, 5L), primaryKeys);
                } else {
                    assertEquals(Set.of(1L, 2L, 4L, 5L), new HashSet<>(primaryKeys));
                }
            }
        }
    }

    @ParameterizedTest(name = "continuations[sorted={0}]")
    @BooleanSource
    void continuations(boolean sorted) throws Exception {
        initializeFlat();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter1 = new LuceneQueryComponent("parents", Lists.newArrayList("text"), true);
            RecordQuery.Builder query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(filter1);
            if (sorted) {
                query.setSort(field("doc_id"));
            }
            RecordQueryPlan plan = planQuery(query.build());
            ExecuteProperties executeProperties = ExecuteProperties.newBuilder().setReturnedRowLimit(2).build();
            List<Long> primaryKeys = new ArrayList<>();
            byte[] continuation = null;
            AtomicReference<RecordCursorResult<Long>> holder = new AtomicReference<>();
            do {
                try (RecordCursor<FDBQueriedRecord<Message>> recordCursor = recordStore.executeQuery(plan, continuation, executeProperties)) {
                    primaryKeys.addAll(recordCursor.map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList(holder).get());
                    continuation = holder.get().getContinuation().toBytes();
                }
            } while (continuation != null);
            if (sorted) {
                assertEquals(List.of(2L, 4L, 5L), primaryKeys);
            } else {
                assertEquals(Set.of(2L, 4L, 5L), new HashSet<>(primaryKeys));
            }
        }
    }

    /**
     * This test is reproducing a thread deadlock that happened with Lucene threads when reading schema information.
     * The fix for the deadlock has removed the {@code Map.computeIfAbsent} call (with the {@code synchronized} block)
     * that blocked the {@code asyncToSync} calls.
     * This test creates a large enough set of {@link org.apache.lucene.index.SegmentReader} futures so as to flood the thread pool,
     * and then queries Lucene. The large number of futures in the pool, together with the {@code synchronized} block
     * would cause a deadlock.
     */
    @Test
    void testQueryWithManyDocuments() {
        // Since the test tries to create many segments (each one opens in its own thread), we need to use random data
        // (random words) rather than using random English words from a canned text. With English text, Lucene compression
        // reduces the size of the segment such that we need many more records to create the required number of segments
        final FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        // limit the FJP size to try and force the # segments to exceed the # threads
        factory.setExecutor(new ForkJoinPool(PARALLELISM,
                ForkJoinPool.defaultForkJoinWorkerThreadFactory,
                null, false));
        fdb.setAsyncToSyncTimeout(event -> {
            // Make AsyncToSync calls timeout after one second, otherwise a deadlock would just result in the test taking forever
            return Duration.ofSeconds(1L);
        });
        // Save many records (create many segments)
        for (long i = 0; i < 20; i++) {
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context);
                for (int j = 0; j < 10; j++) {
                    TestRecordsTextProto.SimpleDocument document1 = TestRecordsTextProto.SimpleDocument.newBuilder()
                            .setDocId(i * 1000 + j)
                            .setGroup(1)
                            .setText(LuceneIndexTestUtils.generateRandomWords(1000)[1])
                            .build();
                    recordStore.saveRecord(document1);
                }
                commit(context);
            }
        }
        // Run a query
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            // Random words are up to 10 characters long, so this would never match
            assertPrimaryKeys("text:morningstart", false, Set.of());
        }
    }
}
