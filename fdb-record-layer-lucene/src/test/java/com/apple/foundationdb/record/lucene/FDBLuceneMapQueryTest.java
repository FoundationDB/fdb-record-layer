/*
 * FDBLuceneMapQueryTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.lucene.synonym.EnglishSynonymMapConfig;
import com.apple.foundationdb.record.lucene.synonym.SynonymMapRegistryImpl;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.text.TextSamples;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.test.Tags;
import com.google.common.collect.Sets;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.SIMPLE_TEXT_SUFFIXES;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.metadata.Key.Expressions.value;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Queries that involve maps of non-text fields.
 */
@Tag(Tags.RequiresFDB)
public class FDBLuceneMapQueryTest extends FDBRecordStoreQueryTestBase {
    private static final String MAP_DOC = "MapDocument";

    private static final KeyExpression mapString2LongIndexExpression = field("stringToLongMap").nest(
            function(LuceneFunctionNames.LUCENE_FIELD_NAME,
                    concat(
                            field("values", KeyExpression.FanType.FanOut).nest(
                                    function(LuceneFunctionNames.LUCENE_FIELD_NAME,
                                            concat(
                                                    field("value"),
                                                    field("key")))),
                            value(null))));
    private static final KeyExpression mapStringWrapper2LongIndexExpression = field("stringWrapperToLongMap").nest(
            function(LuceneFunctionNames.LUCENE_FIELD_NAME,
                    concat(
                            field("values", KeyExpression.FanType.FanOut).nest(
                                    function(LuceneFunctionNames.LUCENE_FIELD_NAME,
                                            concat(
                                                    field("value"),
                                                    field("key").nest("value")))),
                            value(null))));
    private static final KeyExpression mapString2IntIndexExpression = field("stringToIntMap").nest(
            function(LuceneFunctionNames.LUCENE_FIELD_NAME,
                    concat(
                            field("values", KeyExpression.FanType.FanOut).nest(
                                    function(LuceneFunctionNames.LUCENE_FIELD_NAME,
                                            concat(
                                                    field("value"),
                                                    field("key")))),
                            value(null))));
    private static final KeyExpression mapString2DoubleIndexExpression = field("stringToDoubleMap").nest(
            function(LuceneFunctionNames.LUCENE_FIELD_NAME,
                    concat(
                            field("values", KeyExpression.FanType.FanOut).nest(
                                    function(LuceneFunctionNames.LUCENE_FIELD_NAME,
                                            concat(
                                                    field("value"),
                                                    field("key")))),
                            value(null))));

    private static final Index MAP_STRING_2_LONG_LUCENE_INDEX = new Index("MapField$string2long", concat(mapString2LongIndexExpression, field("doc_id")), LuceneIndexTypes.LUCENE);
    private static final Index MAP_STRING_WRAPPER_TO_LONG_LUCENE_INDEX = new Index("MapField$stringWrapper2long", concat(mapStringWrapper2LongIndexExpression, field("doc_id")), LuceneIndexTypes.LUCENE);
    private static final Index MAP_STRING_2_INT_LUCENE_INDEX = new Index("MapField$string2int", concat(mapString2IntIndexExpression, field("doc_id")), LuceneIndexTypes.LUCENE);
    private static final Index MAP_STRING_2_DOUBLE_LUCENE_INDEX = new Index("MapField$string2double", concat(mapString2DoubleIndexExpression, field("doc_id")), LuceneIndexTypes.LUCENE);

    @BeforeAll
    public static void setup() {
        //set up the English Synonym Map so that we don't spend forever setting it up for every test, because this takes a long time
        SynonymMapRegistryImpl.instance().getSynonymMap(EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig.CONFIG_NAME);
    }

    private static List<String> textSamples = Arrays.asList(
            TextSamples.ROMEO_AND_JULIET_PROLOGUE,
            TextSamples.AETHELRED,
            TextSamples.ROMEO_AND_JULIET_PROLOGUE,
            TextSamples.ANGSTROM,
            TextSamples.AETHELRED,
            TextSamples.FRENCH
    );

    private static final List<TestRecordsTextProto.MapDocument> mapDocuments = createMapDocuments();

    private ExecutorService executorService = null;

    @Override
    public void setupPlanner(@Nullable PlannableIndexTypes indexTypes) {
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

    @Override
    protected RecordLayerPropertyStorage.Builder addDefaultProps(final RecordLayerPropertyStorage.Builder props) {
        return super.addDefaultProps(props)
                .addProp(LuceneRecordContextProperties.LUCENE_EXECUTOR_SERVICE, (Supplier<ExecutorService>)() -> executorService);
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
        metaDataBuilder.addIndex(MAP_DOC, MAP_STRING_2_LONG_LUCENE_INDEX);
        metaDataBuilder.addIndex(MAP_DOC, MAP_STRING_WRAPPER_TO_LONG_LUCENE_INDEX);
        metaDataBuilder.addIndex(MAP_DOC, MAP_STRING_2_INT_LUCENE_INDEX);
        metaDataBuilder.addIndex(MAP_DOC, MAP_STRING_2_DOUBLE_LUCENE_INDEX);
        hook.apply(metaDataBuilder);
        recordStore = getStoreBuilder(context, metaDataBuilder.getRecordMetaData())
                .setSerializer(TextIndexTestUtils.COMPRESSING_SERIALIZER)
                .createOrOpen();
        setupPlanner(null);
    }

    private void initializeNested() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            mapDocuments.forEach(recordStore::saveRecord);
            commit(context);
        }
    }

    /**
     * Arguments function: construct argument list for queries with the predicate value in the query.
     *
     * @return a stream of Arguments, each consisting of a query filter a boolean and an expected index name, where
     * the boolean indicates whether the query should match a document or not.
     */
    public static Stream<Arguments> valueQueryParameters() {
        return Stream.of(
                Stream.of(Pair.of("d", true),
                                Pair.of("Blah", false),
                                Pair.of("c", false))
                        .map(pair ->
                                Arguments.of(Query.field("stringToLongMap").matches(Query.field("values").oneOfThem().matches(Query.field("key").equalsValue(pair.getLeft()))),
                                        pair.getRight(),
                                        "MapField$string2long")),
                Stream.of(Pair.of("c", true),
                                Pair.of("Blah", false),
                                Pair.of("d", false))
                        .map(pair ->
                                Arguments.of(Query.field("stringWrapperToLongMap").matches(Query.field("values").oneOfThem().matches(Query.field("key").matches(Query.field("value").equalsValue(pair.getLeft())))),
                                        pair.getRight(),
                                        "MapField$stringWrapper2long")),
                Stream.of(Pair.of("f", true),
                                Pair.of("Blah", false),
                                Pair.of("d", false))
                        .map(pair ->
                                Arguments.of(Query.field("stringToIntMap").matches(Query.field("values").oneOfThem().matches(Query.field("key").equalsValue(pair.getLeft()))),
                                        pair.getRight(),
                                        "MapField$string2int")),
                Stream.of(Pair.of("g", true),
                                Pair.of("Blah", false),
                                Pair.of("a", false))
                        .map(pair ->
                                Arguments.of(Query.field("stringToDoubleMap").matches(Query.field("values").oneOfThem().matches(Query.field("key").equalsValue(pair.getLeft()))),
                                        pair.getRight(),
                                        "MapField$string2double"))
        ).flatMap(Function.identity());
    }

    /**
     * Arguments function: construct argument list for queries with the predicate value as a parameter.
     *
     * @return a stream of Arguments, each consisting of query filter, a boolean a value to compare and the
     * expected index, where the boolean indicates whether the query should match a document or not.
     */
    public static Stream<Arguments> parameterQueryParameters() {
        return Stream.of(
                Stream.of(Pair.of("d", true),
                                Pair.of("Blah", false),
                                Pair.of("a", false))
                        .map(pair ->
                                Arguments.of(Query.field("stringToLongMap").matches(Query.field("values").oneOfThem().matches(Query.field("key").equalsParameter("$param"))),
                                        pair.getRight(),
                                        pair.getLeft(),
                                        "MapField$string2long")),
                Stream.of(Pair.of("f", true),
                                Pair.of("Blah", false),
                                Pair.of("d", false))
                        .map(pair ->
                                Arguments.of(Query.field("stringToIntMap").matches(Query.field("values").oneOfThem().matches(Query.field("key").equalsParameter("$param"))),
                                        pair.getRight(),
                                        pair.getLeft(),
                                        "MapField$string2int")),
                Stream.of(Pair.of("g", true),
                                Pair.of("Blah", false),
                                Pair.of("a", false))
                        .map(pair ->
                                Arguments.of(Query.field("stringToDoubleMap").matches(Query.field("values").oneOfThem().matches(Query.field("key").equalsParameter("$param"))),
                                        pair.getRight(),
                                        pair.getLeft(),
                                        "MapField$string2double"))
        ).flatMap(Function.identity());
    }

    @MethodSource("valueQueryParameters")
    @ParameterizedTest
    void mapQueryWithEmbeddedValue(QueryComponent filter, boolean found, String expectedIndex) throws Exception {
        initializeNested();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);

            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(MAP_DOC)
                    .setFilter(filter)
                    .build();
            RecordQueryPlan plan = planQuery(query);
            assertTrue(plan.getUsedIndexes().contains(expectedIndex));
            try (RecordCursor<FDBQueriedRecord<Message>> recordCursor = recordStore.executeQuery(plan)) {
                List<Long> primaryKeys = recordCursor.map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
                final Set<Long> expected = found ? Set.of(0L, 1L, 2L) : Set.of();
                assertEquals(expected, Set.copyOf(primaryKeys));
            }
        }
    }

    @MethodSource("parameterQueryParameters")
    @ParameterizedTest
    void mapQueryWithParameterizedValue(QueryComponent filter, boolean found, String value, String expectedIndex) throws Exception {
        initializeNested();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);

            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(MAP_DOC)
                    .setFilter(filter)
                    .build();
            RecordQueryPlan plan = planQuery(query);
            assertTrue(plan.getUsedIndexes().contains(expectedIndex));
            try (RecordCursor<QueryResult> recordCursor = recordStore.executeQuery(plan, null, EvaluationContext.forBinding("$param", value), ExecuteProperties.SERIAL_EXECUTE)) {
                List<Long> primaryKeys = recordCursor.map(QueryResult::getQueriedRecord).map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
                final Set<Long> expected = found ? Set.of(0L, 1L, 2L) : Set.of();
                assertEquals(expected, Set.copyOf(primaryKeys));
            }
        }
    }

    @Test
    void mapStringToLongValueSearch() throws Exception {
        initializeNested();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);

            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(MAP_DOC)
                    .setFilter(Query.field("stringToLongMap").matches(Query.field("values").oneOfThem().matches(Query.field("value").equalsValue(1L))))
                    .build();
            RecordQueryPlan plan = planQuery(query);
            assertTrue(plan.getUsedIndexes().isEmpty());
            try (RecordCursor<FDBQueriedRecord<Message>> recordCursor = recordStore.executeQuery(plan)) {
                List<Long> primaryKeys = recordCursor.map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
                final Set<Long> expected = Set.of(1L);
                assertEquals(expected, Set.copyOf(primaryKeys));
            }
        }
    }


    @Nonnull
    private static List<TestRecordsTextProto.MapDocument> createMapDocuments() {
        List<TestRecordsTextProto.MapDocument> result = IntStream.range(0, textSamples.size() / 2)
                .mapToObj(i -> TestRecordsTextProto.MapDocument.newBuilder()
                        .setDocId(i)
                        .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("a").setValue(textSamples.get(i * 2)))
                        .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("b").setValue(textSamples.get(i * 2 + 1)))
                        .setStringToLongMap(getStringToLongMap(i, "d"))
                        .setStringWrapperToLongMap(getStringWrapperToLongMap(i, "c"))
                        .setStringToIntMap(getStringToIntMap(i, "f"))
                        .setStringToDoubleMap(getStringToDoubleMap(i, "g"))
                        .setGroup(i % 2)
                        .build()
                )
                .collect(Collectors.toList());
        // add a document with entries that do not match
        result.add(TestRecordsTextProto.MapDocument.newBuilder()
                .setDocId(1000)
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("a").setValue(textSamples.get(0)))
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("b").setValue(textSamples.get(1)))
                .setStringToLongMap(getStringToLongMap(1000, "NOT_FOUND"))
                .setStringWrapperToLongMap(getStringWrapperToLongMap(1000, "NOT_FOUND"))
                .setStringToIntMap(getStringToIntMap(1000, "NOT_FOUND"))
                .setStringToDoubleMap(getStringToDoubleMap(1000, "NOT_FOUND"))
                .setGroup(1000)
                .build());
        // add a document with no entries at all
        result.add(TestRecordsTextProto.MapDocument.newBuilder()
                .setDocId(1001)
                .build());

        return result;
    }

    @Nonnull
    private static TestRecordsTextProto.MapDocument.String2Long.Builder getStringToLongMap(final int i, final String value) {
        TestRecordsTextProto.MapDocument.String2Long.Builder builder = TestRecordsTextProto.MapDocument.String2Long.newBuilder()
                .addValues(TestRecordsTextProto.MapDocument.String2LongPair.newBuilder()
                        .setKey(value)
                        .setValue(i));
        if ((i % 2) == 0) {
            builder.addValues(TestRecordsTextProto.MapDocument.String2LongPair.newBuilder()
                    .setKey("X")
                    .setValue(i + 10));
        }
        return builder;
    }

    @Nonnull
    private static TestRecordsTextProto.MapDocument.StringWrapper2Long.Builder getStringWrapperToLongMap(final int i, final String value) {
        TestRecordsTextProto.MapDocument.StringWrapper2Long.Builder builder = TestRecordsTextProto.MapDocument.StringWrapper2Long.newBuilder()
                .addValues(TestRecordsTextProto.MapDocument.StringWrapper2LongPair.newBuilder()
                        .setKey(TestRecordsTextProto.MapDocument.StringWrapper.newBuilder()
                                .setValue(value)
                                .setFlags(5))
                        .setValue(i));
        if ((i % 2) == 0) {
            builder.addValues(TestRecordsTextProto.MapDocument.StringWrapper2LongPair.newBuilder()
                    .setKey(TestRecordsTextProto.MapDocument.StringWrapper.newBuilder()
                            .setValue("X")
                            .setFlags(5))
                    .setValue(i + 10));
        }
        return builder;
    }

    @Nonnull
    private static TestRecordsTextProto.MapDocument.String2Int.Builder getStringToIntMap(final int i, final String value) {
        TestRecordsTextProto.MapDocument.String2Int.Builder builder = TestRecordsTextProto.MapDocument.String2Int.newBuilder()
                .addValues(TestRecordsTextProto.MapDocument.String2IntPair.newBuilder()
                        .setKey(value)
                        .setValue(i));
        if ((i % 2) == 0) {
            builder.addValues(TestRecordsTextProto.MapDocument.String2IntPair.newBuilder()
                    .setKey("X")
                    .setValue(i + 10));
        }
        return builder;
    }


    @Nonnull
    private static TestRecordsTextProto.MapDocument.String2Double.Builder getStringToDoubleMap(final int i, final String value) {
        TestRecordsTextProto.MapDocument.String2Double.Builder builder = TestRecordsTextProto.MapDocument.String2Double.newBuilder()
                .addValues(TestRecordsTextProto.MapDocument.String2DoublePair.newBuilder()
                        .setKey(value)
                        .setValue(i + 7.8));
        if ((i % 2) == 0) {
            builder.addValues(TestRecordsTextProto.MapDocument.String2DoublePair.newBuilder()
                    .setKey("X")
                    .setValue(i + 17.8));
        }
        return builder;
    }
}
