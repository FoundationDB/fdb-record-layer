/*
 * FDBLuceneQueryTest.java
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

import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.text.AllSuffixesTextTokenizer;
import com.apple.foundationdb.record.provider.common.text.TextSamples;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.LuceneQueryComponent;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.temp.CascadesPlanner;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.Message;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.record.TestHelpers.assertLoadRecord;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
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
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.union;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.unorderedUnion;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Sophisticated queries involving full text predicates.
 */
@Tag(Tags.RequiresFDB)
public class FDBLuceneQueryTest extends FDBRecordStoreQueryTestBase {
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

    private List<TestRecordsTextProto.MapDocument> mapDocuments = IntStream.range(0, textSamples.size() / 2)
            .mapToObj(i -> TestRecordsTextProto.MapDocument.newBuilder()
                    .setDocId(i)
                    .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("a").setValue(textSamples.get(i * 2)).build())
                    .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("b").setValue(textSamples.get(i * 2 + 1)).build())
                    .setGroup(i % 2)
                    .build()
            )
            .collect(Collectors.toList());


    private List<TestRecordsTextProto.MapDocument> mapWithFieldDocuments = IntStream.range(0, textSamples.size() / 2)
            .mapToObj(i -> TestRecordsTextProto.MapDocument.newBuilder()
                    .setDocId(i)
                    .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("a").setValue(textSamples.get(i * 2)).build())
                    .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder().setKey("b").setValue(textSamples.get(i * 2 + 1)).build())
                    .setGroup(i % 2)
                    .build()
            )
            .collect(Collectors.toList());

    private static final String MAP_DOC = "MapDocument";

    private static final Index SIMPLE_TEXT_SUFFIXES = new Index("Complex$text_index", new LuceneFieldKeyExpression("text", LuceneKeyExpression.FieldType.STRING, false, false), IndexTypes.LUCENE,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME));
    private static final List<KeyExpression> keys = Lists.newArrayList(
            new LuceneFieldKeyExpression("key", KeyExpression.FanType.FanOut, Key.Evaluated.NullStandin.NULL,
                    LuceneKeyExpression.FieldType.STRING_KEY_MAP, false, false),
            new LuceneFieldKeyExpression("value", LuceneKeyExpression.FieldType.STRING, false, false));

    private static final LuceneThenKeyExpression mainExpression = new LuceneThenKeyExpression((LuceneFieldKeyExpression) keys.get(0), keys);

    // TODO: This is not a legal index: GroupingKeyExpression cannot be a child of a ThenKeyExpression.
    private static final Index MAP_AND_FIELD_ON_LUCENE_INDEX = new Index("MapField$values", new LuceneThenKeyExpression( null, Lists.newArrayList(new GroupingKeyExpression(field("entry", KeyExpression.FanType.FanOut).nest(mainExpression), 1),
            new LuceneFieldKeyExpression("doc_id",KeyExpression.FanType.None, Key.Evaluated.NullStandin.NULL,
                                                             LuceneKeyExpression.FieldType.LONG, false, false))), IndexTypes.LUCENE);

    private static final Index MAP_ON_LUCENE_INDEX = new Index("Map$entry-value", new GroupingKeyExpression(field("entry", KeyExpression.FanType.FanOut).nest(
            mainExpression), 1), IndexTypes.LUCENE);

    @Override
    public void setupPlanner(@Nullable PlannableIndexTypes indexTypes) {
        if (useRewritePlanner) {
            planner = new CascadesPlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState());
        } else {
            if (indexTypes == null) {
                indexTypes = new PlannableIndexTypes(
                        Sets.newHashSet(IndexTypes.VALUE, IndexTypes.VERSION),
                        Sets.newHashSet(IndexTypes.RANK, IndexTypes.TIME_WINDOW_LEADERBOARD),
                        Sets.newHashSet(IndexTypes.TEXT),
                        Sets.newHashSet(IndexTypes.LUCENE)
                );
            }
            planner = new LucenePlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState(), indexTypes, recordStore.getTimer());
        }
    }

    protected void openRecordStore(FDBRecordContext context) {
        openRecordStore(context, store -> { });
    }

    protected void openRecordStore(FDBRecordContext context, RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsTextProto.getDescriptor());
        metaDataBuilder.getRecordType(TextIndexTestUtils.COMPLEX_DOC).setPrimaryKey(concatenateFields("group", "doc_id"));
        metaDataBuilder.removeIndex("SimpleDocument$text");
        metaDataBuilder.addIndex(TextIndexTestUtils.SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
        metaDataBuilder.addIndex(MAP_DOC, MAP_ON_LUCENE_INDEX);
        metaDataBuilder.addIndex(MAP_DOC, MAP_AND_FIELD_ON_LUCENE_INDEX);
        hook.apply(metaDataBuilder);
        recordStore = getStoreBuilder(context, metaDataBuilder.getRecordMetaData())
                .setSerializer(TextIndexTestUtils.COMPRESSING_SERIALIZER)
                .uncheckedOpen();
        setupPlanner(null);
    }

    private void initializeFlat() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
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

    @ParameterizedTest
    @BooleanSource
    public void simpleLuceneScans(boolean shouldDeferFetch) throws Exception {
        initializeFlat();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter1 = new LuceneQueryComponent("civil blood makes civil hands unclean", Lists.newArrayList());
            // Query for full records
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(filter1)
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            Matcher<RecordQueryPlan> matcher = indexScan(allOf(indexScan("Complex$text_index"),
                    indexScanType(IndexScanType.BY_LUCENE),
                    bounds(hasTupleString("[[civil blood makes civil hands unclean],[civil blood makes civil hands unclean]]"))));
            RecordQueryPlan plan = planner.plan(query);
            //assertThat(plan, matcher);
            RecordCursor<FDBQueriedRecord<Message>> fdbQueriedRecordRecordCursor = recordStore.executeQuery(plan);
            RecordCursor<Tuple> map = fdbQueriedRecordRecordCursor.map(FDBQueriedRecord::getPrimaryKey);
            List<Long> primaryKeys = map.map(t -> t.getLong(0)).asList().get();
            assertEquals(ImmutableSet.of(2L, 4L), ImmutableSet.copyOf(primaryKeys));
        }
    }

    @ParameterizedTest
    @BooleanSource
    public void testThenExpressionBeforeFieldExpression(boolean shouldDeferFetch) throws Exception {
        initializeNestedWithField();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter1 = new LuceneQueryComponent("*:*", Lists.newArrayList("doc_id"));
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(MAP_DOC)
                    .setFilter(filter1)
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planner.plan(query);
            RecordCursor<FDBQueriedRecord<Message>> fdbQueriedRecordRecordCursor = recordStore.executeQuery(plan);
            RecordCursor<Tuple> map = fdbQueriedRecordRecordCursor.map(FDBQueriedRecord::getPrimaryKey);
            List<Long> primaryKeys = map.map(t -> t.getLong(0)).asList().get();
            assertEquals(ImmutableSet.of(0L, 1L, 2L), ImmutableSet.copyOf(primaryKeys));
        }

    }

    @ParameterizedTest
    @BooleanSource
    public void simpleLuceneScansDocId(boolean shouldDeferFetch) throws Exception {
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
            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, matcher);
            List<Long> primaryKeys = recordStore.executeQuery(plan).map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
            assertEquals(ImmutableSet.of(1L), ImmutableSet.copyOf(primaryKeys));
        }
    }

    @ParameterizedTest
    @BooleanSource
    public void delayFetchOnOrOfLuceneScanWithFieldFilter(boolean shouldDeferFetch) throws Exception {
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
            RecordQueryPlan plan = planner.plan(query);
            Matcher<RecordQueryPlan> matcher = primaryKeyDistinct(
                    unorderedUnion(
                            indexScan(allOf(indexScan("Complex$text_index"),
                                    indexScanType(IndexScanType.BY_LUCENE),
                                    bounds(hasTupleString("[[civil blood makes civil hands unclean],[civil blood makes civil hands unclean]]")))),
                            typeFilter(equalTo(Collections.singleton(TextIndexTestUtils.SIMPLE_DOC)),
                                    scan(bounds(hasTupleString("([null],[10000])"))))
                    ));
            assertThat(plan, matcher);
            List<Long> primaryKeys = recordStore.executeQuery(plan).map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
            assertEquals(ImmutableSet.of(2L, 4L, 0L, 1L, 3L, 5L), ImmutableSet.copyOf(primaryKeys));
            if (shouldDeferFetch) {
                assertLoadRecord(5, context);
            } else {
                assertLoadRecord(6, context);
            }
        }
    }

    @ParameterizedTest
    @BooleanSource
    public void delayFetchOnLuceneFilterWithSort(boolean shouldDeferFetch) throws Exception {
        initializeFlat();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter1 = new LuceneQueryComponent("civil blood makes civil hands unclean", Lists.newArrayList("text"), true);
            // Query for full records
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(filter1)
                    .setSort(field("doc_id"))
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planner.plan(query);
            List<Long> primaryKeys = recordStore.executeQuery(plan).map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
            assertEquals(ImmutableSet.of(2L, 4L), ImmutableSet.copyOf(primaryKeys));
            if (shouldDeferFetch) {
                assertLoadRecord(5, context);
            } else {
                assertLoadRecord(6, context);
            }
        }
    }

    @ParameterizedTest
    @BooleanSource
    public void delayFetchOnAndOfLuceneAndFieldFilter(boolean shouldDeferFetch) throws Exception {
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
            Matcher<RecordQueryPlan> scanMatcher = fetch(filter(filter2, coveringIndexScan(indexScan(allOf(indexScanType(IndexScanType.BY_LUCENE_FULL_TEXT), indexScan("Complex$text_index"),
                    bounds(hasTupleString("[[civil blood makes civil hands unclean],[civil blood makes civil hands unclean]]")))))));
            assertThat(plan, scanMatcher);
            RecordCursor<FDBQueriedRecord<Message>> primaryKeys;
            primaryKeys = recordStore.executeQuery(plan);
            final List<Long> keys = primaryKeys.map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
            assertEquals(ImmutableSet.of(2L), ImmutableSet.copyOf(keys));
            if (shouldDeferFetch) {
                assertLoadRecord(3, context);
            } else {
                assertLoadRecord(4, context);
            }
        }
    }

    @ParameterizedTest
    @BooleanSource
    public void delayFetchOnOrOfLuceneFiltersGivesUnion(boolean shouldDeferFetch) throws Exception {
        initializeFlat();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter1 = new LuceneQueryComponent("(civil blood makes civil hands unclean)", Lists.newArrayList("text"), true);
            final QueryComponent filter2 = new LuceneQueryComponent("(was king from 966 to 1016)", Lists.newArrayList());
            // Query for full records
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(Query.or(filter1, filter2))
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planner.plan(query);
            Matcher<RecordQueryPlan> matcher = union(
                    indexScan(allOf(indexScan("Complex$text_index"),
                            indexScanType(IndexScanType.BY_LUCENE_FULL_TEXT),
                            bounds(hasTupleString("[[(civil blood makes civil hands unclean)],[(civil blood makes civil hands unclean)]]")))),
                    indexScan(allOf(indexScan("Complex$text_index"),
                            indexScanType(IndexScanType.BY_LUCENE_FULL_TEXT),
                            bounds(hasTupleString("[[(was king from 966 to 1016)],[(was king from 966 to 1016)]]")))),
                    equalTo(field("doc_id")));
            if (shouldDeferFetch) {
                matcher = fetch(union(
                        coveringIndexScan(indexScan(allOf(indexScan("Complex$text_index"),
                                indexScanType(IndexScanType.BY_LUCENE_FULL_TEXT),
                                bounds(hasTupleString("[[(civil blood makes civil hands unclean)],[(civil blood makes civil hands unclean)]]"))))),
                        coveringIndexScan(indexScan(allOf(indexScan("Complex$text_index"),
                                indexScanType(IndexScanType.BY_LUCENE_FULL_TEXT),
                                bounds(hasTupleString("[[(was king from 966 to 1016)],[(was king from 966 to 1016)]]"))))),
                        equalTo(field("doc_id"))));
            }
            assertThat(plan, matcher);
            List<Long> primaryKeys = recordStore.executeQuery(plan).map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
            assertEquals(ImmutableSet.of(1L, 2L, 4L), ImmutableSet.copyOf(primaryKeys));
            if (shouldDeferFetch) {
                assertLoadRecord(5, context);
            } else {
                assertLoadRecord(6, context);
            }
        }
    }

    @ParameterizedTest
    @BooleanSource
    public void delayFetchOnAndOfLuceneFilters(boolean shouldDeferFetch) throws Exception {
        initializeFlat();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter1 = new LuceneQueryComponent("the continuance", Lists.newArrayList());
            final QueryComponent filter2 = new LuceneQueryComponent("grudge", Lists.newArrayList());
            // Query for full records
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(Query.and(filter1, filter2))
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planner.plan(query);
            Matcher<RecordQueryPlan> matcher = indexScan(allOf(indexScanType(IndexScanType.BY_LUCENE_FULL_TEXT),
                    indexName("Complex$text_index"),
                    bounds(hasTupleString("[[(the continuance) AND (grudge)],[(the continuance) AND (grudge)]]"))));
            assertThat(plan, matcher);
            List<Long> primaryKeys = recordStore.executeQuery(plan).map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
            assertEquals(ImmutableSet.of(4L), ImmutableSet.copyOf(primaryKeys));
            if (shouldDeferFetch) {
                assertLoadRecord(3, context);
            } else {
                assertLoadRecord(4, context);
            }
        }
    }

    @ParameterizedTest
    @BooleanSource
    public void delayFetchOnLuceneComplexStringAnd(boolean shouldDeferFetch) throws Exception {
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
            RecordQueryPlan plan = planner.plan(query);
            Matcher<RecordQueryPlan> matcher = indexScan(allOf(indexScanType(IndexScanType.BY_LUCENE_FULL_TEXT),
                    indexName("Complex$text_index"),
                    bounds(hasTupleString("[[(the continuance AND grudge)],[(the continuance AND grudge)]]"))));
            assertThat(plan, matcher);
            List<Long> primaryKeys = recordStore.executeQuery(plan).map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
            assertEquals(ImmutableSet.of(4L), ImmutableSet.copyOf(primaryKeys));
            if (shouldDeferFetch) {
                assertLoadRecord(3, context);
            } else {
                assertLoadRecord(4, context);
            }
        }
    }

    @ParameterizedTest
    @BooleanSource
    public void delayFetchOnLuceneComplexStringOr(boolean shouldDeferFetch) throws Exception {
        initializeFlat();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter1 = new LuceneQueryComponent("the continuance OR grudge", Lists.newArrayList());
            // Query for full records
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(filter1)
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planner.plan(query);
            Matcher<RecordQueryPlan> matcher = indexScan(allOf(indexScanType(IndexScanType.BY_LUCENE_FULL_TEXT),
                    indexName("Complex$text_index"),
                    bounds(hasTupleString("[[the continuance OR grudge],[the continuance OR grudge]]"))));
            assertThat(plan, matcher);
            List<Long> primaryKeys = recordStore.executeQuery(plan).map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
            assertEquals(ImmutableSet.of(4L, 5L, 2L), ImmutableSet.copyOf(primaryKeys));
            if (shouldDeferFetch) {
                assertLoadRecord(3, context);
            } else {
                assertLoadRecord(4, context);
            }
        }
    }

    @ParameterizedTest
    @BooleanSource
    public void misMatchQueryShouldReturnNoResult(boolean shouldDeferFetch) throws Exception {
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
            RecordQueryPlan plan = planner.plan(query);
            Matcher<RecordQueryPlan> matcher = indexScan(allOf(indexScan("Complex$text_index"), indexScanType(IndexScanType.BY_LUCENE_FULL_TEXT), bounds(hasTupleString("[[doesNotExist],[doesNotExist]]"))));
            assertThat(plan, matcher);
            List<Long> primaryKeys = recordStore.executeQuery(plan).map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
            assertEquals(ImmutableSet.of(), ImmutableSet.copyOf(primaryKeys));
            if (shouldDeferFetch) {
                assertLoadRecord(3, context);
            } else {
                assertLoadRecord(4, context);
            }
        }
    }

    /*
    @ParameterizedTest
    @BooleanSource
    public void nestedLuceneAndQuery(boolean shouldDeferFetch) throws Exception {
        initializeNested();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(MAP_DOC)
                    .setFilter(new AndComponent(Lists.newArrayList(new LuceneQueryComponent("value:king", Lists.newArrayList("value"), false),
                            new NestedField("entry", new FieldWithComparison("key", new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "a"))))))
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planner.plan(query);

            List<Long> primaryKeys = recordStore.executeQuery(plan).map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
            Matcher<RecordQueryPlan> matcher = indexScan(allOf(
                    indexScanType(IndexScanType.BY_LUCENE),
                    indexScan("Map$entry-value"),
                    bounds(hasTupleString("[[a_value:king],[a_value:king]]"))
            ));
            assertThat(plan, matcher);
            assertEquals(Arrays.asList(2L), primaryKeys);
        }
    }

    @ParameterizedTest
    @BooleanSource
    public void nestedLuceneFieldQuery(boolean shouldDeferFetch) throws Exception {
        initializeNested();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(MAP_DOC)
                    .setFilter(new LuceneQueryComponent("a_value:king", Lists.newArrayList("key")))
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planner.plan(query);
            Matcher<RecordQueryPlan> matcher = indexScan(allOf(
                    indexScanType(IndexScanType.BY_LUCENE),
                    indexScan("Map$entry-value"),
                    bounds(hasTupleString("[[a_value:king],[a_value:king]]"))
            ));
            assertThat(plan, matcher);
            List<Long> primaryKeys = recordStore.executeQuery(plan).map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
            assertEquals(Arrays.asList(2L), primaryKeys);
        }
    }

    /*
    @ParameterizedTest
    @BooleanSource
    public void nestedOneOfThemQuery(boolean shouldDeferFetch) throws Exception {
        initializeNested();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);

            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(MAP_DOC)
                    .setFilter(new OneOfThemWithComponent("entry", Query.field("key").equalsValue("king")))
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planner.plan(query);
            Matcher<RecordQueryPlan> matcher = indexScan(allOf(
                    indexScanType(IndexScanType.BY_LUCENE),
                    indexScan("Map$entry-value"),
                    bounds(hasTupleString("[[entry_key:\"king\"],[entry_key:\"king\"]]"))));
            if (shouldDeferFetch) {
                matcher = fetch(primaryKeyDistinct(coveringIndexScan(matcher)));
            } else {
                matcher = primaryKeyDistinct(matcher);
            }
            assertThat(plan, matcher);
            List<Long> primaryKeys = recordStore.executeQuery(plan).map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
            assertEquals(Collections.emptyList(), primaryKeys);
        }
    }
    @ParameterizedTest
    @BooleanSource
    public void nestedOneOfThemWithAndQuery(boolean shouldDeferFetch) throws Exception {
        initializeNested();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter = Query.field("entry").oneOfThem().matches(Query.and(Query.field("key").equalsValue("b"),
                    Query.field("value").text().containsPhrase("entry_b_value:(+civil blood makes civil hands unclean")));
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(MAP_DOC)
                    .setFilter(filter)
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planner.plan(query);
            Matcher<RecordQueryPlan> matcher = indexScan(allOf(
                    indexName(MAP_ON_LUCENE_INDEX.getName()),
                    indexScanType(IndexScanType.BY_LUCENE),
                    bounds(hasTupleString("[[entry_b_value:(+\"civil blood makes civil hands unclean\")],[entry_b_value:(+\"civil blood makes civil hands unclean\")]]"))
                    ));
            if (shouldDeferFetch) {
                matcher = fetch(primaryKeyDistinct(coveringIndexScan(matcher)));
            } else {
                matcher = primaryKeyDistinct(matcher);
            }
            assertThat(plan, matcher);
            List<Long> primaryKeys = recordStore.executeQuery(plan).map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
            assertEquals(Collections.emptyList(), primaryKeys);
        }

    }

    @ParameterizedTest
    @BooleanSource
    public void nestedOneOfThemWithOrQuery(boolean shouldDeferFetch) throws Exception {
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
            RecordQueryPlan plan = planner.plan(query);
            Matcher<RecordQueryPlan> matcher = indexScan(allOf(
                    indexName(MAP_ON_LUCENE_INDEX.getName()),
                    indexScanType(IndexScanType.BY_LUCENE),
                    bounds(hasTupleString("[[(entry.value:(+\"civil blood makes civil hands unclean\")) OR (entry.key:\"b\")],[(entry.value:(+\"civil blood makes civil hands unclean\")) OR (entry.key:\"b\")]]"))
            ));
            if (shouldDeferFetch) {
                matcher = fetch(primaryKeyDistinct(coveringIndexScan(matcher)));
            } else {
                matcher = primaryKeyDistinct(matcher);
            }
            assertThat(plan, matcher);
            List<Long> primaryKeys = recordStore.executeQuery(plan).map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
            assertEquals(Arrays.asList(0L, 1L, 2L), primaryKeys);
        }

    }
    */

}
