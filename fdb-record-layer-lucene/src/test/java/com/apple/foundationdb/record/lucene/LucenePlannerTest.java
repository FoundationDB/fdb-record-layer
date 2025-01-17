/*
 * LucenePlannerTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsJoinIndexProto;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.JoinedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FunctionKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.NestedField;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.Arrays;
import java.util.List;

import static com.apple.foundationdb.record.lucene.LucenePlanMatchers.query;
import static com.apple.foundationdb.record.lucene.LucenePlanMatchers.scanParams;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.COMPLEX_DOC;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.coveringIndexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.fetch;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.filter;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.inParameter;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.inValues;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScanType;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.intersection;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.primaryKeyDistinct;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.scan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.typeFilter;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.union;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.unorderedUnion;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

/**
 * Tests for {@link LucenePlanner}.
 */
@Tag(Tags.RequiresFDB)
public class LucenePlannerTest extends FDBRecordStoreTestBase {

    private static final String syntheticRecordTypeName = "luceneJoinedIdx";

    private static final FunctionKeyExpression docIdSortKey = function(LuceneFunctionNames.LUCENE_SORTED, field("doc_id"));
    private static final FunctionKeyExpression text1Key = function(LuceneFunctionNames.LUCENE_TEXT, field("text"));
    private static final FunctionKeyExpression text2Key = function(LuceneFunctionNames.LUCENE_TEXT, field("text2"));
    private static final FieldKeyExpression scoreKey = field("score");

    private static final Index COMPLEX_TEXT1_INDEX = new Index("Complex$text1",
            text1Key.groupBy(field("group")),
            LuceneIndexTypes.LUCENE);

    private static final Index COMPLEX_TEXT2_INDEX = new Index("Complex$text2",
            text2Key.groupBy(field("group")),
            LuceneIndexTypes.LUCENE);

    private static final Index COMPLEX_BOTH_INDEX = new Index("Complex$both",
            concat(text1Key, text2Key).groupBy(field("group")),
            LuceneIndexTypes.LUCENE);

    private static final Index COMPLEX_TEXT1_SORTED_INDEX = new Index("Complex$text1",
            concat(docIdSortKey, text1Key).groupBy(field("group")),
            LuceneIndexTypes.LUCENE);

    private static final Index COMPLEX_TEXT2_SORTED_INDEX = new Index("Complex$text2",
            concat(docIdSortKey, text2Key).groupBy(field("group")),
            LuceneIndexTypes.LUCENE);

    private static final Index COMPLEX_BOTH_SORTED_INDEX = new Index("Complex$both",
            concat(docIdSortKey, text1Key, text2Key).groupBy(field("group")),
            LuceneIndexTypes.LUCENE);

    private static final Index COMPLEX_SCORE_INDEX = new Index("Complex$score",
            scoreKey.groupBy(field("group")),
            LuceneIndexTypes.LUCENE);

    private static final Index LUCENE_JOIN_INDEX = new Index("joinNestedConcat", concat(
            field("cust").nest(function(LuceneFunctionNames.LUCENE_STORED, field("name"))),
            field("order").nest(concat(function(LuceneFunctionNames.LUCENE_STORED, field("order_no")),
                    function(LuceneFunctionNames.LUCENE_TEXT, field("order_desc"))
            ))
    ), LuceneIndexTypes.LUCENE);

    private static final RecordMetaDataHook separateHook = metaDataBuilder -> {
        metaDataBuilder.addIndex(COMPLEX_DOC, COMPLEX_TEXT1_INDEX);
        metaDataBuilder.addIndex(COMPLEX_DOC, COMPLEX_TEXT2_INDEX);
    };

    private static final RecordMetaDataHook combinedHook = metaDataBuilder -> {
        metaDataBuilder.addIndex(COMPLEX_DOC, COMPLEX_BOTH_INDEX);
    };

    private static final RecordMetaDataHook separateSortedHook = metaDataBuilder -> {
        metaDataBuilder.addIndex(COMPLEX_DOC, COMPLEX_TEXT1_SORTED_INDEX);
        metaDataBuilder.addIndex(COMPLEX_DOC, COMPLEX_TEXT2_SORTED_INDEX);
    };

    private static final RecordMetaDataHook combinedSortedHook = metaDataBuilder -> {
        metaDataBuilder.addIndex(COMPLEX_DOC, COMPLEX_BOTH_SORTED_INDEX);
    };

    private static final RecordMetaDataHook scoreHook = metaDataBuilder -> {
        metaDataBuilder.addIndex(COMPLEX_DOC, COMPLEX_SCORE_INDEX);
    };

    private static final RecordMetaDataHook syntheticLuceneRecordMetaDataHook = metaDataBuilder -> {
        metaDataBuilder.getRecordType("CustomerWithHeader")
                .setPrimaryKey(Key.Expressions.concat(field("___header").nest("z_key"), field("___header").nest("rec_id")));
        metaDataBuilder.getRecordType("OrderWithHeader")
                .setPrimaryKey(Key.Expressions.concat(field("___header").nest("z_key"), field("___header").nest("rec_id")));

        //set up the joined index
        final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType(syntheticRecordTypeName);
        joined.addConstituent("order", "OrderWithHeader");
        joined.addConstituent("cust", "CustomerWithHeader");
        joined.addJoin("order", field("___header").nest("z_key"),
                "cust", field("___header").nest("z_key"));
        joined.addJoin("order", field("custRef").nest("string_value"),
                "cust", field("___header").nest("rec_id"));

        metaDataBuilder.addIndex(joined, LUCENE_JOIN_INDEX);
        metaDataBuilder.addIndex("OrderWithHeader", "order$custRef", concat(field("___header").nest("z_key"), field("custRef").nest("string_value")));
    };

    private static final LuceneQueryComponent luceneSyntaxAnd =
            new LuceneQueryComponent("text:\"first\" AND text2:\"second\"", List.of("text", "text2"));

    private static final LuceneQueryComponent luceneText1 =
            new LuceneQueryComponent("text:\"first\"", List.of("text"));

    private static final LuceneQueryComponent luceneText2 =
            new LuceneQueryComponent("text2:\"second\"", List.of("text2"));

    private static final LuceneQueryComponent luceneText3 =
            new LuceneQueryComponent("cust_name: \"John Smith\"", List.of("name"));

    private static final QueryComponent nestedFieldWithLuceneText =
            new NestedField("cust", luceneText3);

    private static final RecordQuery luceneSyntaxAndQuery = RecordQuery.newBuilder()
            .setRecordType(COMPLEX_DOC)
            .setFilter(Query.and(
                    Query.field("group").equalsParameter("group_value"),
                    luceneSyntaxAnd
            ))
            .build();

    private static final RecordQuery andLuceneQuery = RecordQuery.newBuilder()
            .setRecordType(COMPLEX_DOC)
            .setFilter(Query.and(
                    Query.field("group").equalsParameter("group_value"),
                    luceneText1,
                    luceneText2
            ))
            .build();

    private static final RecordQuery andLuceneQueryOrdered = RecordQuery.newBuilder()
            .setRecordType(COMPLEX_DOC)
            .setFilter(Query.and(
                    Query.field("group").equalsParameter("group_value"),
                    luceneText1,
                    luceneText2
            ))
            .setSort(field("doc_id"))
            .build();

    private static final RecordQuery orLuceneQuery = RecordQuery.newBuilder()
            .setRecordType(COMPLEX_DOC)
            .setFilter(Query.and(
                    Query.field("group").equalsParameter("group_value"),
                    Query.or(luceneText1, luceneText2)
                    ))
            .build();

    private static final RecordQuery orLuceneQueryOrdered = RecordQuery.newBuilder()
            .setRecordType(COMPLEX_DOC)
            .setFilter(Query.and(
                    Query.field("group").equalsParameter("group_value"),
                    Query.or(luceneText1, luceneText2)
                    ))
            .setSort(field("doc_id"))
            .build();

    private static final RecordQuery inQuery = RecordQuery.newBuilder()
            .setRecordType(COMPLEX_DOC)
            .setFilter(Query.and(
                    Query.field("group").equalsParameter("group_value"),
                    Query.field("score").in(List.of(1, 3, 7))
                    ))
            .build();

    private static final RecordQuery inParameterQuery = RecordQuery.newBuilder()
            .setRecordType(COMPLEX_DOC)
            .setFilter(Query.and(
                    Query.field("group").equalsParameter("group_value"),
                    Query.field("score").in("scores")
                    ))
            .build();

    private static final QueryComponent customerRecIdFilter = new NestedField("cust", new NestedField("___header", new FieldWithComparison("rec_id", new Comparisons.NullComparison(Comparisons.Type.IS_NULL))));

    private static final RecordQuery andLuceneNestedField = RecordQuery.newBuilder()
            .setRecordType(syntheticRecordTypeName)
            .setFilter(Query.and(
                    customerRecIdFilter,
                    nestedFieldWithLuceneText
            ))
            .build();

    protected void openRecordStore(FDBRecordContext context, FDBRecordStoreTestBase.RecordMetaDataHook hook) {
        openRecordStore(context, hook, true);
    }

    protected void openRecordStore(FDBRecordContext context, FDBRecordStoreTestBase.RecordMetaDataHook hook, boolean attemptWholeFilter) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsTextProto.getDescriptor());
        metaDataBuilder.getRecordType(TextIndexTestUtils.COMPLEX_DOC).setPrimaryKey(concatenateFields("group", "doc_id"));
        hook.apply(metaDataBuilder);
        recordStore = getStoreBuilder(context, metaDataBuilder.getRecordMetaData()).createOrOpen();
        setPlannerWholeFilterConfig(attemptWholeFilter);
    }

    protected void setPlannerWholeFilterConfig(boolean isTrue) {
        planner = new LucenePlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState(), PlannableIndexTypes.DEFAULT, recordStore.getTimer());
        planner.setConfiguration(planner.getConfiguration()
                .asBuilder()
                .setPlanOtherAttemptWholeFilter(isTrue)
                .build());
    }

    @Test
    void testLuceneSyntaxAndCombined() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, combinedHook);
            RecordQueryPlan plan = planner.plan(luceneSyntaxAndQuery);
            Matcher<RecordQueryPlan> matcher = indexScan(allOf(
                    indexName(COMPLEX_BOTH_INDEX.getName()),
                    indexScanType(LuceneScanTypes.BY_LUCENE),
                    scanParams(query(hasToString("text:\"first\" AND text2:\"second\"")))));
            assertThat(plan, matcher);

            assertThat(plan.toString(), allOf(
                    containsString(COMPLEX_BOTH_INDEX.getName()),
                    containsString(LuceneScanTypes.BY_LUCENE.toString()),
                    containsString("text:\"first\" AND text2:\"second\"")));
        }
    }

    @Test
    void testLuceneSyntaxAndSeparate() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, separateHook);
            RecordQueryPlan plan = planner.plan(luceneSyntaxAndQuery);
            // This residual filter doesn't actually work, but there isn't an index.
            Matcher<RecordQueryPlan> matcher = filter(luceneSyntaxAnd, typeFilter(contains(COMPLEX_DOC), scan(bounds(hasTupleString("[EQUALS $group_value]")))));
            assertThat(plan, matcher);
        }
    }

    @Test
    void testAndCombined() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, combinedHook);
            RecordQueryPlan plan = planner.plan(andLuceneQuery);
            Matcher<RecordQueryPlan> matcher = indexScan(allOf(
                    indexName(COMPLEX_BOTH_INDEX.getName()),
                    indexScanType(LuceneScanTypes.BY_LUCENE),
                    scanParams(query(hasToString("text:\"first\" AND text2:\"second\"")))));
            assertThat(plan, matcher);
        }
    }

    @Test
    void testAndSeparate() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, separateHook);
            RecordQueryPlan plan = planner.plan(andLuceneQuery);
            // Again, this does not actually run.
            Matcher<RecordQueryPlan> matcher = filter(luceneText2,
                    indexScan(allOf(
                            indexName(COMPLEX_TEXT1_INDEX.getName()),
                            indexScanType(LuceneScanTypes.BY_LUCENE),
                            scanParams(query(hasToString("text:\"first\""))))));
            assertThat(plan, matcher);
        }
    }

    @Test
    void testAndSeparateOrdered() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, separateSortedHook);
            RecordQueryPlan plan = planner.plan(andLuceneQueryOrdered);
            Matcher<RecordQueryPlan> matcher = intersection(
                    indexScan(allOf(
                            indexName(COMPLEX_TEXT1_INDEX.getName()),
                            indexScanType(LuceneScanTypes.BY_LUCENE),
                            scanParams(query(hasToString("text:\"first\""))))),
                    indexScan(allOf(
                            indexName(COMPLEX_TEXT2_INDEX.getName()),
                            indexScanType(LuceneScanTypes.BY_LUCENE),
                            scanParams(query(hasToString("text2:\"second\""))))));
            assertThat(plan, matcher);
            assertThat(plan.toString(), allOf(
                    containsString(COMPLEX_TEXT1_INDEX.getName()),
                    containsString(COMPLEX_TEXT2_INDEX.getName()),
                    containsString("text:\"first\""),
                    containsString("text2:\"second\""),
                    containsString(LuceneScanTypes.BY_LUCENE.toString())
            ));
        }
    }

    @ParameterizedTest
    @BooleanSource
    void testOrCombined(boolean attemptWholeFilter) {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, combinedHook, attemptWholeFilter);
            RecordQueryPlan plan = planner.plan(orLuceneQuery);
            Matcher<RecordQueryPlan> matcher;
            if (attemptWholeFilter) {
                matcher = indexScan(allOf(
                        indexName(COMPLEX_BOTH_INDEX.getName()),
                        indexScanType(LuceneScanTypes.BY_LUCENE),
                        scanParams(query(hasToString("text:\"first\" OR text2:\"second\"")))));
            } else {
                matcher = primaryKeyDistinct(unorderedUnion(
                        indexScan(allOf(
                                indexName(COMPLEX_BOTH_INDEX.getName()),
                                indexScanType(LuceneScanTypes.BY_LUCENE),
                                scanParams(query(hasToString("text:\"first\""))))),
                        indexScan(allOf(
                                indexName(COMPLEX_BOTH_INDEX.getName()),
                                indexScanType(LuceneScanTypes.BY_LUCENE),
                                scanParams(query(hasToString("text2:\"second\"")))))));
            }
            assertThat(plan, matcher);
        }
    }

    @Test
    void testOrSeparate() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, separateHook);
            RecordQueryPlan plan = planner.plan(orLuceneQuery);
            Matcher<RecordQueryPlan> matcher = primaryKeyDistinct(unorderedUnion(
                    indexScan(allOf(
                            indexName(COMPLEX_TEXT1_INDEX.getName()),
                            indexScanType(LuceneScanTypes.BY_LUCENE),
                            scanParams(query(hasToString("text:\"first\""))))),
                    indexScan(allOf(
                            indexName(COMPLEX_TEXT2_INDEX.getName()),
                            indexScanType(LuceneScanTypes.BY_LUCENE),
                            scanParams(query(hasToString("text2:\"second\"")))))));
            assertThat(plan, matcher);
        }
    }

    @Test
    void testOrSeparateOrdered() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, separateSortedHook, false);
            RecordQueryPlan plan = planner.plan(orLuceneQueryOrdered);
            Matcher<RecordQueryPlan> matcher = union(
                    indexScan(allOf(
                            indexName(COMPLEX_TEXT1_INDEX.getName()),
                            indexScanType(LuceneScanTypes.BY_LUCENE),
                            scanParams(query(hasToString("text:\"first\""))))),
                    indexScan(allOf(
                            indexName(COMPLEX_TEXT2_INDEX.getName()),
                            indexScanType(LuceneScanTypes.BY_LUCENE),
                            scanParams(query(hasToString("text2:\"second\""))))));
            assertThat(plan, matcher);
        }
    }

    @ParameterizedTest
    @BooleanSource
    void testIn(boolean attemptWholeFilter) {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, scoreHook, attemptWholeFilter);
            RecordQueryPlan plan = planner.plan(inQuery);
            Matcher<RecordQueryPlan> matcher;
            if (attemptWholeFilter) {
                matcher = indexScan(allOf(
                        indexName(COMPLEX_SCORE_INDEX.getName()),
                        indexScanType(LuceneScanTypes.BY_LUCENE),
                        scanParams(query(hasToString("score:INT IN [1, 3, 7]")))));
            } else {
                matcher = inValues(equalTo(Arrays.asList(1, 3, 7)), indexScan(allOf(
                        indexName(COMPLEX_SCORE_INDEX.getName()),
                        indexScanType(LuceneScanTypes.BY_LUCENE),
                        scanParams(query(hasToString("score:INT EQUALS $__in_score__0"))))));
            }
            assertThat(plan, matcher);
        }
    }

    @ParameterizedTest
    @BooleanSource
    void testInParameter(boolean attemptWholeFilter) {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, scoreHook, attemptWholeFilter);
            RecordQueryPlan plan = planner.plan(inParameterQuery);
            Matcher<RecordQueryPlan> matcher;
            if (attemptWholeFilter) {
                matcher = indexScan(allOf(
                        indexName(COMPLEX_SCORE_INDEX.getName()),
                        indexScanType(LuceneScanTypes.BY_LUCENE),
                        scanParams(query(hasToString("score:INT IN $scores")))));
            } else {
                matcher = inParameter(equalTo("scores"), indexScan(allOf(
                        indexName(COMPLEX_SCORE_INDEX.getName()),
                        indexScanType(LuceneScanTypes.BY_LUCENE),
                        scanParams(query(hasToString("score:INT EQUALS $__in_score__0"))))));
            }
            assertThat(plan, matcher);
        }
    }

    @ParameterizedTest
    @BooleanSource
    void testLuceneQueryUnderNestedField(boolean attemptWholeFilter) {
        try (FDBRecordContext context = openContext()) {
            RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsJoinIndexProto.getDescriptor());
            syntheticLuceneRecordMetaDataHook.apply(metaDataBuilder);
            recordStore = getStoreBuilder(context, metaDataBuilder.getRecordMetaData()).createOrOpen();
            setPlannerWholeFilterConfig(attemptWholeFilter);
            RecordQueryPlan plan = planner.plan(andLuceneNestedField);
            final var filter = customerRecIdFilter;
            Matcher<RecordQueryPlan> matcher;
            if (attemptWholeFilter) {
                matcher = fetch(filter(filter, coveringIndexScan(indexScan(allOf(indexName(LUCENE_JOIN_INDEX.getName()), indexScanType(LuceneScanTypes.BY_LUCENE),
                        scanParams(query(hasToString("cust_name: \"John Smith\""))))))));
            } else {
                matcher = fetch(filter(filter, coveringIndexScan(indexScan(allOf(indexName(LUCENE_JOIN_INDEX.getName()), indexScanType(LuceneScanTypes.BY_LUCENE),
                        scanParams(query(hasToString("cust_name: \"John Smith\""))))))));
            }
            assertThat(plan, matcher);
        }
    }

}
