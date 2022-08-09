/*
 * LucenePlannerTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2022 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FunctionKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

/**
 * Tests for {@link LucenePlanner}.
 */
@Tag(Tags.RequiresFDB)
public class LucenePlannerTest extends FDBRecordStoreTestBase {

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

    private static final LuceneQueryComponent luceneSyntaxAnd =
            new LuceneQueryComponent("text:\"first\" AND text2:\"second\"", List.of("text", "text2"));

    private static final LuceneQueryComponent luceneText1 =
            new LuceneQueryComponent("text:\"first\"", List.of("text"));

    private static final LuceneQueryComponent luceneText2 =
            new LuceneQueryComponent("text2:\"second\"", List.of("text2"));

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

    protected void openRecordStore(FDBRecordContext context, FDBRecordStoreTestBase.RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsTextProto.getDescriptor());
        metaDataBuilder.getRecordType(TextIndexTestUtils.COMPLEX_DOC).setPrimaryKey(concatenateFields("group", "doc_id"));
        hook.apply(metaDataBuilder);
        recordStore = getStoreBuilder(context, metaDataBuilder.getRecordMetaData())
                .setSerializer(TextIndexTestUtils.COMPRESSING_SERIALIZER)
                .createOrOpen();
        planner = new LucenePlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState(), PlannableIndexTypes.DEFAULT, recordStore.getTimer());
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
        }
    }

    @Test
    void testOrCombined() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, combinedHook);
            RecordQueryPlan plan = planner.plan(orLuceneQuery);
            // TODO: Better would be to combine in Lucene term queries, since it's the same index.
            Matcher<RecordQueryPlan> matcher = primaryKeyDistinct(unorderedUnion(
                    indexScan(allOf(
                            indexName(COMPLEX_BOTH_INDEX.getName()),
                            indexScanType(LuceneScanTypes.BY_LUCENE),
                            scanParams(query(hasToString("text:\"first\""))))),
                    indexScan(allOf(
                            indexName(COMPLEX_BOTH_INDEX.getName()),
                            indexScanType(LuceneScanTypes.BY_LUCENE),
                            scanParams(query(hasToString("text2:\"second\"")))))));
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
            openRecordStore(context, separateSortedHook);
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

    @Test
    void testIn() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, scoreHook);
            RecordQueryPlan plan = planner.plan(inQuery);
            // TODO: Better might be to unroll as separate Lucene term queries.
            Matcher<RecordQueryPlan> matcher = inValues(equalTo(Arrays.asList(1, 3, 7)), indexScan(allOf(
                    indexName(COMPLEX_SCORE_INDEX.getName()),
                    indexScanType(LuceneScanTypes.BY_LUCENE),
                    scanParams(query(hasToString("score:INT EQUALS $__in_score__0"))))));
            assertThat(plan, matcher);
        }
    }

    @Test
    void testInParameter() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, scoreHook);
            RecordQueryPlan plan = planner.plan(inParameterQuery);
            // TODO: Better might be a clause that expands into the necessary terms.
            Matcher<RecordQueryPlan> matcher = inParameter(equalTo("scores"), indexScan(allOf(
                    indexName(COMPLEX_SCORE_INDEX.getName()),
                    indexScanType(LuceneScanTypes.BY_LUCENE),
                    scanParams(query(hasToString("score:INT EQUALS $__in_score__0"))))));
            assertThat(plan, matcher);
        }
    }

}
