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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.common.text.AllSuffixesTextTokenizer;
import com.apple.foundationdb.record.provider.common.text.TextSamples;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.AndComponent;
import com.apple.foundationdb.record.query.expressions.LuceneQueryComponent;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.apple.foundationdb.record.TestHelpers.assertLoadRecord;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.filter;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
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
            TextSamples.ROMEO_AND_JULIET_PROLOGUE,
            TextSamples.FRENCH
    ));

    private static final Index SIMPLE_TEXT_SUFFIXES = new Index("Complex$text_index", field("text"), IndexTypes.LUCENE,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME));

    protected void openRecordStore(FDBRecordContext context) throws Exception {
        openRecordStore(context, store -> { });
    }

    protected void openRecordStore(FDBRecordContext context, RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsTextProto.getDescriptor());
        metaDataBuilder.getRecordType(TextIndexTestUtils.COMPLEX_DOC).setPrimaryKey(concatenateFields("group", "doc_id"));
        metaDataBuilder.removeIndex("SimpleDocument$text");
        metaDataBuilder.addIndex(TextIndexTestUtils.SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
        hook.apply(metaDataBuilder);
        recordStore = getStoreBuilder(context, metaDataBuilder.getRecordMetaData())
                .setSerializer(TextIndexTestUtils.COMPRESSING_SERIALIZER)
                .uncheckedOpen();
        setupPlanner(null);
    }

    @BeforeEach
    private void initialize() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            DOCUMENTS.forEach(recordStore::saveRecord);
            commit(context);
        }
    }

    @ParameterizedTest
    @BooleanSource
    public void simpleLuceneScans(boolean shouldDeferFetch) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter1 = new LuceneQueryComponent("civil blood makes civil hands unclean");
            // Query for full records
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(filter1)
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            Matcher<RecordQueryPlan> matcher = filter(filter1, indexScan(allOf(indexScan("Complex$text_index"), indexScanType(IndexScanType.BY_LUCENE), bounds(hasTupleString("[[civil blood makes civil hands unclean],[civil blood makes civil hands unclean]]")))));
            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, matcher);
            List<Long> primaryKeys = recordStore.executeQuery(plan).map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
            assertEquals(ImmutableSet.of(2L), ImmutableSet.copyOf(primaryKeys));
        }
    }

    @ParameterizedTest
    @BooleanSource
    public void delayFetchOnUnionOfLuceneScanWithFilter(boolean shouldDeferFetch) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter1 = new LuceneQueryComponent("civil blood makes civil hands unclean");
            // Query for full records
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(Query.or(filter1, Query.field("doc_id").lessThan(Long.valueOf(10000))))
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planner.plan(query);
            Matcher<RecordQueryPlan> matcher = primaryKeyDistinct(
                    unorderedUnion(
                            filter(filter1, indexScan(allOf(indexScan("Complex$text_index"), indexScanType(IndexScanType.BY_LUCENE), bounds(hasTupleString("[[civil blood makes civil hands unclean],[civil blood makes civil hands unclean]]"))))),
                            typeFilter(equalTo(Collections.singleton(TextIndexTestUtils.SIMPLE_DOC)), scan(bounds(hasTupleString("([null],[10000])"))))
                    ));
            assertThat(plan, matcher);
            List<Long> primaryKeys = recordStore.executeQuery(plan).map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
            assertEquals(ImmutableSet.of(0L, 1L, 2L, 3L), ImmutableSet.copyOf(primaryKeys));
            if (shouldDeferFetch) {
                assertLoadRecord(3, context);
            } else {
                assertLoadRecord(4, context);
            }
        }
    }

    @ParameterizedTest
    @BooleanSource
    public void delayFetchOnIntersectionOfLuceneScans(boolean shouldDeferFetch) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter1 = new LuceneQueryComponent("civil blood makes civil hands unclean");
            // Query for full records
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(Query.and(filter1, Query.field("doc_id").lessThan(Long.valueOf(10000))))
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planner.plan(query);
            Matcher<RecordQueryPlan> scanMatcher =  filter(filter1, typeFilter(equalTo(Collections.singleton(TextIndexTestUtils.SIMPLE_DOC)), scan(bounds(hasTupleString("([null],[10000])")))));
            assertThat(plan, scanMatcher);
            List<Long> primaryKeys = recordStore.executeQuery(plan).map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
            assertEquals(ImmutableSet.of(2L), ImmutableSet.copyOf(primaryKeys));
            if (shouldDeferFetch) {
                assertLoadRecord(3, context);
            } else {
                assertLoadRecord(4, context);
            }
        }
    }

    @ParameterizedTest
    @BooleanSource
    public void delayFetchOnUnionOfLuceneScanWithFilters(boolean shouldDeferFetch) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter1 = new LuceneQueryComponent("civil blood makes civil hands unclean");
            final QueryComponent filter2 = new LuceneQueryComponent("was king from 966 to 1016");
            // Query for full records
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(Query.or(filter1, filter2))
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planner.plan(query);
            Matcher<RecordQueryPlan> matcher = union(
                            filter(filter1, indexScan(allOf(indexScan("Complex$text_index"), indexScanType(IndexScanType.BY_LUCENE), bounds(hasTupleString("[[civil blood makes civil hands unclean],[civil blood makes civil hands unclean]]"))))),
                            filter(filter2, indexScan(allOf(indexScan("Complex$text_index"), indexScanType(IndexScanType.BY_LUCENE), bounds(hasTupleString("[[was king from 966 to 1016],[was king from 966 to 1016]]"))))),
                            equalTo(field("doc_id"))
                    );
            assertThat(plan, matcher);
            List<Long> primaryKeys = recordStore.executeQuery(plan).map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
            assertEquals(ImmutableSet.of(1L, 2L), ImmutableSet.copyOf(primaryKeys));
            if (shouldDeferFetch) {
                assertLoadRecord(3, context);
            } else {
                assertLoadRecord(4, context);
            }
        }
    }

    @ParameterizedTest
    @BooleanSource
    public void delayFetchOnIntersectionOfLuceneScanWithFilters(boolean shouldDeferFetch) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter1 = new LuceneQueryComponent("the elephant");
            final QueryComponent filter2 = new LuceneQueryComponent("Two households, both alike in dignity");
            // Query for full records
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(Query.and(filter1, filter2))
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planner.plan(query);
            Matcher<RecordQueryPlan> matcher = filter(AndComponent.from(Lists.newArrayList(filter1, filter2)),
                    typeFilter(equalTo(Collections.singleton(TextIndexTestUtils.SIMPLE_DOC)), scan()));
            assertThat(plan, matcher);
            List<Long> primaryKeys = recordStore.executeQuery(plan).map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
            assertEquals(ImmutableSet.of(2L), ImmutableSet.copyOf(primaryKeys));
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
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final QueryComponent filter1 = new LuceneQueryComponent("doesNotExist");
            // Query for full records
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(filter1)
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);
            RecordQueryPlan plan = planner.plan(query);
            Matcher<RecordQueryPlan> matcher = filter(filter1, indexScan(allOf(indexScan("Complex$text_index"), indexScanType(IndexScanType.BY_LUCENE), bounds(hasTupleString("[[doesNotExist],[doesNotExist]]")))));
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

}
