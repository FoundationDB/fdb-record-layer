/*
 * FDBFullTextQueryTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.query;

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.provider.common.text.DefaultTextTokenizer;
import com.apple.foundationdb.record.provider.common.text.TextSamples;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.apple.foundationdb.record.TestHelpers.assertLoadRecord;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.coveringIndexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.fetch;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.primaryKeyDistinct;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.textComparison;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.textIndexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.unorderedUnion;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Sophisticated queries involving full text predicates.
 */
@Tag(Tags.RequiresFDB)
public class FDBFullTextQueryTest extends FDBRecordStoreQueryTestBase {
    protected void openRecordStore(FDBRecordContext context) throws Exception {
        openRecordStore(context, store -> { });
    }

    protected void openRecordStore(FDBRecordContext context, RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsTextProto.getDescriptor());
        metaDataBuilder.getRecordType(TextIndexTestUtils.COMPLEX_DOC).setPrimaryKey(concatenateFields("group", "doc_id"));
        hook.apply(metaDataBuilder);
        recordStore = getStoreBuilder(context, metaDataBuilder.getRecordMetaData())
                .setSerializer(TextIndexTestUtils.COMPRESSING_SERIALIZER)
                .uncheckedOpen();
        setupPlanner(null);
    }

    @ParameterizedTest
    @BooleanSource
    public void delayFetchOnUnionOfFullTextScans(boolean shouldDeferFetch) throws Exception {
        final List<TestRecordsTextProto.SimpleDocument> documents = TextIndexTestUtils.toSimpleDocuments(Arrays.asList(
                TextSamples.ANGSTROM,
                TextSamples.AETHELRED,
                TextSamples.ROMEO_AND_JULIET_PROLOGUE,
                TextSamples.FRENCH
        ));

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            documents.forEach(recordStore::saveRecord);
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);

            final QueryComponent filter1 = Query.field("text").text().containsPhrase("civil blood makes civil hands unclean");
            final Comparisons.Comparison comparison1 = new Comparisons.TextComparison(Comparisons.Type.TEXT_CONTAINS_PHRASE, "civil blood makes civil hands unclean", null, DefaultTextTokenizer.NAME);
            final QueryComponent filter2 = Query.field("text").text().containsPrefix("th");
            final Comparisons.Comparison comparison2 = new Comparisons.TextComparison(Comparisons.Type.TEXT_CONTAINS_PREFIX,  Collections.singletonList("th"), null, DefaultTextTokenizer.NAME);

            // Query for full records
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(Query.or(filter1, filter2))
                    .build();
            setDeferFetchAfterUnionAndIntersection(shouldDeferFetch);

            // Unordered(TextIndex(SimpleDocument$text null, TEXT_CONTAINS_PHRASE civil blood makes civil hands unclean, null) ∪ TextIndex(SimpleDocument$text null, TEXT_CONTAINS_PREFIX [th], null) | UnorderedPrimaryKeyDistinct()) | UnorderedPrimaryKeyDistinct()
            // Fetch(Unordered(Covering(TextIndex(SimpleDocument$text null, TEXT_CONTAINS_PHRASE civil blood makes civil hands unclean, null) -> [doc_id: KEY[1]]) ∪ Covering(TextIndex(SimpleDocument$text null, TEXT_CONTAINS_PREFIX [th], null) -> [doc_id: KEY[1]]) | UnorderedPrimaryKeyDistinct()) | UnorderedPrimaryKeyDistinct())
            RecordQueryPlan plan = planner.plan(query);
            if (shouldDeferFetch) {
                assertThat(plan, fetch(primaryKeyDistinct(unorderedUnion(
                        coveringIndexScan(textIndexScan(allOf(indexName(TextIndexTestUtils.SIMPLE_DEFAULT_NAME), textComparison(equalTo(comparison1))))),
                        primaryKeyDistinct(coveringIndexScan(textIndexScan(allOf(indexName(TextIndexTestUtils.SIMPLE_DEFAULT_NAME), textComparison(equalTo(comparison2))))))))));
                assertEquals(-683922391, plan.planHash(PlanHashable.CURRENT_LEGACY));
                assertEquals(-833837033, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            } else {
                assertThat(plan, primaryKeyDistinct(unorderedUnion(
                        textIndexScan(allOf(indexName(TextIndexTestUtils.SIMPLE_DEFAULT_NAME), textComparison(equalTo(comparison1)))),
                        primaryKeyDistinct(textIndexScan(allOf(indexName(TextIndexTestUtils.SIMPLE_DEFAULT_NAME), textComparison(equalTo(comparison2))))))));
                assertEquals(515863556, plan.planHash(PlanHashable.CURRENT_LEGACY));
                assertEquals(1307589440, plan.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            }
            List<Long> primaryKeys = recordStore.executeQuery(plan).map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
            assertEquals(ImmutableSet.of(0L, 1L, 2L, 3L), ImmutableSet.copyOf(primaryKeys));
            if (shouldDeferFetch) {
                assertLoadRecord(4, context);
            } else {
                assertLoadRecord(8, context);
            }
        }
    }

    @Test
    public void fullTextCovering() throws Exception {
        final List<TestRecordsTextProto.SimpleDocument> documents = TextIndexTestUtils.toSimpleDocuments(Arrays.asList(
                TextSamples.ANGSTROM,
                TextSamples.AETHELRED,
                TextSamples.ROMEO_AND_JULIET_PROLOGUE,
                TextSamples.FRENCH
        ));

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            documents.forEach(recordStore::saveRecord);
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);

            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(Query.field("text").text().contains("civil"))
                    .setRequiredResults(ImmutableList.of(field("text")))
                    .build();
            RecordQueryPlan plan = planner.plan(query);
            // No covering index scan
            assertThat(plan, textIndexScan(indexName(TextIndexTestUtils.SIMPLE_DEFAULT_NAME)));

            Descriptors.FieldDescriptor textFieldDescriptor = recordStore.getRecordMetaData()
                    .getRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .getDescriptor()
                    .findFieldByName("text");
            List<String> texts = recordStore.executeQuery(plan).map(FDBQueriedRecord::getRecord)
                    .map(record -> (String) record.getField(textFieldDescriptor)).asList().get();
            assertEquals(ImmutableList.of(TextSamples.ROMEO_AND_JULIET_PROLOGUE), texts);
        }
    }
}
