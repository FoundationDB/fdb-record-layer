/*
 * LuceneQueryIntegrationTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.NotComponent;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.test.Tags;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;

/**
 * Grab-bag of different tests around "black-box" testing the Lucene query component. Everything from
 * index matching to whatever.
 *
 */
@Tag(Tags.RequiresFDB)
public class LuceneQueryIntegrationTest extends FDBRecordStoreQueryTestBase {

    private final Index textIndex = new Index("Complex$text_index",
            function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
            LuceneIndexTypes.LUCENE);

    private final Index text2Index = new Index("Complex$text2_index",
            function(LuceneFunctionNames.LUCENE_TEXT, field("text2")),
            LuceneIndexTypes.LUCENE);

    private final Index nestedDualIndex = new Index("Complex$nested_index",
            concat(field("header").nest("header_id"), function(LuceneFunctionNames.LUCENE_TEXT, field("text"))),
            LuceneIndexTypes.LUCENE);

    private final Index nestedDualIndex2 = new Index("Complex$nested_index2",
            concat(field("header").nest("header_id"), function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
            LuceneIndexTypes.LUCENE);

    @Override
    public void setupPlanner(@Nullable PlannableIndexTypes indexTypes) {
        if (isUseCascadesPlanner()) {
            planner = CascadesPlanner.forStore(recordStore);
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

    protected void openRecordStore(FDBRecordContext context) {
        openRecordStore(context, store -> { });
    }

    protected void openRecordStore(FDBRecordContext context, RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsTextProto.getDescriptor());
        metaDataBuilder.getRecordType(TextIndexTestUtils.COMPLEX_DOC).setPrimaryKey(concatenateFields("group", "doc_id"));
        metaDataBuilder.removeIndex("SimpleDocument$text");
        hook.apply(metaDataBuilder);
        recordStore = getStoreBuilder(context, metaDataBuilder.getRecordMetaData())
                .setSerializer(TextIndexTestUtils.COMPRESSING_SERIALIZER)
                .uncheckedOpen();
        setupPlanner(null);
    }

    @Test
    void selectsFromMultipleIndexes() throws Exception {
        setUseCascadesPlanner(false);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaData -> {
                metaData.addIndex(TextIndexTestUtils.COMPLEX_DOC, textIndex);
                metaData.addIndex(TextIndexTestUtils.COMPLEX_DOC, text2Index);
            });
            setupPlanner(null);

            QueryComponent filter = new LuceneQueryComponent("text2:test", Arrays.asList("text2"));
            RecordQuery rq = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.COMPLEX_DOC)
                    .setFilter(filter)
                    .build();
            final RecordQueryPlan plan = planQuery(planner, rq);
            Set<String> appliedIndexNames = plan.getUsedIndexes();
            Assertions.assertEquals(1, appliedIndexNames.size(), "index selection is incorrect");
            Assertions.assertTrue(appliedIndexNames.contains(text2Index.getName()), "Did not select the correct index");
        }
    }

    @Test
    void selectsFromMultipleNestedIndexes() throws Exception {
        setUseCascadesPlanner(false);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaData -> {
                metaData.addIndex(TextIndexTestUtils.COMPLEX_DOC, new Index(nestedDualIndex.toProto()));
                metaData.addIndex(TextIndexTestUtils.COMPLEX_DOC, new Index(nestedDualIndex2.toProto()));
            });
            setupPlanner(null);

            QueryComponent filter = new LuceneQueryComponent("text2:test", Arrays.asList("text2"));
            RecordQuery rq = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.COMPLEX_DOC)
                    .setFilter(filter)
                    .build();
            final RecordQueryPlan plan = planQuery(planner, rq);
            Set<String> appliedIndexNames = plan.getUsedIndexes();
            Assertions.assertEquals(1, appliedIndexNames.size(), "index selection is incorrect");
            Assertions.assertTrue(appliedIndexNames.contains(nestedDualIndex2.getName()), "Did not select the correct index");
        }
    }

    @Test
    void notLucene() throws Exception {
        setUseCascadesPlanner(false);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaData -> {
                metaData.addIndex(TextIndexTestUtils.COMPLEX_DOC, new Index(nestedDualIndex.toProto()));
                metaData.addIndex(TextIndexTestUtils.COMPLEX_DOC, new Index(nestedDualIndex2.toProto()));
            });
            setupPlanner(null);

            QueryComponent filter = new LuceneQueryComponent("text2:test", List.of("text2"));
            RecordQuery rq = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.COMPLEX_DOC)
                    .setFilter(filter)
                    .build();

            RecordQuery notQuery = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.COMPLEX_DOC)
                    .setFilter(new NotComponent(filter))
                    .build();

            final RecordQueryPlan plan = planQuery(planner, rq);
            final RecordQueryPlan notPlan = planQuery(planner, notQuery);
            Assertions.assertEquals(71592145, plan.planHash(PlanHashable.CURRENT_LEGACY));
            Assertions.assertEquals(-1378983311, notPlan.planHash(PlanHashable.CURRENT_LEGACY));
        }
    }
}
