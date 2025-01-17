/*
 * LuceneSharedCacheTest.java
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.lucene.directory.FDBDirectorySharedCache;
import com.apple.foundationdb.record.lucene.directory.FDBDirectorySharedCacheManager;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.common.text.TextSamples;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests related to {@link FDBDirectorySharedCache}.
 */
@Tag(Tags.RequiresFDB)
public class LuceneSharedCacheTest extends FDBRecordStoreQueryTestBase {

    FDBDirectorySharedCacheManager sharedCacheManager = FDBDirectorySharedCacheManager.newBuilder().build();

    private static final List<TestRecordsTextProto.SimpleDocument> DOCUMENTS = TextIndexTestUtils.toSimpleDocuments(List.of(
            TextSamples.ANGSTROM, // 0
            TextSamples.ANGSTROM, // 1
            TextSamples.AETHELRED, // 2
            TextSamples.FRENCH,    // 3
            TextSamples.ROMEO_AND_JULIET_PROLOGUE, // 4
            TextSamples.PARTIAL_ROMEO_AND_JULIET_PROLOGUE, // 5
            TextSamples.ROMEO_AND_JULIET_PROLOGUE_END      // 6
    ));

    @Override
    public void setupPlanner(@Nullable PlannableIndexTypes indexTypes) {
        if (indexTypes == null) {
            indexTypes = new PlannableIndexTypes(
                    Set.of(IndexTypes.VALUE, IndexTypes.VERSION),
                    Set.of(IndexTypes.RANK, IndexTypes.TIME_WINDOW_LEADERBOARD),
                    Set.of(IndexTypes.TEXT),
                    Set.of(LuceneIndexTypes.LUCENE)
            );
            planner = new LucenePlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState(), indexTypes, recordStore.getTimer());
        }
    }

    protected void openRecordStore(@Nonnull FDBRecordContext context) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsTextProto.getDescriptor());
        metaDataBuilder.getRecordType(TextIndexTestUtils.COMPLEX_DOC).setPrimaryKey(concatenateFields("group", "doc_id"));
        metaDataBuilder.removeIndex("SimpleDocument$text");
        metaDataBuilder.addIndex(TextIndexTestUtils.SIMPLE_DOC, new Index("grouped_text",
                function(LuceneFunctionNames.LUCENE_TEXT, field("text")).groupBy(field("group")),
                LuceneIndexTypes.LUCENE, Map.of()));
        recordStore = getStoreBuilder(context, metaDataBuilder.getRecordMetaData())
                .setSerializer(TextIndexTestUtils.COMPRESSING_SERIALIZER)
                .uncheckedOpen();
        setupPlanner(null);
    }

    protected void initializeRecords() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            DOCUMENTS.forEach(recordStore::saveRecord);
            commit(context);
        }
    }

    protected Set<Long> groupQueryForPrimaryKeys(@Nonnull RecordQuery query, long group) throws Exception {
        RecordQueryPlan plan = planner.plan(query);
        EvaluationContext context = EvaluationContext.forBinding("g", group);
        List<Long> primaryKeys = plan.execute(recordStore, context)
                .map(FDBQueriedRecord::getPrimaryKey)
                .map(t -> t.getLong(0))
                .asList().get();
        return Set.copyOf(primaryKeys);
    }

    @Test
    void simple() throws Exception {
        initializeRecords();

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            sharedCacheManager.setForContext(context);
            final QueryComponent filter = Query.and(
                    Query.field("group").equalsParameter("g"),
                    new LuceneQueryComponent("text:traffic", List.of("text")));
            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(filter)
                    .build();
            timer.reset();
            assertEquals(Set.of(4L, 6L), groupQueryForPrimaryKeys(query, 0));
            assertThat("no hits", timer.getCount(LuceneEvents.Counts.LUCENE_SHARED_CACHE_HITS), equalTo(0));
            assertThat("some misses", timer.getCount(LuceneEvents.Counts.LUCENE_SHARED_CACHE_MISSES), greaterThan(0));
        }

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            sharedCacheManager.setForContext(context);
            final QueryComponent filter = Query.and(
                    Query.field("group").equalsParameter("g"),
                    new LuceneQueryComponent("text:mutiny", List.of("text")));
            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(filter)
                    .build();
            timer.reset();
            assertEquals(Set.of(4L), groupQueryForPrimaryKeys(query, 0));
            assertThat("some hits", timer.getCount(LuceneEvents.Counts.LUCENE_SHARED_CACHE_HITS), greaterThan(0));
            // This assumes that the index is small enough that we got it all.
            assertThat("no misses", timer.getCount(LuceneEvents.Counts.LUCENE_SHARED_CACHE_MISSES), equalTo(0));
        }
    }

    @Test
    void separateGroups() throws Exception {
        initializeRecords();

        final QueryComponent filter = Query.and(
                Query.field("group").equalsParameter("g"),
                new LuceneQueryComponent("text:mutiny", List.of("text")));
        final RecordQuery query = RecordQuery.newBuilder()
                .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                .setFilter(filter)
                .build();

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            sharedCacheManager.setForContext(context);
            timer.reset();
            assertEquals(Set.of(4L), groupQueryForPrimaryKeys(query, 0));
            assertThat("no hits", timer.getCount(LuceneEvents.Counts.LUCENE_SHARED_CACHE_HITS), equalTo(0));
            assertThat("some misses", timer.getCount(LuceneEvents.Counts.LUCENE_SHARED_CACHE_MISSES), greaterThan(0));
        }

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            sharedCacheManager.setForContext(context);
            timer.reset();
            assertEquals(Set.of(5L), groupQueryForPrimaryKeys(query, 1));
            assertThat("no hits", timer.getCount(LuceneEvents.Counts.LUCENE_SHARED_CACHE_HITS), equalTo(0));
            assertThat("some misses", timer.getCount(LuceneEvents.Counts.LUCENE_SHARED_CACHE_MISSES), greaterThan(0));
        }
    }

    @Test
    void intermediateWrite() throws Exception {
        initializeRecords();

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            sharedCacheManager.setForContext(context);
            final QueryComponent filter = Query.and(
                    Query.field("group").equalsParameter("g"),
                    new LuceneQueryComponent("text:traffic", List.of("text")));
            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(filter)
                    .build();
            timer.reset();
            assertEquals(Set.of(4L, 6L), groupQueryForPrimaryKeys(query, 0));
            assertThat("no hits", timer.getCount(LuceneEvents.Counts.LUCENE_SHARED_CACHE_HITS), equalTo(0));
            assertThat("some misses", timer.getCount(LuceneEvents.Counts.LUCENE_SHARED_CACHE_MISSES), greaterThan(0));
        }

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            TextIndexTestUtils.toSimpleDocuments(List.of(
                    TextSamples.GERMAN
            )).forEach(recordStore::saveRecord);
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            sharedCacheManager.setForContext(context);
            final QueryComponent filter = Query.and(
                    Query.field("group").equalsParameter("g"),
                    new LuceneQueryComponent("text:mutiny", List.of("text")));
            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(filter)
                    .build();
            timer.reset();
            assertEquals(Set.of(4L), groupQueryForPrimaryKeys(query, 0));
            assertThat("no hits", timer.getCount(LuceneEvents.Counts.LUCENE_SHARED_CACHE_HITS), equalTo(0));
            assertThat("some misses", timer.getCount(LuceneEvents.Counts.LUCENE_SHARED_CACHE_MISSES), greaterThan(0));
        }
    }

}
