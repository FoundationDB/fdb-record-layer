/*
 * LuceneBitmapValueTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.COMPLEX_DOC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(Tags.RequiresFDB)
public class LuceneBitmapValueTest extends FDBLuceneTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneBitmapValueTest.class);

    @Override
    protected RecordLayerPropertyStorage.Builder addDefaultProps(final RecordLayerPropertyStorage.Builder props) {
        return super.addDefaultProps(props)
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_COMPRESSION_ENABLED, true);
    }

    protected void openRecordStore(FDBRecordContext context) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsTextProto.getDescriptor());
        metaDataBuilder.getRecordType(COMPLEX_DOC).setPrimaryKey(concatenateFields("group", "doc_id"));

        metaDataBuilder.addIndex("FlaggedDocument",
                new Index("text_lucene",
                        concat(field("group"), function(LuceneFunctionNames.LUCENE_TEXT, field("text"))).group(1),
                        LuceneIndexTypes.LUCENE,
                        Map.of(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED, "true",
                                "tryBitmapValueIndexes", "true")));
        metaDataBuilder.addIndex("FlaggedDocument",
                new Index("seen_bitmap",
                        concatenateFields("group", "is_seen", "doc_id").group(1),
                        IndexTypes.BITMAP_VALUE,
                        Map.of(IndexOptions.BITMAP_VALUE_ENTRY_SIZE_OPTION, "16")));
        metaDataBuilder.addIndex("FlaggedDocument",
                new Index("urgent_bitmap",
                        concatenateFields("group", "is_urgent", "doc_id").group(1),
                        IndexTypes.BITMAP_VALUE,
                        Map.of(IndexOptions.BITMAP_VALUE_ENTRY_SIZE_OPTION, "16")));

        recordStore = getStoreBuilder(context, metaDataBuilder.getRecordMetaData()).createOrOpen();

        PlannableIndexTypes indexTypes = new PlannableIndexTypes(
                Set.of(IndexTypes.VALUE, IndexTypes.VERSION),
                Set.of(),
                Set.of(),
                Set.of(LuceneIndexTypes.LUCENE)
        );
        planner = new LucenePlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState(),
                                    indexTypes, recordStore.getTimer());
        planner.setConfiguration(planner.getConfiguration()
                .asBuilder()
                .setPlanOtherAttemptWholeFilter(false)
                .build());
        recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(true);
    }

    public static TestRecordsTextProto.FlaggedDocument createFlaggedDocument(long group, long docId, String text, boolean isSeen, boolean isUrgent) {
        return TestRecordsTextProto.FlaggedDocument.newBuilder()
                .setGroup(group)
                .setDocId(docId)
                .setText(text)
                .setIsSeen(isSeen)
                .setIsUrgent(isUrgent)
                .build();
    }

    @Test
    void testBitmapValue() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);

            recordStore.saveRecord(createFlaggedDocument(0, 101, "some text for this: pineapple", false, true));
            recordStore.saveRecord(createFlaggedDocument(0, 102, "more text for that: orange", true, false));
            recordStore.saveRecord(createFlaggedDocument(0, 103, "again for this: pineapple", true, true));
            context.commit();
        }
        assertEquals(3, timer.getCount(LuceneEvents.Events.LUCENE_ADD_DOCUMENT));

        final RecordQuery recordQuery = RecordQuery.newBuilder()
                .setRecordType("FlaggedDocument")
                .setFilter(Query.and(Query.field("group").equalsValue(0L),
                        new LuceneQueryComponent("text:pineapple", List.of("text")),
                        Query.field("is_seen").equalsValue(Boolean.FALSE),
                        Query.field("is_urgent").equalsValue(Boolean.TRUE)
                ))
                .build();
        final RecordQueryPlan plan;

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);

            plan = planner.plan(recordQuery);
            assertTrue(plan instanceof LuceneIndexQueryPlan, "just using Lucene");

            final List<Long> results = getRecordPrimaryKeys(plan);
            assertEquals(List.of(101L), results);
        }
        assertEquals(0, timer.getCount(FDBStoreTimer.Counts.QUERY_DISCARDED), "no records filtered after Lucene");

        timer.reset();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final TestRecordsTextProto.FlaggedDocument.Builder recordBuilder = TestRecordsTextProto.FlaggedDocument.newBuilder();
            recordBuilder.mergeFrom(recordStore.loadRecord(Tuple.from(103)).getRecord());
            recordBuilder.setIsSeen(false);
            recordStore.saveRecord(recordBuilder.build());
            context.commit();
        }
        assertEquals(0, timer.getCount(LuceneEvents.Events.LUCENE_ADD_DOCUMENT), "no document changes for mutable field update");

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);

            final List<Long> results = getRecordPrimaryKeys(plan);
            assertEquals(List.of(101L, 103L), results);
        }
    }

    private List<Long> getRecordPrimaryKeys(final RecordQueryPlan plan) {
        return recordStore.executeQuery(plan)
                .map(r -> TestRecordsTextProto.FlaggedDocument.newBuilder().mergeFrom(r.getRecord()).getDocId())
                .asList().join();
    }
}
