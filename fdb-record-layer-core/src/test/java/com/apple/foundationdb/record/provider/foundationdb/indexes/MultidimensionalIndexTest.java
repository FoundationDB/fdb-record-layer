/*
 * RankIndexTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsMultidimensionalProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.DimensionsKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for {@code M} type indexes.
 */
@Tag(Tags.RequiresFDB)
class MultidimensionalIndexTest extends FDBRecordStoreQueryTestBase {

    protected void openRecordStore(FDBRecordContext context) throws Exception {
        openRecordStore(context, NO_HOOK);
    }

    protected void openRecordStore(final FDBRecordContext context, final RecordMetaDataHook hook) throws Exception {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsMultidimensionalProto.getDescriptor());
        metaDataBuilder.addIndex("MyMultidimensionalRecord",
                new Index("XYByStrValue", DimensionsKeyExpression.of(field("str_value_indexed"),
                        concat(field("num_value_x"), field("num_value_y")),
                                concat(field("num_value_x"), field("num_value_y"))),
                        IndexTypes.MULTIDIMENSIONAL));
        hook.apply(metaDataBuilder);
        createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
    }

    static final Object[][] RECORDS = new Object[][] {
        { 1L, "achilles", 100L, 100L },
        { 2L, "helen", 200L, 200L },
        { 3L, "hector", 10L, 50L },
        { 4L, "penelope", 200L, -3L },
        { 5L, "laodice", 300L, 5000L }
    };

    @BeforeEach
    public void loadRecords() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            for (Object[] rec : RECORDS) {
                recordStore.saveRecord(TestRecordsMultidimensionalProto.MyMultidimensionalRecord.newBuilder()
                        .setRecNo((Long) rec[0])
                        .setStrValueIndexed((String) rec[1])
                        .setNumValueX((Long) rec[2])
                        .setNumValueY((Long) rec[3])
                        .build());
            }
            commit(context);
        }
    }

    @Test
    void basicRead() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            FDBStoredRecord<Message> rec = recordStore.loadRecord(Tuple.from(1L));
            assertNotNull(rec);
            TestRecordsMultidimensionalProto.MyMultidimensionalRecord.Builder myrec = TestRecordsMultidimensionalProto.MyMultidimensionalRecord.newBuilder();
            myrec.mergeFrom(rec.getRecord());
            assertEquals("achilles", myrec.getStrValueIndexed());
            commit(context);
        }
    }
}
