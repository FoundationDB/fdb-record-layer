/*
 * OnlineIndexerConflictsTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link OnlineIndexer}. Checking different db manipulations during the indexing process.
 */

@Tag(Tags.RequiresFDB)
public class OnlineIndexerConflictsTest extends OnlineIndexerTest {

    @Test
    void testAddRecordToRangeWhileIndexedIdempotent() {

        List<TestRecords1Proto.MySimpleRecord> records =
                LongStream.range(0, 20).mapToObj(val -> TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2((int)val + 1).build()
                ).collect(Collectors.toList());

        Index index = new Index("newIndex", field("num_value_2"), IndexTypes.VALUE);
        FDBRecordStoreTestBase.RecordMetaDataHook hookAdd = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);

        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            for (int i = 2; i <= 8; i++) {
                // even numbers from 4 to 16
                recordStore.saveRecord(records.get(i * 2));
            }
            context.commit();
        }
        openSimpleMetaData(hookAdd);
        try (FDBRecordContext context = openContext()) {
            context.commit();
        }

        int[] inserts = {2, 5, 11, 17, 15};
        for (int i = 0; i < inserts.length; i++) {
            int record_i = inserts[i];

            try (FDBRecordContext context1 = openContext()) {
                try (OnlineIndexer indexer =
                             OnlineIndexer.newBuilder()
                                     .setRecordStore(recordStore)
                                     .setIndex("newIndex")
                                     .build()) {
                    indexer.rebuildIndex(recordStore);
                    try (FDBRecordContext context2 = openContext()) {
                        recordStore.saveRecord(records.get(record_i));
                        // This record might be added in the indexer's range, but the transaction still commits because it doesn't
                        // change any existing records.
                        context2.commit();
                    }
                    context1.commit();
                }
            }

            try (FDBRecordContext context = openContext()) {
                recordStore.clearAndMarkIndexWriteOnly(index).join();
                context.commit();
            }
        }
    }

    @Test
    public void testAddRecordToRangeWhileIndexedOtherType() {

        final List<TestRecords1Proto.MySimpleRecord> records =
                LongStream.range(0, 7).mapToObj(val -> TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val * 2).setNumValue2((int)val + 1).build()
                ).collect(Collectors.toList());

        final List<TestRecords1Proto.MyOtherRecord> otherRecords =
                LongStream.range(0, 7).mapToObj(val -> TestRecords1Proto.MyOtherRecord.newBuilder().setRecNo(val * 2 + 1).setNumValue2((int)val + 1).build()
                ).collect(Collectors.toList());

        final List<TestRecords1Proto.MyOtherRecord> otherRecordsOverwrite =
                LongStream.range(0, 7).mapToObj(val -> TestRecords1Proto.MyOtherRecord.newBuilder().setRecNo(val * 2 + 1).setNumValue2((int)val + 101).build()
                ).collect(Collectors.toList());

        Index index = new Index("newIndex", field("num_value_2"), IndexTypes.VALUE);
        FDBRecordStoreTestBase.RecordMetaDataHook hookAdd = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);

        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            records.forEach(recordStore::saveRecord);
            otherRecords.forEach(recordStore::saveRecord);
            context.commit();
        }
        openSimpleMetaData(hookAdd);
        try (FDBRecordContext context = openContext()) {
            context.commit();
        }

        otherRecordsOverwrite.forEach(rec -> {

            try (FDBRecordContext context1 = openContext()) {
                try (OnlineIndexer indexer =
                             OnlineIndexer.newBuilder()
                                     .setRecordStore(recordStore)
                                     .setIndex("newIndex")
                                     .build()) {
                    indexer.rebuildIndex(recordStore);
                    try (FDBRecordContext context2 = openContext()) {
                        recordStore.saveRecord(rec);
                        // This record's type is different than the indexer's, so both commits should succeed
                        context2.commit();
                    }
                    context1.commit();
                }
            }

            try (FDBRecordContext context = openContext()) {
                recordStore.clearAndMarkIndexWriteOnly(index).join();
                context.commit();
            }
        });
    }

    @Test
    void testModifyRecordInRangeWhileIndexedIdempotentFailure() {

        List<TestRecords1Proto.MySimpleRecord> records =
                LongStream.range(0, 20).mapToObj(val -> TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2((int)val + 1).build()
                ).collect(Collectors.toList());

        Index index = new Index("newIndex", field("num_value_2"), IndexTypes.VALUE);
        FDBRecordStoreTestBase.RecordMetaDataHook hookAdd = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);

        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            for (int i = 2; i <= 8; i++) {
                // even numbers from 4 to 16
                recordStore.saveRecord(records.get(i * 2));
            }
            context.commit();
        }
        openSimpleMetaData(hookAdd);
        try (FDBRecordContext context = openContext()) {
            context.commit();
        }

        int[] inserts = {10, 4, 16};
        for (int record_i : inserts) {
            try (FDBRecordContext context1 = openContext()) {
                try (OnlineIndexer indexer =
                             OnlineIndexer.newBuilder()
                                     .setRecordStore(recordStore)
                                     .setIndex("newIndex")
                                     .build()) {
                    indexer.rebuildIndex(recordStore);
                    try (FDBRecordContext context2 = openContext()) {
                        recordStore.saveRecord(records.get(record_i));
                        context2.commit();
                    }
                    assertThrows(FDBExceptions.FDBStoreTransactionConflictException.class, context1::commit);
                }
            }

            try (FDBRecordContext context = openContext()) {
                recordStore.clearAndMarkIndexWriteOnly(index).join();
                context.commit();
            }
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    void testAddRecordOutsideRangeWhileIndexedIdempotent() {

        List<TestRecords1Proto.MySimpleRecord> records =
                LongStream.range(0, 20).mapToObj(val -> TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2((int)val + 1).build()
                ).collect(Collectors.toList());

        Index index = new Index("newIndex", field("num_value_2"), IndexTypes.VALUE);
        FDBRecordStoreTestBase.RecordMetaDataHook hookAdd = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);

        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            for (int i = 2; i <= 8; i++) {
                // even numbers from 4 to 16
                recordStore.saveRecord(records.get(i * 2));
            }
            context.commit();
        }
        openSimpleMetaData(hookAdd);
        try (FDBRecordContext context = openContext()) {
            context.commit();
        }

        int[] inserts = {2, 1, 18, 19};
        for (int record_i : inserts) {
            try (FDBRecordContext context1 = openContext()) {
                try (OnlineIndexer indexer =
                             OnlineIndexer.newBuilder()
                                     .setRecordStore(recordStore)
                                     .setIndex("newIndex")
                                     .build()) {
                    indexer.rebuildIndex(recordStore);
                    try (FDBRecordContext context2 = openContext()) {
                        recordStore.saveRecord(records.get(record_i));
                        // This record is added outside of the indexer's range
                        context2.commit();
                    }
                    context1.commit();
                }
            }

            try (FDBRecordContext context = openContext()) {
                recordStore.clearAndMarkIndexWriteOnly(index).join();
                context.commit();
            }
        }
    }

}
