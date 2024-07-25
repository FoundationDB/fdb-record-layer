/*
 * FDBRecordStoreIndexPrefetchSplitRecordsTest.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.CursorStreamingMode;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;

import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_WITH_NAMES_PLACEHOLDER;

/**
 * A test for the Remote Fetch with large records that are split (more than just the version split).
 */
@Tag(Tags.RequiresFDB)
class RemoteFetchSplitRecordsTest extends RemoteFetchTestBase {

    @BeforeEach
    void setup() throws Exception {
        complexQuerySetup(simpleMetadataHook);
    }

    @ParameterizedTest(name = "indexPrefetchSplitRecordTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    void indexPrefetchSplitRecordTest(IndexFetchMethod useIndexPrefetch) throws Exception {
        saveLargeRecord(1, 200, 2000);
        RecordQueryPlan plan = plan(NUM_VALUES_LARGER_THAN_990, useIndexPrefetch);

        executeAndVerifyData(plan, 11, (rec, i) -> {
            if (i < 10) {
                int primaryKey = 9 - i;
                String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
                int numValue = 1000 - primaryKey;
                assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey);
            } else {
                // this is the extra, split, record
                assertRecord(rec, 200, "even", 2000, "MySimpleRecord$num_value_unique", (long)2000, 0);
            }
        }, simpleMetadataHook);
    }

    @ParameterizedTest(name = "indexPrefetchSplitRecordReverseTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    void indexPrefetchSplitRecordReverseTest(IndexFetchMethod useIndexPrefetch) throws Exception {
        saveLargeRecord(1, 200, 2000);
        RecordQueryPlan plan = plan(NUM_VALUES_LARGER_THAN_990_REVERSE, useIndexPrefetch);

        executeAndVerifyData(plan, 11, (rec, i) -> {
            if (i > 0) {
                int primaryKey = i - 1;
                String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
                int numValue = 1000 - primaryKey;
                assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, primaryKey);
            } else {
                // this is the extra, split, record
                assertRecord(rec, 200, "even", 2000, "MySimpleRecord$num_value_unique", (long)2000, 0);
            }
        }, simpleMetadataHook);
    }

    @Tag(Tags.Slow)
    @ParameterizedTest(name = "indexPrefetchManySplitRecordTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("fetchMethodAndStreamMode")
    void indexPrefetchManySplitRecordTest(IndexFetchMethod useIndexPrefetch, CursorStreamingMode streamingMode) throws Exception {
        // TODO: This test actually runs the API in a way that returns results that are too large: Over 50MB
        // FDB will fix the issue to limit the bytes returned and then this test would need to adjust accordingly.
        int numTransactions = 8;
        int numRecordsPerTransaction = 6;
        for (int i = 0; i < numTransactions; i++) {
            int firstPrimaryKey = 200 + (i * numRecordsPerTransaction);
            int numValueUniqueOffset = 2000 - (i * numRecordsPerTransaction);
            saveLargeRecord(numRecordsPerTransaction, firstPrimaryKey, numValueUniqueOffset);
        }
        RecordQueryPlan plan = plan(NUM_VALUES_LARGER_THAN_1000_REVERSE, useIndexPrefetch);

        executeAndVerifyData(plan, null, serializableWithStreamingMode(streamingMode), numRecordsPerTransaction * numTransactions, (rec, i) -> {
            int primaryKey = 200 + i;
            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
            int numValue = 2000 - i;
            int localVersion = i % numRecordsPerTransaction;
            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, localVersion);
        }, simpleMetadataHook);
    }

    private void saveLargeRecord(final int numRecords, final int firstPrimaryKey, final int numValueUniqueOffset) {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, simpleMetadataHook);
            for (int i = 0; i < numRecords; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(firstPrimaryKey + i);
                recBuilder.setStrValueIndexed((i & 1) == 1 ? "odd" : "even");
                recBuilder.setNumValueUnique(numValueUniqueOffset - i);
                recBuilder.setNumValue2(i % 3);
                recBuilder.setNumValue3Indexed(numValueUniqueOffset % 5);

                for (int j = 0; j < 100000; j++) {
                    recBuilder.addRepeater(j);
                }
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }
    }

    @Nonnull
    private final RecordMetaDataHook simpleMetadataHook = metaDataBuilder -> {
        // UseSplitRecords can be set to different values to impact the way the store is opened
        metaDataBuilder.setSplitLongRecords(true);
    };
}
