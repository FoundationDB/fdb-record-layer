/*
 * FDBRecordStoreIndexPrefetchOldVersionTest.java
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

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test Remote Fetch with old version format.
 */
@Tag(Tags.RequiresFDB)
public class RemoteFetchOldVersionTest extends RemoteFetchTestBase {

    @BeforeEach
    void setup() throws Exception {
        complexQuerySetupWithVersion(simpleVersionHook, FDBRecordStore.SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION - 1);
    }

    @ParameterizedTest
    @MethodSource("testedParams")
    void oldVersionFormatTest(IndexFetchMethod fetchMethod, IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        RecordQueryPlan plan = plan(NUM_VALUES_LARGER_THAN_990, fetchMethod, indexEntryReturnPolicy);

        int count = 0;
        try (FDBRecordContext context = openContext()) {
            openStoreWithVersion(context, simpleVersionHook, FDBRecordStore.SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION - 1);
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, null, ExecuteProperties.SERIAL_EXECUTE).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> record = cursor.next();
                    long primaryKey = 9 - count;
                    String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
                    int numValue = 1000 - (int)primaryKey;
                    // Use "ALL" as the index return policy since for old versions the code will fall back to ALL
                    assertRecord(record, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, fetchMethod, IndexEntryReturnPolicy.ALL);
                    count++;
                }
            }
        }
        assertThat(count, equalTo(10));
    }

    @Nonnull
    protected final RecordMetaDataHook simpleVersionHook = metaDataBuilder -> {
        metaDataBuilder.setSplitLongRecords(false);
        metaDataBuilder.addUniversalIndex(new Index("globalCount", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
        metaDataBuilder.addIndex("MySimpleRecord", new Index("MySimpleRecord$num2-version", concat(field("num_value_2"), VersionKeyExpression.VERSION), IndexTypes.VERSION));
        metaDataBuilder.addUniversalIndex(
                new Index("globalVersion", VersionKeyExpression.VERSION, IndexTypes.VERSION));
    };

    protected void complexQuerySetupWithVersion(RecordMetaDataHook hook, int formatVersion) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openStoreWithVersion(context, hook, formatVersion);

            for (int i = 0; i < 100; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i);
                recBuilder.setStrValueIndexed((i & 1) == 1 ? "odd" : "even");
                recBuilder.setNumValueUnique(1000 - i);
                recBuilder.setNumValue2(i % 3);
                recBuilder.setNumValue3Indexed(i % 5);

                for (int j = 0; j < i % 10; j++) {
                    recBuilder.addRepeater(j);
                }
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }
    }

    // TODO: This can be combined with the existing openStore such that it takes a version (and resets it in @AfterEach).
    protected void openStoreWithVersion(final FDBRecordContext context, @Nullable RecordMetaDataHook hook, int formatVersion) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        if (hook != null) {
            hook.apply(metaDataBuilder);
        }

        recordStore = FDBRecordStore.newBuilder()
                .setMetaDataProvider(metaDataBuilder)
                .setContext(context)
                .setKeySpacePath(path)
                // .setSubspace(subspace)
                .setFormatVersion(formatVersion)
                .createOrOpen();
        RecordMetaData metaData = recordStore.getRecordMetaData();
        planner = new RecordQueryPlanner(metaData, recordStore.getRecordStoreState());
    }
}
