/*
 * FDBSimpleJoinQueryTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.query;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TestRecordsParentChildRelationshipProto;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryLoadByKeysPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.TestHelpers.assertDiscardedNone;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests of the planning and execution of queries that produce an in-join plan.
 */
@Tag(Tags.RequiresFDB)
public class FDBSimpleJoinQueryTest extends FDBRecordStoreQueryTestBase {
    private void openJoinRecordStore(FDBRecordContext context) throws Exception {
        createOrOpenRecordStore(context, RecordMetaData.build(TestRecordsParentChildRelationshipProto.getDescriptor()));
    }

    /**
     * Verify that simple binding joins in parent/child relationships work.
     */
    @Test
    public void joinChildToParent() throws Exception {
        createJoinRecords(false);

        RecordQuery parentQuery = RecordQuery.newBuilder()
                .setRecordType("MyParentRecord")
                .setFilter(Query.field("str_value_indexed").equalsValue("even"))
                .build();
        RecordQuery childQuery = RecordQuery.newBuilder()
                .setRecordType("MyChildRecord")
                .setFilter(Query.field("parent_rec_no").equalsParameter("parent"))
                .build();
        RecordQueryPlan parentPlan = planQuery(parentQuery);
        RecordQueryPlan childPlan = planQuery(childQuery);

        try (FDBRecordContext context = openContext()) {
            openJoinRecordStore(context);
            RecordCursor<FDBQueriedRecord<Message>> childCursor = RecordCursor.flatMapPipelined(
                    ignore -> recordStore.executeQuery(parentPlan),
                    (rec, ignore) -> {
                        TestRecordsParentChildRelationshipProto.MyParentRecord.Builder parentRec =
                                TestRecordsParentChildRelationshipProto.MyParentRecord.newBuilder();
                        parentRec.mergeFrom(rec.getRecord());
                        EvaluationContext childContext = EvaluationContext.forBinding("parent", parentRec.getRecNo());
                        return childPlan.execute(recordStore, childContext);
                    }, null, 10);
            RecordCursor<String> resultsCursor = childCursor.map(rec -> {
                TestRecordsParentChildRelationshipProto.MyChildRecord.Builder childRec = TestRecordsParentChildRelationshipProto.MyChildRecord.newBuilder();
                childRec.mergeFrom(rec.getRecord());
                return childRec.getStrValue();
            });
            assertEquals(Arrays.asList("2.1", "2.2", "2.3", "4.1", "4.2", "4.3"), resultsCursor.asList().join());
            assertDiscardedNone(context);
        }
    }

    /**
     * Verify that simple binding joins in parent/child relationships work.
     */
    @Test
    public void joinParentToChild() throws Exception {
        createJoinRecords(true);

        RecordQuery parentQuery = RecordQuery.newBuilder()
                .setRecordType("MyParentRecord")
                .setFilter(Query.field("str_value_indexed").equalsValue("even"))
                .build();
        RecordQueryPlan parentPlan = planQuery(parentQuery);
        RecordQueryPlan childPlan = new RecordQueryLoadByKeysPlan("children");

        try (FDBRecordContext context = openContext()) {
            openJoinRecordStore(context);
            RecordCursor<FDBQueriedRecord<Message>> parentCursor = recordStore.executeQuery(parentPlan);
            RecordCursor<FDBQueriedRecord<Message>> childCursor = RecordCursor.flatMapPipelined(
                    ignore -> recordStore.executeQuery(parentPlan),
                    (rec, ignore) -> {
                        TestRecordsParentChildRelationshipProto.MyParentRecord.Builder parentRec =
                                TestRecordsParentChildRelationshipProto.MyParentRecord.newBuilder();
                        parentRec.mergeFrom(rec.getRecord());
                        EvaluationContext childContext = EvaluationContext.forBinding("children", parentRec.getChildRecNosList().stream()
                                .map(Tuple::from)
                                .collect(Collectors.toList()));
                        return childPlan.execute(recordStore, childContext);
                    }, null, 10);
            RecordCursor<String> resultsCursor = childCursor.map(rec -> {
                TestRecordsParentChildRelationshipProto.MyChildRecord.Builder childRec = TestRecordsParentChildRelationshipProto.MyChildRecord.newBuilder();
                childRec.mergeFrom(rec.getRecord());
                return childRec.getStrValue();
            });
            assertEquals(Arrays.asList("2.1", "2.2", "2.3", "4.1", "4.2", "4.3"), resultsCursor.asList().join());
            assertDiscardedNone(context);
        }
    }

    protected void createJoinRecords(boolean parentToChild) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openJoinRecordStore(context);

            for (int i = 1; i <= 4; i++) {
                TestRecordsParentChildRelationshipProto.MyParentRecord.Builder parentBuilder = TestRecordsParentChildRelationshipProto.MyParentRecord.newBuilder();
                parentBuilder.setRecNo(i);
                parentBuilder.setStrValueIndexed((i & 1) == 1 ? "odd" : "even");
                for (int j = 1; j <= 3; j++) {
                    TestRecordsParentChildRelationshipProto.MyChildRecord.Builder childBuilder = TestRecordsParentChildRelationshipProto.MyChildRecord.newBuilder();
                    childBuilder.setRecNo(i * 10 + j);
                    childBuilder.setStrValue(i + "." + j);
                    if (parentToChild) {
                        parentBuilder.addChildRecNos(childBuilder.getRecNo());
                    } else {
                        childBuilder.setParentRecNo(i);
                    }
                    recordStore.saveRecord(childBuilder.build());
                }
                recordStore.saveRecord(parentBuilder.build());
            }
            commit(context);
        }
    }
}

