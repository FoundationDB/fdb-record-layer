/*
 * FDBCoveringJoinIndexTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsGroupedParentChildProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.JoinedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests of covering plans over joined record types. A covering plan over a joined type with an outer constituent only
 * copies that constituent's fields out of the index entry if the constituent is present, which is the only place
 * where a {@link com.apple.foundationdb.record.query.plan.AvailableFields.ConditionalUponPathPredicate} is used.
 */
@Tag(Tags.RequiresFDB)
class FDBCoveringJoinIndexTest extends FDBRecordStoreQueryTestBase {
    private static final String JOIN_TYPE = "JoinChildren";
    private static final String JOIN_INDEX = "ACoveringJoinIndex";
    private static final String PARENT_TYPE = "MyParentRecord";
    private static final String CHILD_TYPE = "MyChildRecord";

    private static void addJoinIndex(@Nonnull RecordMetaDataBuilder builder) {
        final KeyExpression primaryKey = Key.Expressions.concatenateFields("group", "rec_no");
        builder.getRecordType(PARENT_TYPE).setPrimaryKey(primaryKey);
        builder.getRecordType(CHILD_TYPE).setPrimaryKey(primaryKey);

        final JoinedRecordTypeBuilder joinBuilder = builder.addJoinedRecordType(JOIN_TYPE);
        joinBuilder.addConstituent("parent", builder.getRecordType(PARENT_TYPE));
        joinBuilder.addConstituent("child", builder.getRecordType(CHILD_TYPE));
        joinBuilder.addJoin("parent", Key.Expressions.field("group"), "child", Key.Expressions.field("group"));
        joinBuilder.addJoin("parent", Key.Expressions.field("child_rec_no"), "child", Key.Expressions.field("rec_no"));

        builder.addIndex(JOIN_TYPE, new Index(JOIN_INDEX, Key.Expressions.concat(
                Key.Expressions.field("parent").nest("group"),
                Key.Expressions.field("parent").nest("int_value"),
                Key.Expressions.field("child").nest("other_value"))));
    }

    private static PRecordQueryPlan serialize(@Nonnull final RecordQueryPlan plan) {
        return plan.toRecordQueryPlanProto(
                new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION));
    }

    @Test
    void coveringPlanOverOuterJoinSurvivesSerialization() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openAnyRecordStore(TestRecordsGroupedParentChildProto.getDescriptor(), context,
                    FDBCoveringJoinIndexTest::addJoinIndex);
            recordStore.saveRecord(child(1, 5, 500));
            recordStore.saveRecord(parent(1, 1, 10, 5));
            // The parent has no matching child, so the child constituent is null in the join and absent from the index entry
            recordStore.saveRecord(parent(1, 2, 20, 99));
            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            openAnyRecordStore(TestRecordsGroupedParentChildProto.getDescriptor(), context,
                    FDBCoveringJoinIndexTest::addJoinIndex);
            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(JOIN_TYPE)
                    .setFilter(Query.field("parent").matches(Query.field("group").equalsValue(1L)))
                    .setRequiredResults(List.of(
                            Key.Expressions.field("parent").nest("int_value"),
                            Key.Expressions.field("child").nest("other_value")))
                    .build();

            // planQuery only round-trips plans from the Cascades planner, so the plan below is not already
            // deserialized and the comparison against its round-tripped form is meaningful
            assertThat(planner).isInstanceOf(RecordQueryPlanner.class);
            final RecordQueryPlan plan = planQuery(query);
            assertThat(plan).isInstanceOf(RecordQueryCoveringIndexPlan.class);

            final RecordQueryPlan deserialized = verifySerialization(plan);
            assertThat(deserialized).isInstanceOf(RecordQueryCoveringIndexPlan.class);
            // Neither the plan hash nor structural equality looks at the copiers that build the partial record, so
            // compare the serialized forms to be sure their copy-if predicates survived.
            assertThat(serialize(deserialized)).isEqualTo(serialize(plan));

            assertThat(run(plan)).containsExactly(Tuple.from(10L, 500L));
            assertThat(run(deserialized)).containsExactly(Tuple.from(10L, 500L));
        }
    }

    /**
     * Run a plan and return, for each result, the parent's {@code int_value} and the child's {@code other_value}, the
     * latter being {@code null} if the child constituent is missing from the result.
     */
    @Nonnull
    private List<Tuple> run(@Nonnull final RecordQueryPlan plan) {
        try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = executeQuery(plan)) {
            final List<Tuple> results = new ArrayList<>();
            while (cursor.hasNext()) {
                final Message record = cursor.next().getRecord();
                final Descriptors.Descriptor descriptor = record.getDescriptorForType();
                final Message parent = (Message)record.getField(descriptor.findFieldByName("parent"));
                final Descriptors.FieldDescriptor childField = descriptor.findFieldByName("child");
                final Long otherValue = record.hasField(childField)
                                        ? (Long)((Message)record.getField(childField)).getField(
                                                childField.getMessageType().findFieldByName("other_value"))
                                        : null;
                results.add(Tuple.from(parent.getField(parent.getDescriptorForType().findFieldByName("int_value")), otherValue));
            }
            return results;
        }
    }

    private static TestRecordsGroupedParentChildProto.MyParentRecord parent(long group, long recNo, int intValue, long childRecNo) {
        return TestRecordsGroupedParentChildProto.MyParentRecord.newBuilder()
                .setGroup(group).setRecNo(recNo).setIntValue(intValue).setChildRecNo(childRecNo).build();
    }

    private static TestRecordsGroupedParentChildProto.MyChildRecord child(long group, long recNo, long otherValue) {
        return TestRecordsGroupedParentChildProto.MyChildRecord.newBuilder()
                .setGroup(group).setRecNo(recNo).setOtherValue(otherValue).build();
    }
}
