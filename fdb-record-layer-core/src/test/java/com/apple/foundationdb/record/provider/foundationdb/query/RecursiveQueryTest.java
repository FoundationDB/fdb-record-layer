/*
 * RecursiveQueryTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsParentChildRelationshipProto;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.ObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryRecursivePlan;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Recursive query experiments.
 */
@Tag(Tags.RequiresFDB)
public class RecursiveQueryTest extends FDBRecordStoreQueryTestBase {

    @Test
    public void testUp() throws Exception {
        loadRecords();

        final RecordQueryPlan plan;
        if (planner instanceof RecordQueryPlanner) {
            plan = buildAncestorPlan((RecordQueryPlanner)planner);
        } else {
            plan = null;
            fail("unsupported planner type");
        }

        final List<String> actual = queryNames(plan);
        final List<String> expected = List.of("three", "two", "root");
        assertEquals(expected, actual);
    }

    private RecordQueryPlan buildAncestorPlan(@Nonnull RecordQueryPlanner planner) throws Exception {
        final RecordQuery rootQuery = RecordQuery.newBuilder()
                .setRecordType("MyChildRecord")
                .setFilter(Query.field("str_value").equalsValue("three"))
                .build();
        final RecordQuery childQuery = RecordQuery.newBuilder()
                .setRecordType("MyChildRecord")
                .setFilter(Query.field("rec_no").equalsParameter("parent_rec_no"))
                .build();
        return recursiveQuery(planner, rootQuery, childQuery, "parent_rec_no");
    }

    @Test
    public void testDown() throws Exception {
        loadRecords();

        final RecordQueryPlan plan;
        if (planner instanceof RecordQueryPlanner) {
            plan = buildDescendantPlan((RecordQueryPlanner)planner);
        } else {
            plan = null;
            fail("unsupported planner type");
        }

        final List<String> actual = queryNames(plan);
        final List<String> expected = List.of("root", "two", "three", "five", "four");
        assertEquals(expected, actual);
    }

    private static RecordQueryPlan buildDescendantPlan(@Nonnull RecordQueryPlanner planner) throws Exception {
        final RecordQuery rootQuery = RecordQuery.newBuilder()
                .setRecordType("MyChildRecord")
                .setFilter(Query.field("parent_rec_no").isNull())
                .build();
        final RecordQuery childQuery = RecordQuery.newBuilder()
                .setRecordType("MyChildRecord")
                .setFilter(Query.field("parent_rec_no").equalsParameter("rec_no"))
                .build();

        return recursiveQuery(planner, rootQuery, childQuery, "rec_no");
    }

    private void openParentChildRecordStore(FDBRecordContext context) throws Exception {
        final RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsParentChildRelationshipProto.getDescriptor());
        createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
    }

    private void loadRecords() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openParentChildRecordStore(context);

            saveRecord(1, -1, "root");
            saveRecord(2, 1, "two");
            saveRecord(3, 2, "three");
            saveRecord(4, 1, "four");
            saveRecord(5, 2, "five");

            context.commit();
        }
    }

    private void saveRecord(int recNo, int parent, String name) {
        TestRecordsParentChildRelationshipProto.MyChildRecord.Builder builder = TestRecordsParentChildRelationshipProto.MyChildRecord.newBuilder();
        builder.setRecNo(recNo);
        if (parent > 0) {
            builder.setParentRecNo(parent);
        }
        builder.setStrValue(name);
        recordStore.saveRecord(builder.build());
    }

    private List<String> queryNames(RecordQueryPlan plan) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openParentChildRecordStore(context);
            return plan.execute(recordStore).map(result -> {
                TestRecordsParentChildRelationshipProto.MyChildRecord record = TestRecordsParentChildRelationshipProto.MyChildRecord.newBuilder()
                        .mergeFrom(result.getRecord())
                        .build();
                return record.getStrValue();
            }).asList().join();
        }
    }

    private static RecordQueryPlan recursiveQuery(@Nonnull QueryPlanner planner,
                                                  @Nonnull RecordQuery rootQuery, @Nonnull RecordQuery childQuery,
                                                  @Nonnull String glueField) throws Exception {
        RecordQueryPlan rootPlan = planner.plan(rootQuery);
        // Bit of a mess making up for a proper Expression, but also allowing use of old planner.
        CorrelationIdentifier childId = CorrelationIdentifier.of("child");
        final CorrelationIdentifier priorId = CorrelationIdentifier.of("prior_" + childId.getId());
        RecordQueryPlan childPlan = new GluePlan(planner.plan(childQuery), priorId, glueField);
        final Quantifier.Physical rootQuantifier = Quantifier.physical(Reference.of(rootPlan));
        final Quantifier.Physical childQuantifier = Quantifier.physical(Reference.of(childPlan), childId);
        return new RecordQueryRecursivePlan(
                rootQuantifier,
                childQuantifier,
                ObjectValue.of(childQuantifier.getAlias(), childQuantifier.getFlowedObjectType()),
                true);
    }

    static class GluePlan implements RecordQueryPlan {
        private final RecordQueryPlan inner;
        private final CorrelationIdentifier resultBinding;
        private final String fieldName;

        public GluePlan(final RecordQueryPlan inner, final CorrelationIdentifier resultBinding, final String fieldName) {
            this.inner = inner;
            this.resultBinding = resultBinding;
            this.fieldName = fieldName;
        }

        @Nonnull
        @Override
        public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context, @Nullable final byte[] continuation, @Nonnull final ExecuteProperties executeProperties) {
            final QueryResult result = (QueryResult)context.getBinding(resultBinding);
            final Message record = result.getQueriedRecord().getRecord();
            final Object fieldValue = record.getField(record.getDescriptorForType().findFieldByName(fieldName));
            final EvaluationContext newContext = context.withBinding(fieldName, fieldValue);
            return inner.executePlan(store, newContext, continuation, executeProperties);
        }

        @Nonnull
        @Override
        public List<RecordQueryPlan> getChildren() {
            return List.of(inner);
        }

        @Nonnull
        @Override
        public AvailableFields getAvailableFields() {
            return inner.getAvailableFields();
        }

        @Nonnull
        @Override
        public Message toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return inner.toProto(serializationContext);
        }

        @Nonnull
        @Override
        public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
            return inner.toRecordQueryPlanProto(serializationContext);
        }

        @Nonnull
        @Override
        public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
            return inner.rewritePlannerGraph(childGraphs);
        }

        @Override
        public boolean isReverse() {
            return inner.isReverse();
        }

        @Override
        public boolean hasRecordScan() {
            return inner.hasRecordScan();
        }

        @Override
        public boolean hasFullRecordScan() {
            return inner.hasFullRecordScan();
        }

        @Override
        public boolean hasIndexScan(@Nonnull final String indexName) {
            return inner.hasIndexScan(indexName);
        }

        @Nonnull
        @Override
        public Set<String> getUsedIndexes() {
            return inner.getUsedIndexes();
        }

        @Override
        public boolean hasLoadBykeys() {
            return inner.hasLoadBykeys();
        }

        @Override
        public void logPlanStructure(final StoreTimer timer) {
            inner.logPlanStructure(timer);
        }

        @Override
        public int getComplexity() {
            return inner.getComplexity();
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode hashMode) {
            return inner.planHash(hashMode);
        }

        @Nonnull
        @Override
        public Value getResultValue() {
            return inner.getResultValue();
        }

        @Nonnull
        @Override
        public List<? extends Quantifier> getQuantifiers() {
            return inner.getQuantifiers();
        }

        @Override
        public boolean equalsWithoutChildren(@Nonnull final RelationalExpression other, @Nonnull final AliasMap equivalences) {
            return inner.equalsWithoutChildren(other, equivalences);
        }

        @Override
        public int hashCodeWithoutChildren() {
            return inner.hashCodeWithoutChildren();
        }

        @Nonnull
        @Override
        public RelationalExpression translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
            return inner.translateCorrelations(translationMap, translatedQuantifiers);
        }

        @Nonnull
        @Override
        public Set<CorrelationIdentifier> getCorrelatedTo() {
            return inner.getCorrelatedTo();
        }
    }

}
