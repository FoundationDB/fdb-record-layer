/*
 * RecordQueryTableFunctionPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryTableFunctionPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.FinalMemoizer;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.ExplainPlanVisitor;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.AbstractRelationalExpressionWithoutChildren;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.StreamingValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;

import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A query plan that delegates its execution to a table-valued {@link StreamingValue}.
 */
@API(API.Status.INTERNAL)
public class RecordQueryTableFunctionPlan extends AbstractRelationalExpressionWithoutChildren implements RecordQueryPlanWithNoChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Table-Function-Plan");

    private final StreamingValue value;

    public RecordQueryTableFunctionPlan(final StreamingValue collectionValue) {
        this.value = collectionValue;
    }

    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(final FDBRecordStoreBase<M> store,
                                                                     final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     final ExecuteProperties executeProperties) {
        return value.evalAsStream(store, context, continuation, executeProperties);
    }

    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return value.getCorrelatedTo();
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public RecordQueryTableFunctionPlan translateCorrelations(final TranslationMap translationMap,
                                                              final boolean shouldSimplifyValues,
                                                              final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.isEmpty());
        if (translationMap.definesOnlyIdentities()) {
            return this;
        }
        final Value translatedValue = value.translateCorrelations(translationMap, shouldSimplifyValues);
        if (translatedValue != value) {
            return new RecordQueryTableFunctionPlan((StreamingValue)translatedValue);
        }
        return this;
    }

    @Override
    public boolean isReverse() {
        return false;
    }

    @Override
    public RecordQueryTableFunctionPlan strictlySorted(final FinalMemoizer memoizer) {
        return this;
    }

    @Override
    public boolean hasRecordScan() {
        return false;
    }

    @Override
    public boolean hasFullRecordScan() {
        return false;
    }

    @Override
    public boolean hasIndexScan(final String indexName) {
        return false;
    }

    @Override
    public Set<String> getUsedIndexes() {
        return ImmutableSet.of();
    }

    @Override
    public boolean hasLoadBykeys() {
        return false;
    }

    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.NO_FIELDS;
    }

    @Override
    public Value getResultValue() {
        return new QueriedValue(value.getResultType());
    }

    public StreamingValue getValue() {
        return value;
    }

    @Override
    public Set<Type> getDynamicTypes() {
        return value.getDynamicTypes();
    }


    @Override
    public String toString() {
        return ExplainPlanVisitor.toStringForDebugging(this);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(final RelationalExpression otherExpression,
                                         final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        final var otherTableFunctionPlan =  (RecordQueryTableFunctionPlan)otherExpression;

        return value.semanticEquals(otherTableFunctionPlan.value, equivalencesMap);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return structuralEquals(other);
    }

    @Override
    public int hashCode() {
        return structuralHashCode();
    }

    @Override
    public int computeHashCodeWithoutChildren() {
        return Objects.hash(getResultValue());
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        // nothing to increment
    }

    @Override
    public int getComplexity() {
        return 1;
    }

    @Override
    public int planHash(final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, getResultValue());
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public PlannerGraph rewritePlannerGraph(final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.VALUE_COMPUTATION_OPERATOR,
                        ImmutableList.of("TFUNC {{expr}}"),
                        ImmutableMap.of("expr", Attribute.gml(value.toString()))),
                childGraphs);
    }

    @Override
    public PRecordQueryTableFunctionPlan toProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryTableFunctionPlan.newBuilder()
                .setValue(value.toValueProto(serializationContext))
                .build();
    }

    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setTableFunctionPlan(toProto(serializationContext)).build();
    }

    public static RecordQueryTableFunctionPlan fromProto(final PlanSerializationContext serializationContext,
                                                         final PRecordQueryTableFunctionPlan recordQueryTableFunctionPlanProto) {
        final var value = Value.fromValueProto(serializationContext,
                Objects.requireNonNull(recordQueryTableFunctionPlanProto.getValue()));
        if (!(value instanceof StreamingValue)) {
            throw new RecordCoreException("invalid value, expecting streaming value").addLogInfo(LogMessageKeys.VALUE, value);
        }
        return new RecordQueryTableFunctionPlan((StreamingValue)value);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryTableFunctionPlan, RecordQueryTableFunctionPlan> {
        @Override
        public Class<PRecordQueryTableFunctionPlan> getProtoMessageClass() {
            return PRecordQueryTableFunctionPlan.class;
        }

        @Override
        public RecordQueryTableFunctionPlan fromProto(final PlanSerializationContext serializationContext,
                                                      final PRecordQueryTableFunctionPlan message) {
            return RecordQueryTableFunctionPlan.fromProto(serializationContext, message);
        }
    }
}

