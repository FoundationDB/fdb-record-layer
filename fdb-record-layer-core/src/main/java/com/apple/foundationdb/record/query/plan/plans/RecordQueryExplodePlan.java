/*
 * RecordQueryExplodePlan.java
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.planprotos.PRecordQueryExplodePlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.PlanStringRepresentation;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Memoizer;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A query plan that applies the values it contains over the incoming ones. In a sense, this is similar to the {@code Stream.map()}
 * method: Mapping one {@link Value} to another.
 */
@API(API.Status.INTERNAL)
public class RecordQueryExplodePlan implements RecordQueryPlanWithNoChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Explode-Plan");

    @Nonnull
    private final Value collectionValue;

    public RecordQueryExplodePlan(@Nonnull Value collectionValue) {
        this.collectionValue = collectionValue;
    }

    @Nonnull
    public Value getCollectionValue() {
        return collectionValue;
    }

    @SuppressWarnings("resource")
    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        final var result = collectionValue.eval(store, context);
        return RecordCursor.fromList(result == null ? ImmutableList.of() : (List<?>)result, continuation)
                .map(QueryResult::ofComputed);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return collectionValue.getCorrelatedTo();
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public RecordQueryExplodePlan translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        final Value translatedCollectionValue = collectionValue.translateCorrelations(translationMap);
        if (translatedCollectionValue != collectionValue) {
            return new RecordQueryExplodePlan(translatedCollectionValue);
        }
        return this;
    }

    @Override
    public boolean isReverse() {
        return false;
    }

    @Override
    public RecordQueryExplodePlan strictlySorted(@Nonnull final Memoizer memoizer) {
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
    public boolean hasIndexScan(@Nonnull final String indexName) {
        return false;
    }

    @Nonnull
    @Override
    public Set<String> getUsedIndexes() {
        return ImmutableSet.of();
    }

    @Override
    public boolean hasLoadBykeys() {
        return false;
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.NO_FIELDS;
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        Verify.verify(collectionValue.getResultType().isArray());

        return new QueriedValue(Objects.requireNonNull(((Type.Array)collectionValue.getResultType()).getElementType()));
    }

    @Nonnull
    @Override
    public Set<Type> getDynamicTypes() {
        return collectionValue.getDynamicTypes();
    }


    @Nonnull
    @Override
    public String toString() {
        return PlanStringRepresentation.toString(this);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        final var otherExplodePlan =  (RecordQueryExplodePlan)otherExpression;

        return collectionValue.semanticEquals(otherExplodePlan.getCollectionValue(), equivalencesMap) &&
               semanticEqualsForResults(otherExpression, equivalencesMap);
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
    public int hashCodeWithoutChildren() {
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
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, getResultValue());
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.VALUE_COMPUTATION_OPERATOR,
                        ImmutableList.of("EXPLODE {{expr}}"),
                        ImmutableMap.of("expr", Attribute.gml(collectionValue.toString()))),
                childGraphs);
    }

    @Nonnull
    @Override
    public PRecordQueryExplodePlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryExplodePlan.newBuilder()
                .setCollectionValue(collectionValue.toValueProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setExplodePlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQueryExplodePlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                   @Nonnull final PRecordQueryExplodePlan recordQueryRangePlanProto) {
        return new RecordQueryExplodePlan(Value.fromValueProto(serializationContext, Objects.requireNonNull(recordQueryRangePlanProto.getCollectionValue())));
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryExplodePlan, RecordQueryExplodePlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryExplodePlan> getProtoMessageClass() {
            return PRecordQueryExplodePlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryExplodePlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                @Nonnull final PRecordQueryExplodePlan recordQueryExplodePlanProto) {
            return RecordQueryExplodePlan.fromProto(serializationContext, recordQueryExplodePlanProto);
        }
    }
}
