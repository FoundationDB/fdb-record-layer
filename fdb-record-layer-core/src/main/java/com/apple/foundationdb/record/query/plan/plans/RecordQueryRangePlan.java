/*
 * RecordQueryRangePlan.java
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.cursors.RangeCursor;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryRangePlan;
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
 * A query plan that produced a stream of integers for {@code 0}(inclusive) to some {@code limit} (exclusive). It is
 * primarily used as a record producer for the translation of values-like constructs. It differs from its cousin
 * {@link RecordQueryExplodePlan} in a sense that it can provide records without materializing an array first.
 * Semantically, however, {@code explode([1, 2, 3, ..., n - 1])} produces the same result as {@code range(n)}.
 */
@API(API.Status.INTERNAL)
public class RecordQueryRangePlan implements RecordQueryPlanWithNoChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Range-Plan");

    private final Value exclusiveLimitValue;

    public RecordQueryRangePlan(final Value exclusiveLimitValue) {
        this.exclusiveLimitValue = exclusiveLimitValue;
    }

    public Value getExclusiveLimitValue() {
        return exclusiveLimitValue;
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return new QueriedValue(Type.primitiveType(Type.TypeCode.INT));
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        final int exclusiveLimit = (int)Verify.verifyNotNull(exclusiveLimitValue.eval(store, context));
        return new RangeCursor(store.getExecutor(), exclusiveLimit, continuation).map(QueryResult::ofComputed);
    }

    @Override
    public boolean isReverse() {
        return false;
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
    public boolean hasIndexScan(@Nonnull String indexName) {
        return false;
    }

    @Nonnull
    @Override
    public Set<String> getUsedIndexes() {
        return ImmutableSet.of();
    }

    @Override
    public boolean isStrictlySorted() {
        return true;
    }

    @Override
    public RecordQueryRangePlan strictlySorted(@Nonnull final Memoizer memoizer) {
        return this;
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.ALL_FIELDS;
    }

    @Override
    public boolean hasLoadBykeys() {
        return false;
    }

    @Nonnull
    @Override
    public String toString() {
        return PlanStringRepresentation.toString(this);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return exclusiveLimitValue.getCorrelatedTo();
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public RelationalExpression translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        final var translatedExclusiveLimitValue = exclusiveLimitValue.translateCorrelations(translationMap);
        if (translatedExclusiveLimitValue == exclusiveLimitValue) {
            return this;
        }
        return new RecordQueryRangePlan(translatedExclusiveLimitValue);
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap aliasMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        final RecordQueryRangePlan other = (RecordQueryRangePlan) otherExpression;
        return exclusiveLimitValue.semanticEquals(other, aliasMap);
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
        return Objects.hash(exclusiveLimitValue);
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        // no timer to increment
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
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, exclusiveLimitValue);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of();
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        Verify.verify(childGraphs.isEmpty());

        final PlannerGraph.DataNodeWithInfo dataNodeWithInfo;
        dataNodeWithInfo = new PlannerGraph.DataNodeWithInfo(NodeInfo.TEMPORARY_BUFFER_DATA,
                exclusiveLimitValue.getResultType(),
                ImmutableList.of("range: 0 <= i < {{exclusiveLimit}}"),
                ImmutableMap.of("exclusiveLimit", Attribute.gml(exclusiveLimitValue)));

        return PlannerGraph.fromNodeAndChildGraphs(
                dataNodeWithInfo,
                childGraphs);
    }

    @Nonnull
    @Override
    public PRecordQueryRangePlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryRangePlan.newBuilder()
                .setExclusiveLimitValue(exclusiveLimitValue.toValueProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setRangePlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQueryRangePlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                 @Nonnull final PRecordQueryRangePlan recordQueryRangePlanProto) {
        return new RecordQueryRangePlan(Value.fromValueProto(serializationContext, Objects.requireNonNull(recordQueryRangePlanProto.getExclusiveLimitValue())));
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryRangePlan, RecordQueryRangePlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryRangePlan> getProtoMessageClass() {
            return PRecordQueryRangePlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryRangePlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                              @Nonnull final PRecordQueryRangePlan recordQueryRangePlanProto) {
            return RecordQueryRangePlan.fromProto(serializationContext, recordQueryRangePlanProto);
        }
    }
}
