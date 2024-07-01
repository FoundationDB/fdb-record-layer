/*
 * RecordQueryTypeFilterPlan.java
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
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.PlanStringRepresentation;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A query plan that filters out records from a child plan that are not of the designated record type(s).
 */
@API(API.Status.INTERNAL)
public class RecordQueryTypeFilterPlan implements RecordQueryPlanWithChild, TypeFilterExpression {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Type-Filter-Plan");

    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryTypeFilterPlan.class);

    @Nonnull
    private final Quantifier.Physical inner;
    @Nonnull
    private final Collection<String> recordTypes;
    @Nonnull
    private final Type resultType;
    @Nonnull
    private static final Set<StoreTimer.Count> inCounts = ImmutableSet.of(FDBStoreTimer.Counts.QUERY_FILTER_GIVEN, FDBStoreTimer.Counts.QUERY_TYPE_FILTER_PLAN_GIVEN);
    @Nonnull
    private static final Set<StoreTimer.Event> duringEvents = Collections.singleton(FDBStoreTimer.Events.QUERY_TYPE_FILTER);
    @Nonnull
    private static final Set<StoreTimer.Count> successCounts = ImmutableSet.of(FDBStoreTimer.Counts.QUERY_FILTER_PASSED, FDBStoreTimer.Counts.QUERY_TYPE_FILTER_PLAN_PASSED);
    @Nonnull
    private static final Set<StoreTimer.Count> failureCounts = Collections.singleton(FDBStoreTimer.Counts.QUERY_DISCARDED);

    public RecordQueryTypeFilterPlan(@Nonnull RecordQueryPlan inner, @Nonnull Collection<String> recordTypes) {
        this(Quantifier.physical(Reference.of(inner)), recordTypes, new Type.Any());
    }

    public RecordQueryTypeFilterPlan(@Nonnull Quantifier.Physical inner, @Nonnull Collection<String> recordTypes, @Nonnull Type resultType) {
        this.inner = inner;
        this.recordTypes = recordTypes;
        this.resultType = resultType;
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CloseResource")
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        final RecordCursor<QueryResult> results = getInnerPlan().executePlan(store, context, continuation, executeProperties.clearSkipAndLimit());

        return results
                .filterInstrumented(result -> recordTypes.contains(Objects.requireNonNull(result.getRecordType()).getName()), store.getTimer(),
                        inCounts, duringEvents, successCounts, failureCounts)
                .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
    }

    @Override
    public boolean isReverse() {
        return getInnerPlan().isReverse();
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(this.inner);
    }

    @Nonnull
    @Override
    public String toString() {
        return PlanStringRepresentation.toString(this);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public RecordQueryTypeFilterPlan translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                           @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryTypeFilterPlan(
                Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class),
                getRecordTypes(),
                resultType);
    }

    @Nonnull
    @Override
    public RecordQueryPlanWithChild withChild(@Nonnull final Reference childRef) {
        return new RecordQueryTypeFilterPlan(Quantifier.physical(childRef), getRecordTypes(), resultType);
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return QuantifiedObjectValue.of(inner.getAlias(), resultType);
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
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return getInnerPlan().planHash(mode) + stringHashUnordered(recordTypes);
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, getInnerPlan(), stringHashUnordered(recordTypes));
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Nonnull
    public RecordQueryPlan getInnerPlan() {
        return inner.getRangesOverPlan();
    }

    @Override
    @Nonnull
    public RecordQueryPlan getChild() {
        return getInnerPlan();
    }

    @Override
    @Nonnull
    public Collection<String> getRecordTypes() {
        return recordTypes;
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_TYPE_FILTER);
        getInnerPlan().logPlanStructure(timer);
    }

    @Override
    public int getComplexity() {
        return 1 + getInnerPlan().getComplexity();
    }

    /**
     * Rewrite the planner graph for better visualization of a query index plan.
     * @param childGraphs planner graphs of children expression that already have been computed
     * @return the rewritten planner graph that models the filter as a node that uses the expression attribute
     *         to depict the record types this operator filters.
     */
    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.TYPE_FILTER_OPERATOR,
                        ImmutableList.of("WHERE record IS {{types}}"),
                        ImmutableMap.of("types", Attribute.gml(getRecordTypes().stream().map(Attribute::gml).collect(ImmutableList.toImmutableList())))),
                childGraphs);
    }

    @Nonnull
    @Override
    public PRecordQueryTypeFilterPlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final var builder = PRecordQueryTypeFilterPlan.newBuilder()
                .setInner(inner.toProto(serializationContext));
        for (final String recordType : recordTypes) {
            builder.addRecordTypes(recordType);
        }
        builder.setResultType(resultType.toTypeProto(serializationContext));
        return builder.build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setTypeFilterPlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQueryTypeFilterPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                      @Nonnull final PRecordQueryTypeFilterPlan recordQueryTypeFilterPlanProto) {
        final Quantifier.Physical q =
                Quantifier.Physical.fromProto(serializationContext, Objects.requireNonNull(recordQueryTypeFilterPlanProto.getInner()));
        final ImmutableSet.Builder<String> recordTypesBuilder = ImmutableSet.builder();
        for (int i = 0; i < recordQueryTypeFilterPlanProto.getRecordTypesCount(); i ++) {
            recordTypesBuilder.add(recordQueryTypeFilterPlanProto.getRecordTypes(i));
        }
        return new RecordQueryTypeFilterPlan(q, recordTypesBuilder.build(), Type.fromTypeProto(serializationContext, Objects.requireNonNull(recordQueryTypeFilterPlanProto.getResultType())));
    }

    private static int stringHashUnordered(@Nonnull Iterable<String> strings) {
        // TODO just use AbstractSet.hashCode() instead which prevents the sorting. We need plan hash rolling for that.
        final ArrayList<Integer> hashes = new ArrayList<>();
        for (String str : strings) {
            hashes.add(str != null ? str.hashCode() : 0);
        }
        hashes.sort(Comparator.naturalOrder());
        return PlanHashable.combineHashes(hashes);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryTypeFilterPlan, RecordQueryTypeFilterPlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryTypeFilterPlan> getProtoMessageClass() {
            return PRecordQueryTypeFilterPlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryTypeFilterPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                   @Nonnull final PRecordQueryTypeFilterPlan recordQueryTypeFilterPlanProto) {
            return RecordQueryTypeFilterPlan.fromProto(serializationContext, recordQueryTypeFilterPlanProto);
        }
    }
}
