/*
 * RecordQueryUnorderedPrimaryKeyDistinctPlan.java
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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.PlanStringRepresentation;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A query plan that removes duplicates by means of a hash table of primary keys already seen.
 */
@API(API.Status.INTERNAL)
public class RecordQueryUnorderedPrimaryKeyDistinctPlan implements RecordQueryPlanWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Unordered-Primary-Key-Distinct-Plan");

    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryUnorderedPrimaryKeyDistinctPlan.class);

    @Nonnull
    private final Quantifier.Physical inner;
    @Nonnull
    private static final Set<StoreTimer.Event> duringEvents = Collections.singleton(FDBStoreTimer.Events.QUERY_PK_DISTINCT);
    @Nonnull
    private static final Set<StoreTimer.Count> uniqueCounts = Collections.singleton(FDBStoreTimer.Counts.QUERY_PK_DISTINCT_PLAN_UNIQUES);
    @Nonnull
    private static final Set<StoreTimer.Count> duplicateCounts =
            ImmutableSet.of(FDBStoreTimer.Counts.QUERY_PK_DISTINCT_PLAN_DUPLICATES, FDBStoreTimer.Counts.QUERY_DISCARDED);

    public RecordQueryUnorderedPrimaryKeyDistinctPlan(@Nonnull RecordQueryPlan innerPlan) {
        this(Quantifier.physical(GroupExpressionRef.of(innerPlan)));
    }

    public RecordQueryUnorderedPrimaryKeyDistinctPlan(@Nonnull Quantifier.Physical inner) {
        this.inner = inner;
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        final Set<Tuple> seen = new HashSet<>();
        return getInner().executePlan(store, context, continuation, executeProperties.clearSkipAndLimit())
                .filterInstrumented(result -> seen.add(Objects.requireNonNull(result.getPrimaryKey())), store.getTimer(),
                        Collections.emptySet(), duringEvents, uniqueCounts, duplicateCounts)
                .skipThenLimit(executeProperties.getSkip(), executeProperties.getReturnedRowLimit());
    }

    @Override
    public boolean isReverse() {
        return getInner().isReverse();
    }

    @Nonnull
    private RecordQueryPlan getInner() {
        return inner.getRangesOverPlan();
    }

    @Override
    @Nonnull
    public RecordQueryPlan getChild() {
        return getInner();
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

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
    public RecordQueryUnorderedPrimaryKeyDistinctPlan translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                                            @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryUnorderedPrimaryKeyDistinctPlan(Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class));
    }

    @Nonnull
    @Override
    public RecordQueryPlanWithChild withChild(@Nonnull final ExpressionRef<? extends RecordQueryPlan> childRef) {
        return new RecordQueryUnorderedPrimaryKeyDistinctPlan(Quantifier.physical(childRef));
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return inner.getFlowedObjectValue();
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        return (getClass() == otherExpression.getClass());
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
        return BASE_HASH.planHash();
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
                return getInner().planHash(hashKind) + 1;
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, getInner());
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_PK_DISTINCT);
        getInner().logPlanStructure(timer);
    }

    @Override
    public int getComplexity() {
        return 1 + getInner().getComplexity();
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this, NodeInfo.UNORDERED_PRIMARY_KEY_DISTINCT_OPERATOR),
                childGraphs);
    }
}
