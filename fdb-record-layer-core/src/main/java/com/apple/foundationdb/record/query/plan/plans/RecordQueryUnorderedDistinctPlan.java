/*
 * RecordQueryUnorderedDistinctPlan.java
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
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.explain.Attribute;
import com.apple.foundationdb.record.query.plan.temp.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
 * A query plan that removes duplicates by means of a hash table of previously seen values.
 */
@API(API.Status.MAINTAINED)
public class RecordQueryUnorderedDistinctPlan implements RecordQueryPlanWithChild {
    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryUnorderedDistinctPlan.class);

    @Nonnull
    private final Quantifier.Physical inner;
    @Nonnull
    private final KeyExpression comparisonKey;
    @Nonnull
    private static final Set<StoreTimer.Event> duringEvents = Collections.singleton(FDBStoreTimer.Events.QUERY_DISTINCT);
    @Nonnull
    private static final Set<StoreTimer.Count> uniqueCounts = Collections.singleton(FDBStoreTimer.Counts.QUERY_DISTINCT_PLAN_UNIQUES);
    @Nonnull
    private static final Set<StoreTimer.Count> duplicateCounts =
            ImmutableSet.of(FDBStoreTimer.Counts.QUERY_DISTINCT_PLAN_DUPLICATES, FDBStoreTimer.Counts.QUERY_DISCARDED);

    public RecordQueryUnorderedDistinctPlan(@Nonnull final RecordQueryPlan plan,
                                            @Nonnull final KeyExpression comparisonKey) {
        this(Quantifier.physical(GroupExpressionRef.of(plan)), comparisonKey);
    }

    private RecordQueryUnorderedDistinctPlan(@Nonnull final Quantifier.Physical inner,
                                             @Nonnull KeyExpression comparisonKey) {
        this.inner = inner;
        this.comparisonKey = comparisonKey;
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<FDBQueriedRecord<M>> execute(@Nonnull FDBRecordStoreBase<M> store,
                                                                         @Nonnull EvaluationContext context,
                                                                         @Nullable byte[] continuation,
                                                                         @Nonnull ExecuteProperties executeProperties) {
        final Set<Key.Evaluated> seen = new HashSet<>();
        return getInner().execute(store, context, continuation, executeProperties.clearSkipAndLimit())
            .filterInstrumented(record -> seen.add(getComparisonKey().evaluateSingleton(record)),
                store.getTimer(), Collections.emptySet(), duringEvents, uniqueCounts, duplicateCounts)
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
    private KeyExpression getComparisonKey() {
        return comparisonKey;
    }

    @Nonnull
    @Override
    @API(API.Status.EXPERIMENTAL)
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    @Override
    public String toString() {
        return getInner() + " | UnorderedDistinct(" + getComparisonKey() + ")";
    }

    @Nonnull
    @Override
    @API(API.Status.EXPERIMENTAL)
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public RecordQueryUnorderedDistinctPlan rebaseWithRebasedQuantifiers(@Nonnull final AliasMap translationMap,
                                                                         @Nonnull final List<Quantifier> rebasedQuantifiers) {
        return new RecordQueryUnorderedDistinctPlan(Iterables.getOnlyElement(rebasedQuantifiers).narrow(Quantifier.Physical.class),
                getComparisonKey());
    }

    @Override
    @API(API.Status.EXPERIMENTAL)
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (!RecordQueryPlanWithChild.super.equalsWithoutChildren(otherExpression, equivalencesMap)) {
            return false;
        }

        return otherExpression instanceof RecordQueryUnorderedDistinctPlan &&
               comparisonKey.equals(((RecordQueryUnorderedDistinctPlan)otherExpression).comparisonKey);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return resultEquals(other);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getInner(), getComparisonKey());
    }

    @Override
    public int planHash() {
        return getInner().planHash() + getComparisonKey().planHash();
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_DISTINCT);
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
                new PlannerGraph.OperatorNodeWithInfo(this, NodeInfo.UNORDERED_DISTINCT_OPERATOR,
                        ImmutableList.of("comparison key: {{comparisonKey}}"),
                        ImmutableMap.of("comparisonKey", Attribute.gml(comparisonKey.toString()))),
                childGraphs);
    }
}
