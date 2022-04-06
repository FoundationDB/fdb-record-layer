/*
 * RecordQueryFilterPlan.java
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
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.explain.Attribute;
import com.apple.foundationdb.record.query.plan.temp.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * A query plan that filters out records from a child plan that do not satisfy a filter component.
 */
@API(API.Status.INTERNAL)
public class RecordQueryFilterPlan extends RecordQueryFilterPlanBase {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Filter-Plan");

    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryFilterPlan.class);

    @Nonnull
    private final List<QueryComponent> filters;
    @Nonnull
    private final QueryComponent conjunctedFilter;

    public RecordQueryFilterPlan(@Nonnull RecordQueryPlan inner, @Nonnull List<QueryComponent> filters) {
        this(Quantifier.physical(GroupExpressionRef.of(inner)), filters);
    }

    public RecordQueryFilterPlan(@Nonnull RecordQueryPlan inner, @Nonnull QueryComponent filter) {
        this(Quantifier.physical(GroupExpressionRef.of(inner)), ImmutableList.of(filter));
    }

    public RecordQueryFilterPlan(@Nonnull Quantifier.Physical inner,
                                 @Nonnull List<QueryComponent> filters) {
        super(inner);
        this.filters = ImmutableList.copyOf(filters);
        this.conjunctedFilter = this.filters.size() == 1 ? Iterables.getOnlyElement(this.filters) : Query.and(this.filters);
    }

    @Override
    protected boolean hasAsyncFilter() {
        return conjunctedFilter.isAsync();
    }

    @Nullable
    @Override
    protected <M extends Message> Boolean evalFilter(@Nonnull FDBRecordStoreBase<M> store,
                                                     @Nonnull EvaluationContext context,
                                                     @Nonnull QueryResult datum) {
        return conjunctedFilter.eval(store, context, datum.getQueriedRecord());
    }

    @Nullable
    @Override
    protected <M extends Message> CompletableFuture<Boolean> evalFilterAsync(@Nonnull FDBRecordStoreBase<M> store,
                                                                             @Nonnull EvaluationContext context,
                                                                             @Nonnull QueryResult datum) {
        return conjunctedFilter.evalAsync(store, context, datum.getQueriedRecord());
    }

    @Override
    public boolean isReverse() {
        return getInnerPlan().isReverse();
    }

    @Nonnull
    @Override
    public String toString() {
        return getInnerPlan() + " | " + getConjunctedFilter();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public RecordQueryFilterPlan rebaseWithRebasedQuantifiers(@Nonnull final AliasMap translationMap,
                                                              @Nonnull final List<Quantifier> rebasedQuantifiers) {
        return new RecordQueryFilterPlan(Iterables.getOnlyElement(rebasedQuantifiers).narrow(Quantifier.Physical.class),
                getFilters());
    }

    @Nonnull
    @Override
    public RecordQueryPlanWithChild withChild(@Nonnull final RecordQueryPlan child) {
        return new RecordQueryFilterPlan(child, getFilters());
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return getInner().getFlowedObjectValue();
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        final RecordQueryFilterPlan otherPlan = (RecordQueryFilterPlan)otherExpression;
        return conjunctedFilter.equals(otherPlan.getConjunctedFilter());
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
        return Objects.hash(getConjunctedFilter());
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
                return getInnerPlan().planHash(hashKind) + getConjunctedFilter().planHash(hashKind);
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.planHash(hashKind, BASE_HASH, getInnerPlan(), getConjunctedFilter());
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }

    @Nonnull
    public List<QueryComponent> getFilters() {
        return filters;
    }

    @Nonnull
    public QueryComponent getConjunctedFilter() {
        return conjunctedFilter;
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.PREDICATE_FILTER_OPERATOR,
                        ImmutableList.of("WHERE {{pred}}"),
                        ImmutableMap.of("pred", Attribute.gml(getConjunctedFilter().toString()))),
                childGraphs);
    }
}
