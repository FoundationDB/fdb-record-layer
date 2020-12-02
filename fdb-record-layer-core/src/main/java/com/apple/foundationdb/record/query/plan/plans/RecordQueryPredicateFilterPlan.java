/*
 * RecordQueryPredicateFilterPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpressionWithPredicate;
import com.apple.foundationdb.record.query.plan.temp.explain.Attribute;
import com.apple.foundationdb.record.query.plan.temp.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * A query plan that filters out records from a child plan that do not satisfy a {@link QueryPredicate}.
 */
@API(API.Status.EXPERIMENTAL)
public class RecordQueryPredicateFilterPlan extends RecordQueryFilterPlanBase implements RelationalExpressionWithPredicate {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Predicate-Filter-Plan");

    @Nonnull
    private final QueryPredicate filter;

    public RecordQueryPredicateFilterPlan(@Nonnull Quantifier.Physical inner,
                                          @Nonnull QueryPredicate filter) {
        super(inner);
        this.filter = filter;
    }

    @Override
    protected boolean hasAsyncFilter() {
        return false;
    }

    @Nullable
    @Override
    protected <M extends Message> Boolean evalFilter(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context, @Nullable FDBRecord<M> record) {
        if (record == null) {
            return null;
        }

        final EvaluationContext nestedContext = context.withBinding(getInner().getAlias(), record.getRecord());
        return filter.eval(store, nestedContext, record, record.getRecord());
    }

    @Nullable
    @Override
    protected <M extends Message> CompletableFuture<Boolean> evalFilterAsync(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context, @Nullable FDBRecord<M> record) {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public QueryPredicate getPredicate() {
        return filter;
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.ALL_FIELDS;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return filter.getCorrelatedTo();
    }

    @Nonnull
    @Override
    public RecordQueryPredicateFilterPlan rebaseWithRebasedQuantifiers(@Nonnull final AliasMap translationMap,
                                                                       @Nonnull final List<Quantifier> rebasedQuantifiers) {
        return new RecordQueryPredicateFilterPlan(
                Iterables.getOnlyElement(rebasedQuantifiers).narrow(Quantifier.Physical.class),
                getPredicate().rebase(translationMap));
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
        final RecordQueryPredicateFilterPlan otherPlan = (RecordQueryPredicateFilterPlan)otherExpression;
        return getInnerPlan().equals(otherPlan.getInnerPlan()) &&
               filter.semanticEquals(otherPlan.getPredicate(), equivalencesMap);
    }

    @Nonnull
    @Override
    public String toString() {
        return getInnerPlan() + " | " + getPredicate();
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
        return Objects.hash(getPredicate());
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
                return getInnerPlan().planHash(hashKind) + getPredicate().planHash(hashKind);
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                // Not using baseSource, since it uses Object.hashCode()
                return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, getInnerPlan(), getPredicate());
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.PREDICATE_FILTER_OPERATOR,
                        ImmutableList.of("WHERE {{pred}}"),
                        ImmutableMap.of("pred", Attribute.gml(getPredicate().toString()))),
                childGraphs);
    }
}
