/*
 * RecordQueryPredicatesFilterPlan.java
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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.AsyncBoolean;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionWithPredicates;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryComponentPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
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
public class RecordQueryPredicatesFilterPlan extends RecordQueryFilterPlanBase implements RelationalExpressionWithPredicates {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Predicate-Filter-Plan");

    @Nonnull
    private final List<QueryPredicate> predicates;
    @Nonnull
    private final QueryPredicate conjunctedPredicate;

    public RecordQueryPredicatesFilterPlan(@Nonnull Quantifier.Physical inner,
                                           @Nonnull Iterable<? extends QueryPredicate> predicates) {
        super(inner);
        this.predicates = ImmutableList.copyOf(predicates);
        this.conjunctedPredicate = AndPredicate.and(this.predicates);
    }

    @Nonnull
    @Override
    public List<? extends QueryPredicate> getPredicates() {
        return predicates;
    }

    @Override
    protected boolean hasAsyncFilter() {
        // TODO Query components can be evaluated in an async way as they may access indexes, etc. themselves. In
        //      QGM such an access is always explicitly modelled. Therefore predicates don't need this functionality
        //      and we should not add evalAsync() and isAsync() in the way it is implemented on query components.
        //      Since predicates cannot speak up for themselves when it comes to async-ness of the filter, we need
        //      to special case this (shim) class.
        return predicates.stream()
                .filter(predicate -> predicate instanceof QueryComponentPredicate)
                .anyMatch(predicate -> ((QueryComponentPredicate)predicate).hasAsyncQueryComponent());
    }

    @Nullable
    @Override
    protected <M extends Message> Boolean evalFilter(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context, @Nonnull QueryResult queryResult) {
        final var datum = queryResult.getDatum();
        if (datum == null) {
            return null;
        }

        final var nestedContext = context.withBinding(getInner().getAlias(), queryResult);
        return conjunctedPredicate.eval(store, nestedContext);
    }

    @Nullable
    @Override
    protected <M extends Message> CompletableFuture<Boolean> evalFilterAsync(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context, @Nonnull QueryResult queryResult) {
        final var nestedContext = context.withBinding(getInner().getAlias(), queryResult);

        return new AsyncBoolean<>(false,
                getPredicates(),
                predicate -> {
                    if (predicate instanceof QueryComponentPredicate) {
                        final QueryComponentPredicate queryComponentPredicate = (QueryComponentPredicate)predicate;
                        if (queryComponentPredicate.hasAsyncQueryComponent()) {
                            return queryComponentPredicate.evalMessageAsync(store, nestedContext);
                        }
                    }
                    return CompletableFuture.completedFuture(predicate.eval(store, nestedContext));
                },
                store).eval();
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.ALL_FIELDS;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return predicates.stream()
                .flatMap(queryPredicate -> queryPredicate.getCorrelatedTo().stream())
                .collect(ImmutableSet.toImmutableSet());
    }

    @SuppressWarnings("UnstableApiUsage")
    @Nonnull
    @Override
    public RecordQueryPredicatesFilterPlan translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        final var translatedPredicates = predicates.stream().map(queryPredicate -> queryPredicate.translateCorrelations(translationMap)).collect(ImmutableList.toImmutableList());
        return new RecordQueryPredicatesFilterPlan(
                Iterables.getOnlyElement(translatedQuantifiers).narrow(Quantifier.Physical.class),
                translatedPredicates);
    }

    @Nonnull
    @Override
    public RecordQueryPlanWithChild withChild(@Nonnull final RecordQueryPlan child) {
        return new RecordQueryPredicatesFilterPlan(Quantifier.physical(GroupExpressionRef.of(child)), getPredicates());
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return getInner().getFlowedObjectValue();
    }

    @Override
    @SuppressWarnings({"UnstableApiUsage", "PMD.CompareObjectsWithEquals"})
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        final var otherPlan = (RecordQueryPredicatesFilterPlan)otherExpression;
        final var otherPredicates = otherPlan.getPredicates();
        if (predicates.size() != otherPredicates.size()) {
            return false;
        }
        return Streams.zip(this.predicates.stream(),
                otherPredicates.stream(),
                (queryPredicate, otherQueryPredicate) -> queryPredicate.semanticEquals(otherQueryPredicate, equivalencesMap))
                .allMatch(isSame -> isSame);
    }

    @Nonnull
    @Override
    public String toString() {
        return getInnerPlan() + " | " + conjunctedPredicate;
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
        return Objects.hash(conjunctedPredicate);
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
                return getInnerPlan().planHash(hashKind) + conjunctedPredicate.planHash(hashKind);
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                // Not using baseSource, since it uses Object.hashCode()
                return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, getInnerPlan(), conjunctedPredicate);
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
                        ImmutableMap.of("pred", Attribute.gml(conjunctedPredicate.toString()))),
                childGraphs);
    }
}
