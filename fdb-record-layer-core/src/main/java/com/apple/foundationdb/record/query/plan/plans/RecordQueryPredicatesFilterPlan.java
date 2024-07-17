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
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryPredicatesFilterPlan;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.AsyncBoolean;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.PlanStringRepresentation;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionWithPredicates;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.auto.service.AutoService;
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

    protected RecordQueryPredicatesFilterPlan(@Nonnull final PlanSerializationContext serializationContext,
                                              @Nonnull final PRecordQueryPredicatesFilterPlan recordQueryPredicatesFilterPlanProto) {
        super(serializationContext, Objects.requireNonNull(recordQueryPredicatesFilterPlanProto.getSuper()));
        final ImmutableList.Builder<QueryPredicate> predicatesBuilder = ImmutableList.builder();
        for (int i = 0; i < recordQueryPredicatesFilterPlanProto.getPredicatesCount(); i ++) {
            predicatesBuilder.add(QueryPredicate.fromQueryPredicateProto(serializationContext, recordQueryPredicatesFilterPlanProto.getPredicates(i)));
        }
        this.predicates = predicatesBuilder.build();
        this.conjunctedPredicate = AndPredicate.and(this.predicates);
    }

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

    @Nonnull
    public QueryPredicate getConjunctedPredicate() {
        return conjunctedPredicate;
    }

    @Override
    protected boolean hasAsyncFilter() {
        return false;
    }

    @Nullable
    @Override
    protected <M extends Message> Boolean evalFilter(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context, @Nonnull QueryResult queryResult) {
        final var nestedContext = context.withBinding(getInner().getAlias(), queryResult);
        return conjunctedPredicate.eval(store, nestedContext);
    }

    @Nullable
    @Override
    protected <M extends Message> CompletableFuture<Boolean> evalFilterAsync(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context, @Nonnull QueryResult queryResult) {
        final var nestedContext = context.withBinding(getInner().getAlias(), queryResult);

        return new AsyncBoolean<>(false,
                getPredicates(),
                predicate -> CompletableFuture.completedFuture(predicate.eval(store, nestedContext)),
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
    public RecordQueryPlanWithChild withChild(@Nonnull final Reference childRef) {
        return new RecordQueryPredicatesFilterPlan(Quantifier.physical(childRef), getPredicates());
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
        return PlanStringRepresentation.toString(this);
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
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return getInnerPlan().planHash(mode) + conjunctedPredicate.planHash(mode);
            case FOR_CONTINUATION:
                // Not using baseSource, since it uses Object.hashCode()
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, getInnerPlan(), conjunctedPredicate);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
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

    @Nonnull
    @Override
    public PRecordQueryPredicatesFilterPlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final PRecordQueryPredicatesFilterPlan.Builder builder = PRecordQueryPredicatesFilterPlan.newBuilder()
                .setSuper(toRecordQueryFilterPlanBaseProto(serializationContext));
        for (final QueryPredicate predicate : predicates) {
            builder.addPredicates(predicate.toQueryPredicateProto(serializationContext));
        }
        return builder.build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setPredicatesFilterPlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQueryPredicatesFilterPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                            @Nonnull final PRecordQueryPredicatesFilterPlan recordQueryPredicatesFilterPlanProto) {
        return new RecordQueryPredicatesFilterPlan(serializationContext, recordQueryPredicatesFilterPlanProto);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryPredicatesFilterPlan, RecordQueryPredicatesFilterPlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryPredicatesFilterPlan> getProtoMessageClass() {
            return PRecordQueryPredicatesFilterPlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryPredicatesFilterPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                         @Nonnull final PRecordQueryPredicatesFilterPlan recordQueryPredicatesFilterPlanProto) {
            return RecordQueryPredicatesFilterPlan.fromProto(serializationContext, recordQueryPredicatesFilterPlanProto);
        }
    }
}
