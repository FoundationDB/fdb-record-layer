/*
 * RecordQueryScoreForRankPlan.java
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
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.EvaluationContextBuilder;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryScoreForRankPlan;
import com.apple.foundationdb.record.planprotos.PScoreForRank;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.indexes.RankedSetIndexHelper;
import com.apple.foundationdb.record.query.expressions.Comparisons;
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
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.planning.BindingFunction;
import com.apple.foundationdb.tuple.Tuple;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * A query plan that converts ranks to scores and executes a child plan with the conversion results bound in named parameters.
 */
@API(API.Status.INTERNAL)
public class RecordQueryScoreForRankPlan implements RecordQueryPlanWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Score-For-Rank-Plan");

    @Nonnull
    private final Quantifier.Physical inner;
    @Nonnull
    private final List<ScoreForRank> ranks;

    public RecordQueryScoreForRankPlan(@Nonnull RecordQueryPlan plan, @Nonnull List<ScoreForRank> ranks) {
        this(Quantifier.physical(Reference.of(plan)), ranks);
    }

    private RecordQueryScoreForRankPlan(@Nonnull final Quantifier.Physical inner,
                                        @Nonnull List<ScoreForRank> ranks) {
        this.inner = inner;
        this.ranks = ranks;
    }

    private <M extends Message> CompletableFuture<Tuple> bindScore(@Nonnull FDBRecordStoreBase<M> store,
                                                                   @Nonnull EvaluationContext context,
                                                                   ScoreForRank scoreForRank,
                                                                   @Nonnull IsolationLevel isolationLevel) {
        final Tuple operand = Tuple.fromList(scoreForRank.comparisons.stream().map(c -> c.getComparand(store, context)).collect(Collectors.toList()));
        return store.evaluateAggregateFunction(context, Collections.emptyList(), scoreForRank.function, TupleRange.allOf(operand), isolationLevel);
    }
    
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private <M extends Message> CompletableFuture<EvaluationContext> bindScores(@Nonnull FDBRecordStoreBase<M> store,
                                                                                @Nonnull EvaluationContext context,
                                                                                @Nonnull IsolationLevel isolationLevel) {
        final List<CompletableFuture<Tuple>> scores = ranks.stream().map(r -> bindScore(store, context, r, isolationLevel)).collect(Collectors.toList());
        return AsyncUtil.whenAll(scores).thenApply(vignore -> {
            EvaluationContextBuilder builder = context.childBuilder();
            for (int i = 0; i < scores.size(); i++) {
                final ScoreForRank rank = ranks.get(i);
                final Tuple score = store.getContext().joinNow(scores.get(i));
                final Object binding;
                if (score == null) {
                    binding = null;
                } else if (score == RankedSetIndexHelper.COMPARISON_SKIPPED_SCORE) {
                    binding = Comparisons.COMPARISON_SKIPPED_BINDING;
                } else {
                    binding = rank.bindingFunction.apply(score);
                }
                builder.setBinding(rank.bindingName, binding);
            }
            return builder.build(context.getTypeRepository());
        });
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store,
                                                                     @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation,
                                                                     @Nonnull final ExecuteProperties executeProperties) {
        return RecordCursor.mapFuture(store.getExecutor(), bindScores(store, context, executeProperties.getIsolationLevel()), continuation,
                (innerContext, innerContinuation) -> getChild().executePlan(store, innerContext, innerContinuation, executeProperties));
    }

    @Nonnull
    public List<ScoreForRank> getRanks() {
        return ranks;
    }

    @Override
    @Nonnull
    public RecordQueryPlan getChild() {
        return inner.getRangesOverPlan();
    }

    @Nonnull
    @Override
    @API(API.Status.EXPERIMENTAL)
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return new QueriedValue();
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.ALL_FIELDS;
    }

    @Override
    public boolean isReverse() {
        return getChild().isReverse();
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
    public RecordQueryScoreForRankPlan translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                             @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new RecordQueryScoreForRankPlan((Quantifier.Physical)Iterables.getOnlyElement(translatedQuantifiers),
                getRanks());
    }

    @Nonnull
    @Override
    public RecordQueryPlanWithChild withChild(@Nonnull final Reference childRef) {
        return new RecordQueryScoreForRankPlan(Quantifier.physical(childRef), getRanks());
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
        return ranks.equals(((RecordQueryScoreForRankPlan) otherExpression).ranks);
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
        return Objects.hash(ranks);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return getChild().planHash(mode) + PlanHashable.planHash(mode, ranks);
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, getChild(), ranks);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_SCORE_FOR_RANK);
        getChild().logPlanStructure(timer);
    }

    @Override
    public int getComplexity() {
        return 1 + getChild().getComplexity();
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        final PlannerGraph.Node root =
                new PlannerGraph.OperatorNodeWithInfo(this,
                        NodeInfo.NESTED_LOOP_JOIN_OPERATOR);
        final PlannerGraph graphForInner = Iterables.getOnlyElement(childGraphs);
        final PlannerGraph.DataNodeWithInfo valuesNode =
                new PlannerGraph.DataNodeWithInfo(NodeInfo.VALUES_DATA,
                        getResultType(),
                        ImmutableList.of("VALUES({{values}}"),
                        ImmutableMap.of("values",
                                Attribute.gml(Objects.requireNonNull(getRanks()).stream()
                                        .map(ScoreForRank::callToString)
                                        .map(Attribute::gml)
                                        .collect(ImmutableList.toImmutableList()))));
        final PlannerGraph.Edge fromValuesEdge = new PlannerGraph.Edge();
        return PlannerGraph.builder(root)
                .addGraph(graphForInner)
                .addNode(valuesNode)
                .addEdge(valuesNode, root, fromValuesEdge)
                .addEdge(graphForInner.getRoot(), root, new PlannerGraph.Edge(ImmutableSet.of(fromValuesEdge)))
                .build();
    }

    @Nonnull
    @Override
    public PRecordQueryScoreForRankPlan toProto(@Nonnull final PlanSerializationContext serializationContext) {
        final PRecordQueryScoreForRankPlan.Builder builder =
                PRecordQueryScoreForRankPlan.newBuilder()
                        .setInner(inner.toProto(serializationContext));
        for (final ScoreForRank rank : ranks) {
            builder.addRanks(rank.toProto(serializationContext));
        }

        return builder.build();
    }

    @Nonnull
    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setScoreForRankPlan(toProto(serializationContext)).build();
    }

    @Nonnull
    public static RecordQueryScoreForRankPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                        @Nonnull final PRecordQueryScoreForRankPlan recordQueryScoreForRankPlanProto) {
        final ImmutableList.Builder<ScoreForRank> ranksBuilder = ImmutableList.builder();
        for (int i = 0; i < recordQueryScoreForRankPlanProto.getRanksCount(); i++) {
            ranksBuilder.add(ScoreForRank.fromProto(serializationContext, recordQueryScoreForRankPlanProto.getRanks(i)));
        }
        return new RecordQueryScoreForRankPlan(Quantifier.Physical.fromProto(serializationContext, Objects.requireNonNull(recordQueryScoreForRankPlanProto.getInner())),
                ranksBuilder.build());
    }

    /**
     * A single conversion of a rank to a score to be bound to some name.
     */
    public static class ScoreForRank implements PlanHashable, PlanSerializable {
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Score-For-Rank");

        @Nonnull
        private final String bindingName;
        @Nonnull
        private final BindingFunction bindingFunction;
        @Nonnull
        private final IndexAggregateFunction function;
        @Nonnull
        private final List<Comparisons.Comparison> comparisons;

        public ScoreForRank(@Nonnull String bindingName, @Nonnull BindingFunction bindingFunction,
                            @Nonnull IndexAggregateFunction function, @Nonnull List<Comparisons.Comparison> comparisons) {
            this.bindingName = bindingName;
            this.bindingFunction = bindingFunction;
            this.function = function;
            this.comparisons = comparisons;
        }

        @Nonnull
        public String getBindingName() {
            return bindingName;
        }

        @Nonnull
        public List<Comparisons.Comparison> getComparisons() {
            return comparisons;
        }

        @Override
        public String toString() {
            return bindingName + " = " + callToString();
        }

        public String callToString() {
            return function.getIndex() + "." + function.getName() + comparisons.stream().map(Comparisons.Comparison::typelessString).collect(Collectors.joining(", ", "(", ")"));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ScoreForRank that = (ScoreForRank) o;
            return Objects.equals(bindingName, that.bindingName) &&
                   Objects.equals(bindingFunction, that.bindingFunction) &&
                   Objects.equals(function, that.function) &&
                   Objects.equals(comparisons, that.comparisons);
        }

        @Override
        public int hashCode() {
            return Objects.hash(bindingName, bindingFunction, function, comparisons);
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode mode) {
            switch (mode.getKind()) {
                case LEGACY:
                    return bindingName.hashCode() + function.getName().hashCode() + PlanHashable.planHash(mode, comparisons);
                case FOR_CONTINUATION:
                    // TODO: Use function.planHash()?
                    return PlanHashable.objectsPlanHash(mode, BASE_HASH, bindingName, function.getName(), comparisons);
                default:
                    throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
            }
        }

        @Nonnull
        @Override
        public PScoreForRank toProto(@Nonnull final PlanSerializationContext serializationContext) {
            final var builder = PScoreForRank.newBuilder()
                    .setBindingName(bindingName)
                    .setBindingFunction(bindingFunction.toProto(serializationContext))
                    .setFunction(function.toProto(serializationContext));
            for (final Comparisons.Comparison comparison : comparisons) {
                builder.addComparisons(comparison.toComparisonProto(serializationContext));
            }
            return builder.build();
        }

        @Nonnull
        public static ScoreForRank fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                             @Nonnull final PScoreForRank scoreForRankProto) {
            final ImmutableList.Builder<Comparisons.Comparison> comparisonsBuilder = ImmutableList.builder();
            for (int i = 0; i < scoreForRankProto.getComparisonsCount(); i++) {
                comparisonsBuilder.add(Comparisons.Comparison.fromComparisonProto(serializationContext, scoreForRankProto.getComparisons(i)));
            }
            return new ScoreForRank(Objects.requireNonNull(scoreForRankProto.getBindingName()),
                    BindingFunction.fromProto(serializationContext, Objects.requireNonNull(scoreForRankProto.getBindingFunction())),
                    IndexAggregateFunction.fromProto(serializationContext, Objects.requireNonNull(scoreForRankProto.getFunction())),
                    comparisonsBuilder.build());
        }
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryScoreForRankPlan, RecordQueryScoreForRankPlan> {
        @Nonnull
        @Override
        public Class<PRecordQueryScoreForRankPlan> getProtoMessageClass() {
            return PRecordQueryScoreForRankPlan.class;
        }

        @Nonnull
        @Override
        public RecordQueryScoreForRankPlan fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                     @Nonnull final PRecordQueryScoreForRankPlan recordQueryScoreForRankPlanProto) {
            return RecordQueryScoreForRankPlan.fromProto(serializationContext, recordQueryScoreForRankPlanProto);
        }
    }
}
