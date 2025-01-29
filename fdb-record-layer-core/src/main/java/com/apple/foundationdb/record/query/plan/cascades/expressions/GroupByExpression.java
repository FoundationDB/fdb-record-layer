/*
 * GroupByExpression.java
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

package com.apple.foundationdb.record.query.plan.cascades.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.BooleanWithConstraint;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.Compensation;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.IdentityBiMap;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentityMap;
import com.apple.foundationdb.record.query.plan.cascades.MatchInfo;
import com.apple.foundationdb.record.query.plan.cascades.MatchInfo.RegularMatchInfo;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedSortOrder;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMap;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.ValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithComparisons;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValue;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValueAndRanges;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.MaxMatchMap;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.PullUp;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static com.apple.foundationdb.record.query.plan.cascades.BooleanWithConstraint.alwaysTrue;
import static com.apple.foundationdb.record.query.plan.cascades.BooleanWithConstraint.falseValue;

/**
 * A logical {@code group by} expression that represents grouping incoming tuples and aggregating each group.
 */
@API(API.Status.EXPERIMENTAL)
public class GroupByExpression implements RelationalExpressionWithChildren, InternalPlannerGraphRewritable {

    @Nullable
    private final Value groupingValue;

    @Nonnull
    private final AggregateValue aggregateValue;

    @Nonnull
    private final BiFunction<Value /* groupingValue */, Value, Value> resultValueFunction;

    @Nonnull
    private final Supplier<Value> computeResultSupplier;

    @Nonnull
    private final Supplier<RequestedOrdering> computeRequestedOrderingSupplier;

    @Nonnull
    private final Quantifier innerQuantifier;

    /**
     * Creates a new instance of {@link GroupByExpression}.
     *
     * @param groupingValue The grouping {@code Value} used to determine individual groups, can be {@code null} indicating no grouping.
     * @param aggregateValue The aggregation {@code Value} applied to each group.
     * @param resultValueFunction a bi-function that allows us to create the actual result value of this expression
     * @param innerQuantifier The underlying source of tuples to be grouped.
     */
    public GroupByExpression(@Nullable final Value groupingValue,
                             @Nonnull final AggregateValue aggregateValue,
                             @Nonnull final BiFunction<Value /* groupingValue */, Value, Value> resultValueFunction,
                             @Nonnull final Quantifier innerQuantifier) {
        this.groupingValue = groupingValue;
        this.aggregateValue = aggregateValue;
        this.resultValueFunction = resultValueFunction;
        this.computeResultSupplier = Suppliers.memoize(() -> resultValueFunction.apply(groupingValue, aggregateValue));
        this.computeRequestedOrderingSupplier = Suppliers.memoize(this::computeRequestedOrdering);
        this.innerQuantifier = innerQuantifier;
    }

    @Override
    public int getRelationalChildCount() {
        return 1;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return getResultValue().getCorrelatedTo();
    }

    @Nonnull
    public BiFunction<Value, Value, Value> getResultValueFunction() {
        return resultValueFunction;
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return computeResultSupplier.get();
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(innerQuantifier);
    }

    @Nonnull
    public Quantifier getInnerQuantifier() {
        return innerQuantifier;
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression other, @Nonnull final AliasMap equivalences) {
        if (this == other) {
            return true;
        }
        if (getClass() != other.getClass()) {
            return false;
        }
        final var otherGroupByExpr = ((GroupByExpression)other);

        if ( (otherGroupByExpr.getGroupingValue() == null) ^ (getGroupingValue() == null) ) {
            return false;
        }

        if (otherGroupByExpr.getGroupingValue() != null) {
            return Objects.requireNonNull(getGroupingValue()).semanticEquals(otherGroupByExpr.getGroupingValue(), equivalences)
                   && getAggregateValue().semanticEquals(otherGroupByExpr.getAggregateValue(), equivalences);
        } else {
            return getAggregateValue().semanticEquals(otherGroupByExpr.getAggregateValue(), equivalences);
        }
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(getResultValue());
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object other) {
        return semanticEquals(other);
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public RelationalExpression translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                      final boolean shouldSimplifyValues,
                                                      @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        final AggregateValue translatedAggregateValue =
                (AggregateValue)getAggregateValue().translateCorrelations(translationMap, shouldSimplifyValues);
        final Value translatedGroupingValue = getGroupingValue() == null
                ? null
                : getGroupingValue().translateCorrelations(translationMap, shouldSimplifyValues);
        Verify.verify(translatedGroupingValue instanceof FieldValue);
        if (translatedAggregateValue != getAggregateValue() || translatedGroupingValue != getGroupingValue()) {
            return new GroupByExpression(translatedGroupingValue, translatedAggregateValue, resultValueFunction,
                    Iterables.getOnlyElement(translatedQuantifiers));
        }
        return this;
    }

    @Override
    public String toString() {
        if (getGroupingValue() != null) {
            return "GroupBy(" + getGroupingValue() + "), aggregationValue: " + getAggregateValue() + ", resultValue: " + computeResultSupplier.get();
        } else {
            return "GroupBy(NULL), aggregationValue: " + getAggregateValue() + ", resultValue: " + computeResultSupplier.get();
        }
    }

    @Nonnull
    @Override
    public PlannerGraph rewriteInternalPlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        if (getGroupingValue() == null) {
            return PlannerGraph.fromNodeAndChildGraphs(
                    new PlannerGraph.LogicalOperatorNode(this,
                            "GROUP BY",
                            List.of("AGG {{agg}}"),
                            ImmutableMap.of("agg", Attribute.gml(getAggregateValue().toString()))),
                    childGraphs);
        } else {
            return PlannerGraph.fromNodeAndChildGraphs(
                    new PlannerGraph.LogicalOperatorNode(this,
                            "GROUP BY",
                            List.of("AGG {{agg}}", "GROUP BY {{grouping}}"),
                            ImmutableMap.of("agg", Attribute.gml(getAggregateValue().toString()),
                                    "grouping", Attribute.gml(getGroupingValue().toString()))),
                    childGraphs);
        }
    }

    @Nullable
    public Value getGroupingValue() {
        return groupingValue;
    }

    @Nonnull
    public AggregateValue getAggregateValue() {
        return aggregateValue;
    }

    /**
     * Returns the ordering requirements of the underlying scan for the group by to work. This is used by the planner
     * to choose a compatibly-ordered access path.
     *
     * @return The ordering requirements.
     */
    @Nonnull
    public RequestedOrdering getRequestedOrdering() {
        return computeRequestedOrderingSupplier.get();
    }

    @Nonnull
    @Override
    public Iterable<MatchInfo> subsumedBy(@Nonnull final RelationalExpression candidateExpression,
                                          @Nonnull final AliasMap bindingAliasMap,
                                          @Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap,
                                          @Nonnull final EvaluationContext evaluationContext) {
        // the candidate must be a GROUP-BY expression.
        if (candidateExpression.getClass() != this.getClass()) {
            return ImmutableList.of();
        }

        final var candidateGroupByExpression = (GroupByExpression)candidateExpression;
        final var candidateInnerQuantifier = candidateGroupByExpression.getInnerQuantifier();

        if (!(innerQuantifier instanceof Quantifier.ForEach)) {
            return ImmutableList.of();
        }

        final var candidateAlias = bindingAliasMap.getTarget(innerQuantifier.getAlias());
        if (candidateAlias == null) {
            return ImmutableList.of();
        }
        Verify.verify(candidateAlias.equals(candidateInnerQuantifier.getAlias()));
        if (!(candidateInnerQuantifier instanceof Quantifier.ForEach)) {
            return ImmutableList.of();
        }
        if (((Quantifier.ForEach)innerQuantifier).isNullOnEmpty() !=
                ((Quantifier.ForEach)candidateInnerQuantifier).isNullOnEmpty()) {
            return ImmutableList.of();
        }

        final var translationMapOptional =
                RelationalExpression.pullUpAndComposeTranslationMapsMaybe(candidateExpression, bindingAliasMap, partialMatchMap);
        if (translationMapOptional.isEmpty()) {
            return ImmutableList.of();
        }
        final var translationMap = translationMapOptional.get();

        // check that the aggregate value is the same, and that the grouping value is the same.
        final var otherAggregateValue = candidateGroupByExpression.getAggregateValue();
        final var candidateGroupingValue = candidateGroupByExpression.getGroupingValue();

        final var valueEquivalence =
                ValueEquivalence.fromAliasMap(bindingAliasMap)
                        .then(ValueEquivalence.constantEquivalenceWithEvaluationContext(evaluationContext));

        final var translatedAggregateValue =
                aggregateValue.translateCorrelations(translationMap, true);
        final var translatedAggregateValues =
                Values.primitiveAccessorsForType(translatedAggregateValue.getResultType(),
                                () -> translatedAggregateValue).stream()
                        .map(primitiveGroupingValue -> primitiveGroupingValue.simplify(AliasMap.emptyMap(),
                                ImmutableSet.of()))
                        .collect(ImmutableSet.toImmutableSet());
        if (translatedAggregateValues.size() != 1) {
            return ImmutableList.of();
        }

        final var otherAggregateValues =
                Values.primitiveAccessorsForType(otherAggregateValue.getResultType(),
                                () -> otherAggregateValue).stream()
                        .map(primitiveAggregateValue -> primitiveAggregateValue.simplify(AliasMap.emptyMap(),
                                ImmutableSet.of()))
                        .collect(ImmutableSet.toImmutableSet());
        if (translatedAggregateValues.size() != 1) {
            return ImmutableList.of();
        }

        final var subsumedAggregations =
                Iterables.getOnlyElement(translatedAggregateValues).semanticEquals(Iterables.getOnlyElement(otherAggregateValues),
                        valueEquivalence);
        if (subsumedAggregations.isFalse()) {
            return ImmutableList.of();
        }

        final var subsumedGroupings =
                subsumedAggregations
                        .compose(ignored -> groupingSubsumedBy(candidateInnerQuantifier,
                                Objects.requireNonNull(partialMatchMap.getUnwrapped(innerQuantifier)), candidateGroupingValue,
                                translationMap, valueEquivalence));

        if (subsumedGroupings.isFalse()) {
            return ImmutableList.of();
        }

        final var translatedResultValue = getResultValue().translateCorrelations(translationMap, true);
        final var maxMatchMap =
                MaxMatchMap.compute(translatedResultValue, candidateExpression.getResultValue(),
                        Quantifiers.aliases(candidateExpression.getQuantifiers()), valueEquivalence);
        final var queryPlanConstraint =
                subsumedGroupings.getConstraint().compose(maxMatchMap.getQueryPlanConstraint());

        return RegularMatchInfo.tryMerge(bindingAliasMap, partialMatchMap, ImmutableMap.of(), PredicateMap.empty(),
                        maxMatchMap, queryPlanConstraint)
                .map(ImmutableList::of)
                .orElse(ImmutableList.of());
    }

    @Nonnull
    private BooleanWithConstraint groupingSubsumedBy(@Nonnull final Quantifier candidateInnerQuantifier,
                                                     @Nonnull final PartialMatch childMatch,
                                                     @Nullable final Value candidateGroupingValue,
                                                     @Nonnull final TranslationMap translationMap,
                                                     @Nonnull final ValueEquivalence valueEquivalence) {
        if (groupingValue == null && candidateGroupingValue == null) {
            return alwaysTrue();
        }
        if (candidateGroupingValue == null) {
            return falseValue();
        }

        final Set<Value> translatedGroupingValues;
        if (groupingValue != null) {
            final var translatedGroupingValue = groupingValue.translateCorrelations(translationMap, true);
            translatedGroupingValues =
                    Values.primitiveAccessorsForType(translatedGroupingValue.getResultType(),
                                    () -> translatedGroupingValue).stream()
                            .map(primitiveGroupingValue -> primitiveGroupingValue.simplify(AliasMap.emptyMap(),
                                    ImmutableSet.of()))
                            .collect(ImmutableSet.toImmutableSet());
        } else {
            translatedGroupingValues = ImmutableSet.of();
        }

        final var candidateGroupingValues =
                Values.primitiveAccessorsForType(candidateGroupingValue.getResultType(),
                                () -> candidateGroupingValue).stream()
                        .map(primitiveGroupingValue -> primitiveGroupingValue.simplify(AliasMap.emptyMap(),
                                ImmutableSet.of()))
                        .collect(ImmutableSet.toImmutableSet());

        //
        // If there are more groupingValues than candidateGroupingValues, we cannot match the index.
        //
        if (translatedGroupingValues.size() > candidateGroupingValues.size()) {
            return falseValue();
        }

        //
        // Implicit group by value can be inferred if there is an equality-bound predicate:
        //
        //   query:                     candidate:
        //       GROUP BY a, b              GROUP BY a, b, c
        //       WHERE c = 5
        //
        // 1. Ensure that at least the values on the query side have a corresponding counterpart on the candidate side.
        //    Form a set of candidate grouping values that cannot be matched in this way.
        // 2. Pull up all already matched equality predicates from the child match.
        // 3. For each candidate grouping value in the set of (yet) unmatched candidate group values, try to find a
        //    predicate that binds that groupingValue.
        //
        final var unmatchedCandidateValues = new LinkedHashSet<>(candidateGroupingValues);
        var booleanWithConstraint = alwaysTrue();
        for (final var groupingPartValue : translatedGroupingValues) {
            var found = false;

            for (final var iterator = unmatchedCandidateValues.iterator(); iterator.hasNext(); ) {
                final var candidateGroupingPartValue = iterator.next();
                final var semanticEquals =
                        groupingPartValue.semanticEquals(candidateGroupingPartValue, valueEquivalence);
                if (semanticEquals.isTrue()) {
                    found = true;
                    booleanWithConstraint = booleanWithConstraint.composeWithOther(semanticEquals);
                    iterator.remove();

                    if (unmatchedCandidateValues.isEmpty()) {
                        break;
                    }
                    // no unconditional break intentionally since there could be more than one match
                }
            }
            if (!found) {
                return falseValue();
            }
            if (unmatchedCandidateValues.isEmpty()) {
                break;
            }
        }

        if (unmatchedCandidateValues.isEmpty()) {
            // return with a positive result if sets where in fact semantically equal
            return booleanWithConstraint;
        }

        //
        // Consult already bound predicates from the child match.
        //
        final var equalityPredicates =
                childMatch.pullUpToParent(candidateInnerQuantifier.getAlias(), predicate -> {
                    if (!(predicate instanceof PredicateWithValue)) {
                        return false;
                    }
                    if (predicate instanceof PredicateWithComparisons) {
                        final List<Comparisons.Comparison> comparisons;
                        if (predicate instanceof PredicateWithValueAndRanges) {
                            final var ranges = ((PredicateWithValueAndRanges)predicate).getRanges();
                            if (ranges.size() == 1) {
                                final var range = Iterables.getOnlyElement(ranges);
                                comparisons = range.getComparisons();
                            } else {
                                return false;
                            }
                        } else {
                            comparisons = ((PredicateWithComparisons)predicate).getComparisons();
                        }

                        return comparisons.stream()
                                .anyMatch(comparison -> comparison.getType().isEquality() &&
                                        comparison.getType() == Comparisons.Type.EQUALS);
                    }
                    return false;
                });

        for (final var predicateMapping : equalityPredicates.values()) {
            final var translatedPredicate = predicateMapping.getTranslatedQueryPredicate();
            if (translatedPredicate instanceof PredicateWithValue) {
                final var comparedValue = Objects.requireNonNull(((PredicateWithValue)translatedPredicate).getValue());

                for (final var iterator = unmatchedCandidateValues.iterator(); iterator.hasNext(); ) {
                    final var candidateGroupingPartValue = iterator.next();
                    final var semanticEquals =
                            comparedValue.semanticEquals(candidateGroupingPartValue, valueEquivalence);
                    if (semanticEquals.isTrue()) {
                        booleanWithConstraint = booleanWithConstraint.composeWithOther(semanticEquals);
                        iterator.remove();

                        if (unmatchedCandidateValues.isEmpty()) {
                            break;
                        }
                        // no unconditional break intentionally since there could be more than one match
                    }
                }
                if (unmatchedCandidateValues.isEmpty()) {
                    break;
                }
            }
        }

        return unmatchedCandidateValues.isEmpty() ? booleanWithConstraint : falseValue();
    }

    @Nonnull
    private RequestedOrdering computeRequestedOrdering() {
        if (groupingValue == null || groupingValue.isConstant()) {
            return RequestedOrdering.preserve();
        }

        final var groupingValueType = groupingValue.getResultType();
        Verify.verify(groupingValueType.isRecord());

        final var currentGroupingValue =
                groupingValue.rebase(AliasMap.ofAliases(innerQuantifier.getAlias(), Quantifier.current()));

        return RequestedOrdering.ofParts(
                ImmutableList.of(new RequestedOrderingPart(currentGroupingValue, RequestedSortOrder.ANY)),
                RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS,
                false,
                innerQuantifier.getCorrelatedTo());
    }

    @Nonnull
    @Override
    public Compensation compensate(@Nonnull final PartialMatch partialMatch,
                                   @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap,
                                   @Nullable final PullUp pullUp,
                                   @Nonnull final CorrelationIdentifier nestingAlias) {
        final var matchInfo = partialMatch.getMatchInfo();
        final var regularMatchInfo = partialMatch.getRegularMatchInfo();
        final var quantifier = Iterables.getOnlyElement(getQuantifiers());

        final var adjustedPullUp = partialMatch.nestPullUp(pullUp, nestingAlias);
        // if the match requires, for the moment, any, compensation, we reject it.
        final Optional<Compensation> childCompensationOptional =
                regularMatchInfo.getChildPartialMatchMaybe(quantifier)
                        .map(childPartialMatch -> {
                            final var bindingAliasMap = regularMatchInfo.getBindingAliasMap();
                            return childPartialMatch.compensate(boundParameterPrefixMap, adjustedPullUp,
                                    Objects.requireNonNull(bindingAliasMap.getTarget(quantifier.getAlias())));
                        });

        if (childCompensationOptional.isEmpty()) {
            return Compensation.impossibleCompensation();
        }

        final var childCompensation = childCompensationOptional.get();

        if (childCompensation.isImpossible() ||
                //
                // TODO This needs some improvement as GB a, b, c WHERE a= AND c= needs to reapply the
                //      predicate on c which is currently refused here.
                //
                childCompensation.isNeededForFiltering()) {
            return Compensation.impossibleCompensation();
        }

        final PredicateMultiMap.ResultCompensationFunction resultCompensationFunction;
        if (pullUp != null) {
            resultCompensationFunction = PredicateMultiMap.ResultCompensationFunction.noCompensationNeeded();
        } else {
            final var rootPullUp = adjustedPullUp.getRootPullUp();
            final var maxMatchMap = matchInfo.getMaxMatchMap();
            final var pulledUpResultValueOptional =
                    rootPullUp.pullUpMaybe(maxMatchMap.getQueryValue());
            if (pulledUpResultValueOptional.isEmpty()) {
                return Compensation.impossibleCompensation();
            }

            final var pulledUpResultValue = pulledUpResultValueOptional.get();

            resultCompensationFunction =
                    PredicateMultiMap.ResultCompensationFunction.of(baseAlias -> pulledUpResultValue.translateCorrelations(
                            TranslationMap.ofAliases(rootPullUp.getNestingAlias(), baseAlias), false));
        }

        final var unmatchedQuantifiers = partialMatch.getUnmatchedQuantifiers();
        Verify.verify(unmatchedQuantifiers.isEmpty());

        if (!resultCompensationFunction.isNeeded()) {
            return Compensation.noCompensation();
        }

        return childCompensation.derived(false,
                new LinkedIdentityMap<>(),
                getMatchedQuantifiers(partialMatch),
                unmatchedQuantifiers,
                partialMatch.getCompensatedAliases(),
                resultCompensationFunction);
    }

    @Nonnull
    public static Value nestedResults(@Nullable final Value groupingValue, @Nonnull final Value aggregateValue) {
        final var aggregateColumn = Column.unnamedOf(aggregateValue);
        if (groupingValue == null) {
            return RecordConstructorValue.ofColumns(ImmutableList.of(aggregateColumn));
        } else {
            final var groupingColumn = Column.unnamedOf(groupingValue);
            return RecordConstructorValue.ofColumns(ImmutableList.of(groupingColumn, aggregateColumn));
        }
    }

    @Nonnull
    public static Value flattenedResults(@Nullable final Value groupingKeyValue,
                                         @Nonnull final Value aggregateValue) {
        final var valuesBuilder = ImmutableList.<Value>builder();
        if (groupingKeyValue != null) {
            final var groupingResultType = groupingKeyValue.getResultType();
            if (groupingResultType.isRecord()) {
                Verify.verify(groupingResultType instanceof Type.Record);
                final var groupingResultRecordType = (Type.Record)groupingResultType;
                List<Type.Record.Field> fields = groupingResultRecordType.getFields();
                for (var i = 0; i < fields.size(); i++) {
                    valuesBuilder.add(FieldValue.ofOrdinalNumber(groupingKeyValue, i));
                }
            } else {
                valuesBuilder.add(groupingKeyValue);
            }
        }

        final var aggregateResultType = aggregateValue.getResultType();
        if (aggregateResultType.isRecord()) {
            Verify.verify(aggregateResultType instanceof Type.Record);
            final var aggregateResultRecordType = (Type.Record)aggregateResultType;
            List<Type.Record.Field> fields = aggregateResultRecordType.getFields();
            for (var i = 0; i < fields.size(); i++) {
                valuesBuilder.add(FieldValue.ofOrdinalNumber(aggregateValue, i));
            }
        } else {
            valuesBuilder.add(aggregateValue);
        }

        final var rcv = RecordConstructorValue.ofUnnamed(valuesBuilder.build());
        return rcv.simplify(AliasMap.identitiesFor(rcv.getCorrelatedTo()), ImmutableSet.of());
    }
}
