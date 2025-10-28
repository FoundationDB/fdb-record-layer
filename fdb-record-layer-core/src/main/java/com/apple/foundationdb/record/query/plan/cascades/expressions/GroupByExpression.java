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
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AggregateIndexExpansionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.Compensation;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GroupByMappings;
import com.apple.foundationdb.record.query.plan.cascades.IdentityBiMap;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentityMap;
import com.apple.foundationdb.record.query.plan.cascades.MatchInfo;
import com.apple.foundationdb.record.query.plan.cascades.MatchInfo.RegularMatchInfo;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedSortOrder;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMap;
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
import com.apple.foundationdb.record.query.plan.cascades.values.AbstractValue;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.IndexableAggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.MaxMatchMap;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.PullUp;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

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

/**
 * A logical {@code group by} expression that represents grouping incoming tuples and aggregating each group.
 */
@API(API.Status.EXPERIMENTAL)
public class GroupByExpression extends AbstractRelationalExpressionWithChildren implements InternalPlannerGraphRewritable {

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
     * @param groupingValue The grouping {@code Value} used to determine individual groups, can be {@code null}
     *        indicating no grouping.
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
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
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
    public int computeHashCodeWithoutChildren() {
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
        Verify.verify(translatedQuantifiers.size() == 1);

        final AggregateValue translatedAggregateValue =
                (AggregateValue)getAggregateValue().translateCorrelations(translationMap, shouldSimplifyValues);
        final Value translatedGroupingValue =
                getGroupingValue() == null
                ? null
                : getGroupingValue().translateCorrelations(translationMap, shouldSimplifyValues);
        return new GroupByExpression(translatedGroupingValue, translatedAggregateValue, resultValueFunction,
                Iterables.getOnlyElement(translatedQuantifiers));
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
                RelationalExpression.pullUpAndComposeTranslationMapsMaybe(candidateExpression, bindingAliasMap,
                        partialMatchMap);
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

        final var aggregateValues =
                Values.primitiveAccessorsForType(aggregateValue.getResultType(), () -> aggregateValue).stream()
                        .map(primitiveAggregateValue -> primitiveAggregateValue.simplify(evaluationContext,
                                AliasMap.emptyMap(), ImmutableSet.of()))
                        .collect(ImmutableSet.toImmutableSet());
        if (aggregateValues.isEmpty()) {
            return ImmutableList.of();
        }

        final var otherAggregateValues =
                Values.primitiveAccessorsForType(otherAggregateValue.getResultType(),
                                () -> otherAggregateValue).stream()
                        .map(primitiveAggregateValue -> primitiveAggregateValue.simplify(evaluationContext,
                                AliasMap.emptyMap(), ImmutableSet.of()))
                        .collect(ImmutableSet.toImmutableSet());
        if (otherAggregateValues.size() != 1) {
            return ImmutableList.of();
        }
        final var otherPrimitiveAggregateValue = (IndexableAggregateValue)Iterables.getOnlyElement(otherAggregateValues);
        final var matchedAggregatesMapBuilder = ImmutableBiMap.<Value, Value>builder();
        final var unmatchedAggregatesMapBuilder =
                ImmutableBiMap.<CorrelationIdentifier, Value>builder();
        final var unmatchedTranslatedAggregatesValueMapBuilder =
                ImmutableMap.<Value, CorrelationIdentifier>builder();
        var subsumedAggregations = ConstrainedBoolean.falseValue();
        for (final var primitiveAggregateValue : aggregateValues) {
            final var translatedPrimitiveAggregateValue =
                    primitiveAggregateValue.translateCorrelations(translationMap, true);

            final var semanticEquals =
                    translatedPrimitiveAggregateValue.semanticEquals(otherPrimitiveAggregateValue, valueEquivalence);
            if (semanticEquals.isTrue()) {
                matchedAggregatesMapBuilder.put(primitiveAggregateValue, otherPrimitiveAggregateValue);
                subsumedAggregations = semanticEquals;
            } else {
                final var unmatchedId = UnmatchedAggregateValue.uniqueId();
                unmatchedAggregatesMapBuilder.put(unmatchedId, primitiveAggregateValue);
                unmatchedTranslatedAggregatesValueMapBuilder.put(translatedPrimitiveAggregateValue, unmatchedId);
            }
        }

        if (subsumedAggregations.isFalse()) {
            return ImmutableList.of();
        }

        final var subsumedGroupingsResult =
                groupingSubsumedBy(candidateInnerQuantifier,
                        Objects.requireNonNull(partialMatchMap.getUnwrapped(innerQuantifier)),
                        candidateGroupingValue, translationMap, valueEquivalence, evaluationContext);
        final var subsumedGroupings = Objects.requireNonNull(subsumedGroupingsResult.getSubsumedGroups());
        if (subsumedGroupings.isFalse()) {
            return ImmutableList.of();
        }
        final var matchedGroupingsMap = subsumedGroupingsResult.getMatchedGroupingsMap();
        final var rollUpToGroupingValues = subsumedGroupingsResult.getRollUpToValues();

        if (rollUpToGroupingValues != null &&
                !AggregateIndexExpansionVisitor.canBeRolledUp(otherPrimitiveAggregateValue.getIndexTypeName())) {
            // We determined we need a roll up, but we cannot do it base on the aggregations.
            return ImmutableList.of();
        }

        final var unmatchedTranslatedAggregateValueMap =
                unmatchedTranslatedAggregatesValueMapBuilder.buildKeepingLast();
        final var translatedResultValue = getResultValue().translateCorrelations(translationMap, true);
        final var maxMatchMap =
                MaxMatchMap.compute(translatedResultValue, candidateExpression.getResultValue(),
                        Quantifiers.aliases(candidateExpression.getQuantifiers()), valueEquivalence,
                        translatedUnmatchedValue -> onUnmatchedValue(unmatchedTranslatedAggregateValueMap,
                                translatedUnmatchedValue));
        final var queryPlanConstraint =
                subsumedGroupings.getConstraint().compose(maxMatchMap.getQueryPlanConstraint());

        return RegularMatchInfo.tryMerge(bindingAliasMap, partialMatchMap, ImmutableMap.of(), PredicateMap.empty(),
                        maxMatchMap,
                        GroupByMappings.of(matchedGroupingsMap, matchedAggregatesMapBuilder.build(),
                                unmatchedAggregatesMapBuilder.build()),
                        rollUpToGroupingValues, queryPlanConstraint)
                .map(ImmutableList::of)
                .orElse(ImmutableList.of());
    }

    @Nonnull
    private Optional<Value> onUnmatchedValue(@Nonnull final Map<Value, CorrelationIdentifier> unmatchedTranslatedAggregateValueMap,
                                             @Nonnull final Value translatedUnmatchedValue) {
        final var unmatchedId = unmatchedTranslatedAggregateValueMap.get(translatedUnmatchedValue);
        if (unmatchedId == null) {
            return Optional.empty();
        }
        return Optional.of(new UnmatchedAggregateValue(unmatchedId));
    }

    @Nonnull
    private SubsumedGroupingsResult groupingSubsumedBy(@Nonnull final Quantifier candidateInnerQuantifier,
                                                       @Nonnull final PartialMatch childMatch,
                                                       @Nullable final Value candidateGroupingValue,
                                                       @Nonnull final TranslationMap translationMap,
                                                       @Nonnull final ValueEquivalence valueEquivalence,
                                                       @Nonnull final EvaluationContext evaluationContext) {
        if (groupingValue == null && candidateGroupingValue == null) {
            return SubsumedGroupingsResult.withoutRollUp(ConstrainedBoolean.alwaysTrue(), ImmutableBiMap.of());
        }
        if (candidateGroupingValue == null) {
            return SubsumedGroupingsResult.noSubsumption();
        }

        final List<Value> translatedGroupingValues; // with duplicate groupings if present
        final BiMap<Value, Value> matchedGroupingsMap;
        if (groupingValue != null) {
            final var translatedGroupingsValuesBuilder = ImmutableList.<Value>builder();
            final var matchedGroupingsMapBuilder = ImmutableMap.<Value, Value>builder();
            final var groupingValues =
                    Values.primitiveAccessorsForType(groupingValue.getResultType(), () -> groupingValue).stream()
                            .map(primitiveGroupingValue -> primitiveGroupingValue.simplify(evaluationContext,
                                    AliasMap.emptyMap(), ImmutableSet.of()))
                            .collect(ImmutableList.toImmutableList());
            for (final var primitiveGroupingValue : groupingValues) {
                final var translatedPrimitiveGroupingValue =
                        primitiveGroupingValue.translateCorrelations(translationMap, true);
                // TODO is this needed?.simplify(evaluationContext, AliasMap.emptyMap(), ImmutableSet.of());
                translatedGroupingsValuesBuilder.add(translatedPrimitiveGroupingValue);
                matchedGroupingsMapBuilder.put(primitiveGroupingValue, translatedPrimitiveGroupingValue);
            }
            translatedGroupingValues = translatedGroupingsValuesBuilder.build();

            //
            // We know that if there are duplicates, they will be on the query side. Immutable bi-maps do not support
            // duplicated keys at all while regular maps do. The simplest and also the cheapest solution is to just
            // use an immutable map builder (which then is de-duped when built) and then use that map to build the
            // bi-map.
            //
            matchedGroupingsMap = ImmutableBiMap.copyOf(matchedGroupingsMapBuilder.buildKeepingLast());
        } else {
            translatedGroupingValues = ImmutableList.of();
            matchedGroupingsMap = ImmutableBiMap.of();
        }

        final Set<Value> translatedGroupingValuesSet = ImmutableSet.copyOf(translatedGroupingValues);

        final var candidateGroupingValues =
                Values.primitiveAccessorsForType(candidateGroupingValue.getResultType(),
                                () -> candidateGroupingValue).stream()
                        .map(primitiveGroupingValue -> primitiveGroupingValue.simplify(evaluationContext,
                                AliasMap.emptyMap(), ImmutableSet.of()))
                        .collect(ImmutableList.toImmutableList());

        //
        // If there are more groupingValues than candidateGroupingValues, we cannot match the index.
        //
        final var unmatchedCandidateValues = new LinkedHashSet<>(candidateGroupingValues);
        if (translatedGroupingValuesSet.size() > unmatchedCandidateValues.size()) {
            return SubsumedGroupingsResult.noSubsumption();
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
        var booleanWithConstraint = ConstrainedBoolean.alwaysTrue();
        for (final var translatedGroupingValue : translatedGroupingValuesSet) {
            var found = false;

            for (final var iterator = unmatchedCandidateValues.iterator(); iterator.hasNext(); ) {
                final var candidateGroupingPartValue = iterator.next();
                final var semanticEquals =
                        translatedGroupingValue.semanticEquals(candidateGroupingPartValue, valueEquivalence);
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
                return SubsumedGroupingsResult.noSubsumption();
            }
            if (unmatchedCandidateValues.isEmpty()) {
                break;
            }
        }

        if (unmatchedCandidateValues.isEmpty()) {
            // return with a positive result if sets where in fact semantically equal
            return SubsumedGroupingsResult.withoutRollUp(booleanWithConstraint, matchedGroupingsMap);
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
                                .anyMatch(comparison -> comparison.getType().isEquality());
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

        if (!unmatchedCandidateValues.isEmpty()) {
            Verify.verify(candidateGroupingValues.size() > translatedGroupingValuesSet.size());

            //
            // This is a potential roll-up case, but only if the query side's groupings are completely subsumed
            // by the prefix of the candidate side. Iterate up to the smaller query side's grouping values to
            // find out.
            //
            for (final var translatedGroupingValue : translatedGroupingValuesSet) {
                if (unmatchedCandidateValues.contains(translatedGroupingValue)) {
                    return SubsumedGroupingsResult.noSubsumption();
                }
            }
            return SubsumedGroupingsResult.of(booleanWithConstraint, matchedGroupingsMap, translatedGroupingValues);
        }

        return SubsumedGroupingsResult.withoutRollUp(booleanWithConstraint, matchedGroupingsMap);
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
                                   @Nonnull final CorrelationIdentifier candidateAlias) {
        final var regularMatchInfo = partialMatch.getRegularMatchInfo();
        final var quantifier = Iterables.getOnlyElement(getQuantifiers());

        final var nestedPullUpPair =
                partialMatch.nestPullUp(pullUp, candidateAlias);
        final var rootOfMatchPullUp = nestedPullUpPair.getKey();
        final var adjustedPullUp = Objects.requireNonNull(nestedPullUpPair.getRight());

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

        if (childCompensation.isImpossible()) {
            //
            // Note that it may be better to just return the child compensation verbatim as that compensation
            // may be combinable with something else to make it possible while the statically impossible compensation
            // can never combine into anything that is possible.
            //
            return Compensation.impossibleCompensation();
        }

        final var compensatedResultOptional =
                Compensation.computeResultCompensation(partialMatch, rootOfMatchPullUp);
        if (compensatedResultOptional.isEmpty()) {
            return Compensation.impossibleCompensation();
        }
        final var compensatedResult = compensatedResultOptional.get();
        if (!childCompensation.isNeeded() &&
                !compensatedResult.getResultCompensationFunction().isNeeded()) {
            return Compensation.noCompensation();
        }

        final var unmatchedQuantifiers = partialMatch.getUnmatchedQuantifiers();
        Verify.verify(unmatchedQuantifiers.isEmpty());

        return childCompensation.derived(compensatedResult.isCompensationImpossible(),
                new LinkedIdentityMap<>(),
                getMatchedQuantifiers(partialMatch),
                unmatchedQuantifiers,
                partialMatch.getCompensatedAliases(),
                compensatedResult.getResultCompensationFunction(),
                compensatedResult.getGroupByMappings());
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
        return rcv.simplify(EvaluationContext.empty(),
                AliasMap.identitiesFor(rcv.getCorrelatedTo()), ImmutableSet.of());
    }

    public static class UnmatchedAggregateValue extends AbstractValue implements Value.NonEvaluableValue {
        @Nonnull
        private final CorrelationIdentifier unmatchedId;

        public UnmatchedAggregateValue(@Nonnull final CorrelationIdentifier unmatchedId) {
            this.unmatchedId = unmatchedId;
        }

        @Nonnull
        public CorrelationIdentifier getUnmatchedId() {
            return unmatchedId;
        }

        @Nonnull
        @Override
        protected Iterable<? extends Value> computeChildren() {
            return ImmutableList.of();
        }

        @Nonnull
        @Override
        public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
            Verify.verify(Iterables.isEmpty(explainSuppliers));
            return ExplainTokensWithPrecedence.of(new ExplainTokens().addFunctionCall("unmatched",
                    new ExplainTokens().addIdentifier(unmatchedId.getId())));
        }

        @Override
        public int hashCodeWithoutChildren() {
            return unmatchedId.hashCode();
        }

        @Nonnull
        @Override
        public PValue toValueProto(@Nonnull final PlanSerializationContext serializationContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int planHash(@Nonnull final PlanHashMode hashMode) {
            throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public Message toProto(@Nonnull final PlanSerializationContext serializationContext) {
            throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public Value withChildren(final Iterable<? extends Value> newChildren) {
            Verify.verify(Iterables.isEmpty(newChildren));
            return this;
        }

        @Nonnull
        public static CorrelationIdentifier uniqueId() {
            return CorrelationIdentifier.uniqueId(UnmatchedAggregateValue.class);
        }
    }

    private static class SubsumedGroupingsResult {
        @Nonnull
        private final ConstrainedBoolean subsumedGroups;
        @Nonnull
        private final BiMap<Value, Value> matchedGroupingsMap;
        @Nullable
        private final List<Value> rollUpToValues;

        private SubsumedGroupingsResult(@Nonnull final ConstrainedBoolean subsumedGroups,
                                        @Nonnull final BiMap<Value, Value> matchedGroupingsMap,
                                        @Nullable final List<Value> rollUpToValues) {
            this.subsumedGroups = subsumedGroups;
            this.matchedGroupingsMap = matchedGroupingsMap;
            this.rollUpToValues = rollUpToValues;
        }

        @Nonnull
        public ConstrainedBoolean getSubsumedGroups() {
            return subsumedGroups;
        }

        @Nonnull
        public BiMap<Value, Value> getMatchedGroupingsMap() {
            return matchedGroupingsMap;
        }

        @Nullable
        public List<Value> getRollUpToValues() {
            return rollUpToValues;
        }

        @Nonnull
        public static SubsumedGroupingsResult noSubsumption() {
            return of(ConstrainedBoolean.falseValue(), ImmutableBiMap.of(), null);
        }

        @Nonnull
        public static SubsumedGroupingsResult withoutRollUp(@Nonnull final ConstrainedBoolean subsumedGroups,
                                                            @Nonnull final BiMap<Value, Value> matchedGroupingsMap) {
            return of(subsumedGroups, matchedGroupingsMap, null);
        }

        @Nonnull
        public static SubsumedGroupingsResult of(@Nonnull final ConstrainedBoolean subsumedGroups,
                                                 @Nonnull final BiMap<Value, Value> matchedGroupingsMap,
                                                 @Nullable final List<Value> rollUpToValues) {
            return new SubsumedGroupingsResult(subsumedGroups, ImmutableBiMap.copyOf(matchedGroupingsMap),
                    rollUpToValues == null ? null : ImmutableList.copyOf(rollUpToValues));
        }
    }
}
