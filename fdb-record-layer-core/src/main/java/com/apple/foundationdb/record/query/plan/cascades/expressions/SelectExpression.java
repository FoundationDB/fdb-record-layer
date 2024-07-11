/*
 * SelectExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.combinatorics.CrossProduct;
import com.apple.foundationdb.record.query.combinatorics.PartiallyOrderedSet;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.Compensation;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.IdentityBiMap;
import com.apple.foundationdb.record.query.plan.cascades.IterableHelpers;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentityMap;
import com.apple.foundationdb.record.query.plan.cascades.MatchInfo;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMap;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap.ExpandCompensationFunction;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap.PredicateMapping;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.ValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndOrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ExistsPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.Placeholder;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValue;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValueAndRanges;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.RangeConstraints;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A select expression.
 */
@API(API.Status.EXPERIMENTAL)
public class SelectExpression implements RelationalExpressionWithChildren.ChildrenAsSet, RelationalExpressionWithPredicates, InternalPlannerGraphRewritable {
    @Nonnull
    private final Value resultValue;
    @Nonnull
    private final List<Quantifier> children;
    @Nonnull
    private final List<? extends QueryPredicate> predicates;
    @Nonnull
    private final Supplier<Integer> hashCodeWithoutChildrenSupplier;
    @Nonnull
    private final Supplier<Set<CorrelationIdentifier>> correlatedToWithoutChildrenSupplier;
    @Nonnull
    private final Supplier<Map<CorrelationIdentifier, ? extends Quantifier>> aliasToQuantifierMapSupplier;
    @Nonnull
    private final Supplier<PartiallyOrderedSet<CorrelationIdentifier>> correlationOrderSupplier;
    @Nonnull
    private final Supplier<Set<Set<CorrelationIdentifier>>> independentQuantifiersPartitioningSupplier;

    public SelectExpression(@Nonnull Value resultValue,
                            @Nonnull List<? extends Quantifier> children,
                            @Nonnull List<? extends QueryPredicate> predicates) {
        this.resultValue = resultValue;
        this.children = ImmutableList.copyOf(children);
        this.predicates = predicates.isEmpty()
                          ? ImmutableList.of()
                          : partitionPredicates(predicates);
        this.hashCodeWithoutChildrenSupplier = Suppliers.memoize(this::computeHashCodeWithoutChildren);
        this.correlatedToWithoutChildrenSupplier = Suppliers.memoize(this::computeCorrelatedToWithoutChildren);
        this.aliasToQuantifierMapSupplier = Suppliers.memoize(() -> Quantifiers.aliasToQuantifierMap(children));
        this.correlationOrderSupplier = Suppliers.memoize(this::computeCorrelationOrder);
        this.independentQuantifiersPartitioningSupplier = Suppliers.memoize(this::computeIndependentQuantifiersPartitioning);
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return resultValue;
    }

    @Nonnull
    public List<? extends Value> getResultValues() {
        return Values.deconstructRecord(getResultValue());
    }

    @Nonnull
    @Override
    public List<? extends QueryPredicate> getPredicates() {
        return predicates;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return children;
    }

    @Override
    public int getRelationalChildCount() {
        return children.size();
    }

    @Override
    public boolean canCorrelate() {
        return true;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return correlatedToWithoutChildrenSupplier.get();
    }

    @Nonnull
    private Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return Streams.concat(predicates.stream().flatMap(queryPredicate -> queryPredicate.getCorrelatedTo().stream()),
                        resultValue.getCorrelatedTo().stream())
                .collect(ImmutableSet.toImmutableSet());
    }

    @Nonnull
    @Override
    public SelectExpression translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        List<QueryPredicate> translatedPredicates = predicates.stream().map(p -> p.translateCorrelations(translationMap)).collect(Collectors.toList());
        final Value translatedResultValue = resultValue.translateCorrelations(translationMap);
        return new SelectExpression(translatedResultValue, translatedQuantifiers, translatedPredicates);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    @SuppressWarnings({"UnstableApiUsage", "PMD.CompareObjectsWithEquals"})
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap aliasMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }

        final var otherPredicates = ((SelectExpression)otherExpression).getPredicates();
        return semanticEqualsForResults(otherExpression, aliasMap) &&
               predicates.size() == otherPredicates.size() &&
               Streams.zip(predicates.stream(),
                       otherPredicates.stream(),
                       (queryPredicate, otherQueryPredicate) -> queryPredicate.semanticEquals(otherQueryPredicate, aliasMap))
                .allMatch(isSame -> isSame);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return hashCodeWithoutChildrenSupplier.get();
    }

    private int computeHashCodeWithoutChildren() {
        return Objects.hash(getPredicates(), getResultValue());
    }

    @Nonnull
    public Map<CorrelationIdentifier, ? extends Quantifier> getAliasToQuantifierMap() {
        return aliasToQuantifierMapSupplier.get();
    }

    @Nonnull
    @Override
    public PartiallyOrderedSet<CorrelationIdentifier> getCorrelationOrder() {
        return correlationOrderSupplier.get();
    }

    @Nonnull
    private PartiallyOrderedSet<CorrelationIdentifier> computeCorrelationOrder() {
        return RelationalExpressionWithChildren.ChildrenAsSet.super.getCorrelationOrder();
    }

    /**
     * Returns the partitioning of independent {@link Quantifier}s (via their aliases). A set of quantifiers is
     * independent of another set of {@link Quantifier}s if no elements in these two sets are correlated to one another
     * or are connected by join predicates.
     * @return a set of sets of aliases where each element is a set of aliases that is only correlated or connected to
     *         other elements of the same set. Any element (alias) from two different partitions are independent.
     */
    @Nonnull
    public Set<Set<CorrelationIdentifier>> getIndependentQuantifiersPartitioning() {
        return independentQuantifiersPartitioningSupplier.get();
    }

    @Nonnull
    private Set<Set<CorrelationIdentifier>> computeIndependentQuantifiersPartitioning() {
        final var fullCorrelationOrder = getCorrelationOrder().getTransitiveClosure();

        final var partitioning = Sets.<Set<CorrelationIdentifier>>newHashSet();

        // initially create one-partitions
        final var quantifiers = getQuantifiers();
        quantifiers.forEach(quantifier -> partitioning.add(Sets.newHashSet(quantifier.getAlias())));

        // compute a map from each predicate to all local aliases this predicate is transitively correlated to
        final var predicateTransitivelyCorrelatedToMap = new IdentityHashMap<QueryPredicate, Set<CorrelationIdentifier>>();
        for (final var predicate : getPredicates()) {
            final var transitivelyCorrelatedTo = predicate.getCorrelatedTo()
                    .stream()
                    .flatMap(alias -> fullCorrelationOrder.get(alias).stream())
                    .collect(ImmutableSet.toImmutableSet());
            predicateTransitivelyCorrelatedToMap.put(predicate, transitivelyCorrelatedTo);
        }

        // got through all quantifiers and use both correlation order among them and connecting predicates
        // to establish the partitioning
        for (final var quantifier : getQuantifiers()) {
            final var alias = quantifier.getAlias();

            final var connectedAliasesBuilder = ImmutableSet.<CorrelationIdentifier>builder();
            connectedAliasesBuilder.add(alias);
            connectedAliasesBuilder.addAll(fullCorrelationOrder.get(alias));

            predicateTransitivelyCorrelatedToMap.forEach((predicate, transitivelyCorrelatedTo) -> {
                if (transitivelyCorrelatedTo.contains(alias)) {
                    connectedAliasesBuilder.addAll(transitivelyCorrelatedTo);
                }
            });
            final var connectedAliases = connectedAliasesBuilder.build();

            final var newPartition = Sets.<CorrelationIdentifier>newHashSet();
            final var partitioningIterator = partitioning.iterator();
            while (partitioningIterator.hasNext()) {
                final var partition = partitioningIterator.next();
                if (connectedAliases.stream().anyMatch(partition::contains)) {
                    newPartition.addAll(partition);
                    partitioningIterator.remove();
                }
            }

            partitioning.add(newPartition);

            // everything is connected -- we can early out
            if (partitioning.size() == 1) {
                return partitioning;
            }
        }
        return partitioning;
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public Iterable<MatchInfo> subsumedBy(@Nonnull final RelationalExpression candidateExpression,
                                          @Nonnull final AliasMap bindingAliasMap,
                                          @Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap,
                                          @Nonnull final EvaluationContext evaluationContext) {
        // TODO This method should be simplified by adding some structure to it.
        final Collection<MatchInfo> matchInfos = PartialMatch.matchInfosFromMap(partialMatchMap);

        Verify.verify(this != candidateExpression);

        if (getClass() != candidateExpression.getClass()) {
            return ImmutableList.of();
        }
        final var candidateSelectExpression = (SelectExpression)candidateExpression;

        // merge parameter maps -- early out if a binding clashes
        final var parameterBindingMaps =
                matchInfos
                        .stream()
                        .map(MatchInfo::getParameterBindingMap)
                        .collect(ImmutableList.toImmutableList());
        final var mergedParameterBindingMapOptional =
                MatchInfo.tryMergeParameterBindings(parameterBindingMaps);
        if (mergedParameterBindingMapOptional.isEmpty()) {
            return ImmutableList.of();
        }
        final var mergedParameterBindingMap = mergedParameterBindingMapOptional.get();
        
        final var matchedCorrelatedToBuilder = ImmutableSet.<CorrelationIdentifier>builder();
        // Loop through all child matches and reject a match if the children matches were unable to match all
        // for-each quantifiers. Also keep track of all aliases the matched quantifiers are correlated to.
        for (final var quantifier : getQuantifiers()) {
            if (partialMatchMap.containsKeyUnwrapped(quantifier)) {
                if (quantifier instanceof Quantifier.ForEach) {
                    // current quantifier is matched
                    final var childPartialMatch = Objects.requireNonNull(partialMatchMap.getUnwrapped(quantifier));
                    if (!childPartialMatch.compensationCanBeDeferred()) {
                        return ImmutableList.of();
                    }
                }

                matchedCorrelatedToBuilder.addAll(quantifier.getCorrelatedTo());
            }
        }

        // Loop through all for-each quantifiers on the other side to ensure that they are all matched.
        // If any are not matched we cannot establish a match at all.
        final var allOtherForEachQuantifiersMatched =
                candidateSelectExpression.getQuantifiers()
                        .stream()
                        .filter(quantifier -> quantifier instanceof Quantifier.ForEach)
                        .allMatch(quantifier -> bindingAliasMap.containsTarget(quantifier.getAlias()));

        // TODO this is not really needed if we assign a property to the quantifier that allows us to reason about the
        //      the "default on empty" property as in does this quantifier flow a scalar result such as an "empty" value,
        //      a real value, or even null if the underlying graph evaluates to empty. The presence of such a property
        //      would help us here to make sure the additional non-matched quantifier is not eliminating records.
        if (!allOtherForEachQuantifiersMatched) {
            return ImmutableList.of();
        }

        // TODO this should be inverted, i.e. go through the predicates and make sure the referred alias is among the
        //      quantifiers owned by this expression
        //
        // Go through all matched existential quantifiers. Make sure that there is a top level exists() predicate
        // corresponding to each one.
        //
        if (getQuantifiers()
                .stream()
                .filter(quantifier -> quantifier instanceof Quantifier.Existential && bindingAliasMap.containsSource(quantifier.getAlias()))
                .anyMatch(quantifier -> getPredicates()
                        .stream()
                        .noneMatch(predicate -> predicate instanceof ExistsPredicate &&
                                                ((ExistsPredicate)predicate).getExistentialAlias().equals(quantifier.getAlias()))
                )) {
            return ImmutableList.of();
        }

        //
        // Check the result values of both expressions to see if we can match and if we can, whether we need a
        // compensating computation.
        //
        
        final var candidateResultValue = candidateSelectExpression.getResultValue();
        final Optional<Value> remainingValueComputationOptional;
        if (!resultValue.semanticEquals(candidateResultValue, bindingAliasMap)) {
            // we potentially need to compensate
            remainingValueComputationOptional = Optional.of(resultValue);
        } else {
            remainingValueComputationOptional = Optional.empty();
        }

        //
        // Map predicates on the query side to predicates on the candidate side. Record parameter bindings and/or
        // compensations for each mapped predicate.
        // A predicate on this side (the query side) can cause us to filter out rows, a mapped predicate (for that
        // predicate) can only filter out fewer rows which is correct and can be compensated for. The important part
        // is that we must not have predicates on the other (candidate) side at the end of this mapping process which
        // would mean that the candidate eliminates records that the query side may not eliminate. If we detect that
        // case we MUST not create a match.
        //
        final var predicateMappingsBuilder = ImmutableList.<Iterable<PredicateMapping>>builder();

        //
        // Handle the "on empty" case, i.e., the case where there are no predicates on the query side that can
        // map to anything on the candidate side. Conceptually that could be handled with a fake tautology predicate
        // but this seems to be simpler albeit a little more verbose here. The important restriction in order to
        // produce a match is that the candidate side MUST NOT be filtering at all, as the query side is not either.
        //
        if (getPredicates().isEmpty()) {
            final var allNonFiltering = candidateSelectExpression.getPredicates()
                    .stream()
                    .allMatch(QueryPredicate::isTautology);
            if (allNonFiltering) {
                return MatchInfo.tryMerge(partialMatchMap, mergedParameterBindingMap, PredicateMap.empty(), PredicateMap.empty(),
                                remainingValueComputationOptional, Optional.empty(), QueryPlanConstraint.tautology())
                        .map(ImmutableList::of)
                                .orElse(ImmutableList.of());
            } else {
                return ImmutableList.of();
            }
        }

        final var correlationOrder = getCorrelationOrder();
        final var localAliases = correlationOrder.getSet();
        final var dependsOnMap = correlationOrder.getTransitiveClosure();
        final var aliasToQuantifierMap = Quantifiers.aliasToQuantifierMap(getQuantifiers());

        final var bindingValueEquivalence = ValueEquivalence.fromAliasMap(bindingAliasMap)
                .then(ValueEquivalence.constantEquivalenceWithEvaluationContext(evaluationContext));

        for (final QueryPredicate predicate : getPredicates()) {
            // find all local correlations
            final var predicateCorrelatedTo =
                    predicate.getCorrelatedTo()
                            .stream()
                            .filter(localAliases::contains)
                            .collect(ImmutableSet.toImmutableSet());

            for (final var correlatedAlias : predicateCorrelatedTo) {
                if (!bindingAliasMap.containsSource(correlatedAlias)) {
                    //
                    // The reason for the following if is tricky. An EXISTS() over an existential can be matched
                    // even though the existential quantifier itself is not matched. This can happen even if the candidate
                    // does not have a quantifier counterpart for the existential quantifier.
                    // This means that is impossible for the match to apply the existential as part of the sargables
                    // itself. It is, however, possible that another match (presumably using a different candidate) is
                    // created independently of this one that can actually use the existential in its own match. Then,
                    // the data access rule may intersect the two matches (and their intersections) and get rid of the
                    // need of compensating for this existential altogether.
                    // TODO Figure out if we can go ahead with a match that creates an impossible compensation. In that
                    //      way the match cannot be used by itself but can participate in an intersection that may
                    //      eliminate that impossible compensation
                    //
                    if (aliasToQuantifierMap.get(correlatedAlias) instanceof Quantifier.Existential) {
                        final var correlatedDependsOn = dependsOnMap.get(correlatedAlias);
                        for (final var dependsOnAlias : correlatedDependsOn) {
                            if (!bindingAliasMap.containsSource(dependsOnAlias)) {
                                return ImmutableList.of();
                            }
                        }
                    } else {
                        return ImmutableList.of();
                    }
                }
            }

            final Iterable<PredicateMapping> impliedMappingsForPredicate =
                    predicate.findImpliedMappings(bindingValueEquivalence, candidateSelectExpression.getPredicates(),
                            evaluationContext);

            predicateMappingsBuilder.add(impliedMappingsForPredicate);
        }

        //
        // We now have a multimap from predicates on the query side to predicates on the candidate side. In the trivial
        // case, this multimap only contains singular mappings for a query predicate. If it doesn't, we need to enumerate
        // through their cross product exhaustively. Each complete and non-contradictory element of that cross product
        // can lead to a match.
        //
        final var crossedMappings =
                CrossProduct.crossProduct(predicateMappingsBuilder.build());

        return IterableHelpers.flatMap(crossedMappings,
                predicateMappings -> {
                    final var remainingUnmappedCandidatePredicates = Sets.<QueryPredicate>newIdentityHashSet();
                    remainingUnmappedCandidatePredicates.addAll(candidateSelectExpression.getPredicates());

                    final var parameterBindingMap = Maps.<CorrelationIdentifier, ComparisonRange>newHashMap();
                    final var predicateMapBuilder = PredicateMap.builder();

                    for (final var predicateMapping : predicateMappings) {
                        final var queryPredicate = predicateMapping.getQueryPredicate();
                        final var candidatePredicate = predicateMapping.getCandidatePredicate();
                        predicateMapBuilder.put(queryPredicate, predicateMapping);
                        remainingUnmappedCandidatePredicates.remove(candidatePredicate);

                        final var parameterAliasOptional = predicateMapping.getParameterAliasOptional();
                        final var comparisonRangeOptional = predicateMapping.getComparisonRangeOptional();
                        if (parameterAliasOptional.isPresent() &&
                                comparisonRangeOptional.isPresent()) {
                            parameterBindingMap.put(parameterAliasOptional.get(), comparisonRangeOptional.get());
                        }
                    }

                    //
                    // Last chance for unmapped predicates - if there is a placeholder or a tautology on the other side that is still
                    // unmapped, we can (and should) remove it from the unmapped other set now. The reasoning is that this predicate is
                    // not filtering, so it does not cause records to be filtered that are not filtered on the query side.
                    //
                    remainingUnmappedCandidatePredicates.removeIf(QueryPredicate::isTautology);

                    if (!remainingUnmappedCandidatePredicates.isEmpty()) {
                        return ImmutableList.of();
                    }

                    final var predicateMapOptional = predicateMapBuilder.buildMaybe();
                    return predicateMapOptional
                            .map(predicateMap -> {
                                final Optional<Map<CorrelationIdentifier, ComparisonRange>> allParameterBindingMapOptional =
                                        MatchInfo.tryMergeParameterBindings(ImmutableList.of(mergedParameterBindingMap, parameterBindingMap));
                                return allParameterBindingMapOptional
                                        .flatMap(allParameterBindingMap -> MatchInfo.tryMerge(partialMatchMap,
                                                allParameterBindingMap, predicateMap, PredicateMap.empty(),
                                                remainingValueComputationOptional, Optional.empty(),
                                                QueryPlanConstraint.tautology()))
                                        .map(ImmutableList::of)
                                        .orElse(ImmutableList.of());
                            })
                            .orElse(ImmutableList.of());
                });
    }

    @Nonnull
    @Override
    public Optional<MatchInfo> adjustMatch(@Nonnull final PartialMatch partialMatch) {
        final var childMatchInfo = partialMatch.getMatchInfo();
        if (childMatchInfo.getRemainingComputationValueOptional().isPresent()) {
            return Optional.empty();
        }

        for (final var predicate : getPredicates()) {
            if (predicate instanceof Placeholder) {
                if (!((Placeholder)predicate).getRanges().isEmpty()) {
                    // placeholder with a constraint, we need to bail
                    return Optional.empty();
                }
            } else if (!predicate.isTautology()) {
                // predicate that is not a tautology
                return Optional.empty();
            }
        }
        return Optional.of(childMatchInfo);
    }

    @Nonnull
    @Override
    public PlannerGraph rewriteInternalPlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.LogicalOperatorNode(this,
                        "SELECT " + resultValue,
                        getPredicates().isEmpty() ? ImmutableList.of() : ImmutableList.of("WHERE " + AndPredicate.and(getPredicates())),
                        ImmutableMap.of()),
                childGraphs);
    }

    @Override
    public String toString() {
        return "SELECT " + resultValue + " WHERE " + AndPredicate.and(getPredicates());
    }

    /**
     * Breaks a list of predicates into a number of sargables and value predicates.
     * @param predicates The predicates to partition.
     * @return a list of sargables and value predicates.
     */
    private static List<? extends QueryPredicate> partitionPredicates(@Nonnull final List<? extends QueryPredicate> predicates) {
        final var flattenedAndPredicates =
                predicates.stream()
                        .flatMap(predicate -> flattenPredicate(AndPredicate.class, predicate).stream())
                        .collect(ImmutableList.toImmutableList());

        final var predicateWithValuesBuilder = ImmutableList.<PredicateWithValue>builder();
        final var resultPredicatesBuilder = ImmutableList.<QueryPredicate>builder();

        for (final var flattenedAndPredicate : flattenedAndPredicates) {
            if (flattenedAndPredicate instanceof PredicateWithValue) {
                predicateWithValuesBuilder.add((PredicateWithValue)flattenedAndPredicate);
            } else {
                resultPredicatesBuilder.add(flattenedAndPredicate);
            }
        }

        final var predicateWithValues = predicateWithValuesBuilder.build();

        final var boundIdentitiesMap = AliasMap.identitiesFor(
                flattenedAndPredicates.stream()
                        .flatMap(predicate -> predicate.getCorrelatedTo().stream())
                        .collect(ImmutableSet.toImmutableSet()));

        final var boundEquivalence = new BoundEquivalence<Value>(boundIdentitiesMap);

        final var partitionedPredicatesWithValues =
                predicateWithValues
                        .stream()
                        .collect(Multimaps.toMultimap(
                                predicate -> boundEquivalence.wrap(predicate.getValue()), Function.identity(), LinkedHashMultimap::create));

        partitionedPredicatesWithValues
                .asMap()
                .forEach((valueWrapper, predicatesOnValue) -> {
                    final var value = Objects.requireNonNull(valueWrapper.get());
                    final var simplifiedConjunction = simplifyConjunction(value, predicatesOnValue);
                    resultPredicatesBuilder.addAll(simplifiedConjunction);
                });

        return resultPredicatesBuilder.build();
    }

    /**
     * Simplifies the conjunction of predicates defined on a value into a sargable with coalesced compile-time range (and
     * other predicates which are not compile-time evaluable but can be used as scan prefix) and a set of residual predicates
     * comprising those which are not compile-time and can not be used as a scan prefix.
     *
     * @param value The value on which the predicates are defined.
     * @param predicates a conjunction of predicates defined on the value.
     * @return a simplified list of predicates.
     */
    @Nonnull
    private static List<QueryPredicate> simplifyConjunction(@Nonnull final Value value,
                                                            @Nonnull final Collection<PredicateWithValue> predicates) {
        final ImmutableList.Builder<QueryPredicate> result = ImmutableList.builder();
        final var rangeBuilder = RangeConstraints.newBuilder();

        for (final var predicate : predicates) {
            if (predicate instanceof ValuePredicate) {
                final var predicateRange = ((ValuePredicate)predicate).getComparison();
                if (!rangeBuilder.addComparisonMaybe(predicateRange)) {
                    result.add(value.withComparison(predicateRange));  // give up.
                }
            } else if (predicate instanceof PredicateWithValueAndRanges && ((PredicateWithValueAndRanges)predicate).isSargable()) {
                final var predicateRange = Iterables.getOnlyElement(((PredicateWithValueAndRanges)predicate).getRanges());
                rangeBuilder.add(predicateRange);
            } else {
                result.add(predicate);
            }
        }

        // If the compile-time range is defined, create a sargable from it.
        final var rangeMaybe = rangeBuilder.build();
        rangeMaybe.ifPresent(range -> result.add(PredicateWithValueAndRanges.sargable(value, range)));

        return result.build();
    }

    /**
     * Flattens a {@link QueryPredicate} (potentially a tree) of predicates into an equivalent linear
     * structure of predicates.
     * @param classToLift a class of at least type {@link AndOrPredicate}.
     * @param predicate The {@link QueryPredicate}.
     * @return an equivalent, linear, {@link QueryPredicate}.
     */
    @Nonnull
    private static List<QueryPredicate> flattenPredicate(@Nonnull final Class<? extends AndOrPredicate> classToLift,
                                                         @Nonnull final QueryPredicate predicate) {
        final var result = ImmutableList.<QueryPredicate>builder();

        if (predicate.isAtomic()) {
            result.add(predicate);
        } else {
            if (classToLift.isInstance(predicate)) {
                for (final var child : ((AndOrPredicate)predicate).getChildren()) {
                    result.addAll(flattenPredicate(classToLift, child));
                }
            } else {
                final QueryPredicate flattenedChildPredicate;
                if (predicate instanceof AndPredicate) {
                    flattenedChildPredicate = AndPredicate.and(flattenPredicate(AndPredicate.class, predicate));
                } else if (predicate instanceof OrPredicate) {
                    flattenedChildPredicate = OrPredicate.or(flattenPredicate(OrPredicate.class, predicate));
                } else {
                    flattenedChildPredicate = predicate;
                }
                result.add(flattenedChildPredicate);
            }
        }
        return result.build();
    }

    @Override
    @SuppressWarnings({"java:S135", "java:S1066"})
    public Compensation compensate(@Nonnull final PartialMatch partialMatch, @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap) {
        final var predicateCompensationMap = new LinkedIdentityMap<QueryPredicate, ExpandCompensationFunction>();
        final var matchInfo = partialMatch.getMatchInfo();
        final var predicateMap = matchInfo.getPredicateMap();

        final var quantifiers = getQuantifiers();

        //
        // The partial match we are called with here has child matches that have compensations on their own.
        // Given a pair of these matches that we reach along two for-each quantifiers (forming a join) we have to
        // apply both compensations. The compensation class has a union method to combine two compensations in an
        // optimal way. We need to fold over all those compensations to form one child compensation. The tree that
        // is formed by partial matches therefore collapses into a chain of compensations.
        //
        final Compensation childCompensation = quantifiers
                .stream()
                .filter(quantifier -> quantifier instanceof Quantifier.ForEach)
                .flatMap(quantifier ->
                        matchInfo.getChildPartialMatch(quantifier)
                                .map(childPartialMatch -> childPartialMatch.compensate(boundParameterPrefixMap)).stream())
                .reduce(Compensation.noCompensation(), Compensation::union);

        if (childCompensation.isImpossible() ||
                !childCompensation.canBeDeferred()) {
            return Compensation.impossibleCompensation();
        }

        final var unmatchedQuantifiers = partialMatch.getUnmatchedQuantifiers();
        final var unmatchedQuantifierAliases = unmatchedQuantifiers.stream().map(Quantifier::getAlias).collect(ImmutableList.toImmutableList());
        boolean isImpossible = false;

        //
        // Go through all predicates and invoke the reapplication logic for each associated mapping. Remember, each
        // predicate MUST have a mapping to the other side (which may just be a tautology). If something needs to be
        // reapplied that logic creates the correct predicates. The reapplication logic is also passed enough context
        // to skip reapplication in which case we won't do anything when compensation needs to be applied.
        //
        for (final var predicate : getPredicates()) {
            final var predicateMappingOptional = predicateMap.getMappingOptional(predicate);
            if (predicateMappingOptional.isPresent()) {
                final var predicateMapping = predicateMappingOptional.get();

                final var queryPredicate = predicateMapping.getQueryPredicate();
                if (queryPredicate.getCorrelatedTo().stream().anyMatch(unmatchedQuantifierAliases::contains)) {
                    isImpossible = true;
                }

                final Optional<ExpandCompensationFunction> injectCompensationFunctionOptional =
                        predicateMapping
                                .compensatePredicateFunction()
                                .injectCompensationFunctionMaybe(partialMatch, boundParameterPrefixMap);

                injectCompensationFunctionOptional.ifPresent(injectCompensationFunction -> predicateCompensationMap.put(predicate, injectCompensationFunction));
            }
        }

        final var isCompensationNeeded =
                !unmatchedQuantifiers.isEmpty() || !predicateCompensationMap.isEmpty() || matchInfo.getRemainingComputationValueOptional().isPresent();

        if (!isCompensationNeeded) {
            return Compensation.noCompensation();
        }

        //
        // We now know we need compensation, and if we have more than one quantifier, we would have to translate
        // the references of the values from the query graph to values operating on the MQT in order to do that
        // compensation. We cannot do that (yet). If we, however, do not have to worry about compensation we just
        // this select entirely with the scan and there are no additional references to be considered.
        //
        final var partialMatchMap = partialMatch.getMatchInfo().getQuantifierToPartialMatchMap();
        if (quantifiers.stream()
                    .filter(quantifier -> quantifier instanceof Quantifier.ForEach && partialMatchMap.containsKeyUnwrapped(quantifier))
                    .count() > 1) {
            return Compensation.impossibleCompensation();
        }

        return Compensation.ofChildCompensationAndPredicateMap(isImpossible,
                childCompensation,
                predicateCompensationMap,
                getMatchedQuantifiers(partialMatch),
                unmatchedQuantifiers,
                partialMatch.getCompensatedAliases(),
                matchInfo.getRemainingComputationValueOptional());
    }
}
