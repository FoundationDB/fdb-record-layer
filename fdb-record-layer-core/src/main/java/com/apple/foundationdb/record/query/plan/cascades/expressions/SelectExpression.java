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
import com.apple.foundationdb.record.query.combinatorics.CrossProduct;
import com.apple.foundationdb.record.query.combinatorics.PartiallyOrderedSet;
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
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ExistsPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValue;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValueComparisonRangePredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValueComparisonRangePredicate.Placeholder;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValueComparisonRangePredicate.Sargable;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public Iterable<MatchInfo> subsumedBy(@Nonnull final RelationalExpression candidateExpression,
                                          @Nonnull final AliasMap aliasMap,
                                          @Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap) {
        // TODO This method should be simplified by adding some structure to it.
        final Collection<MatchInfo> matchInfos = PartialMatch.matchesFromMap(partialMatchMap);

        Verify.verify(this != candidateExpression);

        if (getClass() != candidateExpression.getClass()) {
            return ImmutableList.of();
        }
        final var otherSelectExpression = (SelectExpression)candidateExpression;

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
                otherSelectExpression.getQuantifiers()
                        .stream()
                        .filter(quantifier -> quantifier instanceof Quantifier.ForEach)
                        .allMatch(quantifier -> aliasMap.containsTarget(quantifier.getAlias()));

        // TODO this is not really needed if we assign a property to the quantifier that allows us to reason about the
        //      the "default on empty" property as in does this quantifier flow a scalar result such as an "empty" value,
        //      a real value, or even null if the underlying graph evaluates to empty. The presence of such a property
        //      would help us here to make sure the additional non-matched quantifier is not eliminating records.
        if (!allOtherForEachQuantifiersMatched) {
            return ImmutableList.of();
        }

        //
        // Go through all matched existential quantifiers. Make sure that there is a top level exists() predicate
        // corresponding to each one.
        //
        if (getQuantifiers()
                .stream()
                .filter(quantifier -> quantifier instanceof Quantifier.Existential && aliasMap.containsSource(quantifier.getAlias()))
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
        
        final var otherResultValue = otherSelectExpression.getResultValue();
        final Optional<Value> remainingValueComputationOptional;
        if (!resultValue.semanticEquals(otherResultValue, aliasMap)) {
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
            final var allNonFiltering = otherSelectExpression.getPredicates()
                    .stream()
                    .allMatch(queryPredicate -> queryPredicate instanceof Placeholder || queryPredicate.isTautology());
            if (allNonFiltering) {
                return MatchInfo.tryMerge(partialMatchMap, mergedParameterBindingMap, PredicateMap.empty(), remainingValueComputationOptional,
                                Pair.of(candidateExpression.getResultValue(), getResultValue()))
                        .map(ImmutableList::of)
                        .orElse(ImmutableList.of());
            } else {
                return ImmutableList.of();
            }
        }

        //
        // Go through each predicate on the query side in an attempt to find predicates on the candidate side that
        // can subsume the current predicate. For instance, `x < 5` on the query side can be subsumed by a placeholder
        // for x in the candidate. A predicate `x < 5' is not subsumed by 'x = 10' as the set of records retained by
        // applying 'x = 10' does not contain all of those retained after applying 'x < 5'.
        // Predicates on the query side can at least be subsumed by a tautology 'true' which should always be possible.
        // Whenever the pairing is lossy, for instance `x < 5' is subsumed by 'x < 10', the query side predicate needs
        // to be compensated if and when the match is realized. Compensating a predicate usually means its reapplication
        // as a residual filter. If a predicate is matched to a placeholder, the match is considered lossless as the
        // placeholder can be morphed into the right lossless pairing.
        //
        // Join predicates: Join predicates are matched the same way as local predicates with a complication.
        // If a join come across a join predicate we need to consider the potential other application of this predicate
        // in another match and/or match candidate.
        //
        // Example:
        //
        // SELECT *
        // FROM R r, S s
        // WHERE r.a < 5 AND s.b = 10 AND r.x = s.y
        //
        // The predicate 'r.x = s.y' can be used as a predicate for matching an index on R(x, a) or for
        // matching an index on S(b, y). In fact the predicate needs to be shared in some way such that the planner
        // can later on make the right decision based on cost, etc.
        //
        // The way this is implemented is to create two matches one where the predicate is repossessed to the match
        // at hand. When we match R(x, a) we repossess r.x = r.y to be subsumed by r.x = ? on
        // the candidate side. Vice versa, when we match S(b, y) we repossess s.y = r.x to be subsumed by s.y = ? on the
        // candidate side.
        //
        // Using this approach we create a problem that these two matches can coexist in a way that they cannot
        // be realized, that is planned together at all as both matches provide the other's placeholder value. In fact,
        // we have forced the match to (if it were to be planned) become the inner of a join. It would be beneficial,
        // however, to also create a version of the match that does not consider the join predicate at all.
        // This would allow a match on S(b, y) to only consider the s.b = 10 predicate and would allow us to use the
        // same index but plan its access as the outer. This match is then planned together with the match using the
        // repossessed predicate:
        //
        // FLATMAP(INDEXSCAN(S(b, y, [10, -inf], [10, inf]) s, INDEXSCAN(R(x, a) [s.y, -inf], [s.y, 5]))
        //

        final var correlationOrder = getCorrelationOrder();
        final var localAliases = correlationOrder.getSet();
        final var dependsOnMap = correlationOrder.getTransitiveClosure();
        final var aliasToQuantifierMap = Quantifiers.aliasToQuantifierMap(getQuantifiers());

        for (final QueryPredicate predicate : getPredicates()) {
            // find all local correlations
            final var predicateCorrelatedTo =
                    predicate.getCorrelatedTo()
                            .stream()
                            .filter(localAliases::contains)
                            .collect(ImmutableSet.toImmutableSet());

            boolean correlatedToMatchedAliases = false;
            boolean correlatedToUnmatchedAliases = false;
            for (final var correlatedAlias : predicateCorrelatedTo) {
                if (aliasMap.containsSource(correlatedAlias)) {
                    // definitely correlated to local alias
                    correlatedToMatchedAliases = true;
                } else if (aliasToQuantifierMap.get(correlatedAlias) instanceof Quantifier.Existential) {
                    final var correlatedDependsOn = dependsOnMap.get(correlatedAlias);
                    for (final var dependsOnAlias : correlatedDependsOn) {
                        if (!correlatedToMatchedAliases && aliasMap.containsSource(dependsOnAlias)) {
                            correlatedToMatchedAliases = true;
                        } else {
                            correlatedToUnmatchedAliases = true;
                        }
                    }
                } else {
                    correlatedToUnmatchedAliases = true;
                }
            }

            if (correlatedToMatchedAliases) {
                final Set<PredicateMapping> impliedMappingsForPredicate =
                        predicate.findImpliedMappings(aliasMap, otherSelectExpression.getPredicates());

                if (!correlatedToUnmatchedAliases) {
                    predicateMappingsBuilder.add(impliedMappingsForPredicate);
                } else {
                    //
                    // The current predicate is a join predicate, thus we want to create different ways of mapping it:
                    // one where the data bound through other quantifiers is considered a bound constant value,
                    // and one were there is no mapping at all. This may be used during the data access rules to
                    // indicate that a particular quantifier is not available because it is not planned ahead of the
                    // matched quantifiers.
                    //
                    predicateMappingsBuilder.add(ImmutableSet.<PredicateMapping>builder()
                            .addAll(impliedMappingsForPredicate)
                            .add(PredicateMapping.noMappingCorrelated(predicate))
                            .build());
                }
            } else {
                predicateMappingsBuilder.add(ImmutableSet.of(PredicateMapping.noMappingUncorrelated(predicate)));
            }
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
                    final var unmappedOtherPredicates = Sets.<QueryPredicate>newIdentityHashSet();
                    unmappedOtherPredicates.addAll(otherSelectExpression.getPredicates());

                    final var parameterBindingMap = Maps.<CorrelationIdentifier, ComparisonRange>newHashMap();
                    final var predicateMapBuilder = PredicateMap.builder();
                    final var allLocalNonMatchedCorrelationsBuilder = ImmutableSet.<CorrelationIdentifier>builder();

                    for (final var predicateMapping : predicateMappings) {
                        if (predicateMapping.hasMapping()) {
                            predicateMapBuilder.put(predicateMapping.getQueryPredicate(), predicateMapping);
                            unmappedOtherPredicates.remove(predicateMapping.getCandidatePredicate());

                            final var parameterAliasOptional = predicateMapping.getParameterAliasOptional();
                            final var comparisonRangeOptional = predicateMapping.getComparisonRangeOptional();
                            if (parameterAliasOptional.isPresent() &&
                                    comparisonRangeOptional.isPresent()) {
                                parameterBindingMap.put(parameterAliasOptional.get(), comparisonRangeOptional.get());
                            }
                            
                            final var predicate = predicateMapping.getQueryPredicate();

                            allLocalNonMatchedCorrelationsBuilder
                                    .addAll(computeLocalNonMatchedCorrelations(aliasMap, aliasToQuantifierMap, dependsOnMap, predicate));
                        }
                    }

                    //
                    // This holds all correlations to things outside the matched quantifier set.
                    //
                    final var allLocalNonMatchedCorrelations = allLocalNonMatchedCorrelationsBuilder.build();

                    //
                    // It is possible that we created a mapping that can never be planned due to the
                    // potential introduction of a circular dependencies for join predicates. For instance,
                    //
                    // SELECT *
                    // FROM (SELECT * FROM T1) AS c1,
                    //      (SELECT * FROM T2 WHERE c1.a < T2.b) as c2
                    // WHERE c1.x > 5 AND c1.x = c2.y
                    //
                    // should not yield a predicate map that covers both
                    // c1.x > 5 AND c1.x = c2.y
                    // as that would later result in a graph like this:
                    //
                    // SELECT *
                    // FROM (SELECT * FROM T1 c1.x > 5 AND c1.x = c2.y) AS c1',
                    //      (SELECT * FROM T2 WHERE c1'.a < T2.b) as c2
                    //
                    // which introduces a circular dependency on c1 <- c1' <- c2 <- c1.
                    //

                    //
                    // Check if any of these aliases is in turn correlated to anything in the matched
                    // quantifier set.
                    //
                    for (final CorrelationIdentifier alias : allLocalNonMatchedCorrelations) {
                        if (dependsOnMap.get(alias)
                                .stream()
                                .anyMatch(aliasMap::containsSource)) {
                            return ImmutableList.of();
                        }
                    }
                    
                    //
                    // Go through the set of predicates a second time to check if using this match may not be optimal.
                    //
                    for (final var predicateMapping : predicateMappings) {
                        if (!predicateMapping.hasMapping() &&
                                predicateMapping.getMappingKind() == PredicateMapping.Kind.CORRELATED) {
                            //
                            // We didn't record a mapping for this particular predicate. That can only have happened
                            // because we didn't want to repossess the predicate for this candidate for this set of
                            // mappings.
                            // What can happen, though, is that there are other predicate mappings in this set that
                            // have repossessed predicates thus creating dependencies to set of aliases the
                            // respective predicate is correlated to. That set of aliases is free to use and if the
                            // current predicate has lesser requirements, that is, its correlations are a subset of the
                            // alias we already know are free to use, we reject this match as it is not optimal.
                            //
                            final var predicate = predicateMapping.getQueryPredicate();

                            if (StreamSupport.stream(computeLocalNonMatchedCorrelations(aliasMap,
                                                    aliasToQuantifierMap,
                                                    dependsOnMap,
                                                    predicate).spliterator(),
                                            false)
                                    .allMatch(allLocalNonMatchedCorrelations::contains)) {
                                // This match is not optimal and could be better
                                return ImmutableList.of();
                            }
                        }
                    }

                    //
                    // Last chance for unmapped predicates - if there is a placeholder or a tautology on the other side that is still
                    // unmapped, we can (and should) remove it from the unmapped other set now. The reasoning is that this predicate is
                    // not filtering, so it does not cause records to be filtered that are not filtered on the query side.
                    //
                    unmappedOtherPredicates
                            .removeIf(queryPredicate -> queryPredicate instanceof Placeholder || queryPredicate.isTautology());

                    if (!unmappedOtherPredicates.isEmpty()) {
                        return ImmutableList.of();
                    }

                    final var predicateMapOptional = predicateMapBuilder.buildMaybe();
                    return predicateMapOptional
                            .map(predicateMap -> {
                                final Optional<Map<CorrelationIdentifier, ComparisonRange>> allParameterBindingMapOptional =
                                        MatchInfo.tryMergeParameterBindings(ImmutableList.of(mergedParameterBindingMap, parameterBindingMap));

                                return allParameterBindingMapOptional
                                        .flatMap(allParameterBindingMap -> MatchInfo.tryMerge(partialMatchMap, allParameterBindingMap, predicateMap, remainingValueComputationOptional,
                                                Pair.of(candidateExpression.getResultValue().simplify(aliasMap, Set.of()), getResultValue().simplify(aliasMap, Set.of()))))
                                        .map(ImmutableList::of)
                                        .orElse(ImmutableList.of());
                            })
                            .orElse(ImmutableList.of());
                });
    }

    private Iterable<CorrelationIdentifier> computeLocalNonMatchedCorrelations(@Nonnull final AliasMap aliasMap,
                                                                               @Nonnull final Map<CorrelationIdentifier, Quantifier> aliasToQuantifierMap,
                                                                               @Nonnull final ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> dependsOnMap,
                                                                               @Nonnull final QueryPredicate predicate) {
        final var localAliases = aliasToQuantifierMap.keySet();
        return () -> predicate.getTransitivelyCorrelatedTo(localAliases, dependsOnMap)
                .stream()
                .filter(alias -> !(aliasToQuantifierMap.get(alias) instanceof Quantifier.Existential))
                .filter(alias -> !aliasMap.containsSource(alias))
                .iterator();
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

    private static List<? extends QueryPredicate> partitionPredicates(final List<? extends QueryPredicate> predicates) {
        final var flattenedAndPredicates =
                predicates.stream()
                        .flatMap(predicate -> flattenAndPredicate(predicate).stream())
                        .collect(ImmutableList.toImmutableList());

        // partition predicates in value-based predicates and non-value-based predicates
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

        final var boundEquivalence = new BoundEquivalence(boundIdentitiesMap);

        final var partitionedPredicatesWithValues =
                predicateWithValues
                        .stream()
                        .collect(Multimaps.toMultimap(
                                predicate -> boundEquivalence.wrap(predicate.getValue()), Function.identity(), LinkedHashMultimap::create));

        partitionedPredicatesWithValues
                .asMap()
                .forEach((valueWrapper, predicatesOnValue) -> {
                    final var value = Objects.requireNonNull(valueWrapper.get());
                    var resultRange = ComparisonRange.EMPTY;
                    for (final PredicateWithValue predicateOnValue : predicatesOnValue) {
                        if (predicateOnValue instanceof ValuePredicate) {
                            final var comparison = ((ValuePredicate)predicateOnValue).getComparison();

                            final var mergeResult = resultRange.merge(comparison);

                            resultRange = mergeResult.getComparisonRange();

                            mergeResult.getResidualComparisons()
                                    .forEach(residualComparison ->
                                            resultPredicatesBuilder.add(value.withComparison(residualComparison)));
                        } else if (predicateOnValue instanceof Sargable) {
                            final var valueComparisonRangePredicate = (Sargable)predicateOnValue;
                            final var comparisonRange = valueComparisonRangePredicate.getComparisonRange();

                            final var mergeResult = resultRange.merge(comparisonRange);

                            resultRange = mergeResult.getComparisonRange();

                            mergeResult.getResidualComparisons()
                                    .forEach(residualComparison ->
                                            resultPredicatesBuilder.add(value.withComparison(residualComparison)));
                        } else {
                            resultPredicatesBuilder.add(predicateOnValue);
                        }
                    }
                    if (!resultRange.isEmpty()) {
                        resultPredicatesBuilder.add(ValueComparisonRangePredicate.sargable(value, resultRange));
                    }
                });

        return resultPredicatesBuilder.build();
    }

    private static List<QueryPredicate> flattenAndPredicate(final QueryPredicate predicate) {
        final var result = ImmutableList.<QueryPredicate>builder();

        if (predicate instanceof AndPredicate) {
            for (final var child : ((AndPredicate)predicate).getChildren()) {
                result.addAll(flattenAndPredicate(child));
            }
            return result.build();
        }
        return result.add(predicate).build();
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

                final Optional<ExpandCompensationFunction> injectCompensationFunctionOptional =
                        predicateMapping
                                .compensatePredicateFunction()
                                .injectCompensationFunctionMaybe(partialMatch, boundParameterPrefixMap);

                injectCompensationFunctionOptional.ifPresent(injectCompensationFunction -> predicateCompensationMap.put(predicate, injectCompensationFunction));
            }
        }

        final var unmatchedQuantifiers = partialMatch.computeUnmatchedQuantifiers(this);
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

        return Compensation.ofChildCompensationAndPredicateMap(childCompensation,
                predicateCompensationMap,
                computeMatchedQuantifiers(partialMatch),
                unmatchedQuantifiers,
                partialMatch.getCompensatedAliases(),
                matchInfo.getRemainingComputationValueOptional());
    }
}
