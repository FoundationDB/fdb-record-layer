/*
 * Quantifiers.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.combinatorics.CrossProduct;
import com.apple.foundationdb.record.query.combinatorics.EnumeratingIterable;
import com.apple.foundationdb.record.query.combinatorics.EnumeratingIterator;
import com.apple.foundationdb.record.query.combinatorics.TopologicalSort;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier.Existential;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier.ForEach;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier.Physical;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.graph.BoundMatch;
import com.apple.foundationdb.record.query.plan.cascades.matching.graph.ComputingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.graph.DependencyUtils;
import com.apple.foundationdb.record.query.plan.cascades.matching.graph.FindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.graph.GenericMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.graph.MatchFunction;
import com.apple.foundationdb.record.query.plan.cascades.matching.graph.MatchPredicate;
import com.apple.foundationdb.record.query.plan.cascades.matching.graph.PredicatedMatcher;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.plans.QueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.base.Verify;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Auxiliary class containing factory methods and helpers for {@link Quantifier}.
 */
public class Quantifiers {

    private Quantifiers() {
        // prevent instantiation
    }

    @Nonnull
    public static Set<CorrelationIdentifier> aliases(@Nonnull final Iterable<? extends Quantifier> quantifiers) {
        return DependencyUtils.computeAliases(quantifiers, Quantifier::getAlias);
    }

    @Nonnull
    public static Map<CorrelationIdentifier, Quantifier> aliasToQuantifierMap(@Nonnull final Iterable<? extends Quantifier> quantifiers) {
        return DependencyUtils.computeAliasToElementMap(quantifiers, Quantifier::getAlias);
    }

    /**
     * Create a list of for-each quantifiers from a list of references to range over.
     * @param rangesOverExpressions iterable {@link Reference}s of {@link RelationalExpression}s
     * @return a list of for-each quantifiers where each quantifier ranges over one of the given references
     */
    @Nonnull
    public static List<ForEach> forEachQuantifiers(@Nonnull final Iterable<Reference> rangesOverExpressions) {
        return fromExpressions(rangesOverExpressions, Quantifier::forEach);
    }

    /**
     * Create a list of existential quantifiers from a list of expression references these quantifiers should range over.
     * @param rangesOverPlans iterable {@link Reference}s of of {@link RelationalExpression}s.
     * @return a list of physical quantifiers where each quantifier ranges over one of the given references
     */
    @Nonnull
    public static List<Existential> existentialQuantifiers(@Nonnull final Iterable<Reference> rangesOverPlans) {
        return fromExpressions(rangesOverPlans, Quantifier::existential);
    }

    /**
     * Create a list of physical quantifiers given a list of references these quantifiers should range over.
     * @param rangesOverPlans iterable {@link Reference}s of {@link RecordQueryPlan}
     * @return a list of physical quantifiers where each quantifier ranges over a reference contained in the given iterable
     */
    @Nonnull
    public static List<Physical> fromPlans(@Nonnull final Iterable<? extends Reference> rangesOverPlans) {
        return fromExpressions(rangesOverPlans, Quantifier::physical);
    }

    /**
     * Create a list of quantifiers given a list of references these quantifiers should range over.
     * @param rangesOverExpressions iterable of {@link Reference}s the quantifiers will be created to range over
     * @param creator lambda to to be called for each expression reference contained in {@code rangesOverExpression}s
     *        to create the actual quantifier. This allows for callers to create different kinds of quantifier based
     *        on needs.
     * @param <Q> the type of the quantifier to be created
     * @return a list of quantifiers where each quantifier ranges over an reference contained in the given iterable
     */
    @Nonnull
    public static <Q extends Quantifier> List<Q> fromExpressions(@Nonnull final Iterable<? extends Reference> rangesOverExpressions,
                                                                 @Nonnull final Function<Reference, Q> creator) {
        return StreamSupport
                .stream(rangesOverExpressions.spliterator(), false)
                .map(creator)
                .collect(Collectors.toList());
    }

    /**
     * Convenience helper to create a single ID translation map from the alias of one quantifier to the alias
     * of another quantifier.
     * @param from quantifier
     * @param to quantifier
     * @return a new translation map mapping from {@code from.getAlias()} to {@code to.getAlias()}
     */
    @Nonnull
    public static AliasMap translate(@Nonnull final Quantifier from, @Nonnull final Quantifier to) {
        return AliasMap.ofAliases(from.getAlias(), to.getAlias());
    }

    /**
     * Convenience helper to create an alias translation map based on a translation map using quantifiers.
     * of another quantifier.
     * @param map quantifier to quantifier bi-map
     * @return a new {@link AliasMap} mapping from {@code from.getAlias()} to {@code to.getAlias()}
     */
    @Nonnull
    public static AliasMap toAliasMap(@Nonnull final BiMap<Quantifier, Quantifier> map) {
        return AliasMap.copyOf(map.entrySet()
                .stream()
                .collect(ImmutableBiMap.toImmutableBiMap(entry -> entry.getKey().getAlias(),
                        entry -> entry.getValue().getAlias())));
    }

    /**
     * Convenience helper to create an alias to quantifier map using a collection of quantifiers.
     * @param quantifiers collection of quantifiers
     * @return a new {@link BiMap} mapping from {@code q.getAlias()} to {@code q} for every {@code q} in {@code quantifiers}
     */
    @Nonnull
    public static BiMap<CorrelationIdentifier, Quantifier> toBiMap(@Nonnull final Collection<? extends Quantifier> quantifiers) {
        return quantifiers
                .stream()
                .collect(ImmutableBiMap
                        .toImmutableBiMap(Quantifier::getAlias,
                                Function.identity()));
    }

    @Nonnull
    public static <Q extends Quantifier> List<Q> narrow(@Nonnull Class<Q> narrowedClass,
                                                        @Nonnull final List<? extends Quantifier> quantifiers) {
        return quantifiers.stream()
                .map(narrowedClass::cast)
                .collect(Collectors.toList());
    }

    @Nonnull
    public static <Q extends Quantifier> Set<Q> narrow(@Nonnull Class<Q> narrowedClass,
                                                       @Nonnull final Set<? extends Quantifier> quantifiers) {
        return quantifiers.stream()
                .map(narrowedClass::cast)
                .collect(Collectors.toSet());
    }

    @Nonnull
    public static SetMultimap<CorrelationIdentifier, CorrelationIdentifier> computeDependsOnMap(@Nonnull Iterable<? extends Quantifier> quantifiers) {
        return computeDependsOnMap(quantifiers, aliasToQuantifierMap(quantifiers));
    }

    @Nonnull
    public static SetMultimap<CorrelationIdentifier, CorrelationIdentifier> computeDependsOnMap(@Nonnull Iterable<? extends Quantifier> quantifiers, @Nonnull Map<CorrelationIdentifier, Quantifier> aliasToQuantifierMap) {
        return DependencyUtils.computeDependsOnMapWithAliases(aliasToQuantifierMap.keySet(), aliasToQuantifierMap, Quantifier::getCorrelatedTo);
    }

    /**
     * Method to find matches between the given set of quantifiers and the given set of other quantifiers using an alias map
     * and a {@code matchPredicate}.
     *
     * This method makes use of {@link #predicatedMatcher} to create a matcher that is then used to perform actual
     * matching.
     *
     * Two quantifiers can only match if they are equal on kind and the {@code matchPredicate} handed in is also
     * satisfied.
     *
     * @param boundAliasesMap aliases map of already bound quantifiers
     * @param quantifiers collection of quantifiers
     * @param otherQuantifiers collection of other quantifiers
     * @param matchPredicate that tests if two quantifiers and their graph they range over can be considered equivalent
     * @return an {@link Iterable} of {@link AliasMap}s containing a mapping from the quantifiers this expression owns to
     *         the quantifiers in {@code otherQuantifiers}. Note that the mapping is bijective and can therefore be inverted
     */
    @SuppressWarnings("squid:S135")
    @Nonnull
    public static Iterable<AliasMap> findMatches(@Nonnull final AliasMap boundAliasesMap,
                                                 @Nonnull final Collection<? extends Quantifier> quantifiers,
                                                 @Nonnull final Collection<? extends Quantifier> otherQuantifiers,
                                                 @Nonnull final MatchPredicate<Quantifier> matchPredicate) {
        // quantifiers must be equal on kind
        final MatchPredicate<Quantifier> quantifierMatchPredicate =
                (quantifier, otherQuantifier, eM) -> quantifier.semanticEqualsWithoutChildren(otherQuantifier);
        return predicatedMatcher(
                boundAliasesMap,
                quantifiers,
                otherQuantifiers,
                quantifierMatchPredicate.and(matchPredicate))
                .findCompleteMatches();
    }

    /**
     * Method to create a matcher to subsequently find matches between the given set of quantifiers and the given set
     * of other quantifiers using an alias map and a {@code matchPredicate}.
     *
     * Two quantifiers can be considered a match if the given predicate returns {@code true}.
     * This method attempts to match each quantifier from the set of quantifiers this expression owns to a quantifier
     * of the given other set of quantifiers such that the given predicate returns {@code true} for each mapping.
     * Note that there may be multiple distinct matches between the quantifier sets. This method returns an
     * {@link Iterable} of such matches where each match is only computed when it is requested by an iterator's
     * {@link Iterator#hasNext()}. Thus it can be assumed that this method never pre-computes all possible matches.
     *
     * For expressions that cannot introduce correlations, this method's complexity is simply {@code O(n!)} where
     * {@code n} is the number of quantifiers (which is equal for both sets). This path is taken by UNIONs and other set
     * operations.
     *
     * For expressions that introduce correlations, the matching process is more complicated. First, we determine one
     * topologically correct ordering. Then we iterate through all topologically correct orderings of the set of
     * quantifiers this expression owns. For each element in the permutation of quantifiers that is returned by
     * the {@link EnumeratingIterator} we try to match the quantifiers between the sets topologically from left to right
     * (using {@code matchPredicate}).
     * For each match we find we add a mapping between the alias of this and the other quantifier that matched
     * into an {@link AliasMap} that extends the alias map handed in.
     *
     * @param boundAliasesMap aliases map of already bound quantifiers
     * @param quantifiers collection of quantifiers
     * @param otherQuantifiers collection of other quantifiers
     * @param matchPredicate that tests if two quantifiers and their graph they range over can be considered equivalent
     * @return a new predicated matcher
     */
    @Nonnull
    private static PredicatedMatcher predicatedMatcher(@Nonnull final AliasMap boundAliasesMap,
                                                       @Nonnull final Collection<? extends Quantifier> quantifiers,
                                                       @Nonnull final Collection<? extends Quantifier> otherQuantifiers,
                                                       @Nonnull final MatchPredicate<Quantifier> matchPredicate) {
        return FindingMatcher.onAliasDependencies(
                boundAliasesMap,
                quantifiers,
                Quantifier::getAlias,
                Quantifier::getCorrelatedTo,
                otherQuantifiers,
                Quantifier::getAlias,
                Quantifier::getCorrelatedTo,
                matchPredicate);
    }

    /**
     * Method to match the given set of quantifiers and the given set of other quantifiers using an
     * alias map and a {@code matchFunction}. In contrast to {@link #findMatches} this more generic version of the matching
     * algorithm allows to compute an {@link Iterable} of results when a match is found which is then associated
     * with the actual {@link BoundMatch}.
     *
     * Two quantifiers can be considered a match if the given {@link MatchFunction} returns a non-empty
     * {@link Iterable} of some type. This method attempts to match each quantifier from the set of quantifiers this
     * expression owns to a quantifier of the given other set of quantifiers such that the given match function is
     * returns a non-empty iterable for each mapping.
     * Note that there may be multiple distinct matches between the quantifier sets. This method returns an
     * {@link Iterable} of such matches where each match is only computed when it is requested by an iterator's
     * {@link Iterator#hasNext()}. Thus it can be assumed that this method never pre-computes all possible matches.
     *
     * For expressions that cannot introduce correlations, this method's complexity is simply {@code O(n!)} where
     * {@code n} is the number of quantifiers (which is equal for both sets). This path is taken by UNIONs and other set
     * operations.
     *
     * For expressions that introduce correlations, the matching process is more complicated. First, we determine one
     * topologically correct ordering. Then we iterate through all topologically correct orderings of the set of
     * quantifiers this expression owns. For each element in the permutation of quantifiers that is returned by
     * the {@link EnumeratingIterator} we try to match the quantifiers between the sets topologically from left to right
     * (using {@code matchPredicate}).
     * For each match we find we add a mapping between the alias of this and the other quantifier that matched
     * into an {@link AliasMap} that extends the alias map handed in.
     *
     * @param boundAliasesMap aliases map of already bound quantifiers
     * @param quantifiers collection of quantifiers
     * @param otherQuantifiers collection of other quantifiers
     * @param matchFunction that computes an non-empty {@link Iterable} of match results if two quantifiers and their
     *                      graph they range over can be considered matching and an empty {@link Iterable} otherwise
     * @param <M> type that the match function {@code matchFunction} produces
     * @return an {@link Iterable} of {@link BoundMatch}es containing an {@link AliasMap} mapping from the quantifiers
     *         this expression owns to the quantifiers in {@code otherQuantifiers} together with the cross-product of
     *         all individual matches that were computed between during the matching of individual quantifiers.
     */
    @SuppressWarnings("squid:S135")
    @Nonnull
    public static <M> Iterable<BoundMatch<EnumeratingIterable<M>>> match(@Nonnull final AliasMap boundAliasesMap,
                                                                         @Nonnull final Collection<? extends Quantifier> quantifiers,
                                                                         @Nonnull final Collection<? extends Quantifier> otherQuantifiers,
                                                                         @Nonnull final MatchFunction<Quantifier, M> matchFunction) {
        return genericMatcher(
                boundAliasesMap,
                quantifiers,
                otherQuantifiers,
                matchFunction).match();
    }

    /**
     * Method to create a matcher between the given set of quantifiers and the given set of other quantifiers using an
     * alias map and a {@code matchFunction}. In contrast to {@link #findMatches} this more generic version of the matching
     * algorithm allows to compute an {@link Iterable} of results when a match is found which is then associated
     * with the actual {@link BoundMatch}.
     *
     * Two quantifiers can be considered a match if the given {@link MatchFunction} returns a non-empty
     * {@link Iterable} of some type. This method attempts to match each quantifier from the set of quantifiers this
     * expression owns to a quantifier of the given other set of quantifiers such that the given match function is
     * returns a non-empty iterable for each mapping.
     * Note that there may be multiple distinct matches between the quantifier sets. This method returns an
     * {@link Iterable} of such matches where each match is only computed when it is requested by an iterator's
     * {@link Iterator#hasNext()}. Thus it can be assumed that this method never pre-computes all possible matches.
     *
     * For expressions that cannot introduce correlations, this method's complexity is simply {@code O(n!)} where
     * {@code n} is the number of quantifiers (which is equal for both sets). This path is taken by UNIONs and other set
     * operations.
     *
     * For expressions that introduce correlations, the matching process is more complicated. First, we determine one
     * topologically correct ordering. Then we iterate through all topologically correct orderings of the set of
     * quantifiers this expression owns. For each element in the permutation of quantifiers that is returned by
     * the {@link EnumeratingIterator} we try to match the quantifiers between the sets topologically from left to right
     * (using {@code matchPredicate}).
     * For each match we find we add a mapping between the alias of this and the other quantifier that matched
     * into an {@link AliasMap} that extends the alias map handed in.
     *
     * @param boundAliasesMap aliases map of already bound quantifiers
     * @param quantifiers collection of quantifiers
     * @param otherQuantifiers collection of other quantifiers
     * @param matchFunction that computes an non-empty {@link Iterable} of match results if two quantifiers and their
     *                      graph they range over can be considered matching and an empty {@link Iterable} otherwise
     * @param <M> type that the match function {@code matchFunction} produces
     * @return a new generic matcher
     */
    @Nonnull
    public static <M> GenericMatcher<BoundMatch<EnumeratingIterable<M>>> genericMatcher(@Nonnull final AliasMap boundAliasesMap,
                                                                                        @Nonnull final Collection<? extends Quantifier> quantifiers,
                                                                                        @Nonnull final Collection<? extends Quantifier> otherQuantifiers,
                                                                                        @Nonnull final MatchFunction<Quantifier, M> matchFunction) {
        return ComputingMatcher.onAliasDependencies(
                boundAliasesMap,
                quantifiers,
                Quantifier::getAlias,
                Quantifier::getCorrelatedTo,
                otherQuantifiers,
                Quantifier::getAlias,
                Quantifier::getCorrelatedTo,
                matchFunction,
                ComputingMatcher::productAccumulator);
    }

    /**
     * Method to enumerate the bound aliases due to constraints. This reduces the number of permutations the matching
     * algorithm has to enumerate in a subsequent step.
     *
     * Consider the following example:
     *
     * Let's says we have {@link AliasMap}s for three given quantifiers:
     *
     * <pre>{@code
     * q1: Iterable.of((a11 -> aa, a2 -> ab), (a11 -> ab, a2 -> aa), (a11 -> ac, a2 -> ad))
     * q2: Iterable.of((a11 -> aa, a2 -> ab), (a11 -> ab, a2 -> aa))
     * q3: Iterable.of((a11 -> aa, a2 -> ab), (a11 -> ab, a2 -> aa), (a11 -> ac, a2 -> ad))
     * }</pre>
     *
     * All quantifiers have a constraint {@code (a11 -> aa, a2 -> ab)}. This means that in this example,
     * for that alias map in {@code q1} we find exactly one compatible alias map for {@code q2} and {@code q3}. The same is
     * true for {@code (a11 -> ab, a2 -> aa)}. The map {@code (a11 -> ac, a2 -> ad)}, however, is not one of the constraints
     * in {@code q2}. Therefore, that map can never produce a proper match for all three quantifiers as they cannot
     * all use that mapping ({@code q2} only uses incompatible mappings). Similarly if the filtered cross product of
     * alias maps is empty we know that there cannot be any match between these quantifiers at all.
     *
     * If the list of quantifiers passed in is empty, this method returns a singleton iterable of the given
     * {@link AliasMap}. This is equivalent to saying that the (empty) set of quantifiers are not imposing a constraint
     * on matching.
     *
     * If the quantifiers that are passed in use only empty alias maps, we consider the empty alias map compatible with
     * any other alias map.
     *
     * @param aliasMap alias map of bound aliases
     * @param quantifiers list of quantifiers
     * @param constraintsFunction a function returning a {@link Collection} of {@link AliasMap}s with potentially several
     *        alias maps. This function must be defined over th set of {@code quantifiers} also passed in.
     * @param eligibleAliases a set of aliases to filter the aliases under consideration during enumeration on the source
     *        side
     * @param otherEligibleAliases a set of aliases to filter the aliases under consideration during enumeration on the
     *        target side
     * @return an {@link Iterable} of possible {@link AliasMap}s where each such map contains a set of compatible bindings
     *         over all quantifiers.
     */
    @Nonnull
    public static Iterable<AliasMap> enumerateConstraintAliases(@Nonnull final AliasMap aliasMap,
                                                                @Nonnull final List<? extends Quantifier> quantifiers,
                                                                @Nonnull final Function<Quantifier, Collection<AliasMap>> constraintsFunction,
                                                                @Nonnull final Set<CorrelationIdentifier> eligibleAliases,
                                                                @Nonnull final Set<CorrelationIdentifier> otherEligibleAliases) {
        // return aliasMap if quantifiers is empty -- this is the default-on-empty case
        if (quantifiers.isEmpty()) {
            return ImmutableList.of(aliasMap);
        }

        // create an iterable to produce the cross-product of all alias maps produced by the constraints function
        final EnumeratingIterable<AliasMap> enumeratingIterable =
                CrossProduct.crossProduct(quantifiers.stream().map(constraintsFunction).collect(ImmutableList.toImmutableList()));

        return () -> {
            final EnumeratingIterator<AliasMap> iterator = enumeratingIterable.iterator();

            return new AbstractIterator<AliasMap>() {
                @Override
                protected AliasMap computeNext() {
                    while (iterator.hasNext()) {
                        final List<AliasMap> next = iterator.next();

                        AliasMap nestedAliasMap = aliasMap.toBuilder(next.size()).build();

                        // reduce-left the alias maps in the list, but skip if needed
                        int i;
                        for (i = 0; i < next.size(); i++) {
                            final AliasMap currentAliasMap =
                                    next.get(i)
                                            .filterMappings((source, target) ->
                                                    eligibleAliases.contains(source) && otherEligibleAliases.contains(target));
                            final Optional<AliasMap> aliasMapOptional = nestedAliasMap.combineMaybe(currentAliasMap);
                            if (!aliasMapOptional.isPresent()) {
                                break;
                            }
                            nestedAliasMap = aliasMapOptional.get();
                        }

                        if (i == next.size()) {
                            return nestedAliasMap;
                        } else {
                            iterator.skip(i);
                        }
                    }

                    return endOfData();
                }
            };
        };
    }

    public static boolean isReversed(@Nonnull List<Physical> quantifiers) {
        return quantifiers
                .stream()
                .map(Physical::getRangesOver)
                .flatMap(reference -> reference.getMembers().stream())
                .map(expression -> {
                    Verify.verify(expression instanceof RecordQueryPlan);
                    return (RecordQueryPlan)expression;
                })
                .map(QueryPlan::isReverse)
                .findAny()
                .orElseThrow(() -> new RecordCoreException("unable to determine reversed-ness"));
    }

    @Nonnull
    public static List<? extends Quantifier> anyTopologicalOrderPermutation(@Nonnull List<? extends Quantifier> quantifiers) {
        final var aliasToQuantifierMap =
                quantifiers.stream()
                        .collect(ImmutableMap.toImmutableMap(Quantifier::getAlias, Function.identity()));

        final var aliases =
                quantifiers.stream()
                        .map(Quantifier::getAlias)
                        .collect(ImmutableSet.toImmutableSet());

        final var aliasesPermutationOptional =
                TopologicalSort.anyTopologicalOrderPermutation(aliases,
                        alias -> Objects.requireNonNull(aliasToQuantifierMap.get(alias)).getCorrelatedTo());
        Verify.verify(aliasesPermutationOptional.isPresent());

        return aliasesPermutationOptional.get()
                .stream()
                .map(alias -> Objects.requireNonNull(aliasToQuantifierMap.get(alias)))
                .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public static List<? extends Quantifier> translateCorrelations(@Nonnull final Iterable<? extends Quantifier> quantifiers,
                                                                   @Nonnull final TranslationMap translationMap,
                                                                   final boolean shouldSimplifyValues) {
        //
        // Take care of the case that two distinct quantifiers range over the same ref (CSE).
        //
        final var oldToNewRefMap =
                Maps.<Reference, Reference>newIdentityHashMap();
        final var translatedQuantifiersBuilder = ImmutableList.<Quantifier>builder();

        for (final Quantifier quantifier : quantifiers) {
            @Nullable final var newRef = oldToNewRefMap.get(quantifier.getRangesOver());
            if (newRef != null) {
                // already translated
                if (quantifier.getRangesOver() == newRef) {
                    // old == new means that the translation did not change the subgraph
                    translatedQuantifiersBuilder.add(quantifier);
                } else {
                    translatedQuantifiersBuilder.add(quantifier.overNewReference(newRef));
                }
            } else {
                final var translatedQuantifier = quantifier.translateCorrelations(translationMap, shouldSimplifyValues);
                oldToNewRefMap.put(quantifier.getRangesOver(), translatedQuantifier.getRangesOver());
                translatedQuantifiersBuilder.add(translatedQuantifier);
            }
        }

        return translatedQuantifiersBuilder.build();
    }

    @Nonnull
    public static Type getFlowedTypeForSetOperation(@Nonnull final Iterable<? extends Quantifier> quantifiers) {
        return Streams.stream(quantifiers)
                .findFirst()
                .map(Quantifier::getFlowedObjectType)
                .orElseThrow(() -> new RecordCoreException("unable to resolve object type from quantifiers"));
    }

    /**
     * Resolver to resolve aliases to quantifiers.
     */
    public static class AliasResolver {
        @Nonnull
        private final Traversal traversal;

        public AliasResolver(@Nonnull final Traversal traversal) {
            this.traversal = traversal;
        }

        public Set<Quantifier> resolveCorrelationAlias(@Nonnull RelationalExpression expression,
                                                       @Nonnull final CorrelationIdentifier alias) {
            final Set<Reference> refsContaining = traversal.getRefsContaining(expression);
            final Set<Quantifier> resolvedQuantifiers = Sets.newIdentityHashSet();

            for (final Reference reference : refsContaining) {
                resolveCorrelationAlias(reference, alias, resolvedQuantifiers);
            }

            return resolvedQuantifiers;
        }

        public Set<Quantifier> resolveCorrelationAlias(@Nonnull Reference reference,
                                                       @Nonnull final CorrelationIdentifier alias) {

            final Set<Quantifier> resolvedQuantifiers = Sets.newIdentityHashSet();
            resolveCorrelationAlias(reference, alias, resolvedQuantifiers);
            return resolvedQuantifiers;
        }

        private void resolveCorrelationAlias(@Nonnull Reference reference,
                                             @Nonnull final CorrelationIdentifier alias,
                                             @Nonnull Set<Quantifier> resolvedQuantifiers) {
            final Set<Traversal.ReferencePath> referencePaths = traversal.getParentRefPaths(reference);

            for (final Traversal.ReferencePath referencePath : referencePaths) {
                final RelationalExpression expression = referencePath.getExpression();
                for (final Quantifier quantifier : expression.getQuantifiers()) {
                    if (quantifier.getAlias().equals(alias)) {
                        Verify.verify(expression.canCorrelate());
                        resolvedQuantifiers.add(quantifier);
                    }
                }
                resolveCorrelationAlias(referencePath.getReference(),
                        alias,
                        resolvedQuantifiers);
            }
        }

        public static AliasResolver withRoot(@Nonnull final Reference rootRef) {
            return new AliasResolver(Traversal.withRoot(rootRef));
        }
    }
}
