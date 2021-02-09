/*
 * FindingMatcher.java
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

package com.apple.foundationdb.record.query.plan.temp.matching;

import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.EnumeratingIterator;
import com.apple.foundationdb.record.query.plan.temp.TransitiveClosure;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * This class implements a {@link PredicatedMatcher} which matches two sets of elements of type {@code T} to compute
 * an {@link Iterable} of {@link AliasMap}s. Individual elements are matched by means of a {@link MatchPredicate}
 * which accepts or rejects that pair. In order to match this set against the other set, we need to find matching pairs
 * of elements for each element identified by {@link #getAliases()}.
 *
 * @param <T> the element type
 */
public class FindingMatcher<T> extends BaseMatcher<T> implements PredicatedMatcher {
    /**
     * Match predicate that rejects or accepts the matching for an individual pair of elements.
     */
    @Nonnull
    private final MatchPredicate<T> matchPredicate;

    /**
     * Dumb private constructor. Use static factory methods instead!
     * @param boundAliasesMap a map with previously-bound aliases. Any match that is computed and passed back to the
     *        client is an amendment of this map, i.e., every resulting match's {@link AliasMap} always contains at
     *        least the bindings in this map
     * @param aliases a set of correlation identifiers representing the domain on this side
     * @param elementToAliasFn a function to map elements of type {@code T} on this side to correlation identifiers
     * @param aliasToElementMap a map from correlation identifiers to elements of type {@code T} on this side
     * @param dependsOnMap a multimap from correlation identifiers to correlation identifiers describing the depends-on
     *        relationships between the elements (translated to aliases) on this side
     * @param otherAliases a set of correlation identifiers representing the domain on the other side
     * @param otherElementToAliasFn a function to map elements of type {@code T} on the other side to correlation
     *        identifiers
     * @param otherAliasToElementMap a function to map elements of type {@code T} on the other side to correlation
     *        identifiers.
     * @param otherDependsOnMap a multimap from correlation identifiers to correlation identifiers describing the depends-on
     *        relationships between the elements (translated to aliases) on the other side
     * @param matchPredicate a match predicate that rejects or accepts the matching for an individual pair
     *        of elements
     */
    private FindingMatcher(@Nonnull final AliasMap boundAliasesMap,
                           @Nonnull final Set<CorrelationIdentifier> aliases,
                           @Nonnull final Function<T, CorrelationIdentifier> elementToAliasFn,
                           @Nonnull final Map<CorrelationIdentifier, T> aliasToElementMap,
                           @Nonnull final ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> dependsOnMap,
                           @Nonnull final Set<CorrelationIdentifier> otherAliases,
                           @Nonnull final Function<T, CorrelationIdentifier> otherElementToAliasFn,
                           @Nonnull final Map<CorrelationIdentifier, ? extends T> otherAliasToElementMap,
                           @Nonnull final ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> otherDependsOnMap,
                           @Nonnull final MatchPredicate<T> matchPredicate) {
        super(boundAliasesMap, aliases, elementToAliasFn, aliasToElementMap, dependsOnMap, otherAliases, otherElementToAliasFn, otherAliasToElementMap, otherDependsOnMap);
        this.matchPredicate = matchPredicate;
    }

    /**
     * Match using the method {@link #enumerate} as {@link EnumerationFunction}.
     * @return an iterable of {@link AliasMap}s.
     */
    @Nonnull
    @Override
    public Iterable<AliasMap> findMatches() {
        if (getAliases().size() != getOtherAliases().size()) {
            return ImmutableList.of();
        }

        return match(this::enumerate);
    }

    @Nonnull
    @Override
    protected Iterable<List<CorrelationIdentifier>> otherCombinations(final List<CorrelationIdentifier> otherPermutation, final int limitInclusive) {
        return ImmutableList.of(otherPermutation);
    }

    /**
     * Method to enumerate the permutations on this side against the permutation of the other side in order
     * to form matches (bijective mappings between the permutations). The match predicate is called for each pair of
     * elements (for a match attempt). If the match predicate returns {@code true} the pair is recorded
     * as a matching pair. We attempt to find a matching pair (one element from this side; one from the
     * other side) for each element identified by {@link #getAliases()}. For each individual new such pair that is found,
     * we continue in the matching attempt. Once a set of bindings is established for all aliases
     * in {@link #getAliases} this method then includes that {@link AliasMap} in the computable of {@link AliasMap}s that
     * form the result and proceeds to consume further permutations from the iterator that is passed in in order to
     * establish other matches between this and other.
     * @param iterator an enumerating iterable for the permutations on this side
     * @param otherOrdered one permutation (that is not violating dependencies, constraints, etc.) of the other side
     * @return an {@link Iterator} of match results (of type {@link AliasMap})
     */
    @SuppressWarnings("java:S135")
    @Nonnull
    public Iterator<AliasMap> enumerate(@Nonnull final EnumeratingIterator<CorrelationIdentifier> iterator,
                                        @Nonnull final List<CorrelationIdentifier> otherOrdered) {
        final Set<CorrelationIdentifier> aliases = getAliases();
        final AliasMap boundAliasesMap = getBoundAliasesMap();

        if (aliases.isEmpty()) {
            return ImmutableList.of(boundAliasesMap).iterator();
        }

        return new AbstractIterator<AliasMap>() {
            @Override
            protected AliasMap computeNext() {
                while (iterator.hasNext()) {
                    final List<CorrelationIdentifier> ordered = iterator.next();
                    final AliasMap.Builder aliasMapBuilder = AliasMap.builder(aliases.size());

                    int i;
                    for (i = 0; i < aliases.size(); i++) {
                        final AliasMap aliasMap = aliasMapBuilder.build();

                        final CorrelationIdentifier alias = ordered.get(i);
                        final CorrelationIdentifier otherAlias = otherOrdered.get(i);

                        final Optional<AliasMap> dependsOnMapOptional = mapDependenciesToOther(aliasMap, alias, otherAlias);
                        if (!dependsOnMapOptional.isPresent()) {
                            break;
                        }
                        final AliasMap dependsOnMap = dependsOnMapOptional.get();

                        final T entity = Objects.requireNonNull(getAliasToElementMap().get(alias));
                        final T otherEntity = Objects.requireNonNull(getOtherAliasToElementMap().get(otherAlias));

                        if (!matchPredicate.test(entity, otherEntity, boundAliasesMap.combine(dependsOnMap))) {
                            break;
                        }

                        // We now amend the equivalences passed in by adding the already known bound aliases left
                        // of i and make them equivalent as well
                        aliasMapBuilder.put(alias, otherAlias);
                    }

                    if (i == aliases.size()) {
                        return boundAliasesMap.derived(ordered.size()).zip(ordered, otherOrdered).build();
                    } else {
                        // we can skip all permutations where the i-th value is bound the way it currently is
                        iterator.skip(i);
                    }
                }

                return endOfData();
            }
        };
    }

    /**
     * Static factory method to create a generic matcher using element type {@code T}. This method is optimized to the
     * case when the dependsOn sets are already given by sets of {@link CorrelationIdentifier}s.
     * @param boundAliasesMap a map with previously-bound aliases. Any match that is computed and passed back to the
     *        client is an amendment of this map, i.e., every resulting match's {@link AliasMap} always contains at
     *        least the bindings in this map
     * @param aliases a set of {@link CorrelationIdentifier} representing this set
     * @param dependsOnFn a function from elements to sets of correlation identifiers describing the depends-on
     *        relationships between the elements on this side
     * @param otherAliases a set of other elements representing the other side
     * @param otherDependsOnFn a function from elements to sets of correlation identifiers describing the depends-on
     *        relationships between the other elements
     * @param matchPredicate a match predicate that rejects or accepts the matching for an individual pair
     *        of elements
     * @return a newly created predicated matcher of type {@code BoundMatch<R>}
     */
    @Nonnull
    public static FindingMatcher<CorrelationIdentifier> onAliases(@Nonnull final AliasMap boundAliasesMap,
                                                                  @Nonnull final Set<CorrelationIdentifier> aliases,
                                                                  @Nonnull final Function<CorrelationIdentifier, Set<CorrelationIdentifier>> dependsOnFn,
                                                                  @Nonnull final Set<CorrelationIdentifier> otherAliases,
                                                                  @Nonnull final Function<CorrelationIdentifier, Set<CorrelationIdentifier>> otherDependsOnFn,
                                                                  @Nonnull final MatchPredicate<CorrelationIdentifier> matchPredicate) {
        final Map<CorrelationIdentifier, CorrelationIdentifier> identityMappingMap = CorrelationIdentifier.identityMappingMap(aliases);
        final Map<CorrelationIdentifier, CorrelationIdentifier> otherIdentityMappingMap = CorrelationIdentifier.identityMappingMap(otherAliases);
        return new FindingMatcher<>(
                boundAliasesMap,
                aliases,
                Function.identity(),
                identityMappingMap,
                TransitiveClosure.transitiveClosure(aliases, BaseMatcher.computeDependsOnMap(aliases, Function.identity(), identityMappingMap, dependsOnFn)),
                otherAliases,
                Function.identity(),
                otherIdentityMappingMap,
                TransitiveClosure.transitiveClosure(otherAliases, BaseMatcher.computeDependsOnMap(otherAliases, Function.identity(), otherIdentityMappingMap, otherDependsOnFn)),
                matchPredicate);
    }

    /**
     * Static factory method to create a generic matcher using element type {@code T}. This method is optimized to the
     * case when the dependsOn sets are already given by sets of {@link CorrelationIdentifier}s.
     * @param boundAliasesMap a map with previously-bound aliases. Any match that is computed and passed back to the
     *        client is an amendment of this map, i.e., every resulting match's {@link AliasMap} always contains at
     *        least the bindings in this map
     * @param elements a collection of elements of type {@code T} representing the domain on this side
     * @param elementToAliasFn a function to map elements of type {@code T} on this side to correlation identifiers
     * @param dependsOnFn a function from elements to sets of correlation identifiers describing the depends-on
     *        relationships between the elements on this side
     * @param otherElements a collection of other elements representing the domain on the other side
     * @param otherElementToAliasFn a function to map elements of type {@code T} on the other side to correlation
     *        identifiers
     * @param otherDependsOnFn a function from elements to sets of correlation identifiers describing the depends-on
     *        relationships between the other elements
     * @param matchPredicate a match predicate that rejects or accepts the matching for an individual pair
     *        of elements
     * @param <T> the element type
     * @return a newly created predicated matcher of type {@code BoundMatch<R>}
     */
    @Nonnull
    public static <T> PredicatedMatcher onAliasDependencies(@Nonnull final AliasMap boundAliasesMap,
                                                            @Nonnull final Collection<? extends T> elements,
                                                            @Nonnull final Function<T, CorrelationIdentifier> elementToAliasFn,
                                                            @Nonnull final Function<T, Set<CorrelationIdentifier>> dependsOnFn,
                                                            @Nonnull final Collection<? extends T> otherElements,
                                                            @Nonnull final Function<T, CorrelationIdentifier> otherElementToAliasFn,
                                                            @Nonnull final Function<T, Set<CorrelationIdentifier>> otherDependsOnFn,
                                                            @Nonnull final MatchPredicate<T> matchPredicate) {
        final ImmutableSet<CorrelationIdentifier> aliases = BaseMatcher.computeAliases(elements, elementToAliasFn);
        final ImmutableMap<CorrelationIdentifier, T> aliasToElementMap = BaseMatcher.computeAliasToElementMap(elements, elementToAliasFn);

        final ImmutableSet<CorrelationIdentifier> otherAliases = BaseMatcher.computeAliases(otherElements, otherElementToAliasFn);
        final ImmutableMap<CorrelationIdentifier, T> otherAliasToElementMap = BaseMatcher.computeAliasToElementMap(otherElements, otherElementToAliasFn);
        return new FindingMatcher<>(
                boundAliasesMap,
                aliases,
                elementToAliasFn,
                aliasToElementMap,
                TransitiveClosure.transitiveClosure(aliases, BaseMatcher.computeDependsOnMapWithAliases(aliases, aliasToElementMap, dependsOnFn)),
                otherAliases,
                otherElementToAliasFn,
                otherAliasToElementMap,
                TransitiveClosure.transitiveClosure(otherAliases, BaseMatcher.computeDependsOnMapWithAliases(otherAliases, otherAliasToElementMap, otherDependsOnFn)),
                matchPredicate);
    }
}
