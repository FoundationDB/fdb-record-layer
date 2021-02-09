/*
 * BaseMatcher.java
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
import com.apple.foundationdb.record.query.plan.temp.EnumeratingIterable;
import com.apple.foundationdb.record.query.plan.temp.IterableHelpers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.apple.foundationdb.record.query.plan.temp.TopologicalSort.anyTopologicalOrderPermutation;
import static com.apple.foundationdb.record.query.plan.temp.TopologicalSort.topologicalOrderPermutations;

/**
 * Abstract base class for all matchers. Matching is an algorithm that <em>matches</em> two collections of things of type
 * {@code T} resulting in a reversible mapping between the elements of the respective collections.
 *
 * By definition two empty collections of elements match with an empty bijective mapping.
 *
 * The bijective mapping that is produced by matching is always encoded in instances of the class {@link AliasMap}.
 *
 * Matchers operate on a generic type {@code T}. However, the underlying mechanics always works on instances of
 * class {@link CorrelationIdentifier}. This separates the concept of equality from the actual objects that the client
 * logic matches over.
 *
 * This class keeps maps to track object-to-alias and alias-to-object relationships. To clients (that is logic extending
 * from this class), this class presents the elements that are the domain of matching to be of type {@code T} as long
 * as the client can provided mappings between objects of type {@code T} and {@link CorrelationIdentifier}s.
 *
 * Matching can be but may not be commutative. Instead of using left/right terminology it seemed better to talk about
 * this/other as this and other are treated in a slightly asymmetric way. Clients still can implement matchers that
 * impose that a subclass of this class returns the same matches with this and other reversed, however, that behavior
 * is not encoded in this class per se.
 *
 * @param <T> the type representing the domain of matching
 */
public abstract class BaseMatcher<T> {
    /**
     * Map with previously-bound aliases. Any match that is computed and passed back to the client is an amendment of
     * this map, i.e., every resulting match's {@link AliasMap} always contains at least the bindings in this map.
     */
    @Nonnull
    private final AliasMap boundAliasesMap;

    /**
     * Set of correlation identifiers representing the domain on this side.
     */
    @Nonnull
    private final Set<CorrelationIdentifier> aliases;

    /**
     * Function to map elements of type {@code T} on this side to correlation identifiers.
     */
    @Nonnull
    private final Function<T, CorrelationIdentifier> elementToAliasFn;

    /**
     * Map from correlation identifiers to elements of type {@code T} on this side.
     */
    @Nonnull
    private final Map<CorrelationIdentifier, T> aliasToElementMap;

    /**
     * Map from correlation identifiers to correlation identifiers describing the depends-on relationships between
     * the elements (translated to aliases) on this side.
     */
    @Nonnull
    private final ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> dependsOnMap;

    /**
     * Set of correlation identifiers representing the domain on the other side.
     */
    @Nonnull
    private final Set<CorrelationIdentifier> otherAliases;

    /**
     * Function to map elements of type {@code T} on the other side to correlation identifiers.
     */
    @Nonnull
    private final Function<T, CorrelationIdentifier> otherElementToAliasFn;

    /**
     * Map from correlation identifiers to elements of type {@code T} on the other side.
     */
    @Nonnull
    private final Map<CorrelationIdentifier, ? extends T> otherAliasToElementMap;

    /**
     * Map from correlation identifiers to correlation identifiers describing the depends-on relationships between
     * the elements (translated to aliases) on the other side.
     */
    @Nonnull
    private final ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> otherDependsOnMap;

    /**
     * Dumb constructor.
     * @param boundAliasesMap a map with previously-bound aliases. Any match that is computed and passed back to the
     *        client is an amendment of this map, i.e., every resulting match's {@link AliasMap} always contains at
     *        least the bindings in this map
     * @param aliases a set of correlation identifiers representing the domain on this side
     * @param elementToAliasFn a function to map elements of type {@code T} on this side to correlation identifiers
     * @param aliasToElementMap a map from correlation identifiers to elements of type {@code T} on this side
     * @param dependsOnMap a map from correlation identifiers to correlation identifiers describing the depends-on
     *        relationships between the elements (translated to aliases) on this side
     * @param otherAliases a set of correlation identifiers representing the domain on the other side
     * @param otherElementToAliasFn a function to map elements of type {@code T} on the other side to correlation
     *        identifiers
     * @param otherAliasToElementMap a function to map elements of type {@code T} on the other side to correlation
     *        identifiers.
     * @param otherDependsOnMap a map from correlation identifiers to correlation identifiers describing the depends-on
     *        relationships between the elements (translated to aliases) on the other side
     */
    protected BaseMatcher(@Nonnull final AliasMap boundAliasesMap,
                          @Nonnull final Set<CorrelationIdentifier> aliases,
                          @Nonnull final Function<T, CorrelationIdentifier> elementToAliasFn,
                          @Nonnull final Map<CorrelationIdentifier, T> aliasToElementMap,
                          @Nonnull final ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> dependsOnMap,
                          @Nonnull final Set<CorrelationIdentifier> otherAliases,
                          @Nonnull final Function<T, CorrelationIdentifier> otherElementToAliasFn,
                          @Nonnull final Map<CorrelationIdentifier, ? extends T> otherAliasToElementMap,
                          @Nonnull final ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> otherDependsOnMap) {
        this.boundAliasesMap = boundAliasesMap;
        this.aliases = aliases;
        this.elementToAliasFn = elementToAliasFn;
        this.aliasToElementMap = aliasToElementMap;
        this.dependsOnMap = dependsOnMap;
        this.otherAliases = otherAliases;
        this.otherElementToAliasFn = otherElementToAliasFn;
        this.otherAliasToElementMap = otherAliasToElementMap;
        this.otherDependsOnMap = otherDependsOnMap;
    }

    @Nonnull
    public AliasMap getBoundAliasesMap() {
        return boundAliasesMap;
    }

    @Nonnull
    public Set<CorrelationIdentifier> getAliases() {
        return aliases;
    }

    @Nonnull
    public Function<T, CorrelationIdentifier> getElementToAliasFn() {
        return elementToAliasFn;
    }

    @Nonnull
    public Map<CorrelationIdentifier, T> getAliasToElementMap() {
        return aliasToElementMap;
    }

    @Nonnull
    public ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> getDependsOnMap() {
        return dependsOnMap;
    }

    @Nonnull
    public Set<CorrelationIdentifier> getOtherAliases() {
        return otherAliases;
    }

    @Nonnull
    public Function<T, CorrelationIdentifier> getOtherElementToAliasFn() {
        return otherElementToAliasFn;
    }

    @Nonnull
    public Map<CorrelationIdentifier, ? extends T> getOtherAliasToElementMap() {
        return otherAliasToElementMap;
    }

    @Nonnull
    public ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> getOtherDependsOnMap() {
        return otherDependsOnMap;
    }

    /**
     * Main method used for matching. This method pairs up the two sides relying on topological sorting logic to
     * create an {@link EnumeratingIterable} that enumerates all alias permutations that are possible based on the
     * depends-on constraints given to the matcher.
     *
     * The enumeration function that is also handed in can filter and skip the stream of permutations thereby pruning
     * entire subtrees of possible matches. The enumeration function also defines the result of the matches if there
     * are any, e.g. for a predicated matcher, the {@link AliasMap}s themselves that map the elements from this to other
     * are the match result themselves, however, a generic matcher may want to create objects specific to the matching
     * use case.
     *
     * The type {@code R} (the resulting type of matching) is a generic type on the method (not on this class) which
     * allows a matcher of a particular type {@code T} to provide matchers of different types of results {@code R}.
     *
     * @param enumerationFunction enumeration function. See {@link EnumerationFunction} for further explanations.
     * @param <R> the result type of the enumeration function
     * @return an {@link Iterable} of type {@code R}
     */
    @Nonnull
    protected <R> Iterable<R> match(@Nonnull final EnumerationFunction<R> enumerationFunction) {
        //
        // We do not know which id in "this" maps to which in "other". In fact, we cannot know as it is
        // intentionally modeled this way. We need to find a feasible ordering that matches. In reality, the
        // quantifiers owned by an expression impose a more or less a complete order leaving no room for many
        // permutations among unrelated quantifiers. In the worst case, we must enumerate them all. Luckily, most
        // algorithms for children have short-circuit semantics, so the uninteresting case where sub-graphs underneath
        // quantifiers do not match should be skipped quickly.
        //
        // get a topologically-sound permutation from other
        final Optional<List<CorrelationIdentifier>> otherOrderedOptional =
                anyTopologicalOrderPermutation(
                        getOtherAliases(),
                        otherDependsOnMap);

        // There should always be a topologically sound ordering that we can use.
        Verify.verify(aliases.isEmpty() || otherOrderedOptional.isPresent());

        final List<CorrelationIdentifier> otherPermutation = otherOrderedOptional.orElse(ImmutableList.of());

        final Iterable<List<CorrelationIdentifier>> otherCombinationsIterable =
                otherCombinations(otherPermutation,
                        Math.min(aliases.size(), otherPermutation.size()));

        return IterableHelpers
                .flatMap(otherCombinationsIterable,
                        otherCombination -> {
                            final EnumeratingIterable<CorrelationIdentifier> permutationsIterable =
                                    topologicalOrderPermutations(
                                            getAliases(),
                                            dependsOnMap);
                            return () -> enumerationFunction.apply(permutationsIterable.iterator(), otherCombination);
                        });
    }

    @Nonnull
    protected abstract Iterable<List<CorrelationIdentifier>> otherCombinations(final List<CorrelationIdentifier> otherPermutation, final int limitInclusive);

    /**
     * Helper to determine whether this {@code set} is equals to {@code otherSet} after translation using the given
     * {@link AliasMap}.
     * @param aliasMap alias map defining a translation from this to other
     * @param set this set
     * @param otherSet other set
     * @return boolean equal to the result of {@code translate(set1, aliasMap).equals(otherSet)}
     */
    protected boolean isIsomorphic(@Nonnull final AliasMap aliasMap,
                                   @Nonnull final Set<CorrelationIdentifier> set,
                                   @Nonnull final Set<CorrelationIdentifier> otherSet) {
        // is the depends-on relation ship isomorphic under matching?
        if (set.size() != otherSet.size()) {
            return false;
        }

        final ImmutableSet<CorrelationIdentifier> mappedSet =
                set.stream()
                        .map(dependentAlias -> Objects.requireNonNull(aliasMap.getTarget(dependentAlias)))
                        .collect(ImmutableSet.toImmutableSet());

        return mappedSet.equals(otherSet);
    }

    /**
     * Helper to map the dependencies of the given alias to the dependencies of the given other alias if such a mapping
     * is sound, i.e. the dependsOn functions for the given aliases sets are isomorphic under translation using the
     * given {@link AliasMap}.
     * @param aliasMap alias map to define the translation
     * @param alias alias on this side
     * @param otherAlias alias on the other side
     * @return {@code Optional.empty()} if the dependsOn functions for the given aliases sets is are isomorphic under
     *         translation using {@code aliasMap},
     *         {@code Optional.of(resultMap)} where {@code resultMap} is an {@link AliasMap} only containing mappings
     *         from the dependsOn set of {@code alias} that are also contained i {@code aliasMap}, otherwise.
     */
    @Nonnull
    protected Optional<AliasMap> mapDependenciesToOther(@Nonnull AliasMap aliasMap,
                                                        @Nonnull final CorrelationIdentifier alias,
                                                        @Nonnull final CorrelationIdentifier otherAlias) {
        final Set<CorrelationIdentifier> dependsOn = getDependsOnMap().get(alias);
        final Set<CorrelationIdentifier> otherDependsOn = getOtherDependsOnMap().get(otherAlias);

        if (!isIsomorphic(aliasMap, dependsOn, otherDependsOn)) {
            return Optional.empty();
        }

        return Optional.of(aliasMap.filterMappings((source, target) -> dependsOn.contains(source)));
    }

    /**
     * Static helper to compute a set of {@link CorrelationIdentifier}s from a collection of elements of type {@code T}
     * using the given element-to-alias function. Note that we allow a collection of elements to be passed in, we
     * compute a set of {@link CorrelationIdentifier}s from it (i.e. duplicate aliases are not allowed).
     * @param elements a collection of elements of type {@code T}
     * @param elementToAliasFn element to alias function
     * @param <T> element type
     * @return a set of aliases
     */
    @Nonnull
    protected static <T> ImmutableSet<CorrelationIdentifier> computeAliases(@Nonnull final Collection<? extends T> elements,
                                                                            @Nonnull final Function<T, CorrelationIdentifier> elementToAliasFn) {
        return elements.stream()
                .map(elementToAliasFn)
                .collect(ImmutableSet.toImmutableSet());
    }

    /**
     * Static helper to compute the alias-to-element map based on a collection of elements and on the element-to-alias function
     * tht are both passed in.
     * @param elements collection of elements of type {@code T}
     * @param elementToAliasFn element to alias function
     * @param <T> element type
     * @return a map from {@link CorrelationIdentifier} to {@code T} representing the conceptual inverse of the
     *         element to alias function passed in
     */
    @Nonnull
    protected static <T> ImmutableMap<CorrelationIdentifier, T> computeAliasToElementMap(@Nonnull final Collection<? extends T> elements,
                                                                                         @Nonnull final Function<T, CorrelationIdentifier> elementToAliasFn) {
        return elements.stream()
                .collect(ImmutableMap.toImmutableMap(elementToAliasFn, Function.identity()));
    }

    /**
     * Static helper to compute a dependency map from {@link CorrelationIdentifier} to {@link CorrelationIdentifier} based
     * on a set of aliases and mappings between {@link CorrelationIdentifier} and type {@code T}.
     * @param aliases a set of aliases
     * @param elementToAliasFn element to alias function
     * @param aliasToElementMap a map from {@link CorrelationIdentifier} to {@code T} representing the conceptual inverse of the
     *        element to alias function passed in
     * @param dependsOnFn function defining the dependOn relationships between an element and other elements (via aliases)
     * @param <T> element type
     * @return a multimap from {@link CorrelationIdentifier} to {@link CorrelationIdentifier} where a contained
     *         {@code key, values} pair signifies that {@code key} depends on each value in {@code values}
     */
    @Nonnull
    protected static <T> ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> computeDependsOnMap(@Nonnull Set<CorrelationIdentifier> aliases,
                                                                                                                @Nonnull final Function<T, CorrelationIdentifier> elementToAliasFn,
                                                                                                                @Nonnull final Map<CorrelationIdentifier, T> aliasToElementMap,
                                                                                                                @Nonnull final Function<T, ? extends Collection<T>> dependsOnFn) {
        final ImmutableSetMultimap.Builder<CorrelationIdentifier, CorrelationIdentifier> builder = ImmutableSetMultimap.builder();
        for (final CorrelationIdentifier alias : aliases) {
            final Collection<T> dependsOn = dependsOnFn.apply(aliasToElementMap.get(alias));
            for (final T dependsOnElement : dependsOn) {
                @Nullable final CorrelationIdentifier dependsOnAlias = elementToAliasFn.apply(dependsOnElement);
                if (dependsOnAlias != null && aliases.contains(dependsOnAlias)) {
                    builder.put(alias, dependsOnAlias);
                }
            }
        }
        return builder.build();
    }

    /**
     * Static helper to compute a dependency map from {@link CorrelationIdentifier} to {@link CorrelationIdentifier} based
     * on a set of aliases and mappings between {@link CorrelationIdentifier} and type {@code T}. This method optimizes
     * for the case that the client uses dependsOn functions that already map to {@link CorrelationIdentifier} as opposed
     * to type {@code T}. See the matching logic in {@link com.apple.foundationdb.record.query.plan.temp.Quantifiers}
     * for examples.
     * @param aliases a set of aliases
     * @param aliasToElementMap a map from {@link CorrelationIdentifier} to {@code T}
     * @param dependsOnFn function defining the dependOn relationships between an element and aliases ({@link CorrelationIdentifier}s)
     * @param <T> element type
     * @return a multimap from {@link CorrelationIdentifier} to {@link CorrelationIdentifier} where a contained
     *         {@code key, values} pair signifies that {@code key} depends on each value in {@code values}
     */
    @Nonnull
    protected static <T> ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> computeDependsOnMapWithAliases(@Nonnull Set<CorrelationIdentifier> aliases,
                                                                                                                           @Nonnull final Map<CorrelationIdentifier, T> aliasToElementMap,
                                                                                                                           @Nonnull final Function<T, Set<CorrelationIdentifier>> dependsOnFn) {
        final ImmutableSetMultimap.Builder<CorrelationIdentifier, CorrelationIdentifier> builder = ImmutableSetMultimap.builder();
        for (final CorrelationIdentifier alias : aliases) {
            final Set<CorrelationIdentifier> dependsOn = dependsOnFn.apply(aliasToElementMap.get(alias));
            for (final CorrelationIdentifier dependsOnAlias : dependsOn) {
                if (dependsOnAlias != null && aliases.contains(dependsOnAlias)) {
                    builder.put(alias, dependsOnAlias);
                }
            }
        }
        return builder.build();
    }
}
