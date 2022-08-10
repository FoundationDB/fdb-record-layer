/*
 * BaseMatcher.java
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

package com.apple.foundationdb.record.query.plan.cascades.matching.graph;

import com.apple.foundationdb.record.query.combinatorics.ChooseK;
import com.apple.foundationdb.record.query.combinatorics.EnumeratingIterable;
import com.apple.foundationdb.record.query.combinatorics.EnumeratingIterator;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.IterableHelpers;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static com.apple.foundationdb.record.query.combinatorics.TopologicalSort.anyTopologicalOrderPermutation;
import static com.apple.foundationdb.record.query.combinatorics.TopologicalSort.topologicalOrderPermutations;

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
public class BaseMatcher<T> {
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
     * @param isCompleteMatchesOnly indicator if only complete matches are warranted by caller
     * @param <R> the result type of the enumeration function
     * @return an {@link Iterable} of type {@code R}
     */
    @Nonnull
    protected <R> Iterable<R> match(@Nonnull final EnumerationFunction<R> enumerationFunction, final boolean isCompleteMatchesOnly) {
        //
        // Short-circuit the case where complete matches are requested but impossible due to
        // the sets having a different cardinality.
        //
        if (isCompleteMatchesOnly && getAliases().size() != getOtherAliases().size()) {
            return ImmutableList.of();
        }

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
        Verify.verify(!isCompleteMatchesOnly || aliases.isEmpty() || otherOrderedOptional.isPresent());

        final List<CorrelationIdentifier> otherPermutation = otherOrderedOptional.orElse(ImmutableList.of());

        final var maxMatchSize = Math.min(aliases.size(), otherPermutation.size());
        final Iterable<Set<CorrelationIdentifier>> otherCombinationsIterable =
                isCompleteMatchesOnly
                ? ImmutableList.of(getOtherAliases())
                : soundCombinations(getOtherAliases(), getOtherDependsOnMap(), 0, maxMatchSize);

        return IterableHelpers
                .flatMap(otherCombinationsIterable,
                        otherCombination -> {
                            final List<CorrelationIdentifier> otherFilteredPermutation =
                                    otherPermutation
                                            .stream()
                                            .filter(otherCombination::contains)
                                            .collect(ImmutableList.toImmutableList());

                            final Iterable<Set<CorrelationIdentifier>> combinationsIterable =
                                    isCompleteMatchesOnly
                                    ? ImmutableList.of(getAliases())
                                    : soundCombinations(getAliases(), getDependsOnMap(), otherCombination.size(), otherCombination.size()); //  limit to the other combination's size

                            return IterableHelpers.flatMap(combinationsIterable,
                                    combination -> {
                                        final EnumeratingIterable<CorrelationIdentifier> permutationsIterable =
                                                topologicalOrderPermutations(
                                                        combination,
                                                        dependsOnMap);
                                        return () -> enumerationFunction.apply(permutationsIterable.iterator(), otherFilteredPermutation);
                                    });
                        });
    }

    /**
     * Helper to determine whether this {@code set} is equals to {@code otherSet} after translation using the given
     * {@link AliasMap}.
     * @param aliasMap alias map defining a translation from this to other
     * @param dependsOn this set
     * @param otherDependsOn other set
     * @return boolean equal to the result of {@code translate(set1, aliasMap).equals(otherSet)}
     */
    protected boolean isIsomorphic(@Nonnull final AliasMap aliasMap,
                                   @Nonnull final Set<CorrelationIdentifier> dependsOn,
                                   @Nonnull final Set<CorrelationIdentifier> otherDependsOn) {
        final ImmutableSet<CorrelationIdentifier> mappedSet =
                dependsOn.stream()
                        .filter(aliasMap::containsSource)
                        .map(dependentAlias -> Objects.requireNonNull(aliasMap.getTarget(dependentAlias)))
                        .collect(ImmutableSet.toImmutableSet());

        return otherDependsOn.containsAll(mappedSet);
    }

    @Nonnull
    @SuppressWarnings("java:S3776")
    private static Iterable<Set<CorrelationIdentifier>> soundCombinations(@Nonnull final Set<CorrelationIdentifier> aliases,
                                                                          @Nonnull final ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> dependsOnMap,
                                                                          final int startInclusive,
                                                                          final int endInclusive) {
        Preconditions.checkArgument(endInclusive <= aliases.size());
        return () -> IntStream.rangeClosed(startInclusive, endInclusive)
                .boxed()
                .flatMap(k -> {
                    final EnumeratingIterator<CorrelationIdentifier> combinationsIterator =
                            ChooseK.chooseK(aliases, k)
                                    .iterator();

                    final Iterator<Set<CorrelationIdentifier>> filteredCombinationsIterator = new AbstractIterator<>() {
                        @Override
                        protected Set<CorrelationIdentifier> computeNext() {
                            while (combinationsIterator.hasNext()) {
                                final List<CorrelationIdentifier> combination = combinationsIterator.next();
                                final Set<CorrelationIdentifier> combinationAsSet = ImmutableSet.copyOf(combination);

                                int i;
                                for (i = 0; i < combination.size(); i++) {
                                    final CorrelationIdentifier alias = combination.get(i);

                                    //
                                    // A broken combination is a combination where aliases contained in the combination
                                    // depend on elements not contained in the combination which in turn again depend
                                    // on elements contained in the combination.
                                    //

                                    // form the dependsOn set of aliases that are dependencies
                                    // that are outside the current combination
                                    final var outsideDependsOn = dependsOnMap.get(alias)
                                            .stream()
                                            .filter(dependsOnAlias -> !combinationAsSet.contains(dependsOnAlias))
                                            .flatMap(dependsOnAlias -> dependsOnMap.get(dependsOnAlias).stream())
                                            .collect(ImmutableSet.toImmutableSet());

                                    // if any of those aliases are in turn back inside the combination, the combination
                                    // is considered to be broken (or unusable).
                                    final boolean brokenCombination = outsideDependsOn.stream().anyMatch(combinationAsSet::contains);
                                    if (brokenCombination) {
                                        break;
                                    }
                                }

                                if (i < combination.size()) {
                                    combinationsIterator.skip(i);
                                } else {
                                    return combinationAsSet;
                                }
                            }
                            return endOfData();
                        }
                    };

                    return StreamSupport.stream(
                            Spliterators.spliteratorUnknownSize(filteredCombinationsIterator, Spliterator.ORDERED),
                            false);
                })
                .iterator();
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
}
