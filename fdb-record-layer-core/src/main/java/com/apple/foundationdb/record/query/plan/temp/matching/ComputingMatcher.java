/*
 * ComputingMatcher.java
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
import com.apple.foundationdb.record.query.plan.temp.ChooseK;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.CrossProduct;
import com.apple.foundationdb.record.query.plan.temp.EnumeratingIterable;
import com.apple.foundationdb.record.query.plan.temp.EnumeratingIterator;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

/**
 * This class implements a {@link GenericMatcher} which matches two sets of elements of type {@code T} to compute
 * a result of type {@code Iterable<BoundMatch<R>>}. Individual elements are matched by means of a {@link MatchFunction}
 * which produces an (intermediate) {@link Iterable} of type {@code M} for each pair of matching elements. In order to
 * match this set against the other set, we need to find matching pairs of elements for each element identified by
 * {@link #getAliases()}.
 * The intermediate iterables of type {@code M} produced by the matching function get accumulated across all such
 * matching pairs to produce an {@link Iterable} of type {@code R} using a {@link MatchAccumulator}.
 *
 * @param <T> the element type
 * @param <M> the type that the {@link MatchFunction} produces {@link Iterable}s of
 * @param <R> the result type
 */
public class ComputingMatcher<T, M, R> extends BaseMatcher<T> implements GenericMatcher<BoundMatch<R>> {
    /**
     * Match function that computes an {@link Iterable} of type {@code M} for each matching pair
     * of elements.
     */
    @Nonnull
    private final MatchFunction<T, M> matchFunction;

    /**
     * Supplier to create accumulators to accumulate intermediate {@link Iterable}s
     * of type {@code M} into an {@link Iterable} of type {@code R}.
     */
    @Nonnull
    private final Supplier<MatchAccumulator<M, R>> matchAccumulatorSupplier;

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
     * @param matchFunction a match function that computes an {@link Iterable} of type {@code M} for each matching pair
     *        of elements
     * @param matchAccumulatorSupplier a supplier to create accumulators to accumulate intermediate {@link Iterable}s
     *        of type {@code M} into an {@link Iterable} of type {@code R}
     */
    private ComputingMatcher(@Nonnull final AliasMap boundAliasesMap,
                             @Nonnull final Set<CorrelationIdentifier> aliases,
                             @Nonnull final Function<T, CorrelationIdentifier> elementToAliasFn,
                             @Nonnull final Map<CorrelationIdentifier, T> aliasToElementMap,
                             @Nonnull final ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> dependsOnMap,
                             @Nonnull final Set<CorrelationIdentifier> otherAliases,
                             @Nonnull final Function<T, CorrelationIdentifier> otherElementToAliasFn,
                             @Nonnull final Map<CorrelationIdentifier, ? extends T> otherAliasToElementMap,
                             @Nonnull final ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> otherDependsOnMap,
                             @Nonnull final MatchFunction<T, M> matchFunction,
                             @Nonnull final Supplier<MatchAccumulator<M, R>> matchAccumulatorSupplier) {
        super(boundAliasesMap, aliases, elementToAliasFn, aliasToElementMap, dependsOnMap, otherAliases, otherElementToAliasFn, otherAliasToElementMap, otherDependsOnMap);
        this.matchFunction = matchFunction;
        this.matchAccumulatorSupplier = matchAccumulatorSupplier;
    }

    /**
     * Match using the method {@link #enumerate} as {@link EnumerationFunction}.
     * @return an iterable of match results.
     */
    @Nonnull
    @Override
    public Iterable<BoundMatch<R>> match() {
        return match(this::enumerate);
    }

    @Override
    @Nonnull
    protected Iterable<List<CorrelationIdentifier>> otherCombinations(final List<CorrelationIdentifier> otherPermutation, final int limitInclusive) {
        final Set<CorrelationIdentifier> otherAliases = getOtherAliases();
        final ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> otherDependsOnMap = getOtherDependsOnMap();
        Preconditions.checkArgument(limitInclusive <= otherAliases.size());
        return () -> IntStream.rangeClosed(0, limitInclusive)
                .boxed()
                .flatMap(k -> {
                    final EnumeratingIterator<CorrelationIdentifier> combinationsIterator =
                            ChooseK.chooseK(otherAliases, k)
                                    .iterator();

                    final Iterator<List<CorrelationIdentifier>> filteredCombinationsIterator = new AbstractIterator<List<CorrelationIdentifier>>() {
                        @Override
                        protected List<CorrelationIdentifier> computeNext() {
                            while (combinationsIterator.hasNext()) {
                                final List<CorrelationIdentifier> combination = combinationsIterator.next();
                                final Set<CorrelationIdentifier> visibleAliases = Sets.newHashSetWithExpectedSize(otherAliases.size());

                                int i;
                                for (i = 0; i < combination.size(); i++) {
                                    final CorrelationIdentifier alias = combination.get(i);
                                    final boolean brokenCombination = otherDependsOnMap.get(alias)
                                            .stream()
                                            .anyMatch(dependsOnAlias -> otherAliases.contains(dependsOnAlias) && // not an external dependency
                                                                        !visibleAliases.contains(dependsOnAlias));
                                    if (brokenCombination) {
                                        break;
                                    } else {
                                        visibleAliases.add(alias);
                                    }
                                }

                                if (i < combination.size()) {
                                    combinationsIterator.skip(i);
                                } else {
                                    return combination;
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
     * Method to enumerate the permutations on this side against the permutation of the other side in order
     * to form matches (bijective mappings between the permutations). The match function is called for each pair of
     * elements (for a match attempt). If the match function returns a non-empty {@link Iterable} the pair is recorded
     * as a matching pair. We attempt to find a matching pair (one from this side; one from the other side) for each
     * element identified by {@link #getAliases()}. For each individual new such pair that is found,
     * the method {@link MatchAccumulator#accumulate} is called. Once a set of bindings is established for all aliases
     * in {@link #getAliases} this method then calls {@link MatchAccumulator#finish} to produce an iterable of type
     * {@code R}.
     * @param iterator an enumerating iterable for the permutations on this side
     * @param otherPermutation one permutation (that is not violating dependencies, constraints, etc.) of the other side
     * @return an {@link Iterator} of match results (of type {@code BoundMatch<R>})
     */
    @SuppressWarnings("java:S135")
    @Nonnull
    public Iterator<BoundMatch<R>> enumerate(@Nonnull final EnumeratingIterator<CorrelationIdentifier> iterator,
                                             @Nonnull final List<CorrelationIdentifier> otherPermutation) {
        final Set<CorrelationIdentifier> aliases = getAliases();
        final AliasMap boundAliasesMap = getBoundAliasesMap();

        if (otherPermutation.isEmpty()) {
            return ImmutableList.of(BoundMatch.<R>withAliasMap(boundAliasesMap)).iterator();
        }

        final int size = otherPermutation.size();

        return new AbstractIterator<BoundMatch<R>>() {
            @Override
            protected BoundMatch<R> computeNext() {
                while (iterator.hasNext()) {
                    final List<CorrelationIdentifier> permutation = iterator.next();
                    final AliasMap.Builder aliasMapBuilder = AliasMap.builder(aliases.size());
                    final MatchAccumulator<M, R> accumulatedMatchResult = matchAccumulatorSupplier.get();

                    int i;
                    for (i = 0; i < size; i++) {
                        final AliasMap aliasMap = aliasMapBuilder.build();

                        final CorrelationIdentifier alias = permutation.get(i);
                        final CorrelationIdentifier otherAlias = otherPermutation.get(i);

                        final Optional<AliasMap> locallyBoundMapOptional = mapDependenciesToOther(aliasMap, alias, otherAlias);
                        if (!locallyBoundMapOptional.isPresent()) {
                            break;
                        }

                        final AliasMap locallyBoundMap = locallyBoundMapOptional.get();

                        final T entity = Objects.requireNonNull(getAliasToElementMap().get(alias));
                            final T otherEntity = Objects.requireNonNull(getOtherAliasToElementMap().get(otherAlias));

                        final Iterable<M> matchResults =
                                matchFunction.apply(entity, otherEntity, boundAliasesMap.combine(locallyBoundMap));

                        if (Iterables.isEmpty(matchResults)) {
                            break;
                        }

                        accumulatedMatchResult.accumulate(matchResults);

                        // We now amend the equivalences passed in by adding the already known bound aliases left
                        // of i and make them equivalent as well
                        aliasMapBuilder.put(alias, otherAlias);
                    }

                    final R result = accumulatedMatchResult.finish();

                    if (i == size) {
                        iterator.skip(i - 1);
                        return BoundMatch.withAliasMapAndMatchResult(boundAliasesMap.combine(aliasMapBuilder.build()), result);
                    } else {
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
     * @param elements a collection of elements of type {@code T} representing the domain on this side
     * @param elementToAliasFn a function to map elements of type {@code T} on this side to correlation identifiers
     * @param dependsOnFn a function from elements to sets of correlation identifiers describing the depends-on
     *        relationships between the elements on this side
     * @param otherElements a collection of other elements representing the domain on the other side
     * @param otherElementToAliasFn a function to map elements of type {@code T} on the other side to correlation
     *        identifiers
     * @param otherDependsOnFn a function from elements to sets of correlation identifiers describing the depends-on
     *        relationships between the other elements
     * @param matchFunction a match function that computes an {@link Iterable} of type {@code M} for each matching pair
     *        of elements
     * @param matchAccumulatorSupplier a supplier to create accumulators to accumulate intermediate {@link Iterable}s
     *        of type {@code M} into an {@link Iterable} of type {@code R}
     * @param <T> the element type
     * @param <M> the type that the {@link MatchFunction} produces {@link Iterable}s of
     * @param <R> r
     * @return a newly created generic matcher of type {@code BoundMatch<R>}
     */
    @Nonnull
    public static <T, M, R> GenericMatcher<BoundMatch<R>> onAliasDependencies(@Nonnull final AliasMap boundAliasesMap,
                                                                              @Nonnull final Collection<? extends T> elements,
                                                                              @Nonnull final Function<T, CorrelationIdentifier> elementToAliasFn,
                                                                              @Nonnull final Function<T, Set<CorrelationIdentifier>> dependsOnFn,
                                                                              @Nonnull final Collection<? extends T> otherElements,
                                                                              @Nonnull final Function<T, CorrelationIdentifier> otherElementToAliasFn,
                                                                              @Nonnull final Function<T, Set<CorrelationIdentifier>> otherDependsOnFn,
                                                                              @Nonnull final MatchFunction<T, M> matchFunction,
                                                                              @Nonnull final Supplier<MatchAccumulator<M, R>> matchAccumulatorSupplier) {
        ImmutableSet<CorrelationIdentifier> aliases = BaseMatcher.computeAliases(elements, elementToAliasFn);
        final ImmutableMap<CorrelationIdentifier, T> aliasToElementMap = BaseMatcher.computeAliasToElementMap(elements, elementToAliasFn);

        final ImmutableSet<CorrelationIdentifier> otherAliases = BaseMatcher.computeAliases(otherElements, otherElementToAliasFn);
        final ImmutableMap<CorrelationIdentifier, T> otherAliasToElementMap = BaseMatcher.computeAliasToElementMap(otherElements, otherElementToAliasFn);

        ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> dependsOnMap = BaseMatcher.computeDependsOnMapWithAliases(aliases, aliasToElementMap, dependsOnFn);
        final ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> otherDependsOnMap = BaseMatcher.computeDependsOnMapWithAliases(otherAliases, otherAliasToElementMap, otherDependsOnFn);

        return new ComputingMatcher<>(
                boundAliasesMap,
                aliases,
                elementToAliasFn,
                aliasToElementMap,
                dependsOnMap,
                otherAliases,
                otherElementToAliasFn,
                otherAliasToElementMap,
                otherDependsOnMap,
                matchFunction,
                matchAccumulatorSupplier);
    }

    /**
     * Method that returns an {@link MatchAccumulator} that accumulates {@link Iterable}s of type {@code M} and finally
     * produces an {@link EnumeratingIterable} of type {@code M} that is the cross product of all elements of the
     * respective {@link Iterable}s.
     *
     * @param <M> the type the {@link MatchFunction} produces {@link Iterable}s of
     * @return a newly created accumulator
     */
    @Nonnull
    public static <M> MatchAccumulator<M, EnumeratingIterable<M>> productAccumulator() {
        return new MatchAccumulator<M, EnumeratingIterable<M>>() {
            private final ImmutableList.Builder<Iterable<M>> state = ImmutableList.builder();

            @Override
            public void accumulate(final Iterable<M> v) {
                state.add(v);
            }

            @Override
            public EnumeratingIterable<M> finish() {
                return CrossProduct.crossProduct(state.build());
            }
        };
    }
}
