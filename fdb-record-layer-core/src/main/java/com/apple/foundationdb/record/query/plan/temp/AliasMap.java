/*
 * AliasMap.java
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

package com.apple.foundationdb.record.query.plan.temp;

import com.google.common.base.Verify;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * This class encapsulates mappings between {@link CorrelationIdentifier}s and helpers to create and maintain
 * these mappings.
 *
 * While various operations or in general algorithms traverse the query graph(s) it is important to keep track of
 * what alias (of a quantifier) should be considered to what other alias (of another quantifier). A quantifier
 * ({@link Quantifier} is referred to via {@link CorrelationIdentifier}. In this way there are no direct pointers
 * to objects which is desirable while the graph is mutated through transformations. At various points during
 * the planning of a query we may want to determine e.g. if a sub-graph would produce the same result as some other
 * sub-graph. Similarly, we may want to determine whether a sub-graph is already properly memoized or not. In general,
 * we need to theorize if two sub-graphs are related to each other (usually equality but also subsumption, etc.)
 *
 * All of that is rather straightforward if both sub-graphs are completely uncorrelated and therefore independent to
 * each other, to some other record producer, or even to some external parameter that is being passed in. In these
 * cases the {@code AliasMap} implemented here would effectively be trivial, i.e. the map would be
 * conceptually empty.
 *
 * There are cases, however, that need some care to establish the relationships mentioned before.
 *
 * Example 1:
 *    sub graph 1
 *        SELECT T.* FROM T WHERE T.x = c1
 *
 *    sub graph 2
 *        SELECT T.* FROM T WHERE T.x = c2
 *
 *    We wold like to establish, if these graphs are equal or not. Both of them are not complete, i.e. we wouldn't
 *    be able to execute them as is. If we asked if these sub graphs where equal, we certainly would need to say that
 *    that depends. If they, however, are embedded in some surrounding query that binds c1 and c2 respectively, we
 *    would be able to say if in the greater context of the surrounding query these sub query do the same thing.
 *
 *            SELECT * FROM R AS c1, (SELECT T.* FROM T WHERE T.x = c1)
 *
 *        is the same as
 *
 *            SELECT * FROM R AS c2, (SELECT T.* FROM T WHERE T.x = c2)
 *
 *    whereas
 *            SELECT * FROM R AS c1, (SELECT T.* FROM T WHERE T.x = c1)
 *
 *        is definitely not the same as
 *
 *            SELECT * FROM S AS c2, (SELECT T.* FROM T WHERE T.x = c2)
 *
 *    In short, a sub-graph can only be considered equal to another sub-graph if we assume an equality over their
 *    correlated references. In other words the sub-graphs define two functions f(x) and g(y). While we can determine
 *    if f and g are structurally equivalent we con only determine actual result equivalence if we assume x = y for
 *    the entire domain of x and y.
 *
 * Example 2:
 *    sub graph 1
 *        SELECT T.* FROM T WHERE T.x = c1 AND T.y = c2
 *
 *    sub graph 2
 *        SELECT T.* FROM T WHERE T.x = c2 AND T.y = c1
 *
 *    This scenario is somewhat more complicated as it offers more degrees of freedom.
 *
 *            SELECT * FROM R AS c1, S AS c2, (SELECT T.* FROM T WHERE T.x = c1 AND T.y = c2)
 *
 *        is the same as
 *
 *            SELECT * FROM R AS c2, S AS c1, (SELECT T.* FROM T WHERE T.x = c2 AND T.y = c1)
 *
 *    This observation is identical to the one in example 1. Due to the commutative nature we also would like to
 *    be able to realize that even this is the same:
 *
 *    SELECT * FROM R AS c1, S AS c2, (SELECT T.* FROM T WHERE T.x = c2 AND T.y = c1)
 *
 * In any case we need to keep track of what's currently considered bound and equal while we traverse the query graph.
 * When we descend into correlated sub-graphs during an algorithm while we e.g. determine {@link Correlated#resultEquals}
 * we need to have all outside references bound and mapped in some way: Should we treat c1 on the left as c2 on the right
 * or the other way around? It is the job of an object of this class to facilitate the construction and the lookup of
 * these mappings. Algorithms walking the graph make use of this map, construct and reconstruct assumed equality while
 * traversing e.g. permutations of possible mappings.
 *
 * Another 
 *
 * Note: This class is immutable, all perceived "mutations" cause a new object to be created.
 *
 */
public class AliasMap {
    private final ImmutableBiMap<CorrelationIdentifier, CorrelationIdentifier> map;

    private AliasMap(final ImmutableBiMap<CorrelationIdentifier, CorrelationIdentifier> map) {
        this.map = map;
    }

    public boolean containsSource(@Nonnull final CorrelationIdentifier alias) {
        return map.containsKey(alias);
    }

    public boolean containsTarget(@Nonnull final CorrelationIdentifier alias) {
        return map.containsValue(alias);
    }

    /**
     * Returns the set of {@link CorrelationIdentifier}s that are mapped by this {@code AliasMap}.
     * @return a set of {@link CorrelationIdentifier}s that this map contains mappings for.
     */
    @Nonnull
    public Set<CorrelationIdentifier> sources() {
        return map.keySet();
    }

    /**
     * Returns the set of all {@link CorrelationIdentifier}s that this map maps to using the set of {@link CorrelationIdentifier}s
     * returned by {@link #sources()}.
     * @return a set of {@link CorrelationIdentifier}s that this map maps to.
     */
    @Nonnull
    public Set<CorrelationIdentifier> targets() {
        return map.values();
    }

    public int size() {
        return map.size();
    }

    @Nonnull
    public Builder derived() {
        return new Builder(map);
    }

    @Nonnull
    public Builder derived(int expectedAdditionalElements) {
        return new Builder(size() + expectedAdditionalElements).putAll(this);
    }

    /**
     * Compute the composition of two {@link AliasMap}s.
     * @param other second alias map
     * @return a translation map that maps {@code a -> c} for {@code a, b, c} if {@code a -> b} is contained in {@code first} and {@code b -> c} is contained in {@code second},
     *         {@code a -> b} for {@code a, b} if {@code a -> b} in {@code first} and no {@code b -> x} for any {@code x} is contained in {@code second}, and
     *         {@code b -> c} for {@code b, c} if {@code a -> x} for any {@code x} is not contained in {@code first} and {@code b -> c} is contained in {@code second}
     */
    @Nonnull
    public AliasMap compose(@Nonnull final AliasMap other) {
        final Builder builder =
                AliasMap.builder(size() + other.size());

        final ImmutableBiMap<CorrelationIdentifier, CorrelationIdentifier> otherMap = other.map;
        this.map.forEach((key, value) -> builder.put(key, otherMap.getOrDefault(value, value)));

        otherMap.forEach((key, value) -> {
            if (!containsSource(key)) {
                builder.put(key, value);
            }
        });
        return builder.build();
    }

    /**
     * Method to match up the given sets of correlation identifiers (using an already given equivalence map).
     *
     * @param aliases aliases of _this_ set
     * @param dependsOnFn function yielding the dependsOn set for an alias of _this_ set
     * @param otherAliases aliases of _the other_ set
     * @param otherDependsOnFn function yielding the dependsOn set for an alias of _the other_ set
     * @param canCorrelate {@code true} if _this_ set (and the _other_ set) can be the source of correlations themselves.
     * @param predicate that tests if two aliases match
     * @return An iterable iterating alias maps containing mappings from the correlation identifiers ({@code aliases}) to
     *         correlations identifiers {@code otherAliases} according to the given predicate. Note that these mappings
     *         are bijective and can therefore be inverted.
     */
    @SuppressWarnings({"squid:S135"})
    @Nonnull
    public Iterable<AliasMap> match(@Nonnull final Set<CorrelationIdentifier> aliases,
                                    @Nonnull final Function<CorrelationIdentifier, Set<CorrelationIdentifier>> dependsOnFn,
                                    @Nonnull final Set<CorrelationIdentifier> otherAliases,
                                    @Nonnull final Function<CorrelationIdentifier, Set<CorrelationIdentifier>> otherDependsOnFn,
                                    final boolean canCorrelate,
                                    @Nonnull final MatchingIdPredicate predicate) {
        if (aliases.size() != otherAliases.size()) {
            return ImmutableList.of();
        }

        if (aliases.isEmpty()) {
            return ImmutableList.of(empty());
        }

        //
        // We do not know which id in "this" maps to which in "other". In fact, we cannot know as it is
        // intentionally modeled this way. We need to find _a_ feasible ordering that matches. In reality, the
        // quantifiers owned by an expression imposes a more or less a complete order leaving no room for many
        // permutations among unrelated quantifiers. In the worst case, we must enumerate them all. Luckily, most
        // algorithms for children have short-circuit semantics, so the uninteresting case where sub-graphs underneath
        // quantifiers do not match should be skipped quickly.
        //
        // get _a_ topologically-sound permutation from other
        final Optional<List<CorrelationIdentifier>> otherOrderedOptional =
                TopologicalSort.anyTopologicalOrderPermutation(
                        otherAliases,
                        alias -> Objects.requireNonNull(otherDependsOnFn.apply(alias)));

        // There should always be a topologically sound ordering that we can use.
        Verify.verify(otherOrderedOptional.isPresent());

        final List<CorrelationIdentifier> otherOrdered = otherOrderedOptional.get();

        final TopologicalSort.TopologicalOrderPermutationIterable<CorrelationIdentifier> iterable =
                TopologicalSort.topologicalOrderPermutations(
                        aliases,
                        alias -> Objects.requireNonNull(dependsOnFn.apply(alias)));

        return () -> {
            final TopologicalSort.TopologicalOrderPermutationIterator<CorrelationIdentifier> iterator = iterable.iterator();

            return new AbstractIterator<AliasMap>() {
                @Override
                protected AliasMap computeNext() {
                    while (iterator.hasNext()) {
                        final Builder equivalenceMapBuilder = derived(aliases.size());

                        final List<CorrelationIdentifier> ordered = iterator.next();
                        int i;
                        for (i = 0; i < aliases.size(); i++) {
                            final CorrelationIdentifier alias = ordered.get(i);
                            final CorrelationIdentifier otherAlias = otherOrdered.get(i);

                            if (canCorrelate) {
                                // We now amend the equivalences passed in by adding the already known bound aliases left
                                // of i and make them equivalent as well
                                equivalenceMapBuilder.put(alias, otherAlias);
                            }

                            if (!predicate.test(alias, otherAlias, equivalenceMapBuilder.build())) {
                                break;
                            }
                        }

                        if (i == aliases.size()) {
                            // zip ordered and otherOrdered as they now match
                            return zip(ordered, otherOrdered);
                        } else {
                            // we can skip all permutations where the i-th value is bound the way it currently is
                            iterator.skip(i);
                        }
                    }

                    return endOfData();
                }
            };
        };
    }

    @Nonnull
    public static Builder builder() {
        return new Builder();
    }

    @Nonnull
    public static Builder builder(final int expectedSize) {
        return new Builder(expectedSize);
    }

    @Nonnull
    public static AliasMap empty() {
        return new AliasMap(ImmutableBiMap.of());
    }

    @Nonnull
    public static AliasMap of(@Nonnull final CorrelationIdentifier source, @Nonnull final CorrelationIdentifier target) {
        return new AliasMap(ImmutableBiMap.of(source, target));
    }

    @Nonnull
    public static AliasMap of(@Nonnull final BiMap<CorrelationIdentifier, CorrelationIdentifier> map) {
        return new AliasMap(ImmutableBiMap.copyOf(map));
    }

    @Nonnull
    public static AliasMap identitiesFor(final Set<CorrelationIdentifier> correlationIdentifiers) {
        return new AliasMap(correlationIdentifiers.stream()
                .collect(ImmutableBiMap.toImmutableBiMap(Function.identity(), Function.identity())));
    }

    @SuppressWarnings("UnstableApiUsage")
    @Nonnull
    public static AliasMap zip(@Nonnull final List<CorrelationIdentifier> left, @Nonnull final List<CorrelationIdentifier> right) {
        return new AliasMap(Streams.zip(left.stream(), right.stream(),
                (l, r) -> Pair.of(Objects.requireNonNull(l), Objects.requireNonNull(r)))
                .collect(ImmutableBiMap.toImmutableBiMap(Pair::getLeft, Pair::getRight)));
    }

    /**
     * Builder class for the {@link AliasMap}.
     */
    public static class Builder {
        private final HashBiMap<CorrelationIdentifier, CorrelationIdentifier> map;

        private Builder() {
            this.map = HashBiMap.create();
        }

        private Builder(final int expectedSize) {
            this.map = HashBiMap.create(expectedSize);
        }

        private Builder(final BiMap<CorrelationIdentifier, CorrelationIdentifier> map) {
            this.map = HashBiMap.create(map);
        }

        @Nonnull
        public Builder put(@Nonnull final CorrelationIdentifier source, @Nonnull final CorrelationIdentifier target) {
            map.put(source, target);
            return this;
        }

        @Nonnull
        public Builder putAll(@Nonnull final AliasMap other) {
            map.putAll(other.map);
            return this;
        }

        public AliasMap build() {
            return new AliasMap(ImmutableBiMap.copyOf(map));
        }
    }

    /**
     * An predicate that tests for a match between quantifiers also taking into account an equivalence maps between
     * {@link CorrelationIdentifier}s.
     */
    @FunctionalInterface
    public interface MatchingIdPredicate {
        boolean test(@Nonnull CorrelationIdentifier id,
                     @Nonnull CorrelationIdentifier otherId,
                     @Nonnull final AliasMap equivalencesMap);
    }
}
