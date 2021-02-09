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

import com.apple.foundationdb.record.query.plan.temp.matching.FindingMatcher;
import com.apple.foundationdb.record.query.plan.temp.matching.MatchPredicate;
import com.apple.foundationdb.record.query.plan.temp.matching.PredicatedMatcher;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
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
 * we need to determine if and how two sub-graphs are related to each other (usually equality but also subsumption, etc.)
 *
 * All of that is rather straightforward if both sub-graphs are completely uncorrelated and therefore independent of
 * each other, or if they are correlated to a mutually external parameter. In these cases the {@code AliasMap} implemented
 * here would effectively be trivial, i.e. the map would be conceptually mapping identifiers to themselves.
 *
 * There are cases, however, that need some care to establish more complex relationships such as those mentioned above.
 *
 * Example 1:
 *    sub graph 1
 *    <pre>
 *    {@code
 *        SELECT T.* FROM T WHERE T.x = c1
 *    }
 *    </pre>
 *
 *    sub graph 2
 *    <pre>
 *    {@code
 *        SELECT T.* FROM T WHERE T.x = c2
 *    }
 *    </pre>
 *
 *    We wold like to establish, if these graphs are equal or not. Both of them are not complete; that is, we wouldn't
 *    be able to execute them as-is. The graphs can only be equal with respect to a particular relationship between
 *    {@code c1} and {@code c2}.
 *
 *    <pre>
 *    {@code
 *            SELECT * FROM R AS c1, (SELECT T.* FROM T WHERE T.x = c1)
 *    }
 *    </pre>
 *
 *    is the same as
 *    <pre>
 *    {@code
 *            SELECT * FROM R AS c2, (SELECT T.* FROM T WHERE T.x = c2)
 *    }
 *    </pre>
 *
 *    whereas
 *
 *    <pre>
 *    {@code
 *            SELECT * FROM R AS c1, (SELECT T.* FROM T WHERE T.x = c1)
 *    }
 *    </pre>
 *
 *        is definitely not the same as
 *
 *    <pre>
 *    {@code
 *            SELECT * FROM S AS c2, (SELECT T.* FROM T WHERE T.x = c2)
 *    }
 *    </pre>
 *
 *    In short, a sub-graph can only be considered equal to another sub-graph if we assume an equality over their
 *    correlated references. If we view a correlated expression as a function of its correlations, then two sub-graph
 *    are equivalent if and only if they are identically equal (i.e., equal on their entire domain) when viewed as
 *    functions.
 *
 * Example 2:
 *    sub graph 1
 *    <pre>
 *    {@code
 *        SELECT T.* FROM T WHERE T.x = c1 AND T.y = c2
 *    }
 *    </pre>
 *
 *    sub graph 2
 *    <pre>
 *    {@code
 *        SELECT T.* FROM T WHERE T.x = c2 AND T.y = c1
 *    }
 *    </pre>
 *
 *    This scenario is somewhat more complicated as it offers more degrees of freedom.
 *
 *    <pre>
 *    {@code
 *            SELECT * FROM R AS c1, S AS c2, (SELECT T.* FROM T WHERE T.x = c1 AND T.y = c2)
 *    }
 *    </pre>
 *
 *        is the same as
 *
 *    <pre>
 *    {@code
 *            SELECT * FROM R AS c2, S AS c1, (SELECT T.* FROM T WHERE T.x = c2 AND T.y = c1)
 *    }
 *    </pre>
 *
 *    This observation is identical to the one in example 1. Due to the commutative nature we also would like to
 *    be able to realize that even this is the same:
 *
 *    <pre>
 *    {@code
 *    SELECT * FROM R AS c1, S AS c2, (SELECT T.* FROM T WHERE T.x = c2 AND T.y = c1)
 *    }
 *    </pre>
 *
 * In any case we need to keep track of what's currently considered bound and equal while we traverse the query graph.
 * When we descend into correlated sub-graphs during an algorithm while we e.g. compute a function like
 * {@link Correlated#semanticEquals} we need to have all outside references bound and mapped in some way: should we treat
 * {@code c1} on the left as {@code c2} on the right or the other way around? It is the job of an object of this class
 * to facilitate the construction and the lookup of these mappings. Algorithms walking the graph make use of this map,
 * construct and reconstruct assumed equality while traversing variation (such as permutations) of possible mappings.
 *
 * This class is immutable, all perceived "mutations" cause a new object to be created.
 *
 */
public class AliasMap {
    private final ImmutableBiMap<CorrelationIdentifier, CorrelationIdentifier> map;

    /**
     * Private constructor. Use static factory methods/builders to instantiate alias maps.
     * @param map the backing bi-map
     */
    private AliasMap(final ImmutableBiMap<CorrelationIdentifier, CorrelationIdentifier> map) {
        this.map = map;
    }

    /**
     * Define equality based on the equality of the backing bimap.
     * @param o other object
     * @return {@code true} if {@code o} is equal to {@code this}, {@code false} otherwise.
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AliasMap aliasMap = (AliasMap)o;
        return Objects.equals(map, aliasMap.map);
    }

    /**
     * Define equality based on the equality of the backing bimap.
     * @return {@code true} if {@code o} is equal to {@code this}.
     */
    @Override
    public int hashCode() {
        return Objects.hash(map);
    }

    @Override
    public String toString() {
        return map.toString();
    }

    public boolean containsSource(@Nonnull final CorrelationIdentifier alias) {
        return map.containsKey(alias);
    }

    public boolean containsTarget(@Nonnull final CorrelationIdentifier alias) {
        return map.containsValue(alias);
    }

    public boolean containsMapping(@Nonnull final CorrelationIdentifier source,
                                   @Nonnull final CorrelationIdentifier target) {
        return containsSource(source) && containsTarget(target) && getTargetOrThrow(source).equals(target);
    }

    /**
     * Returns the set of {@link CorrelationIdentifier}s that are mapped by this {@code AliasMap}.
     * @return a set of {@link CorrelationIdentifier}s that this map contains mappings for
     */
    @Nonnull
    public Set<CorrelationIdentifier> sources() {
        return map.keySet();
    }

    /**
     * Returns the set of all {@link CorrelationIdentifier}s that this map maps to using the set of
     * {@link CorrelationIdentifier}s returned by {@link #sources()}.
     * @return a set of {@link CorrelationIdentifier}s that this map maps to.
     */
    @Nonnull
    public Set<CorrelationIdentifier> targets() {
        return map.values();
    }

    /**
     * Returns the size of the alias map, i.e., the number of contained bindings.
     * @return the size of the alias map.
     */
    public int size() {
        return map.size();
    }

    /**
     * Create a builder for a new alias map using the bindings of this map.
     * @return a new builder derived from the contents of this map.
     */
    @Nonnull
    public Builder derived() {
        return builder().putAll(this);
    }

    /**
     * Create a builder for a new alias map using the bindings of this map.
     * @param expectedAdditionalElements the number of additional elements that the caller expects to add before
     *        build is called.
     * @return a new builder derived from the contents of this map.
     */
    @Nonnull
    public Builder derived(final int expectedAdditionalElements) {
        return builder(expectedAdditionalElements).putAll(this);
    }

    /**
     * Returns the set of entries in this map.
     * @return the set of entries
     */
    @Nonnull
    public Set<Map.Entry<CorrelationIdentifier, CorrelationIdentifier>> entrySet() {
        return map.entrySet();
    }

    /**
     * Get the source for a target passed in.
     * @param target the target to return the source for
     * @return the source that target is bound to in this alias map or {@code null} if there is no binding
     *         to {@code target} in this alias map
     */
    @Nullable
    public CorrelationIdentifier getSource(final CorrelationIdentifier target) {
        return map.inverse().get(target);
    }

    /**
     * Get the source for a target passed in or a default in case there is no mapping for the target alias.
     * @param target the target to return the source for
     * @param defaultValue default value to return is there is no mapping in this map for {@code target}
     * @return the source that target is bound to in this alias map or {@code null} if there is no binding
     *         to {@code target} in this alias map
     */
    @Nonnull
    public CorrelationIdentifier getSourceOrDefault(@Nonnull final CorrelationIdentifier target, @Nonnull final CorrelationIdentifier defaultValue) {
        @Nullable final CorrelationIdentifier source = getSource(target);
        if (source == null) {
            return defaultValue;
        }
        return source;
    }

    /**
     * Get the source for a target passed in or throw an exception in case there is no mapping for the target alias.
     * @param target the target to return the source for
     * @return the source that target is bound to in this alias map or {@code null} if there is no binding
     *         to {@code target} in this alias map
     */
    @Nonnull
    public CorrelationIdentifier getSourceOrThrow(@Nonnull final CorrelationIdentifier target) {
        @Nullable final CorrelationIdentifier source = getTarget(target);
        return Objects.requireNonNull(source);
    }

    /**
     * Get the target for a source passed in.
     * @param source the source to return the target for
     * @return the target that source is bound to in this alias map or {@code null} if there is no binding
     *         from {@code source} in this alias map
     */
    @Nullable
    public CorrelationIdentifier getTarget(final CorrelationIdentifier source) {
        return map.get(source);
    }

    /**
     * Get the target for a source passed in or a default in case there is no mapping for the source alias.
     * @param source the source to return the target for
     * @param defaultValue default value to return is there is no mapping in this map for {@code source}
     * @return the target that source is bound to in this alias map or {@code null} if there is no binding
     *         from {@code source} in this alias map
     */
    @Nonnull
    public CorrelationIdentifier getTargetOrDefault(@Nonnull final CorrelationIdentifier source, @Nonnull final CorrelationIdentifier defaultValue) {
        @Nullable final CorrelationIdentifier target = getTarget(source);
        if (target == null) {
            return defaultValue;
        }
        return target;
    }

    /**
     * Get the target for a source passed in or throw an exception in case there is no mapping for the source alias.
     * @param source the source to return the target for
     * @return the target that source is bound to in this alias map or {@code null} if there is no binding
     *         from {@code source} in this alias map
     */
    @Nonnull
    public CorrelationIdentifier getTargetOrThrow(@Nonnull final CorrelationIdentifier source) {
        @Nullable final CorrelationIdentifier target = getTarget(source);
        return Objects.requireNonNull(target);
    }

    /**
     * Call an action for each mapping contained in this alias map.
     * @param action a bi-consumer that is called for each (source, target) pair
     */
    public void forEachMapping(@Nonnull final BiConsumer<CorrelationIdentifier, CorrelationIdentifier> action) {
        for (final CorrelationIdentifier source : sources()) {
            action.accept(source, Objects.requireNonNull(getTarget(source)));
        }
    }

    /**
     * Filter the bindings of this alias map using some bi-predicate that is handed in. This filter method works
     * by immediately filtering the contents of this map and then creating a new map which is quite different to what
     * e.g. operations on {@link java.util.stream.Stream} would do where the result is computed when the collect
     * happens.
     * @param predicate a bi-predicate that is called for each (source, target) pair
     * @return a new alias map that only retains bindings that where accepted by {@code predicate}, i.e., for which
     *         the predicate returned {@code true}.
     */
    @Nonnull
    public AliasMap filterMappings(@Nonnull final BiPredicate<CorrelationIdentifier, CorrelationIdentifier> predicate) {
        final Builder builder = builder(size());
        for (final CorrelationIdentifier source : sources()) {
            final CorrelationIdentifier target = Objects.requireNonNull(getTarget(source));
            if (predicate.test(source, target)) {
                builder.put(source, target);
            }
        }
        return builder.build();
    }

    /**
     * Compute the composition of two {@link AliasMap}s.
     * @param other second alias map
     * @return a alias map that maps {@code a -> c} for all {@code a, b, c} if {@code a -> b} is contained in
     *         {@code this} and {@code b -> c} is contained in {@code other},
     *         {@code a -> b} for {@code a, b} if {@code a -> b} in {@code this} and no {@code b -> x} for any
     *         {@code x} is contained in {@code other}, and
     *         {@code b -> c} for {@code b, c} if {@code a -> x} for any {@code x} is not contained in {@code this} and
     *         {@code b -> c} is contained in {@code other}
     */
    @Nonnull
    public AliasMap compose(@Nonnull final AliasMap other) {
        final Builder builder =
                AliasMap.builder(size() + other.size());

        map.forEach((source, target) -> builder.put(source, other.getTargetOrDefault(target, target)));

        other.forEachMapping((source, target) -> {
            // add the mappings that originate in the other side but don't have a mapping in this
            if (!containsTarget(source)) {
                Preconditions.checkArgument(!containsSource(source), "conflicting mapping");
                builder.put(source, target);
            }
        });
        return builder.build();
    }

    /**
     * Determine if two {@link AliasMap}s are compatible.
     * @param other second alias map
     * @return {@code true} if this {@code AliasMap} is compatible to {@code other}, that is there are no conflicting
     *         mappings in a sense that a union of the mappings of {@code this} and {@code other} can form a
     *         {@link BiMap}, {@code false} otherwise.
     */
    public boolean isCompatible(@Nonnull final AliasMap other) {
        for (final CorrelationIdentifier otherSource : other.sources()) {
            final CorrelationIdentifier otherTarget = Objects.requireNonNull(other.getTarget(otherSource));
            if (containsSource(otherSource)) {
                if (!otherTarget.equals(getTarget(otherSource))) {
                    return false;
                }
            } else {
                if (containsTarget(otherTarget)) {
                    return false;
                }
            }
        }
        return true;
    }


    /**
     * Combine two compatible {@link AliasMap}s.
     * @param other second alias map
     * @return a combined translation map (see {@link #combineMaybe})
     */
    @Nonnull
    public AliasMap combine(@Nonnull final AliasMap other) {
        return combineMaybe(other)
                .orElseThrow(() -> new IllegalArgumentException("duplicate mapping"));
    }

    /**
     * Combine two {@link AliasMap}s if possible
     * @param other second alias map
     * @return {@code Optional} containing a translation map that maps {@code a -> b} for all {@code a, b} if
     *         {@code a -> b} is contained in {@code this} and there is no c with {@code b != c}
     *         such that {@code a -> c} is contained in {@code other}. Empty {@code Optional}, otherwise.
     */
    @Nonnull
    public Optional<AliasMap> combineMaybe(@Nonnull final AliasMap other) {
        final Builder builder =
                AliasMap.builder(size() + other.size());

        for (final CorrelationIdentifier otherSource : other.sources()) {
            final CorrelationIdentifier otherTarget = Objects.requireNonNull(other.getTarget(otherSource));
            if (containsSource(otherSource)) {
                if (!otherTarget.equals(getTarget(otherSource))) {
                    return Optional.empty();
                }
            } else {
                if (containsTarget(otherTarget)) {
                    return Optional.empty();
                }
            }
            builder.put(otherSource, otherTarget);
        }
        map.forEach(builder::put);
        return Optional.of(builder.build());
    }

    /**
     * Create a new empty builder.
     * @return a builder for a new {@link AliasMap}
     */
    @Nonnull
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Create a new empty builder using an expected size that is passed in.
     * @param expectedSize expected size of the eventual {@link AliasMap}
     * @return a builder for a new {@link AliasMap}
     */
    @Nonnull
    public static Builder builder(final int expectedSize) {
        return new Builder(expectedSize);
    }

    /**
     * Factory method to create an empty alias map.
     * @return a new empty {@link AliasMap}
     */
    @Nonnull
    public static AliasMap emptyMap() {
        return new AliasMap(ImmutableBiMap.of());
    }

    /**
     * Factory method to create an alias map based on a given binding.
     * @param source an alias of a source
     * @param target an alias of a target
     * @return a new {@link AliasMap} containing exactly the binding {@code source -> target}
     */
    @Nonnull
    public static AliasMap of(@Nonnull final CorrelationIdentifier source, @Nonnull final CorrelationIdentifier target) {
        return new AliasMap(ImmutableBiMap.of(source, target));
    }

    /**
     * Factory method to create an alias map that is a copy of an {@link BiMap} passed in. Note that no actual
     * copy takes place if the bi-map that is passed in is of type {@link ImmutableBiMap}.
     * @param map a bi0map containing bindings
     * @return a new {@link AliasMap}
     */
    @Nonnull
    public static AliasMap copyOf(@Nonnull final BiMap<CorrelationIdentifier, CorrelationIdentifier> map) {
        return new AliasMap(ImmutableBiMap.copyOf(map));
    }

    /**
     * Factory method to create an alias map that contains entries {@code a -> a, b -> b, ...} for each alias contained
     * in the set of {@link CorrelationIdentifier}s passed in.
     * @param aliases set of aliases that this method should create identity-bindings for
     * @return a new {@link AliasMap}
     */
    @Nonnull
    public static AliasMap identitiesFor(final Set<CorrelationIdentifier> aliases) {
        return builder(aliases.size()).identitiesFor(aliases).build();
    }

    /**
     * Factory method to create an alias map based on a {@code zip} of two parallel lists of aliases.
     * @param left one list
     * @param right other list
     * @return a new {@link AliasMap}
     */
    @Nonnull
    public static AliasMap zip(@Nonnull final List<CorrelationIdentifier> left, @Nonnull final List<CorrelationIdentifier> right) {
        return builder(left.size()).zip(left, right).build();
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

        /**
         * Put a new binding between {@code source} and {@code target}. Note that if there is already a binding for
         * {@code source}, the binding will remain untouched if {@code target.equals(oldTarget)}, it will be
         * replaced with {@code source -> target} otherwise. Also, putting a binding for a {@code target} that already
         * exists will fail.
         * @param source a source alias
         * @param target a target alias
         * @throws IllegalArgumentException if {@code target} is already contained in the builder
         * @return {@code this} that has been modified to contain {@code source -> target}
         */
        @Nonnull
        public Builder put(@Nonnull final CorrelationIdentifier source, @Nonnull final CorrelationIdentifier target) {
            map.put(source, target);
            return this;
        }

        /**
         * Put all binding that are contained in another {@link AliasMap}. Note that if there are already bindings for
         * sources contained in {@code other}, these bindings will remain untouched if {@code target.equals(oldTarget)},
         * they will be replaced otherwise. Also, this call will fail if a target contained in {@code other} already
         * exists in {@code this},
         * @param other another {@link AliasMap}
         * @throws IllegalArgumentException if any target contained in {@code other} is already contained in the builder
         * @return {@code this} that has been modified to contain all bindings from {@code other}
         */
        @Nonnull
        public Builder putAll(@Nonnull final AliasMap other) {
            other.sources()
                    .forEach(source -> put(source,
                            Objects.requireNonNull(other.getTarget(source))));
            return this;
        }

        /**
         * Method to update the builder to contain entries {@code a -> a, b -> b, ...} for each alias contained
         * in the set of {@link CorrelationIdentifier}s passed in.
         * @param aliases set of aliases that this method should create identity-bindings for
         * @return {@code this} that has been modified to contain identity-bindings for all aliases contained in
         *         {@code aliases}
         */
        @Nonnull
        public Builder identitiesFor(final Set<CorrelationIdentifier> aliases) {
            aliases.forEach(id -> put(id, id));
            return this;
        }

        /**
         * Method to update the builder to additionally contain the {@code zip} of two parallel lists of aliases.
         * @param left one list
         * @param right other list
         * @return {@code this} that has been modified to contain the zip of left and right as bindings {@code l -> r};
         */
        @Nonnull
        public Builder zip(@Nonnull final List<CorrelationIdentifier> left, @Nonnull final List<CorrelationIdentifier> right) {
            final int size = left.size();
            Verify.verify(size == right.size());

            for (int i = 0; i < size; i ++) {
                final CorrelationIdentifier leftId = left.get(i);
                put(leftId, right.get(i));
            }

            return this;
        }

        /**
         * Build a new {@link AliasMap}. This will entail a copy of the mappings in order to gain immutability guarantees.
         * @return a new {@link AliasMap}
         */
        @Nonnull
        public AliasMap build() {
            return new AliasMap(ImmutableBiMap.copyOf(map));
        }
    }

    /**
     * Find matches between two sets of aliases, given their depends-on sets and a
     * {@link MatchPredicate}.
     * This method creates an underlying {@link PredicatedMatcher} to do the work.
     * @param aliases a set of aliases
     * @param dependsOnFn a function that returns the set of dependencies for a given alias within {@code aliases}
     * @param otherAliases a set of other aliases
     * @param otherDependsOnFn a function that returns the set of dependencies for a given alias within {@code otherAliases}
     * @param matchPredicate match predicate. see {@link MatchPredicate}
     *        for more info
     * @return an iterable of {@link AliasMap}s where each individual {@link AliasMap} is considered one match
     */
    public Iterable<AliasMap> findMatches(@Nonnull final Set<CorrelationIdentifier> aliases,
                                          @Nonnull final Function<CorrelationIdentifier, Set<CorrelationIdentifier>> dependsOnFn,
                                          @Nonnull final Set<CorrelationIdentifier> otherAliases,
                                          @Nonnull final Function<CorrelationIdentifier, Set<CorrelationIdentifier>> otherDependsOnFn,
                                          @Nonnull final MatchPredicate<CorrelationIdentifier> matchPredicate) {
        return matcher(
                aliases,
                dependsOnFn,
                otherAliases,
                otherDependsOnFn,
                matchPredicate)
                .findMatches();
    }

    public PredicatedMatcher matcher(@Nonnull final Set<CorrelationIdentifier> aliases,
                                     @Nonnull final Function<CorrelationIdentifier, Set<CorrelationIdentifier>> dependsOnFn,
                                     @Nonnull final Set<CorrelationIdentifier> otherAliases,
                                     @Nonnull final Function<CorrelationIdentifier, Set<CorrelationIdentifier>> otherDependsOnFn,
                                     @Nonnull final MatchPredicate<CorrelationIdentifier> matchPredicate) {
        return FindingMatcher.onAliases(
                this,
                aliases,
                dependsOnFn,
                otherAliases,
                otherDependsOnFn,
                matchPredicate);
    }
}
