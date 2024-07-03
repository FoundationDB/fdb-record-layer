/*
 * DependencyUtils.java
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

import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.StreamSupport;

/**
 * Helper methods useful for managing dependency sets using {@link CorrelationIdentifier}s.
 */
public class DependencyUtils {

    private DependencyUtils() {
        // nothing
    }

    /**
     * Static helper to compute a set of {@link CorrelationIdentifier}s from a collection of elements of type {@code T}
     * using the given element-to-alias function. Note that we allow a collection of elements to be passed in, we
     * compute a set of {@link CorrelationIdentifier}s from it (i.e. duplicate aliases are not allowed).
     * @param elements an iterable of elements of type {@code T}
     * @param elementToAliasFn element to alias function
     * @param <T> element type
     * @return a set of aliases
     */
    @Nonnull
    public static <T> ImmutableSet<CorrelationIdentifier> computeAliases(@Nonnull final Iterable<? extends T> elements,
                                                                         @Nonnull final Function<T, CorrelationIdentifier> elementToAliasFn) {
        return StreamSupport.stream(elements.spliterator(), false)
                .map(elementToAliasFn)
                .collect(ImmutableSet.toImmutableSet());
    }

    /**
     * Static helper to compute the alias-to-element map based on a collection of elements and on the element-to-alias function
     * tht are both passed in.
     * @param elements an iterable of elements of type {@code T}
     * @param elementToAliasFn element to alias function
     * @param <T> element type
     * @return a map from {@link CorrelationIdentifier} to {@code T} representing the conceptual inverse of the
     *         element to alias function passed in
     */
    @Nonnull
    public static <T> ImmutableMap<CorrelationIdentifier, T> computeAliasToElementMap(@Nonnull final Iterable<? extends T> elements,
                                                                                      @Nonnull final Function<T, CorrelationIdentifier> elementToAliasFn) {
        return StreamSupport.stream(elements.spliterator(), false)
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
    public static <T> ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> computeDependsOnMap(@Nonnull Set<CorrelationIdentifier> aliases,
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
     * to type {@code T}. See the matching logic in {@link com.apple.foundationdb.record.query.plan.cascades.Quantifiers}
     * for examples.
     * @param aliases a set of aliases
     * @param aliasToElementMap a map from {@link CorrelationIdentifier} to {@code T}
     * @param dependsOnFn function defining the dependOn relationships between an element and aliases ({@link CorrelationIdentifier}s)
     * @param <T> element type
     * @return a multimap from {@link CorrelationIdentifier} to {@link CorrelationIdentifier} where a contained
     *         {@code key, values} pair signifies that {@code key} depends on each value in {@code values}
     */
    @Nonnull
    public static <T> ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> computeDependsOnMapWithAliases(@Nonnull Set<CorrelationIdentifier> aliases,
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
