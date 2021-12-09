/*
 * TransitiveClosure.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.combinatorics;

import com.apple.foundationdb.annotation.API;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Utility class to provide helpers related to the computation of the transitive closure of a partial order.
 */
@API(API.Status.EXPERIMENTAL)
public class TransitiveClosure {

    private TransitiveClosure() {
        // prevent instantiation
    }

    /**
     * Compute the transitive closure of the depends-on map that is passed in.
     * @param set the set determining the universe of entities
     * @param dependsOnMap depends-on map
     * @param <T> type
     * @return the transitive closure
     */
    public static <T> ImmutableSetMultimap<T, T> transitiveClosure(@Nonnull Set<T> set, @Nonnull final ImmutableSetMultimap<T, T> dependsOnMap) {
        return transitiveClosure(new PartialOrder<>(set, dependsOnMap));
    }


    /**
     * Compute the transitive closure of the depends-on map that is passed in.
     * @param partialOrder partial order to compute the transitive closure for
     * @param <T> type
     * @return the transitive closure of the partial order handed in
     */
    public static <T> ImmutableSetMultimap<T, T> transitiveClosure(@Nonnull PartialOrder<T> partialOrder) {
        final var set = partialOrder.getSet();
        final var dependsOnMap = partialOrder.getDependencyMap();
        final ImmutableSetMultimap<T, T> usedByMap = dependsOnMap.inverse();
        final Map<T, Integer> inDegreeMap = computeInDegreeMap(set, usedByMap);
        final Set<T> processed = Sets.newHashSetWithExpectedSize(partialOrder.size());
        final Deque<T> deque = new ArrayDeque<>(partialOrder.size());
        for (final T current : set) {
            if (inDegreeMap.get(current) == 0) {
                deque.add(current);
            }
        }

        final SetMultimap<T, T> resultMap = HashMultimap.create();
        while (!deque.isEmpty()) {
            final T current = deque.pop();
            processed.add(current);
            final ImmutableSet<T> usingEntities = usedByMap.get(current);
            for (final T using : usingEntities) {
                final Integer newInDegree =
                        inDegreeMap.compute(using,
                                (uE, inDegree) -> {
                                    Objects.requireNonNull(inDegree);
                                    Verify.verify(inDegree > 0);
                                    return inDegree - 1;
                                });
                if (newInDegree == 0) {
                    deque.add(using);
                    final ImmutableSet<T> dependsOnSet = dependsOnMap.get(using);
                    for (final T dependsOn : dependsOnSet) {
                        Verify.verify(resultMap.put(using, dependsOn));
                        resultMap.get(dependsOn).forEach(ancestor -> resultMap.put(using, ancestor));
                    }
                }
            }
        }

        Preconditions.checkArgument(processed.size() == partialOrder.size(), "circular dependency");

        return ImmutableSetMultimap.copyOf(resultMap);
    }

    @Nonnull
    private static <T> Map<T, Integer> computeInDegreeMap(@Nonnull final Set<T> set, @Nonnull final SetMultimap<T, T> usedByMap) {
        final HashMap<T, Integer> result = Maps.newHashMapWithExpectedSize(set.size());
        set.forEach(element -> result.put(element, 0));

        for (final Map.Entry<T, T> entry : usedByMap.entries()) {
            result.compute(entry.getValue(), (t, v) -> Objects.requireNonNull(v) + 1);
        }
        return result;
    }
}
