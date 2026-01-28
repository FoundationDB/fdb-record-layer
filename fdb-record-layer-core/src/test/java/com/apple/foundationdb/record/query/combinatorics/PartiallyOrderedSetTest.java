/*
 * PartiallyOrderedSetTest.java
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

import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link PartiallyOrderedSet}.
 */
class PartiallyOrderedSetTest {
    @Test
    void testEligibleSetsImpossibleDependencies() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");
        final CorrelationIdentifier b = CorrelationIdentifier.of("b");
        final CorrelationIdentifier c = CorrelationIdentifier.of("c");

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependencies =
                ImmutableMap.of(b, ImmutableSet.of(a), c, ImmutableSet.of(b), a, ImmutableSet.of(c));

        final var partialOrder =
                PartiallyOrderedSet.of(ImmutableSet.of(a, b, c),
                        id -> dependencies.getOrDefault(id, ImmutableSet.of()));

        var eligibleSet = partialOrder.eligibleSet();
        assertTrue(eligibleSet.eligibleElements().isEmpty());
    }

    @Test
    void testEligibleSetsFullDependencies() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");
        final CorrelationIdentifier b = CorrelationIdentifier.of("b");
        final CorrelationIdentifier c = CorrelationIdentifier.of("c");

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependencies =
                ImmutableMap.of(b, ImmutableSet.of(a), c, ImmutableSet.of(b));

        final var partialOrder =
                PartiallyOrderedSet.of(ImmutableSet.of(a, b, c),
                        id -> dependencies.getOrDefault(id, ImmutableSet.of()));

        var eligibleSet = partialOrder.eligibleSet();
        assertEquals(ImmutableSet.of(a), eligibleSet.eligibleElements());
        eligibleSet = eligibleSet.removeEligibleElements(eligibleSet.eligibleElements());
        assertEquals(ImmutableSet.of(b), eligibleSet.eligibleElements());
        eligibleSet = eligibleSet.removeEligibleElements(eligibleSet.eligibleElements());
        assertEquals(ImmutableSet.of(c), eligibleSet.eligibleElements());
        eligibleSet = eligibleSet.removeEligibleElements(eligibleSet.eligibleElements());
        assertTrue(eligibleSet.eligibleElements().isEmpty());
    }

    @Test
    void testEligibleSetsNoDependencies() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");
        final CorrelationIdentifier b = CorrelationIdentifier.of("b");
        final CorrelationIdentifier c = CorrelationIdentifier.of("c");

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependencies = ImmutableMap.of();

        final var partialOrder =
                PartiallyOrderedSet.of(ImmutableSet.of(a, b, c),
                        id -> dependencies.getOrDefault(id, ImmutableSet.of()));

        var eligibleSet = partialOrder.eligibleSet();
        assertEquals(ImmutableSet.of(a, b, c), eligibleSet.eligibleElements());
        eligibleSet = eligibleSet.removeEligibleElements(eligibleSet.eligibleElements());
        assertTrue(eligibleSet.eligibleElements().isEmpty());
    }

    @Test
    void testEligibleSetsSomeDependencies() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");
        final CorrelationIdentifier b = CorrelationIdentifier.of("b");
        final CorrelationIdentifier c = CorrelationIdentifier.of("c");

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependencies =
                ImmutableMap.of(c, ImmutableSet.of(a));

        final var partialOrder =
                PartiallyOrderedSet.of(ImmutableSet.of(a, b, c),
                        id -> dependencies.getOrDefault(id, ImmutableSet.of()));

        var eligibleSet = partialOrder.eligibleSet();
        assertEquals(ImmutableSet.of(a, b), eligibleSet.eligibleElements());
        eligibleSet = eligibleSet.removeEligibleElements(eligibleSet.eligibleElements());
        assertEquals(ImmutableSet.of(c), eligibleSet.eligibleElements());
        eligibleSet = eligibleSet.removeEligibleElements(eligibleSet.eligibleElements());
        assertTrue(eligibleSet.eligibleElements().isEmpty());
    }

    @Test
    void testEligibleSetsSomeDependencies2() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");
        final CorrelationIdentifier b = CorrelationIdentifier.of("b");
        final CorrelationIdentifier c = CorrelationIdentifier.of("c");
        final CorrelationIdentifier d = CorrelationIdentifier.of("d");

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependencies =
                ImmutableMap.of(b, ImmutableSet.of(a), c, ImmutableSet.of(a), d, ImmutableSet.of(b, c));

        final var partialOrder =
                PartiallyOrderedSet.of(ImmutableSet.of(a, b, c, d),
                        id -> dependencies.getOrDefault(id, ImmutableSet.of()));

        var eligibleSet = partialOrder.eligibleSet();
        assertEquals(ImmutableSet.of(a), eligibleSet.eligibleElements());
        eligibleSet = eligibleSet.removeEligibleElements(eligibleSet.eligibleElements());
        assertEquals(ImmutableSet.of(b, c), eligibleSet.eligibleElements());
        eligibleSet = eligibleSet.removeEligibleElements(eligibleSet.eligibleElements());
        assertEquals(ImmutableSet.of(d), eligibleSet.eligibleElements());
        eligibleSet = eligibleSet.removeEligibleElements(eligibleSet.eligibleElements());
        assertTrue(eligibleSet.eligibleElements().isEmpty());
    }

    @Test
    void testEligibleSetsEmpty() {
        final var partialOrder =
                PartiallyOrderedSet.of(ImmutableSet.of(),
                        id -> ImmutableSet.of());

        var eligibleSet = partialOrder.eligibleSet();
        assertTrue(eligibleSet.eligibleElements().isEmpty());
    }

    @Test
    void testEligibleSetsSingle() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");

        final var partialOrder =
                PartiallyOrderedSet.of(ImmutableSet.of(a),
                        id -> ImmutableSet.of());
        var eligibleSet = partialOrder.eligibleSet();
        assertEquals(ImmutableSet.of(a), eligibleSet.eligibleElements());
        eligibleSet = eligibleSet.removeEligibleElements(eligibleSet.eligibleElements());
        assertTrue(eligibleSet.eligibleElements().isEmpty());
    }

    @Test
    void testFilterElements() {
        final var a = CorrelationIdentifier.of("a");
        final var b = CorrelationIdentifier.of("b");
        final var c = CorrelationIdentifier.of("c");
        final var d = CorrelationIdentifier.of("d");
        final var e = CorrelationIdentifier.of("e");
        final var f = CorrelationIdentifier.of("f");
        final var g = CorrelationIdentifier.of("g");

        // a < c, b < c, c < d, d < e, d < f, e < g, f < g
        final var dependencyMapBuilder = ImmutableSetMultimap.<CorrelationIdentifier, CorrelationIdentifier>builder();
        dependencyMapBuilder.putAll(c, b, a);
        dependencyMapBuilder.putAll(d, c);
        dependencyMapBuilder.putAll(e, d);
        dependencyMapBuilder.putAll(f, d);
        dependencyMapBuilder.putAll(g, e, f);
        final var partialOrder = PartiallyOrderedSet.of(ImmutableSet.of(a, b, c, d, e, f, g), dependencyMapBuilder.build());

        // a < c, b < c
        var filteredSet = ImmutableSet.of(a, b, c);
        var actualSubset = partialOrder.filterElements(filteredSet::contains);
        var expectedDependencyEntries = ImmutableSetMultimap.of(c, a, c, b);
        actualSubset.getDependencyMap().entries().forEach(entry -> assertTrue(expectedDependencyEntries.get(entry.getKey()).contains(entry.getValue())));

        // {a}
        filteredSet = ImmutableSet.of(a);
        actualSubset = partialOrder.filterElements(filteredSet::contains);
        assertTrue(actualSubset.getDependencyMap().isEmpty());
        assertTrue(actualSubset.getSet().containsAll(filteredSet));

        filteredSet = ImmutableSet.of(c, d, e, f, g);
        actualSubset = partialOrder.filterElements(filteredSet::contains);
        assertTrue(actualSubset.getDependencyMap().isEmpty());
        assertTrue(actualSubset.getSet().isEmpty());

        filteredSet = ImmutableSet.of(a, e, g);
        actualSubset = partialOrder.filterElements(filteredSet::contains);
        assertTrue(actualSubset.getDependencyMap().isEmpty());
        assertTrue(actualSubset.getSet().containsAll(ImmutableSet.of(a)));
    }

    @Test
    void testMapAllWithMultiMap() {
        final var a = CorrelationIdentifier.of("a");
        final var b = CorrelationIdentifier.of("b");
        final var c = CorrelationIdentifier.of("c");
        final var d = CorrelationIdentifier.of("d");

        // a < b, a < c, c < d, b < d
        final var dependencyMapBuilder = ImmutableSetMultimap.<CorrelationIdentifier, CorrelationIdentifier>builder();
        dependencyMapBuilder.putAll(c, a);
        dependencyMapBuilder.putAll(b, a);
        dependencyMapBuilder.putAll(d, b, c);
        final var partialOrder = PartiallyOrderedSet.of(ImmutableSet.of(a, b, c, d), dependencyMapBuilder.build());

        var mapToBuilder = ImmutableSetMultimap.<CorrelationIdentifier, CorrelationIdentifier>builder();
        final var a_1 = CorrelationIdentifier.of("a_1");
        final var a_2 = CorrelationIdentifier.of("a_2");
        final var b_1 = CorrelationIdentifier.of("b_1");
        final var d_1 = CorrelationIdentifier.of("d_1");
        final var d_2 = CorrelationIdentifier.of("d_2");
        mapToBuilder.putAll(a, a_1, a_2);
        mapToBuilder.putAll(b, b_1);
        mapToBuilder.putAll(d, d_1, d_2);

        var actualSubset = partialOrder.mapAll(mapToBuilder.build());
        var expectedDependencyEntries = ImmutableSetMultimap.of(b_1, a_1, b_1, a_2, d_1, b_1, d_2, b_1);
        actualSubset.getDependencyMap().entries().forEach(entry -> assertTrue(expectedDependencyEntries.get(entry.getKey()).contains(entry.getValue())));
    }
}
