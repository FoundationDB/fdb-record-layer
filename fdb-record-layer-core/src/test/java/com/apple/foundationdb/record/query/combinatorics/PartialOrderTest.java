/*
 * PartialOrderTest.java
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

import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link PartialOrder}.
 */
class PartialOrderTest {
    @Test
    void testEligibleSetsImpossibleDependencies() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");
        final CorrelationIdentifier b = CorrelationIdentifier.of("b");
        final CorrelationIdentifier c = CorrelationIdentifier.of("c");

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependencies =
                ImmutableMap.of(b, ImmutableSet.of(a), c, ImmutableSet.of(b), a, ImmutableSet.of(c));

        final var partialOrder =
                PartialOrder.of(ImmutableSet.of(a, b, c),
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
                PartialOrder.of(ImmutableSet.of(a, b, c),
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
                PartialOrder.of(ImmutableSet.of(a, b, c),
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
                PartialOrder.of(ImmutableSet.of(a, b, c),
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
                PartialOrder.of(ImmutableSet.of(a, b, c, d),
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
                PartialOrder.of(ImmutableSet.of(),
                        id -> ImmutableSet.of());

        var eligibleSet = partialOrder.eligibleSet();
        assertTrue(eligibleSet.eligibleElements().isEmpty());
    }

    @Test
    void testEligibleSetsSingle() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");

        final var partialOrder =
                PartialOrder.of(ImmutableSet.of(a),
                        id -> ImmutableSet.of());
        var eligibleSet = partialOrder.eligibleSet();
        assertEquals(ImmutableSet.of(a), eligibleSet.eligibleElements());
        eligibleSet = eligibleSet.removeEligibleElements(eligibleSet.eligibleElements());
        assertTrue(eligibleSet.eligibleElements().isEmpty());
    }
}
