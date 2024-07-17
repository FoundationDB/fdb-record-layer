/*
 * TransitiveClosureTest.java
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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import org.junit.jupiter.api.Test;

import static com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier.of;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link TransitiveClosure}.
 */
class TransitiveClosureTest {
    @Test
    void testChainedDependencies() {
        final ImmutableSet<CorrelationIdentifier> set = ImmutableSet.of(of("a"), of("b"), of("c"), of("d"), of("e"));
        final ImmutableSetMultimap.Builder<CorrelationIdentifier, CorrelationIdentifier> builder =
                ImmutableSetMultimap.builder();

        builder.putAll(of("b"), of("a"));
        builder.putAll(of("c"), of("b"));
        builder.putAll(of("d"), of("c"));
        builder.putAll(of("e"), of("d"));
        final ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> dependsOnMap = builder.build();

        final SetMultimap<CorrelationIdentifier, CorrelationIdentifier> transitiveClosure =
                TransitiveClosure.transitiveClosure(set, dependsOnMap);

        assertEquals(
                ImmutableSetMultimap.<CorrelationIdentifier, CorrelationIdentifier>builder()
                        .putAll(of("b"), of("a"))
                        .putAll(of("c"), of("a"), of("b"))
                        .putAll(of("d"), of("a"), of("b"), of("c"))
                        .putAll(of("e"), of("a"), of("b"), of("c"), of("d"))
                        .build(),
                transitiveClosure);
    }

    @Test
    void testNoDependencies() {
        final ImmutableSet<CorrelationIdentifier> set = ImmutableSet.of(of("a"), of("b"), of("c"), of("d"), of("e"));
        final ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> dependsOnMap = ImmutableSetMultimap.of();

        final SetMultimap<CorrelationIdentifier, CorrelationIdentifier> transitiveClosure =
                TransitiveClosure.transitiveClosure(set, dependsOnMap);

        assertEquals(
                ImmutableSetMultimap.of(),
                transitiveClosure);
    }

    @Test
    void testSomeDependencies1() {
        final ImmutableSet<CorrelationIdentifier> set = ImmutableSet.of(of("a"), of("b"), of("c"), of("d"), of("e"));
        final ImmutableSetMultimap.Builder<CorrelationIdentifier, CorrelationIdentifier> builder =
                ImmutableSetMultimap.builder();

        builder.putAll(of("b"), of("a"));
        builder.putAll(of("c"), of("b"));
        builder.putAll(of("d"), of("b"));
        builder.putAll(of("e"), of("a"));
        final ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> dependsOnMap = builder.build();

        final SetMultimap<CorrelationIdentifier, CorrelationIdentifier> transitiveClosure =
                TransitiveClosure.transitiveClosure(set, dependsOnMap);

        assertEquals(
                ImmutableSetMultimap.<CorrelationIdentifier, CorrelationIdentifier>builder()
                        .putAll(of("b"), of("a"))
                        .putAll(of("c"), of("a"), of("b"))
                        .putAll(of("d"), of("a"), of("b"))
                        .putAll(of("e"), of("a"))
                        .build(),
                transitiveClosure);
    }

    @Test
    void testSomeDependencies2() {
        final ImmutableSet<CorrelationIdentifier> set = ImmutableSet.of(of("a"), of("b"), of("c"), of("d"), of("e"));
        final ImmutableSetMultimap.Builder<CorrelationIdentifier, CorrelationIdentifier> builder =
                ImmutableSetMultimap.builder();

        builder.putAll(of("b"), of("a"));
        builder.putAll(of("c"), of("b"));
        builder.putAll(of("e"), of("d"));
        final ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> dependsOnMap = builder.build();

        final SetMultimap<CorrelationIdentifier, CorrelationIdentifier> transitiveClosure =
                TransitiveClosure.transitiveClosure(set, dependsOnMap);

        assertEquals(
                ImmutableSetMultimap.<CorrelationIdentifier, CorrelationIdentifier>builder()
                        .putAll(of("b"), of("a"))
                        .putAll(of("c"), of("a"), of("b"))
                        .putAll(of("e"), of("d"))
                        .build(),
                transitiveClosure);
    }

    @Test
    void testSomeDependencies3() {
        final ImmutableSet<CorrelationIdentifier> set = ImmutableSet.of(of("a"), of("b"), of("c"), of("d"), of("e"));
        final ImmutableSetMultimap.Builder<CorrelationIdentifier, CorrelationIdentifier> builder =
                ImmutableSetMultimap.builder();

        builder.putAll(of("b"), of("a"));
        builder.putAll(of("d"), of("c"));
        final ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> dependsOnMap = builder.build();

        final SetMultimap<CorrelationIdentifier, CorrelationIdentifier> transitiveClosure =
                TransitiveClosure.transitiveClosure(set, dependsOnMap);

        assertEquals(
                ImmutableSetMultimap.<CorrelationIdentifier, CorrelationIdentifier>builder()
                        .putAll(of("b"), of("a"))
                        .putAll(of("d"), of("c"))
                        .build(),
                transitiveClosure);
    }

    @Test
    void testSomeDependencies4() {
        final ImmutableSet<CorrelationIdentifier> set = ImmutableSet.of(of("a"), of("b"), of("c"), of("d"), of("e"));
        final ImmutableSetMultimap.Builder<CorrelationIdentifier, CorrelationIdentifier> builder =
                ImmutableSetMultimap.builder();

        builder.putAll(of("b"), of("a"));
        builder.putAll(of("c"), of("b"));
        builder.putAll(of("d"), of("b"));
        builder.putAll(of("e"), of("a"));
        builder.putAll(of("b"), of("e"));
        final ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> dependsOnMap = builder.build();

        final SetMultimap<CorrelationIdentifier, CorrelationIdentifier> transitiveClosure =
                TransitiveClosure.transitiveClosure(set, dependsOnMap);

        assertEquals(
                ImmutableSetMultimap.<CorrelationIdentifier, CorrelationIdentifier>builder()
                        .putAll(of("b"), of("a"), of("e"))
                        .putAll(of("c"), of("a"), of("e"), of("b"))
                        .putAll(of("d"), of("a"), of("e"), of("b"))
                        .putAll(of("e"), of("a"))
                        .build(),
                transitiveClosure);
    }

    @Test
    void testCircularDependencies1() {
        final ImmutableSet<CorrelationIdentifier> set = ImmutableSet.of(of("a"), of("b"), of("c"), of("d"), of("e"));
        final ImmutableSetMultimap.Builder<CorrelationIdentifier, CorrelationIdentifier> builder =
                ImmutableSetMultimap.builder();

        builder.putAll(of("b"), of("a"));
        builder.putAll(of("c"), of("b"));
        builder.putAll(of("d"), of("b"));
        builder.putAll(of("e"), of("a"));
        builder.putAll(of("b"), of("e"));
        builder.putAll(of("c"), of("d"));
        builder.putAll(of("d"), of("c"));
        final ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> dependsOnMap = builder.build();

        assertThrows(IllegalArgumentException.class, () -> TransitiveClosure.transitiveClosure(set, dependsOnMap));
    }

    @Test
    void testCircularDependencies2() {
        final ImmutableSet<CorrelationIdentifier> set = ImmutableSet.of(of("a"), of("b"), of("c"), of("d"), of("e"));
        final ImmutableSetMultimap.Builder<CorrelationIdentifier, CorrelationIdentifier> builder =
                ImmutableSetMultimap.builder();

        builder.putAll(of("b"), of("a"));
        builder.putAll(of("c"), of("b"));
        builder.putAll(of("d"), of("b"));
        builder.putAll(of("e"), of("a"));
        builder.putAll(of("b"), of("e"));
        builder.putAll(of("a"), of("d"));
        final ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> dependsOnMap = builder.build();

        assertThrows(IllegalArgumentException.class, () -> TransitiveClosure.transitiveClosure(set, dependsOnMap));
    }

    @Test
    void testDoublyUsedDependencies() {
        final ImmutableSet<CorrelationIdentifier> set = ImmutableSet.of(of("c11"), of("c53"), of("c69"), of("c88"));
        final ImmutableSetMultimap.Builder<CorrelationIdentifier, CorrelationIdentifier> builder =
                ImmutableSetMultimap.builder();

        builder.putAll(of("c11"), of("c88"), of("c53"));
        builder.putAll(of("c88"), of("c69"), of("c53"));
        final ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> dependsOnMap = builder.build();

        final SetMultimap<CorrelationIdentifier, CorrelationIdentifier> transitiveClosure =
                TransitiveClosure.transitiveClosure(set, dependsOnMap);

        assertEquals(
                ImmutableSetMultimap.<CorrelationIdentifier, CorrelationIdentifier>builder()
                        .putAll(of("c88"), of("c69"), of("c53"))
                        .putAll(of("c11"), of("c69"), of("c53"), of("c88"))
                        .build(),
                transitiveClosure);
    }

    @Test
    void testPartialDependencies() {
        final ImmutableSet<CorrelationIdentifier> set = ImmutableSet.of(of("a"), of("b"));
        final ImmutableSetMultimap.Builder<CorrelationIdentifier, CorrelationIdentifier> builder =
                ImmutableSetMultimap.builder();

        builder.putAll(of("b"), of("b"));
        final ImmutableSetMultimap<CorrelationIdentifier, CorrelationIdentifier> dependsOnMap = builder.build();

        assertThrows(IllegalArgumentException.class, () -> TransitiveClosure.transitiveClosure(set, dependsOnMap));
    }
}
