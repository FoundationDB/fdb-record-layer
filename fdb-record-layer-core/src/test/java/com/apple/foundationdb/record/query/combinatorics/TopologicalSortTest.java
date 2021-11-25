/*
 * TopologicalSortTest.java
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link TopologicalSort}.
 */
public class TopologicalSortTest {
    @Test
    public void testTopologicalSortImpossibleDependencies() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");
        final CorrelationIdentifier b = CorrelationIdentifier.of("b");
        final CorrelationIdentifier c = CorrelationIdentifier.of("c");

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependencies =
                ImmutableMap.of(b, ImmutableSet.of(a), c, ImmutableSet.of(b), a, ImmutableSet.of(c));

        final EnumeratingIterable<CorrelationIdentifier> topologicalPermutations =
                TopologicalSort.topologicalOrderPermutations(ImmutableSet.of(a, b, c),
                        id -> dependencies.getOrDefault(id, ImmutableSet.of()));

        final EnumeratingIterator<CorrelationIdentifier> iterator =
                topologicalPermutations.iterator();

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testTopologicalSortFullDependencies() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");
        final CorrelationIdentifier b = CorrelationIdentifier.of("b");
        final CorrelationIdentifier c = CorrelationIdentifier.of("c");

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependencies =
                ImmutableMap.of(b, ImmutableSet.of(a), c, ImmutableSet.of(b));

        final EnumeratingIterable<CorrelationIdentifier> topologicalPermutationIterable =
                TopologicalSort.topologicalOrderPermutations(ImmutableSet.of(a, b, c),
                        id -> dependencies.getOrDefault(id, ImmutableSet.of()));

        final ImmutableList<List<CorrelationIdentifier>> topologicalPermutations = ImmutableList.copyOf(topologicalPermutationIterable);

        assertEquals(1, topologicalPermutations.size());

        assertEquals(ImmutableList.of(a, b, c), topologicalPermutations.get(0));
    }

    @Test
    public void testTopologicalSortNoDependencies() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");
        final CorrelationIdentifier b = CorrelationIdentifier.of("b");
        final CorrelationIdentifier c = CorrelationIdentifier.of("c");

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependencies =
                ImmutableMap.of();

        final EnumeratingIterable<CorrelationIdentifier> topologicalPermutationIterable =
                TopologicalSort.topologicalOrderPermutations(ImmutableSet.of(a, b, c),
                        id -> dependencies.getOrDefault(id, ImmutableSet.of()));

        final ImmutableList<List<CorrelationIdentifier>> topologicalPermutations = ImmutableList.copyOf(topologicalPermutationIterable);

        assertEquals(6, topologicalPermutations.size());

        assertTrue(topologicalPermutations.contains(ImmutableList.of(a, b, c)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(a, c, b)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(b, a, c)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(b, c, a)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(c, a, b)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(c, b, a)));
    }

    @Test
    public void testTopologicalSortSomeDependencies() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");
        final CorrelationIdentifier b = CorrelationIdentifier.of("b");
        final CorrelationIdentifier c = CorrelationIdentifier.of("c");

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependencies =
                ImmutableMap.of(c, ImmutableSet.of(a));

        final EnumeratingIterable<CorrelationIdentifier> topologicalPermutationIterable =
                TopologicalSort.topologicalOrderPermutations(ImmutableSet.of(a, b, c),
                        id -> dependencies.getOrDefault(id, ImmutableSet.of()));

        final ImmutableList<List<CorrelationIdentifier>> topologicalPermutations = ImmutableList.copyOf(topologicalPermutationIterable);

        assertEquals(3, topologicalPermutations.size());

        assertTrue(topologicalPermutations.contains(ImmutableList.of(a, b, c)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(a, c, b)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(b, a, c)));
    }

    @Test
    public void testTopologicalSortSkip() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");
        final CorrelationIdentifier b = CorrelationIdentifier.of("b");
        final CorrelationIdentifier c = CorrelationIdentifier.of("c");

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependencies =
                ImmutableMap.of();

        final EnumeratingIterable<CorrelationIdentifier> topologicalPermutationIterable =
                TopologicalSort.topologicalOrderPermutations(ImmutableSet.of(a, b, c),
                        id -> dependencies.getOrDefault(id, ImmutableSet.of()));

        final EnumeratingIterator<CorrelationIdentifier> iterator = topologicalPermutationIterable.iterator();
        final ImmutableList.Builder<List<CorrelationIdentifier>> builder = ImmutableList.builder();

        while (iterator.hasNext()) {
            final List<CorrelationIdentifier> next = iterator.next();

            builder.add(next);

            if (next.get(0).equals(b) && next.get(1).equals(a)) {
                iterator.skip(0);
            }
        }

        final ImmutableList<List<CorrelationIdentifier>> topologicalPermutations = builder.build();

        assertEquals(5, topologicalPermutations.size());

        assertTrue(topologicalPermutations.contains(ImmutableList.of(a, b, c)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(a, c, b)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(b, a, c)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(c, a, b)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(c, b, a)));
    }

    @Test
    public void testTopologicalSortSkip2() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");
        final CorrelationIdentifier b = CorrelationIdentifier.of("b");
        final CorrelationIdentifier c = CorrelationIdentifier.of("c");
        final CorrelationIdentifier d = CorrelationIdentifier.of("d");

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependencies =
                ImmutableMap.of();

        final EnumeratingIterable<CorrelationIdentifier> topologicalPermutationIterable =
                TopologicalSort.topologicalOrderPermutations(ImmutableSet.of(a, b, c, d),
                        id -> dependencies.getOrDefault(id, ImmutableSet.of()));

        final EnumeratingIterator<CorrelationIdentifier> iterator = topologicalPermutationIterable.iterator();
        final ImmutableList.Builder<List<CorrelationIdentifier>> builder = ImmutableList.builder();

        while (iterator.hasNext()) {
            final List<CorrelationIdentifier> next = iterator.next();

            builder.add(next);

            if (next.get(0).equals(b) && next.get(1).equals(a)) {
                iterator.skip(1);
            }
        }

        final ImmutableList<List<CorrelationIdentifier>> topologicalPermutations = builder.build();

        assertEquals(23, topologicalPermutations.size());

        assertTrue(topologicalPermutations.contains(ImmutableList.of(a, b, c, d)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(a, b, d, c)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(a, c, b, d)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(a, c, d, b)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(a, d, b, c)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(a, d, c, b)));

        assertTrue(topologicalPermutations.contains(ImmutableList.of(b, a, c, d)));
        assertFalse(topologicalPermutations.contains(ImmutableList.of(b, a, d, c)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(b, c, a, d)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(b, c, d, a)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(b, d, a, c)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(b, d, c, a)));

        assertTrue(topologicalPermutations.contains(ImmutableList.of(c, a, b, d)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(c, a, d, b)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(c, b, a, d)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(c, b, d, a)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(c, d, a, b)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(c, d, b, a)));

        assertTrue(topologicalPermutations.contains(ImmutableList.of(d, a, b, c)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(d, a, c, b)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(d, b, a, c)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(d, b, c, a)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(d, c, a, b)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(d, c, b, a)));
    }

    @Test
    public void testTopologicalSortSkip3() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");
        final CorrelationIdentifier b = CorrelationIdentifier.of("b");
        final CorrelationIdentifier c = CorrelationIdentifier.of("c");
        final CorrelationIdentifier d = CorrelationIdentifier.of("d");

        final Map<CorrelationIdentifier, Set<CorrelationIdentifier>> dependencies =
                ImmutableMap.of();

        final EnumeratingIterable<CorrelationIdentifier> topologicalPermutationIterable =
                TopologicalSort.topologicalOrderPermutations(ImmutableSet.of(a, b, c, d),
                        id -> dependencies.getOrDefault(id, ImmutableSet.of()));

        final EnumeratingIterator<CorrelationIdentifier> iterator = topologicalPermutationIterable.iterator();
        final ImmutableList.Builder<List<CorrelationIdentifier>> builder = ImmutableList.builder();

        while (iterator.hasNext()) {
            final List<CorrelationIdentifier> next = iterator.next();

            builder.add(next);

            if (next.get(0).equals(b) && next.get(1).equals(a)) {
                iterator.skip(0);
            }
        }

        final ImmutableList<List<CorrelationIdentifier>> topologicalPermutations = builder.build();

        assertEquals(19, topologicalPermutations.size());

        assertTrue(topologicalPermutations.contains(ImmutableList.of(a, b, c, d)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(a, b, d, c)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(a, c, b, d)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(a, c, d, b)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(a, d, b, c)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(a, d, c, b)));

        assertTrue(topologicalPermutations.contains(ImmutableList.of(b, a, c, d)));
        assertFalse(topologicalPermutations.contains(ImmutableList.of(b, a, d, c)));
        assertFalse(topologicalPermutations.contains(ImmutableList.of(b, c, a, d)));
        assertFalse(topologicalPermutations.contains(ImmutableList.of(b, c, d, a)));
        assertFalse(topologicalPermutations.contains(ImmutableList.of(b, d, a, c)));
        assertFalse(topologicalPermutations.contains(ImmutableList.of(b, d, c, a)));

        assertTrue(topologicalPermutations.contains(ImmutableList.of(c, a, b, d)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(c, a, d, b)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(c, b, a, d)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(c, b, d, a)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(c, d, a, b)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(c, d, b, a)));

        assertTrue(topologicalPermutations.contains(ImmutableList.of(d, a, b, c)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(d, a, c, b)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(d, b, a, c)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(d, b, c, a)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(d, c, a, b)));
        assertTrue(topologicalPermutations.contains(ImmutableList.of(d, c, b, a)));
    }

    @Test
    public void testTopologicalSortEmpty() {
        final EnumeratingIterable<CorrelationIdentifier> topologicalPermutations =
                TopologicalSort.topologicalOrderPermutations(ImmutableSet.of(), id -> ImmutableSet.of());

        final EnumeratingIterator<CorrelationIdentifier> iterator =
                topologicalPermutations.iterator();

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testTopologicalSortSingle() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");
        final EnumeratingIterable<CorrelationIdentifier> topologicalPermutationIterable =
                TopologicalSort.topologicalOrderPermutations(ImmutableSet.of(a), id -> ImmutableSet.of());

        final ImmutableList<List<CorrelationIdentifier>> topologicalPermutations = ImmutableList.copyOf(topologicalPermutationIterable);

        assertEquals(1, topologicalPermutations.size());
    }

    @Test
    public void testTopologicalSortSkipEmptyError() {
        final EnumeratingIterable<CorrelationIdentifier> topologicalPermutations =
                TopologicalSort.topologicalOrderPermutations(ImmutableSet.of(), id -> ImmutableSet.of());

        final EnumeratingIterator<CorrelationIdentifier> iterator =
                topologicalPermutations.iterator();
        Assertions.assertThrows(UnsupportedOperationException.class, () -> iterator.skip(0));
    }

    @Test
    public void testTopologicalSortSkipSingleError() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");

        final EnumeratingIterable<CorrelationIdentifier> topologicalPermutations =
                TopologicalSort.topologicalOrderPermutations(ImmutableSet.of(a), id -> ImmutableSet.of());

        final EnumeratingIterator<CorrelationIdentifier> iterator =
                topologicalPermutations.iterator();
        Assertions.assertThrows(UnsupportedOperationException.class, () -> iterator.skip(0));
    }

    @Test
    public void testTopologicalSortSkipComplexError1() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");
        final CorrelationIdentifier b = CorrelationIdentifier.of("b");

        final EnumeratingIterable<CorrelationIdentifier> topologicalPermutations =
                TopologicalSort.topologicalOrderPermutations(ImmutableSet.of(a, b), id -> ImmutableSet.of());

        final EnumeratingIterator<CorrelationIdentifier> iterator =
                topologicalPermutations.iterator();
        Assertions.assertThrows(UnsupportedOperationException.class, () -> iterator.skip(0));
    }

    @Test
    public void testTopologicalSortSkipComplexError2() {
        final CorrelationIdentifier a = CorrelationIdentifier.of("a");
        final CorrelationIdentifier b = CorrelationIdentifier.of("b");

        final EnumeratingIterable<CorrelationIdentifier> topologicalPermutations =
                TopologicalSort.topologicalOrderPermutations(ImmutableSet.of(a, b), id -> ImmutableSet.of());

        final EnumeratingIterator<CorrelationIdentifier> iterator =
                topologicalPermutations.iterator();
        iterator.next();
        Assertions.assertThrows(IndexOutOfBoundsException.class, () -> iterator.skip(2));
    }
}
