/*
 * CrossProductTest.java
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

import com.apple.foundationdb.record.query.combinatorics.CrossProduct;
import com.apple.foundationdb.record.query.combinatorics.EnumeratingIterable;
import com.apple.foundationdb.record.query.combinatorics.EnumeratingIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link CrossProduct}.
 */
public class CrossProductTest {
    @Test
    public void testCrossProductRegular() {
        final CorrelationIdentifier a1 = CorrelationIdentifier.of("a1");
        final CorrelationIdentifier a2 = CorrelationIdentifier.of("a2");
        final CorrelationIdentifier a3 = CorrelationIdentifier.of("a3");

        final CorrelationIdentifier b1 = CorrelationIdentifier.of("b1");
        final CorrelationIdentifier b2 = CorrelationIdentifier.of("b2");

        final CorrelationIdentifier c1 = CorrelationIdentifier.of("c1");
        final CorrelationIdentifier c2 = CorrelationIdentifier.of("c2");
        final CorrelationIdentifier c3 = CorrelationIdentifier.of("c3");
        final CorrelationIdentifier c4 = CorrelationIdentifier.of("c4");

        final ImmutableSet<CorrelationIdentifier> as = ImmutableSet.of(a1, a2, a3);
        final ImmutableSet<CorrelationIdentifier> bs = ImmutableSet.of(b1, b2);
        final ImmutableSet<CorrelationIdentifier> cs = ImmutableSet.of(c1, c2, c3, c4);
        final EnumeratingIterable<CorrelationIdentifier> iterable =
                CrossProduct.crossProduct(ImmutableList.of(as, bs, cs));

        final ImmutableList.Builder<List<CorrelationIdentifier>> builder = ImmutableList.builder();
        for (final List<CorrelationIdentifier> next : iterable) {
            builder.add(next);
        }
        final ImmutableList<List<CorrelationIdentifier>> result = builder.build();

        assertEquals(as.size() * bs.size() * cs.size(), result.size());
    }

    @Test
    public void testCrossProductSkip() {
        final CorrelationIdentifier a1 = CorrelationIdentifier.of("a1");
        final CorrelationIdentifier a2 = CorrelationIdentifier.of("a2");
        final CorrelationIdentifier a3 = CorrelationIdentifier.of("a3");

        final CorrelationIdentifier b1 = CorrelationIdentifier.of("b1");
        final CorrelationIdentifier b2 = CorrelationIdentifier.of("b2");

        final CorrelationIdentifier c1 = CorrelationIdentifier.of("c1");
        final CorrelationIdentifier c2 = CorrelationIdentifier.of("c2");
        final CorrelationIdentifier c3 = CorrelationIdentifier.of("c3");
        final CorrelationIdentifier c4 = CorrelationIdentifier.of("c4");

        final ImmutableSet<CorrelationIdentifier> as = ImmutableSet.of(a1, a2, a3);
        final ImmutableSet<CorrelationIdentifier> bs = ImmutableSet.of(b1, b2);
        final ImmutableSet<CorrelationIdentifier> cs = ImmutableSet.of(c1, c2, c3, c4);
        final EnumeratingIterable<CorrelationIdentifier> iterable =
                CrossProduct.crossProduct(ImmutableList.of(as, bs, cs));

        final ImmutableList.Builder<List<CorrelationIdentifier>> builder = ImmutableList.builder();
        final EnumeratingIterator<CorrelationIdentifier> iterator = iterable.iterator();
        while (iterator.hasNext()) {
            final List<CorrelationIdentifier> next = iterator.next();
            builder.add(next);

            if (next.get(0).equals(a2) && next.get(1).equals(b2)) {
                iterator.skip(1);
            }
        }
        final ImmutableList<List<CorrelationIdentifier>> result = builder.build();

        assertEquals(21, result.size());

        assertEquals(8, result.stream()
                .filter(binding -> binding.contains(a1))
                .count());
        assertEquals(5, result.stream()
                .filter(binding -> binding.contains(a2))
                .count());
        assertEquals(8, result.stream()
                .filter(binding -> binding.contains(a3))
                .count());
        assertEquals(8, result.stream()
                .filter(binding -> binding.contains(a1))
                .count());
        assertEquals(5, result.stream()
                .filter(binding -> binding.contains(a2))
                .count());

        assertEquals(12, result.stream()
                .filter(binding -> binding.contains(b1))
                .count());
        assertEquals(9, result.stream()
                .filter(binding -> binding.contains(b2))
                .count());

        assertEquals(6, result.stream()
                .filter(binding -> binding.contains(c1))
                .count());
        assertEquals(5, result.stream()
                .filter(binding -> binding.contains(c2))
                .count());
        assertEquals(5, result.stream()
                .filter(binding -> binding.contains(c3))
                .count());
        assertEquals(5, result.stream()
                .filter(binding -> binding.contains(c4))
                .count());

        assertFalse(result.contains(ImmutableList.of(a2, b2, c2)));
        assertFalse(result.contains(ImmutableList.of(a2, b2, c3)));
        assertFalse(result.contains(ImmutableList.of(a2, b2, c4)));
    }

    @Test
    public void testCrossProductEmpty() {
        final EnumeratingIterable<CorrelationIdentifier> iterable =
                CrossProduct.crossProduct(ImmutableList.of());

        final EnumeratingIterator<CorrelationIdentifier> iterator =
                iterable.iterator();

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testCrossProductSingle() {
        final CorrelationIdentifier a1 = CorrelationIdentifier.of("a1");
        final CorrelationIdentifier a2 = CorrelationIdentifier.of("a2");
        final CorrelationIdentifier a3 = CorrelationIdentifier.of("a3");

        final ImmutableSet<CorrelationIdentifier> as = ImmutableSet.of(a1, a2, a3);
        final EnumeratingIterable<CorrelationIdentifier> iterable =
                CrossProduct.crossProduct(ImmutableList.of(as));
        final EnumeratingIterator<CorrelationIdentifier> iterator = iterable.iterator();
        final ImmutableList.Builder<List<CorrelationIdentifier>> builder = ImmutableList.builder();
        while (iterator.hasNext()) {
            final List<CorrelationIdentifier> next = iterator.next();
            builder.add(next);
            if (next.get(0).equals(a2)) {
                // should be a no op
                iterator.skip(0);
            }
        }
        final ImmutableList<List<CorrelationIdentifier>> result = builder.build();
        assertTrue(result.contains(ImmutableList.of(a1)));
        assertTrue(result.contains(ImmutableList.of(a2)));
        assertTrue(result.contains(ImmutableList.of(a3)));
    }

    @Test
    public void testCrossProductSkipEmptyError() {
        final EnumeratingIterable<CorrelationIdentifier> iterable =
                CrossProduct.crossProduct(ImmutableList.of());

        final EnumeratingIterator<CorrelationIdentifier> iterator =
                iterable.iterator();

        Assertions.assertThrows(UnsupportedOperationException.class, () -> iterator.skip(0));
    }

    @Test
    public void testCrossProductSkipSingleError() {
        final CorrelationIdentifier a1 = CorrelationIdentifier.of("a1");
        final CorrelationIdentifier a2 = CorrelationIdentifier.of("a2");
        final CorrelationIdentifier a3 = CorrelationIdentifier.of("a3");

        final ImmutableSet<CorrelationIdentifier> as = ImmutableSet.of(a1, a2, a3);
        final EnumeratingIterable<CorrelationIdentifier> iterable =
                CrossProduct.crossProduct(ImmutableList.of(as));
        final EnumeratingIterator<CorrelationIdentifier> iterator = iterable.iterator();
        Assertions.assertThrows(UnsupportedOperationException.class, () -> iterator.skip(0));
    }

    @Test
    public void testCrossProductSkipComplexError1() {
        final CorrelationIdentifier a1 = CorrelationIdentifier.of("a1");
        final CorrelationIdentifier a2 = CorrelationIdentifier.of("a2");

        final CorrelationIdentifier b1 = CorrelationIdentifier.of("b1");
        final CorrelationIdentifier b2 = CorrelationIdentifier.of("b2");

        final ImmutableSet<CorrelationIdentifier> as = ImmutableSet.of(a1, a2);
        final ImmutableSet<CorrelationIdentifier> bs = ImmutableSet.of(b1, b2);
        final EnumeratingIterable<CorrelationIdentifier> iterable =
                CrossProduct.crossProduct(ImmutableList.of(as, bs));

        final EnumeratingIterator<CorrelationIdentifier> iterator = iterable.iterator();

        Assertions.assertThrows(UnsupportedOperationException.class, () -> iterator.skip(0));
    }

    @Test
    public void testCrossProductSkipComplexError2() {
        final CorrelationIdentifier a1 = CorrelationIdentifier.of("a1");
        final CorrelationIdentifier a2 = CorrelationIdentifier.of("a2");

        final CorrelationIdentifier b1 = CorrelationIdentifier.of("b1");
        final CorrelationIdentifier b2 = CorrelationIdentifier.of("b2");

        final ImmutableSet<CorrelationIdentifier> as = ImmutableSet.of(a1, a2);
        final ImmutableSet<CorrelationIdentifier> bs = ImmutableSet.of(b1, b2);
        final EnumeratingIterable<CorrelationIdentifier> iterable =
                CrossProduct.crossProduct(ImmutableList.of(as, bs));

        final EnumeratingIterator<CorrelationIdentifier> iterator = iterable.iterator();
        iterator.next();

        Assertions.assertThrows(IndexOutOfBoundsException.class, () -> iterator.skip(2));
    }
}
