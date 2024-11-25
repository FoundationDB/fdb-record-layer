/*
 * AliasMapTest.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.query.combinatorics.ChooseK;
import com.apple.foundationdb.record.query.combinatorics.EnumeratingIterable;
import com.apple.foundationdb.record.query.combinatorics.EnumeratingIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Testcase class for {@link ChooseK}.
 *
 * <p>
 * Note: the order of elements returned by {@link ChooseK} can be important for plan stability and for skips.
 * For that reason, these tests assert on the order that the combinations are returned.
 * </p>
 */
public class ChooseKTest {
    @Test
    void testChooseK1() {
        final Set<String> elements = ImmutableSet.of("a", "b", "c", "d");

        final EnumeratingIterable<String> combinationsIterable = ChooseK.chooseK(elements, 3);

        final ImmutableList<ImmutableSet<String>> combinations =
                StreamSupport.stream(combinationsIterable.spliterator(), false)
                        .map(ImmutableSet::copyOf)
                        .collect(ImmutableList.toImmutableList());

        assertEquals(ImmutableList.of(
                ImmutableSet.of("a", "b", "c"),
                ImmutableSet.of("a", "b", "d"),
                ImmutableSet.of("a", "c", "d"),
                ImmutableSet.of("b", "c", "d")),
                combinations);
    }
    
    @Test
    void testChooseK2() {
        final Set<String> elements = ImmutableSet.of("a", "b", "c", "d", "e");

        // 0
        EnumeratingIterable<String> combinationsIterable = ChooseK.chooseK(elements, 0);
        ImmutableList<ImmutableSet<String>> combinations =
                StreamSupport.stream(combinationsIterable.spliterator(), false)
                        .map(ImmutableSet::copyOf)
                        .collect(ImmutableList.toImmutableList());

        assertEquals(ImmutableList.of(ImmutableSet.of()),
                combinations);

        // 1
        combinationsIterable = ChooseK.chooseK(elements, 1);
        combinations =
                StreamSupport.stream(combinationsIterable.spliterator(), false)
                        .map(ImmutableSet::copyOf)
                        .collect(ImmutableList.toImmutableList());

        assertEquals(ImmutableList.of(
                ImmutableSet.of("a"),
                ImmutableSet.of("b"),
                ImmutableSet.of("c"),
                ImmutableSet.of("d"),
                ImmutableSet.of("e")),
                combinations);

        // 2
        combinationsIterable = ChooseK.chooseK(elements, 2);
        combinations =
                StreamSupport.stream(combinationsIterable.spliterator(), false)
                        .map(ImmutableSet::copyOf)
                        .collect(ImmutableList.toImmutableList());

        assertEquals(ImmutableList.<Set<String>>builder()
                        .add(ImmutableSet.of("a", "b"))
                        .add(ImmutableSet.of("a", "c"))
                        .add(ImmutableSet.of("a", "d"))
                        .add(ImmutableSet.of("a", "e"))
                        .add(ImmutableSet.of("b", "c"))
                        .add(ImmutableSet.of("b", "d"))
                        .add(ImmutableSet.of("b", "e"))
                        .add(ImmutableSet.of("c", "d"))
                        .add(ImmutableSet.of("c", "e"))
                        .add(ImmutableSet.of("d", "e"))
                        .build(),
                combinations);

        // 3
        combinationsIterable = ChooseK.chooseK(elements, 3);
        combinations =
                StreamSupport.stream(combinationsIterable.spliterator(), false)
                        .map(ImmutableSet::copyOf)
                        .collect(ImmutableList.toImmutableList());

        assertEquals(ImmutableList.<Set<String>>builder()
                        .add(ImmutableSet.of("a", "b", "c"))
                        .add(ImmutableSet.of("a", "b", "d"))
                        .add(ImmutableSet.of("a", "b", "e"))
                        .add(ImmutableSet.of("a", "c", "d"))
                        .add(ImmutableSet.of("a", "c", "e"))
                        .add(ImmutableSet.of("a", "d", "e"))
                        .add(ImmutableSet.of("b", "c", "d"))
                        .add(ImmutableSet.of("b", "c", "e"))
                        .add(ImmutableSet.of("b", "d", "e"))
                        .add(ImmutableSet.of("c", "d", "e"))
                        .build(),
                combinations);

        // 4
        combinationsIterable = ChooseK.chooseK(elements, 4);
        combinations =
                StreamSupport.stream(combinationsIterable.spliterator(), false)
                        .map(ImmutableSet::copyOf)
                        .collect(ImmutableList.toImmutableList());

        assertEquals(ImmutableList.<Set<String>>builder()
                        .add(ImmutableSet.of("a", "b", "c", "d"))
                        .add(ImmutableSet.of("a", "b", "c", "e"))
                        .add(ImmutableSet.of("a", "b", "d", "e"))
                        .add(ImmutableSet.of("a", "c", "d", "e"))
                        .add(ImmutableSet.of("b", "c", "d", "e"))
                        .build(),
                combinations);

        // 5
        combinationsIterable = ChooseK.chooseK(elements, 5);
        combinations =
                StreamSupport.stream(combinationsIterable.spliterator(), false)
                        .map(ImmutableSet::copyOf)
                        .collect(ImmutableList.toImmutableList());

        assertEquals(ImmutableList.<Set<String>>builder()
                        .add(ImmutableSet.of("a", "b", "c", "d", "e"))
                        .build(),
                combinations);
    }

    @Test
    void testChooseK3() {
        final Set<String> elements = ImmutableSet.of("a", "b", "c", "d", "e");

        final EnumeratingIterable<String> combinationsIterable = ChooseK.chooseK(elements, 4);
        final EnumeratingIterator<String> iterator = combinationsIterable.iterator();

        final var actualSetBuilder = ImmutableList.builder();

        actualSetBuilder.add(ImmutableSet.copyOf(iterator.next())); // a,b,c,d
        iterator.skip(1);                                     // skip subtree a,[b,...] combinations.
        actualSetBuilder.add(ImmutableSet.copyOf(iterator.next())); // a,c,d,e
        actualSetBuilder.add(ImmutableSet.copyOf(iterator.next())); // b,c,d,e

        assertEquals(ImmutableList.<Set<String>>builder()
                        .add(ImmutableSet.of("a", "b", "c", "d"))
                        // .add(ImmutableSet.of("a", "b", "c", "e")) // skipped
                        // .add(ImmutableSet.of("a", "b", "d", "e")) // skipped
                        .add(ImmutableSet.of("a", "c", "d", "e"))
                        .add(ImmutableSet.of("b", "c", "d", "e"))
                        .build(),
                actualSetBuilder.build());
    }
}
