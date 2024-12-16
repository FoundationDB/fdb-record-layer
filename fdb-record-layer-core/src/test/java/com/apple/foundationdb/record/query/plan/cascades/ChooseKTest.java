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
import com.apple.test.RandomizedTestUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
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
        final List<String> elements = ImmutableList.of("a", "b", "c", "d");

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
        assertEquals(combinations, computeChooseK(elements, 3));
    }
    
    @Test
    void testChooseK2() {
        final List<String> elements = ImmutableList.of("a", "b", "c", "d", "e");

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
        assertEquals(combinations, computeChooseK(elements, 1));

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
        assertEquals(combinations, computeChooseK(elements, 2));

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
        assertEquals(combinations, computeChooseK(elements, 3));

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
        assertEquals(combinations, computeChooseK(elements, 4));

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
        assertEquals(combinations, computeChooseK(elements, 5));
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

    @Nonnull
    static Stream<Arguments> checkNChooseK() {
        // Compute all for n < 10, k < n
        Stream<Arguments> smallNK = IntStream.range(0, 10)
                .boxed()
                .flatMap(n -> IntStream.range(0, n + 1).boxed()
                        .map(k -> Arguments.of(n, k)));
        // Check a smattering of elements where 10 <= n < 20 and n < k,
        // If randomized tests are disabled, this will be the empty set.
        Stream<Arguments> randomNK = RandomizedTestUtils.randomArguments(r -> {
            int n = r.nextInt(10) + 10;
            int k = r.nextInt(n + 1);
            return Arguments.of(n, k);
        });
        return Stream.concat(smallNK, randomNK);
    }

    /**
     * Compare the iterator implementation to the value computed from the recursive implementation.
     *
     * @param n the number of elements in the total list
     * @param k the number of elements in each combination
     */
    @ParameterizedTest(name = "checkNChooseK[n = {0}, k = {1}]")
    @MethodSource
    void checkNChooseK(int n, int k) {
        final List<Integer> elements = IntStream.range(0, n).boxed().collect(Collectors.toList());
        final EnumeratingIterable<Integer> combinationsIterable = ChooseK.chooseK(elements, k);
        final List<Set<Integer>> combinations = StreamSupport.stream(combinationsIterable.spliterator(), false)
                .map(ImmutableSet::copyOf)
                .collect(Collectors.toList());
        assertEquals(computeChooseK(elements, k), combinations);
        for (Set<Integer> combination : combinations) {
            assertThat(combination, hasSize(k));
        }
        // Expected size is: n!/(k! * (n - k)!
        // That is (factoring out k! from both sides):
        //   (n * (n - 1) * (n - 2) * ... (k + 1))/(n - k)!)
        long expectedNum = 1;
        long expectedDenom = 1;
        for (int i = 0; i < (n - k); i++) {
            expectedNum *= n - i;
            expectedDenom *= n - k - i;
        }
        int expectedSize = (int)(expectedNum / expectedDenom);
        assertThat(combinations, hasSize(expectedSize));
    }

    @Nonnull
    static Stream<Long> checkNChooseKWithSkips() {
        return RandomizedTestUtils.randomSeeds(0x5ca1ab1e, 464976232644684521L, 854522134120263833L);
    }

    @ParameterizedTest(name = "checkNChooseKWithSkips[seed = {0}]")
    @MethodSource
    void checkNChooseKWithSkips(long seed) {
        Random r = new Random(seed);
        int n = r.nextInt(17) + 5;
        int k = r.nextInt(n + 1);
        double skipRatio = r.nextDouble();

        final List<Integer> elements = IntStream.range(0, n).boxed().collect(Collectors.toList());
        final List<Set<Integer>> computed = computeChooseK(elements, k);
        int curr = 0;

        final EnumeratingIterator<Integer> combinationIterator = ChooseK.chooseK(elements, k).iterator();
        while (combinationIterator.hasNext()) {
            List<Integer> combination = combinationIterator.next();
            assertThat("Should not have exhausted the list of computed combinations", curr, lessThanOrEqualTo(computed.size()));
            assertEquals(computed.get(curr), ImmutableSet.copyOf(combination));
            if (k > 0 && r.nextDouble() < skipRatio) {
                int skipLevel = r.nextInt(k);
                combinationIterator.skip(skipLevel); // Advance the iterator, skipping everything after skipLevel

                // Fix a prefix of size skipLevel + 1. Advance our position through the expected list
                // as long as all of the elements of this prefix are still in the combination
                List<Integer> skipPrefix = combination.subList(0, skipLevel + 1);
                while (curr < computed.size() && computed.get(curr).containsAll(skipPrefix)) {
                    curr++;
                }
            } else {
                curr++;
            }
        }
        assertEquals(computed.size(), curr, "Should have exhausted computed combinations");
    }

    /**
     * Recursive implementation of choose K. This computes the list of combinations by choosing
     * a single element, and then finding all combinations of size {@code k - 1}.
     *
     * @param elems a set of elements to construct subsets of
     * @param k the size of each subset
     * @param <T> the type of each element in the original list
     * @return a list of all combinations of size k
     */
    @Nonnull
    private static <T> List<Set<T>> computeChooseK(@Nonnull List<T> elems, int k) {
        if (k == 0) {
            return List.of(Collections.emptySet());
        }
        List<Set<T>> results = new ArrayList<>();
        for (int i = 0; i <= elems.size() - k; i++) {
            // Choose one element
            T elem = elems.get(i);
            // Now compute the set of combinations of the tail of elems of size k - 1.
            List<Set<T>> choseKMinus1 = computeChooseK(elems.subList(i + 1, elems.size()), k - 1);
            // Add back in the original element to get the set of combinations of size k
            // that contain elem
            for (Set<T> subCombination : choseKMinus1) {
                results.add(ImmutableSet.<T>builder().add(elem).addAll(subCombination).build());
            }
        }
        return results;
    }
}
