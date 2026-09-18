/*
 * DistinctTopKTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.async.common;

import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.test.TestExecutors;
import com.google.common.collect.ImmutableList;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DistinctTopKTest {
    private static final Executor EXECUTOR = TestExecutors.defaultThreadPool();

    @Test
    void testDistinctTopK() {
        final DistinctTopK<Integer> distinctTopK = DistinctTopK.min(Comparator.comparingInt(x -> x), 10);

        final List<Integer> shuffledList =
                IntStream.range(0, 40).mapToObj(x -> x / 2).collect(Collectors.toList());
        Collections.shuffle(shuffledList, new Random(0));
        shuffledList.forEach(distinctTopK::add);

        Assertions.assertThat(distinctTopK.toSortedList()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        Assertions.assertThat(distinctTopK.worstElement()).contains(9);
    }

    @Test
    void testCollect() {
        final List<Integer> result =
                DistinctTopK.min(Comparator.comparingInt(Integer::intValue), 5)
                        .collect(asyncIterable(shuffledWithDuplicates()), EXECUTOR)
                        .join();

        Assertions.assertThat(result).containsExactly(0, 1, 2, 3, 4);
    }

    @Test
    void testCollectRemaining() {
        final List<Integer> result =
                DistinctTopK.min(Comparator.comparingInt(Integer::intValue), 5)
                        .collectRemaining(asyncIterable(shuffledWithDuplicates()).iterator(), EXECUTOR)
                        .join();

        Assertions.assertThat(result).containsExactly(0, 1, 2, 3, 4);
    }

    @Test
    void testDistinctTopKMax() {
        final DistinctTopK<Integer> distinctTopK = DistinctTopK.max(Comparator.comparingInt(x -> x), 10);

        final List<Integer> shuffledList =
                IntStream.range(0, 40).mapToObj(x -> x / 2).collect(Collectors.toList());
        Collections.shuffle(shuffledList, new Random(0));
        shuffledList.forEach(distinctTopK::add);

        // max() retains the 10 largest distinct values (10..19), reported greatest-to-least.
        Assertions.assertThat(distinctTopK.toSortedList()).containsExactly(19, 18, 17, 16, 15, 14, 13, 12, 11, 10);
        Assertions.assertThat(distinctTopK.worstElement()).contains(10);
    }

    @Test
    void testAddRetainsEvictsAndRejects() {
        final DistinctTopK<Integer> topK = DistinctTopK.max(Comparator.comparingInt(x -> x), 3);

        Assertions.assertThat(topK.add(5)).isTrue();    // below capacity
        Assertions.assertThat(topK.add(5)).isFalse();   // exact duplicate
        Assertions.assertThat(topK.add(6)).isTrue();
        Assertions.assertThat(topK.add(7)).isTrue();    // now {5, 6, 7}, worst == 5
        Assertions.assertThat(topK.worstElement()).contains(5);

        Assertions.assertThat(topK.add(4)).isFalse();   // strictly worse than the worst → rejected
        Assertions.assertThat(topK.add(5)).isFalse();   // ties the worst → rejected (comparator-duplicate)
        Assertions.assertThat(topK.add(8)).isTrue();    // strictly better → evicts the worst (5)

        Assertions.assertThat(topK.toSortedList()).containsExactly(8, 7, 6);
        Assertions.assertThat(topK.worstElement()).contains(6);
    }

    @Test
    void testAddRejectsComparatorEqualDistinctInstances() {
        // Distinctness is defined by the comparator, not equals/identity: two different arrays with the same key
        // collide, so the second is rejected even though it is a distinct object.
        final DistinctTopK<int[]> topK = DistinctTopK.max(Comparator.comparingInt(a -> a[0]), 3);

        Assertions.assertThat(topK.add(new int[] {5, 100})).isTrue();
        Assertions.assertThat(topK.add(new int[] {5, 999})).isFalse();
    }

    @Test
    void testNonPositiveCapacityIsRejected() {
        Assertions.assertThatThrownBy(() -> DistinctTopK.max(Comparator.comparingInt(Integer::intValue), 0))
                .isInstanceOf(IllegalArgumentException.class);
        Assertions.assertThatThrownBy(() -> DistinctTopK.min(Comparator.comparingInt(Integer::intValue), -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testToSortedListIsImmutable() {
        final DistinctTopK<Integer> topK = DistinctTopK.max(Comparator.comparingInt(x -> x), 3);
        topK.add(1);
        Assertions.assertThatThrownBy(() -> topK.toSortedList().add(2))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testMinEqualsMaxOfReversedComparator() {
        // min(cmp, k) is defined as max(cmp.reversed(), k); assert the two agree on identical input.
        final List<Integer> data = shuffledWithDuplicates();
        final DistinctTopK<Integer> viaMax = DistinctTopK.max(Comparator.comparingInt(Integer::intValue).reversed(), 5);
        final DistinctTopK<Integer> viaMin = DistinctTopK.min(Comparator.comparingInt(Integer::intValue), 5);
        data.forEach(viaMax::add);
        data.forEach(viaMin::add);

        Assertions.assertThat(viaMax.toSortedList()).isEqualTo(viaMin.toSortedList());
    }

    @Test
    void testCollectMax() {
        final List<Integer> result =
                DistinctTopK.max(Comparator.comparingInt(Integer::intValue), 5)
                        .collect(asyncIterable(shuffledWithDuplicates()), EXECUTOR)
                        .join();

        Assertions.assertThat(result).containsExactly(19, 18, 17, 16, 15);
    }

    @Test
    void testCollectEmptyIterable() {
        final List<Integer> result =
                DistinctTopK.min(Comparator.comparingInt(Integer::intValue), 5)
                        .collect(asyncIterable(ImmutableList.<Integer>of()), EXECUTOR)
                        .join();

        Assertions.assertThat(result).isEmpty();
    }

    /**
     * Returns {@code 0..19} with every value appearing twice, deterministically shuffled, so {@code collect} has to
     * both rank and de-duplicate.
     */
    private static List<Integer> shuffledWithDuplicates() {
        final List<Integer> list =
                IntStream.range(0, 40).mapToObj(x -> x / 2).collect(Collectors.toList());
        Collections.shuffle(list, new Random(0));
        return list;
    }

    private static AsyncIterable<Integer> asyncIterable(final List<Integer> items) {
        return MoreAsyncUtil.iterableFromCollection(CompletableFuture.completedFuture(items), EXECUTOR);
    }
}
