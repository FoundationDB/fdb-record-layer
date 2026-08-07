/*
 * TopKTest.java
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

public class TopKTest {
    private static final Executor EXECUTOR = TestExecutors.defaultThreadPool();

    @Test
    void testTopK() {
        final TopK<Integer> topK = TopK.min(Comparator.comparingInt(x -> x), 10);

        final List<Integer> shuffledList = IntStream.range(0, 20).boxed().collect(Collectors.toList());
        Collections.shuffle(shuffledList, new Random(0));
        shuffledList.forEach(topK::add);

        Assertions.assertThat(topK.toSortedList()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        Assertions.assertThat(topK.worstElement()).contains(9);
    }

    @Test
    void testCollect() {
        final List<Integer> result =
                TopK.min(Comparator.comparingInt(Integer::intValue), 5)
                        .collect(asyncIterable(shuffled()), EXECUTOR)
                        .join();

        Assertions.assertThat(result).containsExactly(0, 1, 2, 3, 4);
    }

    @Test
    void testCollectRemaining() {
        final List<Integer> result =
                TopK.min(Comparator.comparingInt(Integer::intValue), 5)
                        .collectRemaining(asyncIterable(shuffled()).iterator(), EXECUTOR)
                        .join();

        Assertions.assertThat(result).containsExactly(0, 1, 2, 3, 4);
    }

    /**
     * Returns {@code 0..19} in a deterministically shuffled order, so the collector has to do real work to recover
     * the top-{@code k}.
     */
    private static List<Integer> shuffled() {
        final List<Integer> list = IntStream.range(0, 20).boxed().collect(Collectors.toList());
        Collections.shuffle(list, new Random(0));
        return list;
    }

    private static AsyncIterable<Integer> asyncIterable(final List<Integer> items) {
        return MoreAsyncUtil.iterableFromCollection(CompletableFuture.completedFuture(items), EXECUTOR);
    }
}
