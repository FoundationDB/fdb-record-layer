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

package com.apple.foundationdb.async.guardiann;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TopKTest {
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
    void testDistinctTopK() {
        final DistinctTopK<Integer> distinctTopK = DistinctTopK.min(Comparator.comparingInt(x -> x), 10);

        final List<Integer> shuffledList = IntStream.range(0, 40).mapToObj(x -> x / 2).collect(Collectors.toList());
        Collections.shuffle(shuffledList, new Random(0));
        shuffledList.forEach(distinctTopK::add);

        Assertions.assertThat(distinctTopK.toSortedList()).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        Assertions.assertThat(distinctTopK.worstElement()).contains(9);
    }
}
