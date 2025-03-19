/*
 * MemoizedFunctionTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.util;

import com.google.common.base.Function;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class MemoizedFunctionTest {

    private static final class StatefulFunction<U, T> {

        private Set<U> seenValues;

        @Nonnull
        private final Function<U, T> underlying;

        private StatefulFunction(Function<U, T> underlying) {
            seenValues = new HashSet<>();
            this.underlying = underlying;
        }

        public T apply(U input) {
            if (seenValues.contains(input)) {
                throw new RuntimeException("attempt to call function more than once for " + input);
            }
            seenValues.add(input);
            return underlying.apply(input);
        }
    }

    @Test
    void memoizedFunctionComputesExactlyOnce() {
        final var input = new ArrayList<Integer>(100000);
        for (int i = 0; i < 100000; i++) {
            input.add(i);
        }
        Collections.shuffle(input);

        final var memoizedFunction = MemoizedFunction.memoize(new StatefulFunction<Integer, Integer>(j -> j * 2)::apply);
        for (final var value : input) {
            Assertions.assertEquals(value * 2, memoizedFunction.apply(value));
        }

        for (final var value : input) {
            Assertions.assertDoesNotThrow(() ->  Assertions.assertEquals(value * 2, memoizedFunction.apply(value)));
        }
    }
}
