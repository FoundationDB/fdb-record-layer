/*
 * MemoizedFunction.java
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

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A function that is able to memoize the codomain of its input.
 *
 * @param <T> The type of the function domain.
 * @param <U> The type of the function codomain.
 */
public class MemoizedFunction<T, U> {

    @Nonnull
    private final Map<T, U> cache;

    private MemoizedFunction() {
        cache = new ConcurrentHashMap<>();
    }

    private Function<T, U> wrap(@Nonnull final Function<T, U> function) {
        return input -> cache.computeIfAbsent(input, function);
    }

    /**
     * Creates a memoized version of the provided function.
     * @param function The function to memoize.
     * @param <T> The type of the function domain.
     * @param <U> The type of the function codomain.
     * @return A memoized version of the provided function.
     */
    public static <T, U> Function<T, U> memoize(@Nonnull final Function<T, U> function) {
        return new MemoizedFunction<T, U>().wrap(function);
    }
}
