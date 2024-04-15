/*
 * Pair.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.util.pair;

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;

/**
 * A simple interface for pairs of two elements. Implementations implement pair-wise equality and hash code,
 * so these should be safe to use in hash maps and hash sets (as long as the underlying types implement
 * {@link #equals(Object)} and {@link #hashCode()}).
 *
 * @param <L> the type of the left element of the pair
 * @param <R> the type of the right element of the pair
 */
@API(API.Status.INTERNAL)
public interface Pair<L, R> extends Map.Entry<L, R> {
    /**
     * Get the left element of the pair.
     *
     * @return the left element of the pair
     */
    @Nullable
    L getLeft();

    /**
     * Get the right element of the pair.
     *
     * @return the right element of the pair
     */
    @Nullable
    R getRight();

    /**
     * Alias of {@link #getLeft()}. This is included so that the interface conforms to the
     * {@link Map.Entry} interface.
     *
     * @return the left element of the pair
     */
    @Override
    default L getKey() {
        return getLeft();
    }

    /**
     * Alias of {@link #getRight()}. This is included so that the interface conforms to the
     * {@link Map.Entry} interface.
     *
     * @return the right element of the pair
     */
    @Override
    default R getValue() {
        return getRight();
    }

    /**
     * Return a new immutable pair from two elements. This is the default method for constructing a new
     * pair.
     *
     * @param left the left element of the pair
     * @param right the right element of the pair
     * @param <L> the type of the left element
     * @param <R> the type of the right element
     * @return a new pair wrapping the two elements
     */
    @Nonnull
    static <L, R> Pair<L, R> of(@Nullable L left, @Nullable R right) {
        return ImmutablePair.of(left, right);
    }
}
