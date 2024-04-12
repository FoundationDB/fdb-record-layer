/*
 * NonnullPair.java
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
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A {@link Pair} implementation where both elements are not null. This utility allows
 * the caller to omit null checks when retrieving elements from the pair.
 *
 * @param <L> the type of the left element
 * @param <R> the type of the right element
 */
@API(API.Status.INTERNAL)
public class NonnullPair<L, R> implements Pair<L, R> {
    @Nonnull
    private final Pair<L, R> pair;

    private NonnullPair(@Nonnull Pair<L, R> pair) {
        this.pair = pair;
    }

    @Nonnull
    @Override
    public L getLeft() {
        // We check the left and right are not null during the static initializer, so we
        // do not need to double check during the get methods
        return pair.getLeft();
    }

    @Nonnull
    @Override
    public R getRight() {
        // We check the left and right are not null during the static initializer, so we
        // do not need to double check during the get methods
        return pair.getRight();
    }

    @Override
    public R setValue(final R value) {
        return pair.setValue(Objects.requireNonNull(value));
    }

    @Override
    @SpotBugsSuppressWarnings(value = "NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION", justification = "SpotBugs is incorrect about nullability of equals argument")
    public boolean equals(@Nullable final Object obj) {
        return pair.equals(obj);
    }

    @Override
    public int hashCode() {
        return pair.hashCode();
    }

    /**
     * Create a new {@link Pair} where both elements are not null.
     *
     * @param left the left element of the pair
     * @param right the right element of the pair
     * @param <L> the type of the left element
     * @param <R> the type of the right element
     * @return a pair wrapping both elements
     */
    @Nonnull
    public static <L, R> NonnullPair<L, R> of(@Nonnull L left, @Nonnull R right) {
        return new NonnullPair<>(Pair.of(Objects.requireNonNull(left), Objects.requireNonNull(right)));
    }
}
