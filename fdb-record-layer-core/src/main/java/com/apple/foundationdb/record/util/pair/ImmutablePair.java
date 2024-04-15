/*
 * ImmutablePair.java
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
import java.util.Objects;

/**
 * Immutable implementation of the {@link Pair} interface.
 *
 * @param <L> the type of the left element of the pair
 * @param <R> the type of the right element of the pair
 */
@API(API.Status.INTERNAL)
public class ImmutablePair<L, R> implements Pair<L, R> {
    @Nullable
    private final L left;
    @Nullable
    private final R right;
    private volatile int hashCode;

    private ImmutablePair(@Nullable L left, @Nullable R right) {
        this.left = left;
        this.right = right;
    }

    @Nullable
    @Override
    public L getLeft() {
        return left;
    }

    @Nullable
    @Override
    public R getRight() {
        return right;
    }

    @Override
    public R setValue(final R value) {
        throw new UnsupportedOperationException("cannot set value on an immmutable Pair");
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Pair<?, ?>)) {
            return false;
        }
        Pair<?, ?> other = (Pair<?, ?>)obj;
        return Objects.equals(getLeft(), other.getLeft()) && Objects.equals(getRight(), other.getRight());
    }

    @Override
    public int hashCode() {
        if (hashCode == 0) {
            synchronized (this) {
                if (hashCode == 0) {
                    hashCode = Objects.hash(left, right);
                }
            }
        }
        return hashCode;
    }

    @Override
    public String toString() {
        return "(" + left + ", " + right + ")";
    }

    /**
     * Create a new {@code Pair} from two elements.
     *
     * @param left the left element of the pair
     * @param right the right element of the pair
     * @param <L> the type of the left element
     * @param <R> the type of the right element
     * @return a new {@code Pair} wrapping the two elements
     */
    @Nonnull
    public static <L, R> ImmutablePair<L, R> of(@Nullable L left, @Nullable R right) {
        return new ImmutablePair<>(left, right);
    }
}
