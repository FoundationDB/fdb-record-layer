/*
 * ComparablePair.java
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

/**
 * An implementation of the {@link Pair} that adds comparison support to the {@link Pair}. This
 * allows this kind of pair to be used within ordered collections. The comparison performs lexicographic comparison
 * on the elements, that is, the left elements are compared first and if equal, comparison proceeds to the right element.
 *
 * @param <L> the type of the left element in the pair
 * @param <R> the type of the right element in the pair
 */
@API(API.Status.INTERNAL)
public class ComparablePair<L extends Comparable<? super L>, R extends Comparable<? super R>> implements Pair<L, R>, Comparable<ComparablePair<L, R>> {
    @Nonnull
    private final NonnullPair<L, R> pair;

    private ComparablePair(@Nonnull NonnullPair<L, R> pair) {
        this.pair = pair;
    }

    @Nonnull
    @Override
    public L getLeft() {
        return pair.getLeft();
    }

    @Nonnull
    @Override
    public R getRight() {
        return pair.getRight();
    }

    @Override
    public int compareTo(final ComparablePair<L, R> o) {
        int lCompare = getLeft().compareTo(o.getLeft());
        if (lCompare != 0) {
            return lCompare;
        }
        return getRight().compareTo(o.getRight());
    }

    @Override
    public R setValue(final R value) {
        return pair.setValue(value);
    }

    @Override
    @SpotBugsSuppressWarnings(
            value = {"EQ_CHECK_FOR_OPERAND_NOT_COMPATIBLE_WITH_THIS", "NP_METHOD_PARAMETER_TIGHTENS_ANNOTATION"},
            justification = "Purposefully delegating to pair member to check type and nullability of obj")
    public boolean equals(@Nullable Object obj) {
        return pair.equals(obj);
    }

    @Override
    public int hashCode() {
        return pair.hashCode();
    }

    @Override
    public String toString() {
        return pair.toString();
    }

    /**
     * Create a new {@code ComparablePair} wrapping two elements. The two elements must be of types that
     * support comparison, and they must not be null.
     *
     * @param left the left element of the pair
     * @param right the right element of the pair
     * @param <L> the type of the left element
     * @param <R> the type of the right element
     * @return new {@code ComparablePair} wrapping the two elements
     */
    @Nonnull
    public static <L extends Comparable<? super L>, R extends Comparable<? super R>> ComparablePair<L, R> of(@Nonnull L left, @Nonnull R right) {
        return new ComparablePair<>(NonnullPair.of(left, right));
    }
}
