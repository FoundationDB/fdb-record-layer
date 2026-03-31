/*
 * Lens.java
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

package com.apple.foundationdb.util;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.function.UnaryOperator;

/**
 * Classic object-oriented lens.
 * @param <C> type parameter of the enclosing class
 * @param <A> type parameter of the attribute class
 */
public interface Lens<C, A> {
    @Nonnull
    default A getNonnull(@Nonnull final C c) {
        return Objects.requireNonNull(get(c));
    }

    @Nullable
    A get(@Nonnull C c);

    @Nonnull
    default C wrap(@Nullable final A a) {
        return set(null, a);
    }

    @Nonnull
    C set(@Nullable C c, @Nullable A a);

    @Nonnull
    default C map(@Nonnull final C c, @Nonnull final UnaryOperator<A> operator) {
        final A oldA = get(c);
        final A newA = operator.apply(oldA);
        if (newA == oldA) {
            return c;
        }
        return set(c, newA);
    }

    default <A2> Lens<C, A2> compose(@Nonnull final Lens<A, A2> downstream) {
        return new Lens<>() {
            @Nullable
            @Override
            public A2 get(@Nonnull final C c) {
                return downstream.get(Lens.this.get(c));
            }

            @Nonnull
            @Override
            public C set(@Nullable final C c, @Nullable final A2 a2) {
                return Lens.this.set(c, downstream.set(Lens.this.get(c), a2));
            }
        };
    }

    @Nonnull
    static <T> Lens<T, T> identity() {
        return new Lens<>() {
            @Nonnull
            @Override
            public T get(@Nonnull final T t) {
                return t;
            }

            @Nonnull
            @Override
            public T set(@Nullable final T t, @Nullable final T t2) {
                return Objects.requireNonNull(t2);
            }
        };
    }

    @Nonnull
    static <T1, T2> List<T2> extract(@Nonnull final Lens<T1, T2> lens, @Nonnull final List<T1> elements) {
        final ImmutableList.Builder<T2> resultsBuilder = ImmutableList.builder();
        for (final T1 element : elements) {
            resultsBuilder.add(lens.getNonnull(element));
        }
        return resultsBuilder.build();
    }
}
