/*
 * IterableHelpers.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.annotation.API;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.StreamSupport;

/**
 * Helper methods for {@link Iterable}s.
 */
@API(API.Status.EXPERIMENTAL)
public class IterableHelpers {
    private IterableHelpers() {
        // prevent instantiation
    }

    /**
     * Flat-maps given function to the given iterable and returns a new iterable. This is akin
     * to compositions based on {@link java.util.stream.Stream}, however, allows for restarting the iteration as per
     * general contract in {@link Iterable}.
     * @param source source iterable
     * @param mapper mapper function to map from {@code T} to {@code Iterable} of {@code R}
     * @param <T> type of source iterable
     * @param <R> type the given mapper function returns iterables of
     * @return an iterable of type {@code R} that is the conceptual flat map of {@code source} using {@code mapper}
     */
    public static <T, R> Iterable<R> flatMap(@Nonnull Iterable<T> source, @Nonnull final Function<? super T, ? extends Iterable<R>> mapper) {
        return () -> StreamSupport.stream(source.spliterator(), false)
                .flatMap(t -> {
                    final Iterable<R> mappedIterable = mapper.apply(t);
                    return StreamSupport.stream(mappedIterable.spliterator(), false);
                }).iterator();
    }

    /**
     * Maps given function to the given iterable and returns a new iterable. This is akin
     * to compositions based on {@link java.util.stream.Stream}, however, allows for restarting the iteration as per
     * general contract in {@link Iterable}.
     * @param source source iterable
     * @param mapper mapper function to map from {@code T} to {@code Iterable} of {@code R}
     * @param <T> type of source iterable
     * @param <R> type the given mapper function returns
     * @return an iterable of type {@code R} that is the conceptual map of {@code source} using {@code mapper}
     */
    public static <T, R> Iterable<R> map(@Nonnull Iterable<T> source, @Nonnull final Function<? super T, R> mapper) {
        return () -> StreamSupport
                .stream(source.spliterator(), false)
                .map(mapper)
                .iterator();
    }

    /**
     * Returns an alternative singleton iterable if the given source iterable is empty.
     * @param source source
     * @param value value to use if source is empty
     * @param <T> type parameter of the given iterable
     * @return an iterable of type {@code T} that returns the original iterable if it is not empty, or a singleton
     *         iterable composed of just {@code value}.
     */
    public static <T> Iterable<T> orElseOf(@Nonnull Iterable<T> source, @Nonnull final T value) {
        return orElse(source, ImmutableList.of(value));
    }

    /**
     * Returns an alternative iterable if the given source iterable is empty.
     * @param source source
     * @param otherIterable iterable to return if source is empty
     * @param <T> type parameter of the given iterable
     * @return an iterable of type {@code T} that returns the original iterable if it is not empty, or {@code otherIterable}
     */
    public static <T> Iterable<T> orElse(@Nonnull Iterable<T> source, @Nonnull final Iterable<T> otherIterable) {
        return () -> new AbstractIterator<T>() {
            private Iterator<T> nestedIterator = source.iterator();
            private boolean isEmpty = true;
            private boolean hasSwitched = false;
            @Override
            protected T computeNext() {
                if (nestedIterator.hasNext()) {
                    isEmpty = false;
                    return nestedIterator.next();
                } else {
                    if (isEmpty && !hasSwitched) {
                        nestedIterator = otherIterable.iterator();
                        hasSwitched = true;
                        return computeNext();
                    }
                    return endOfData();
                }
            }
        };
    }
}
