/*
 * TriFunction.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.util;

import java.util.Objects;
import java.util.function.Function;

/**
 * Represents a function that accepts three arguments and produces a result. This is the tri-arity specialization of
 * Function.
 * This is a functional interface whose functional method is {@link #apply(Object, Object, Object)}
 *
 * @param <T> the type of the first function argument
 * @param <U> the type of the second function argument
 * @param <V> the type of the third function argument
 * @param <R> the type of the function result
 */
@FunctionalInterface
public interface TriFunction<T, U, V, R> {

    /**
     * Applies this function to the given arguments.
     *
     * @param t the first function argument
     * @param u the second function argument
     * @param v the third function argument
     *
     * @return the function result
     */
    R apply(T t, U u, V v);

    /**
     * Returns a composed function that first applies this function to its input, and then applies the {@code after}
     * function to the result. If
     * evaluation of either function throws an exception, it is relayed to the caller of the composed function.
     *
     * @param after the function to apply after this function is applied
     * @param <W> the type of output of the after function, and of the composed function
     *
     * @return a composed function that first applies this function and then applies the {@code after} function
     *
     * @throws NullPointerException if {@code after} is null
     */
    default <W> TriFunction<T, U, V, W> andThen(Function<? super R, ? extends W> after) {
        Objects.requireNonNull(after);
        return (T t, U u, V v) -> after.apply(apply(t, u, v));
    }
}
