/*
 * MatchFunction.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.matching.graph;

import com.apple.foundationdb.record.query.plan.cascades.AliasMap;

import javax.annotation.Nonnull;

/**
 * An functional interface for a match function that computes a match result (an {@link Iterable} of type {@code M}.
 *
 * @param <T> element type
 * @param <M> type of intermediate result
 */
@FunctionalInterface
public interface MatchFunction<T, M> {
    /**
     * Compute a match result.
     * @param element element on this side
     * @param otherElement element on the other side
     * @param aliasMap bindings that already have been established
     * @return an {@link Iterable} of type {@code M}. The matching logic interprets the resulting {@link Iterable}.
     *         A non-empty {@link Iterable} is considered a successful match attempt; an empty {@link Iterable} is
     *         considered a failed match attempt.
     */
    Iterable<M> apply(@Nonnull T element,
                      @Nonnull T otherElement,
                      @Nonnull AliasMap aliasMap);
}
