/*
 * MatchPredicate.java
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
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * An predicate that tests for a match between quantifiers also taking into account an equivalence maps between
 * {@link CorrelationIdentifier}s.
 *
 * @param <T> type
 */
@FunctionalInterface
public interface MatchPredicate<T> {
    /**
     * Test for a match.
     * @param element element on this side
     * @param otherElement element on the other side
     * @param aliasMap bindings that already have been established
     * @return {@code true} for a successful match attempt; {@code false} otherwise
     */
    boolean test(@Nonnull T element,
                 @Nonnull T otherElement,
                 @Nonnull final AliasMap aliasMap);

    /**
     * Method that combines this {@link MatchPredicate} and the {@code other} one passed in to produce a new
     * {@link MatchPredicate} that first tests this predicate and if successful then tests the other.
     * @param other other {@link MatchPredicate}
     * @return a new {@link MatchPredicate}
     */
    default MatchPredicate<T> and(final MatchPredicate<? super T> other) {
        Objects.requireNonNull(other);
        return (entity, otherEntity, equivalencesMap) -> test(entity, otherEntity, equivalencesMap) && other.test(entity, otherEntity, equivalencesMap);
    }

    /**
     * Method that combines this {@link MatchPredicate} and the {@link MatchFunction} {@code after} passed in to
     * produce a new {@link MatchFunction} that first tests this predicate and if successful then applies {@code after}.
     * @param after {@link MatchFunction}
     * @param <R> type that {@code after} produces
     * @return a new {@link MatchFunction}
     */
    default <R> MatchFunction<T, R> andThen(MatchFunction<? super T, R> after) {
        Objects.requireNonNull(after);
        return (entity, otherEntity, equivalencesMap) -> {
            if (test(entity, otherEntity, equivalencesMap)) {
                return after.apply(entity, otherEntity, equivalencesMap);
            }
            return ImmutableList.of();
        };
    }
}
