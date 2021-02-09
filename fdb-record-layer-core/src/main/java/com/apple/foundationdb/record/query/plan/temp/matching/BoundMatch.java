/*
 * BoundMatch.java
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

package com.apple.foundationdb.record.query.plan.temp.matching;

import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import java.util.Optional;

/**
 * Case class to represent a match for matchers that compute a result. Each bound map uses an {@link AliasMap} to
 * describe the match itself together with an (optional) result of some sort of type {@code R}
 *
 * @param <R> result type
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class BoundMatch<R> {
    /**
     * The alias map for this match containing <em>all</em> mappings necessary to describe this match (even bindings
     * coming from external (deep) correlations.
     */
    @Nonnull
    private final AliasMap aliasMap;

    /**
     * An optional match result. We opted to use an optional here in order to give the match combiner
     * {@link com.apple.foundationdb.record.query.plan.temp.RelationalExpression.CombineFunction} the opportunity
     * to produce a meaningful result when the matching sets are empty.
     */
    @Nonnull
    private final Optional<R> matchResultOptional;

    /**
     * Private constructor. Use static factory methods.
     * @param aliasMap the alias map for this match containing <em>all</em> mappings necessary to describe this match (even bindings
     *        coming from external (deep) correlations
     * @param matchResultOptional an optional match result
     */
    private BoundMatch(@Nonnull final AliasMap aliasMap, @Nonnull final Optional<R> matchResultOptional) {
        this.aliasMap = aliasMap;
        this.matchResultOptional = matchResultOptional;
    }

    @Nonnull
    public AliasMap getAliasMap() {
        return aliasMap;
    }

    @Nonnull
    public Optional<R> getMatchResultOptional() {
        return matchResultOptional;
    }

    @Nonnull
    public R getMatchResult() {
        Verify.verify(matchResultOptional.isPresent());
        return matchResultOptional.get();
    }

    /**
     * Factory method to create a bound match with a proper match result.
     * @param aliasMap the alias map for this match containing <em>all</em> mappings necessary to describe this match (even bindings
     *        coming from external (deep) correlations
     * @param matchResult a match result
     * @param <R> result type
     * @return a newly created bound match
     */
    @Nonnull
    public static <R> BoundMatch<R> withAliasMapAndMatchResult(final AliasMap aliasMap, @Nonnull final R matchResult) {
        return new BoundMatch<>(aliasMap, Optional.of(matchResult));
    }

    /**
     * Factory method to create a bound match with no match result.
     * @param aliasMap the alias map for this match containing <em>all</em> mappings necessary to describe this match (even bindings
     *        coming from external (deep) correlations
     * @param <R> result type
     * @return a newly created bound match that does not hold a match result
     */
    @Nonnull
    public static <R> BoundMatch<R> withAliasMap(final AliasMap aliasMap) {
        return new BoundMatch<>(aliasMap, Optional.empty());
    }
}
