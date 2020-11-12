/*
 * PartialMatch.java
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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Case class to represent a partial match. A partial match is stored in a multi map in {@link GroupExpressionRef}s that
 * are part of a query graph. A partial match is never stored in a {@link GroupExpressionRef} of a match candidate.
 * A partial match establishes the a relation ships between a reference {@code ref1} in a query graph and a reference
 * {@code ref2} in a match candidate. It should be interpreted as there is a partial match between {@code ref1} and
 * {@code ref2} if {@code ref1} is result-equivalent to {@code ref2'} under the bindings in the alias map returned by
 * ({@link #getBoundAliasMap()}) where {@code ref2'} is obtained by is applying compensation to {@code ref2}.
 * If there is no compensation this simplifies to: there is a partial match between {@code ref1} and
 * {@code ref2} if {@code ref1} is result-equivalent to {@code ref2} under the bindings in the alias map returned by
 * ({@link #getBoundAliasMap()}).
 * It is the task of a matching algorithm (see {@link CascadesPlanner}) to establish {@link PartialMatch}es
 * between query graph and match candidate graphs. Once there is a partial match for the top-most reference in the
 * match candidate, we can replace the reference on the query graph side with a scan over the match candidate's
 * materialized version (e.g. an index) and subsequent compensation.
 */
public class PartialMatch {
    /**
     * Alias map of all bound correlated references.
     */
    @Nonnull
    private final AliasMap boundAliasMap;

    /**
     * Match candidate.
     */
    @Nonnull
    private final MatchCandidate matchCandidate;

    /**
     * Expression reference in query graph.
     */
    @Nonnull
    private final ExpressionRef<? extends RelationalExpression> queryRef;

    /**
     * Expression in query graph.
     */
    @Nonnull
    private final RelationalExpression queryExpression;

    /**
     * Expression reference in match candidate graph.
     */
    @Nonnull
    private final ExpressionRef<? extends RelationalExpression> candidateRef;

    /**
     * Compensation operator that can be applied to the scan of the materialized version of the match candidate.
     */
    @Nonnull
    private final MatchWithCompensation matchWithCompensation;

    @Nonnull
    private final Supplier<Map<CorrelationIdentifier, ComparisonRange>> boundParameterPrefixMapSupplier;

    public PartialMatch(@Nonnull final AliasMap boundAliasMap,
                        @Nonnull final MatchCandidate matchCandidate,
                        @Nonnull final ExpressionRef<? extends RelationalExpression> queryRef,
                        @Nonnull final RelationalExpression queryExpression,
                        @Nonnull final ExpressionRef<? extends RelationalExpression> candidateRef,
                        @Nonnull final MatchWithCompensation matchWithCompensation) {
        this.boundAliasMap = boundAliasMap;
        this.matchCandidate = matchCandidate;
        this.queryRef = queryRef;
        this.queryExpression = queryExpression;
        this.candidateRef = candidateRef;
        this.matchWithCompensation = matchWithCompensation;
        this.boundParameterPrefixMapSupplier = Suppliers.memoize(this::getBoundParameterPrefixMap);
    }

    @Nonnull
    public AliasMap getBoundAliasMap() {
        return boundAliasMap;
    }

    @Nonnull
    public MatchCandidate getMatchCandidate() {
        return matchCandidate;
    }

    @Nonnull
    public ExpressionRef<? extends RelationalExpression> getQueryRef() {
        return queryRef;
    }

    @Nonnull
    public RelationalExpression getQueryExpression() {
        return queryExpression;
    }

    @Nonnull
    public ExpressionRef<? extends RelationalExpression> getCandidateRef() {
        return candidateRef;
    }

    @Nonnull
    public MatchWithCompensation getMatchWithCompensation() {
        return matchWithCompensation;
    }

    public int getNumBoundParameterPrefix() {
        return boundParameterPrefixMapSupplier.get().size();
    }

    public Map<CorrelationIdentifier, ComparisonRange> getBoundParameterPrefixMap() {
        final ImmutableMap.Builder<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMapBuilder = ImmutableMap.builder();
        final Map<CorrelationIdentifier, ComparisonRange> parameterBindingMap =
                matchWithCompensation.getParameterBindingMap();

        final List<CorrelationIdentifier> parameters = matchCandidate.getParameters();
        for (int i = 0; i < parameters.size(); i++) {
            final CorrelationIdentifier parameter = parameters.get(i);
            Objects.requireNonNull(parameter);
            @Nullable final ComparisonRange comparisonRange = parameterBindingMap.get(parameter);
            if (comparisonRange == null) {
                return boundParameterPrefixMapBuilder.build();
            }
            switch (comparisonRange.getRangeType()) {
                case EQUALITY:
                    boundParameterPrefixMapBuilder.put(parameter, comparisonRange);
                    break;
                case INEQUALITY:
                    boundParameterPrefixMapBuilder.put(parameter, comparisonRange);
                    return boundParameterPrefixMapBuilder.build();
                case EMPTY:
                default:
                    return boundParameterPrefixMapBuilder.build();
            }
        }

        return boundParameterPrefixMapBuilder.build();
    }

    @Nonnull
    public Compensation compensate(@Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap) {
        return queryExpression.compensate(this, boundParameterPrefixMap);
    }

    @Nonnull
    public static Collection<MatchWithCompensation> matchesFromMap(@Nonnull IdentityBiMap<Quantifier, PartialMatch> partialMatchMap) {
        return partialMatchMap.values()
                .stream()
                .map(IdentityBiMap::unwrap)
                .map(Objects::requireNonNull)
                .map(PartialMatch::getMatchWithCompensation)
                .collect(ImmutableList.toImmutableList());
    }
}
