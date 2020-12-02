/*
 * MatchCandidate.java
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

import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Interface to represent a match candidate. A match candidate on code level is just a name and a data flow graph
 * that can be matched against a query graph. The match candidate does not keep the root to the graph to be matched but
 * rather an instance of {@link ExpressionRefTraversal} to allow for navigation of references within the candidate.
 *
 * Match candidates also allow for creation of scans over the materialized data, e.g. the index for an
 * {@link IndexScanMatchCandidate} or the primary range for a {@link PrimaryScanMatchCandidate} given appropriate
 * {@link ComparisonRange}s which usually are the direct result of graph matching.
 */
public interface MatchCandidate {
    /**
     * Returns the name of the match candidate. If this candidate represents and index, it will be the name of the index.
     * @return the name of this match candidate
     */
    @Nonnull
    String getName();

    /**
     * Returns the traversal object for this candidate. The traversal object can either be computed up-front when
     * the candidate is created or lazily when this method is invoked. It is, however, necessary that the traversal
     * once computed is stable, meaning the object returned by implementors of this method must always return the
     * same object.
     * @return the traversal associated for this match candidate
     */
    @Nonnull
    ExpressionRefTraversal getTraversal();

    /**
     * Returns the parameter names for all necessary parameters that need to be bound during matching.
     * @return a list of {@link CorrelationIdentifier}s for all the used parameters in this match candidate
     */
    @Nonnull
    List<CorrelationIdentifier> getParameters();

    /**
     * This method returns a key expression that can be used to actually compute the the keys of this candidate for a
     * given record.
     * The current expression hierarchy cannot be evaluated at runtime (in general). This key expression helps
     * representing compensation or part of compensation if needed.
     * @return a key expression that can be evaluated based on a base record
     */
    @Nonnull
    KeyExpression getAlternativeKeyExpression();

    /**
     * Computes a map from {@link CorrelationIdentifier} to {@link ComparisonRange} that is physically compatible with
     * a scan over the materialized version of the match candidate, so e.g. for an {@link IndexScanMatchCandidate} that
     * would be the scan over the index.
     * As matching progresses it finds mappings from parameters to corresponding comparison ranges. Matching, however,
     * is not sensitive to whether such a binding could actually be uses in an index scan. In fact, in a different maybe
     * future record layer with improved physical operators this method should be revised to account for those improvements.
     * For now, we only consider a prefix of said mappings that consist of n equality-bound mappings and stops either
     * at an inequality bound parameter or before a unbound parameter.
     * @param matchInfo match info
     * @return a map containing parameter to comparison range mappings for a prefix of parameters that is compatible
     *         with a physical scan over the materialized view (of the candidate)
     */
    default Map<CorrelationIdentifier, ComparisonRange> computeBoundParameterPrefixMap(@Nonnull final MatchInfo matchInfo) {
        final Map<CorrelationIdentifier, ComparisonRange> prefixMap = Maps.newHashMap();
        final Map<CorrelationIdentifier, ComparisonRange> parameterBindingMap =
                matchInfo.getParameterBindingMap();

        final List<CorrelationIdentifier> parameters = getParameters();
        for (final CorrelationIdentifier parameter : parameters) {
            Objects.requireNonNull(parameter);
            @Nullable final ComparisonRange comparisonRange = parameterBindingMap.get(parameter);
            if (comparisonRange == null) {
                return ImmutableMap.copyOf(prefixMap);
            }
            if (prefixMap.containsKey(parameter)) {
                Verify.verify(prefixMap.get(parameter).equals(comparisonRange));
                continue;
            }

            switch (comparisonRange.getRangeType()) {
                case EQUALITY:
                    prefixMap.put(parameter, comparisonRange);
                    break;
                case INEQUALITY:
                    prefixMap.put(parameter, comparisonRange);
                    return ImmutableMap.copyOf(prefixMap);
                case EMPTY:
                default:
                    return ImmutableMap.copyOf(prefixMap);
            }
        }

        return ImmutableMap.copyOf(prefixMap);
    }

    /**
     * Creates a logical expression that represents a scan over the materialized candidate data.
     * @param matchInfo the match info to be used
     * @return a new {@link RelationalExpression}
     */
    @SuppressWarnings("java:S135")
    default RelationalExpression toScanExpression(@Nonnull final MatchInfo matchInfo) {
        final Map<CorrelationIdentifier, ComparisonRange> prefixMap = computeBoundParameterPrefixMap(matchInfo);

        final ImmutableList.Builder<ComparisonRange> comparisonRangesForScanBuilder =
                ImmutableList.builder();

        // iterate through the parameters in order -- stop:
        // 1. if the current mapping does not exist
        // 2. the current mapping is EMPTY
        // 3. after the current mapping if the mapping is an INEQUALITY
        for (final CorrelationIdentifier parameterAlias : getParameters()) {
            // get the mapped side
            if (!prefixMap.containsKey(parameterAlias)) {
                break;
            }
            comparisonRangesForScanBuilder.add(prefixMap.get(parameterAlias));
        }

        return toScanExpression(comparisonRangesForScanBuilder.build(), matchInfo.isReverse());
    }

    /**
     * Creates a logical expression that represents a scan over the materialized candidate data. This method is expected
     * to be implemented by specific implementations of {@link MatchCandidate}.
     * @param comparisonRanges a {@link List} of {@link ComparisonRange}s to be applied
     * @param isReverse an indicator whether this expression should conceptually flow data in an ascending (forward) or
     *        descending (backward or reverse) order
     * @return a new {@link RelationalExpression}
     */
    @Nonnull
    RelationalExpression toScanExpression(@Nonnull final List<ComparisonRange> comparisonRanges, final boolean isReverse);

    @Nonnull
    default SetMultimap<ExpressionRef<? extends RelationalExpression>, RelationalExpression> findReferencingExpressions(@Nonnull final ImmutableList<? extends ExpressionRef<? extends RelationalExpression>> references) {
        final ExpressionRefTraversal traversal = getTraversal();

        final SetMultimap<ExpressionRef<? extends RelationalExpression>, RelationalExpression> refToExpressionMap =
                Multimaps.newSetMultimap(new IdentityHashMap<>(), Sets::newIdentityHashSet);

        // going up may yield duplicates -- deduplicate with this multimap
        for (final ExpressionRef<? extends RelationalExpression> rangesOverRef : references) {
            final Set<PartialMatch> partialMatchesForCandidate = rangesOverRef.getPartialMatchesForCandidate(this);
            for (final PartialMatch partialMatch : partialMatchesForCandidate) {
                for (final ExpressionRefTraversal.ReferencePath parentReferencePath : traversal.getParentRefPaths(partialMatch.getCandidateRef())) {
                    refToExpressionMap.put(parentReferencePath.getReference(), parentReferencePath.getExpression());
                }
            }
        }
        return refToExpressionMap;
    }
}
