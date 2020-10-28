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

import com.apple.foundationdb.record.RecordCoreException;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

/**
 * Case class to represent a match candidate. A match candidate on code level is just a name and a data flow graph
 * that can be matches against a query graph. The match candidate does not keep the root to the graph to be matched but
 * rather an instance of {@link ExpressionRefTraversal} to allow for navigation of references within the candidate.
 */
public interface MatchCandidate {
    /**
     * Returns the name of the match candidate. If this candidate represents and index, it will be the name of the index.
     */
    @Nonnull
    String getName();

    /**
     * Returns the traversal object for this candidate.
     */
    @Nonnull
    ExpressionRefTraversal getTraversal();

    /**
     * Returns the parameter names for all necessary parameters that need to be bound during matching.
     */
    @Nonnull
    List<CorrelationIdentifier> getParameters();

    @SuppressWarnings("java:S135")
    default RelationalExpression toScanExpression(@Nonnull final MatchWithCompensation matchWithCompensation) {
        // this match is complete
        final Map<CorrelationIdentifier, ComparisonRange> parameterBindingMap =
                matchWithCompensation.getParameterBindingMap();

        final ImmutableList.Builder<ComparisonRange> comparisonRangesForScanBuilder =
                ImmutableList.builder();

        // iterate through the parameters in order -- stop:
        // 1. if the current mapping does not exist
        // 2. the current mapping is EMPTY
        // 3. after the current mapping if the mapping is an INEQUALITY
        for (final CorrelationIdentifier parameterAlias : getParameters()) {
            // get the mapped side
            if (!parameterBindingMap.containsKey(parameterAlias)) {
                break;
            }
            final ComparisonRange comparisonRange = parameterBindingMap.get(parameterAlias);
            switch (comparisonRange.getRangeType()) {
                case EMPTY:
                    break;
                case EQUALITY:
                case INEQUALITY:
                    comparisonRangesForScanBuilder.add(comparisonRange);
                    break;
                default:
                    throw new RecordCoreException("unknown range comparison type");
            }

            if (!comparisonRange.isEquality()) {
                break;
            }
        }

        return toScanExpression(comparisonRangesForScanBuilder.build());
    }

    @Nonnull
    RelationalExpression toScanExpression(@Nonnull final List<ComparisonRange> comparisonRanges);
}
