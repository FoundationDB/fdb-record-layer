/*
 * AggregateIndexMatchCandidate.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Case class that represents a grouping index with an aggregate function(s).
 */
public class AggregateIndexMatchCandidate implements MatchCandidate {

    // The backing index metadata structure.
    @Nonnull
    private final Index index;

    public AggregateIndexMatchCandidate(@Nonnull final Index index) {
        this.index = index;
    }

    @Nonnull
    @Override
    public String getName() {
        return index.getName();
    }

    @Nonnull
    @Override
    public ExpressionRefTraversal getTraversal() {
        return null;
    }

    @Nonnull
    @Override
    public List<CorrelationIdentifier> getSargableAliases() {
        return null;
    }

    @Nonnull
    @Override
    public List<CorrelationIdentifier> getOrderingAliases() {
        return null;
    }

    @Nonnull
    @Override
    public KeyExpression getAlternativeKeyExpression() {
        return null;
    }

    @Nonnull
    @Override
    public List<BoundKeyPart> computeBoundKeyParts(@Nonnull final MatchInfo matchInfo, @Nonnull final List<CorrelationIdentifier> sortParameterIds, final boolean isReverse) {
        return null;
    }

    @Nonnull
    @Override
    public Ordering computeOrderingFromScanComparisons(@Nonnull final ScanComparisons scanComparisons, final boolean isReverse, final boolean isDistinct) {
        return null;
    }

    @Nonnull
    @Override
    public RelationalExpression toEquivalentExpression(@Nonnull final PartialMatch partialMatch, @Nonnull final PlanContext planContext, @Nonnull final List<ComparisonRange> comparisonRanges) {
        return null;
    }
}
