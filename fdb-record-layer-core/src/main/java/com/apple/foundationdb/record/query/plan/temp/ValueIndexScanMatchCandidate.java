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

import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.IndexScanExpression;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Case class to represent a match candidate that is backed by an index.
 */
public class ValueIndexScanMatchCandidate implements MatchCandidate {
    /**
     * Name of the match candidate. This name is also the name of the index.
     */
    @Nonnull
    private final String name;

    /**
     * Holds the parameter names for all necessary parameters that need to be bound during matching.
     */
    @Nonnull
    private final List<CorrelationIdentifier> parameters;

    /**
     * Traversal object of the expanded index scan graph.
     */
    @Nonnull
    private final ExpressionRefTraversal traversal;

    @Nonnull
    private final KeyExpression alternativeKeyExpression;

    public ValueIndexScanMatchCandidate(@Nonnull String name,
                                        @Nonnull final ExpressionRefTraversal traversal,
                                        @Nonnull final List<CorrelationIdentifier> parameters,
                                        @Nonnull final KeyExpression alternativeKeyExpression) {
        this.name = name;
        this.traversal = traversal;
        this.parameters = ImmutableList.copyOf(parameters);
        this.alternativeKeyExpression = alternativeKeyExpression;
    }

    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    @Nonnull
    @Override
    public ExpressionRefTraversal getTraversal() {
        return traversal;
    }

    @Nonnull
    @Override
    public List<CorrelationIdentifier> getParameters() {
        return parameters;
    }

    @Nonnull
    @Override
    public KeyExpression getAlternativeKeyExpression() {
        return alternativeKeyExpression;
    }

    @Nonnull
    @Override
    public RelationalExpression toScanExpression(@Nonnull final List<ComparisonRange> comparisonRanges, final boolean isReverse) {
        return new IndexScanExpression(getName(),
                IndexScanType.BY_VALUE,
                comparisonRanges,
                isReverse);
    }
}
