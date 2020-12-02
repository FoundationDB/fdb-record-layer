/*
 * BoundKeyPart.java
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
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.predicates.ValueComparisonRangePredicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

/**
 * A key expression that can be bound by a comparison.
 */
public class BoundKeyPart extends KeyPart {
    @Nullable
    private final QueryPredicate queryPredicate;

    @Nullable
    private final QueryPredicate candidatePredicate;

    private BoundKeyPart(@Nonnull final KeyExpression normalizedKeyExpression,
                         @Nonnull final ComparisonRange.Type comparisonRangeType,
                         @Nullable final QueryPredicate queryPredicate,
                         @Nullable final QueryPredicate candidatePredicate) {
        super(normalizedKeyExpression, comparisonRangeType);
        this.queryPredicate = queryPredicate;
        this.candidatePredicate = candidatePredicate;
    }

    @Nullable
    public QueryPredicate getQueryPredicate() {
        return queryPredicate;
    }

    @Nullable
    public QueryPredicate getCandidatePredicate() {
        return candidatePredicate;
    }

    public Optional<CorrelationIdentifier> getParameterAlias() {
        if (candidatePredicate == null) {
            return Optional.empty();
        }

        if (!(candidatePredicate instanceof ValueComparisonRangePredicate.Placeholder)) {
            return Optional.empty();
        }

        return Optional.of(((ValueComparisonRangePredicate.Placeholder)candidatePredicate).getParameterAlias());
    }

    @Nonnull
    public static BoundKeyPart of(@Nonnull final KeyExpression normalizedKeyExpression,
                                  @Nonnull final ComparisonRange.Type comparisonRangeType,
                                  @Nullable final QueryPredicate queryPredicate,
                                  @Nullable final QueryPredicate candidatePredicate) {
        return new BoundKeyPart(normalizedKeyExpression, comparisonRangeType, queryPredicate, candidatePredicate);
    }
}
