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
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A key expression that can be bound by a comparison during graph matching.
 */
public class BoundKeyPart {
    @Nonnull
    private final KeyPart keyPart;

    @Nonnull
    private final ComparisonRange.Type comparisonRangeType;

    @Nullable
    private final QueryPredicate queryPredicate;

    /**
     * Constructor.
     * @param normalizedKeyExpression normalized key expression as an alternative representation of this part
     * @param comparisonRangeType type of comparison
     * @param queryPredicate reference to {@link QueryPredicate} on query side
     */
    private BoundKeyPart(@Nonnull final KeyExpression normalizedKeyExpression,
                         @Nonnull final ComparisonRange.Type comparisonRangeType,
                         @Nullable final QueryPredicate queryPredicate,
                         final boolean isReverse) {
        Preconditions.checkArgument((queryPredicate == null && comparisonRangeType == ComparisonRange.Type.EMPTY) ||
                                    (queryPredicate != null && comparisonRangeType != ComparisonRange.Type.EMPTY));

        this.keyPart = KeyPart.of(normalizedKeyExpression, isReverse);
        this.comparisonRangeType = comparisonRangeType;
        this.queryPredicate = queryPredicate;
    }

    @Nonnull
    public KeyPart getKeyPart() {
        return keyPart;
    }

    @Nonnull
    public KeyExpression getNormalizedKeyExpression() {
        return keyPart.getNormalizedKeyExpression();
    }

    public boolean isReverse() {
        return keyPart.isReverse();
    }

    @Nullable
    public QueryPredicate getQueryPredicate() {
        return queryPredicate;
    }

    @Nonnull
    public ComparisonRange.Type getComparisonRangeType() {
        return comparisonRangeType;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BoundKeyPart)) {
            return false;
        }
        final BoundKeyPart that = (BoundKeyPart)o;
        return getKeyPart().equals(that.getKeyPart()) &&
               Objects.equals(getQueryPredicate(), that.getQueryPredicate());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getKeyPart().hashCode(), getQueryPredicate());
    }

    @Nonnull
    public static BoundKeyPart of(@Nonnull final KeyExpression normalizedKeyExpression,
                                  @Nonnull final ComparisonRange.Type comparisonRangeType,
                                  @Nullable final QueryPredicate queryPredicate,
                                  final boolean isReverse) {
        return new BoundKeyPart(normalizedKeyExpression, comparisonRangeType, queryPredicate, isReverse);
    }
}
