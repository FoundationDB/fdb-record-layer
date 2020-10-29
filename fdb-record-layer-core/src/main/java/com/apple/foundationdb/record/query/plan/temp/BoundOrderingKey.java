/*
 * BoundOrderingKey.java
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

import javax.annotation.Nonnull;

/**
 * A key expression that can be bound by a comparison.
 */
public class BoundOrderingKey {
    @Nonnull
    private final KeyExpression normalizedKeyExpression;

    @Nonnull
    private final ComparisonRange.Type comparisonRangeType;

    private BoundOrderingKey(@Nonnull final KeyExpression normalizedKeyExpression, @Nonnull final ComparisonRange.Type comparisonRangeType) {
        this.normalizedKeyExpression = normalizedKeyExpression;
        this.comparisonRangeType = comparisonRangeType;
    }

    @Nonnull
    public KeyExpression getNormalizedKeyExpression() {
        return normalizedKeyExpression;
    }

    @Nonnull
    public ComparisonRange.Type getComparisonRangeType() {
        return comparisonRangeType;
    }

    @Nonnull
    public static BoundOrderingKey of(@Nonnull final KeyExpression normalizedKeyExpression, @Nonnull final ComparisonRange.Type comparisonRangeType) {
        return new BoundOrderingKey(normalizedKeyExpression, comparisonRangeType);
    }
}
