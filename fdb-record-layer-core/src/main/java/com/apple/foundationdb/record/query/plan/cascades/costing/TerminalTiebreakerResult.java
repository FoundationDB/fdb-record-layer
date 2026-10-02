/*
 * TerminalTiebreakerResult.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.costing;

import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.Set;

class TerminalTiebreakerResult<T extends RelationalExpression> implements TiebreakerResult<T> {
    @Nullable
    private final T bestExpression;

    public TerminalTiebreakerResult(@Nonnull final Set<T> bestExpressions) {
        this.bestExpression = Iterables.getOnlyElement(bestExpressions, null);
    }

    @Nonnull
    @Override
    public final TiebreakerResult<T> thenApply(@Nonnull final Tiebreaker<? super T> nextTiebreakers) {
        return this;
    }

    @Nonnull
    @Override
    public Set<T> getBestExpressions() {
        return bestExpression == null ? ImmutableSet.of() : ImmutableSet.of(bestExpression);
    }

    @Nonnull
    @Override
    public Optional<T> getOnlyExpressionMaybe() {
        return Optional.ofNullable(bestExpression);
    }
}
