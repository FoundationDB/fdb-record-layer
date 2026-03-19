/*
 * TiebreakerResultWithNext.java
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

import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.common.base.Verify;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

class TiebreakerResultWithNext<T extends RelationalExpression> implements TiebreakerResult<T> {
    @Nonnull
    private final RecordQueryPlannerConfiguration plannerConfiguration;
    @Nonnull
    private final LoadingCache<RelationalExpression, Map<Class<? extends RelationalExpression>, Set<RelationalExpression>>> opsCache;
    @Nonnull
    private final Set<T> expressions;
    @Nonnull
    private final Consumer<T> onRemoveConsumer;

    TiebreakerResultWithNext(@Nonnull final RecordQueryPlannerConfiguration plannerConfiguration,
                             @Nonnull final LoadingCache<RelationalExpression, Map<Class<? extends RelationalExpression>, Set<RelationalExpression>>> opsCache,
                             @Nonnull final Set<T> expressions,
                             @Nonnull final Consumer<T> onRemoveConsumer) {
        this.plannerConfiguration = plannerConfiguration;
        this.opsCache = opsCache;
        this.expressions = expressions;
        this.onRemoveConsumer = onRemoveConsumer;
    }

    @Nonnull
    @Override
    public final TiebreakerResult<T> thenApply(@Nonnull final Tiebreaker<? super T> nextTiebreaker) {
        if (expressions.size() <= 1) {
            return new TerminalTiebreakerResult<>(expressions);
        }
        final var bestExpressions =
                expressions.stream()
                        .collect(Tiebreaker.toBestExpressions(plannerConfiguration,
                                nextTiebreaker, opsCache, onRemoveConsumer));

        if (bestExpressions.size() > 1) {
            return new TiebreakerResultWithNext<>(plannerConfiguration, opsCache, bestExpressions, onRemoveConsumer);
        } else {
            return new TerminalTiebreakerResult<>(bestExpressions);
        }
    }

    @Nonnull
    @Override
    public Set<T> getBestExpressions() {
        return expressions;
    }

    @Nonnull
    @Override
    public Optional<T> getOnlyExpressionMaybe() {
        Verify.verify(expressions.size() <= 1);
        return Optional.ofNullable(Iterables.getOnlyElement(expressions, null));
    }
}
