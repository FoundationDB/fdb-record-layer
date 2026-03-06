/*
 * StableSelectorCostModel.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.FindExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

/**
 * A comparator implementing a simple cost model for the {@link CascadesPlanner} to choose the plan with the smallest
 * plan hash.
 */
@API(API.Status.EXPERIMENTAL)
@SpotBugsSuppressWarnings("SE_COMPARATOR_SHOULD_BE_SERIALIZABLE")
public class StableSelectorCostModel implements CascadesCostModel<RelationalExpression> {
    @Nonnull
    private static final Set<Class<? extends RelationalExpression>> interestingExpressionClasses =
            ImmutableSet.of();

    @Nonnull
    private static final Tiebreaker<RelationalExpression> tiebreaker =
            Tiebreaker.combineTiebreakers(ImmutableList.of(
                    RewritingCostModel.semanticHashTiebreaker(),
                    PickLeftTiebreaker.pickLeftTiebreaker()));

    @Nonnull
    @Override
    public RecordQueryPlannerConfiguration getConfiguration() {
        return RecordQueryPlannerConfiguration.defaultPlannerConfiguration();
    }

    @Nonnull
    @Override
    public Optional<RelationalExpression> getBestExpression(@Nonnull final Set<? extends RelationalExpression> expressions,
                                                            @Nonnull final Consumer<RelationalExpression> onRemoveConsumer) {
        return costExpressions(expressions, onRemoveConsumer).getOnlyExpressionMaybe();
    }

    @Nonnull
    @Override
    public Set<RelationalExpression> getBestExpressions(@Nonnull final Set<? extends RelationalExpression> expressions,
                                                        @Nonnull final Consumer<RelationalExpression> onRemoveConsumer) {
        return costExpressions(expressions, onRemoveConsumer).getBestExpressions();
    }

    @Nonnull
    private TiebreakerResult<RelationalExpression> costExpressions(@Nonnull final Set<? extends RelationalExpression> expressions,
                                                                   @Nonnull final Consumer<RelationalExpression> onRemoveConsumer) {
        final LoadingCache<RelationalExpression, Map<Class<? extends RelationalExpression>, Set<RelationalExpression>>> opsCache =
                createOpsCache();

        return Tiebreaker.ofContext(getConfiguration(), opsCache, expressions, RelationalExpression.class, onRemoveConsumer)
                .thenApply(tiebreaker);
    }

    @Nullable
    public Integer compare(@Nonnull final RelationalExpression a,
                           @Nonnull final RelationalExpression b) {
        return tiebreaker.compare(getConfiguration(), ImmutableMap.of(), ImmutableMap.of(), a, b);
    }

    @Nonnull
    private static LoadingCache<RelationalExpression, Map<Class<? extends RelationalExpression>, Set<RelationalExpression>>>
            createOpsCache() {
        return CacheBuilder.newBuilder()
                .build(new CacheLoader<>() {
                    @Override
                    @Nonnull
                    public Map<Class<? extends RelationalExpression>, Set<RelationalExpression>>
                            load(@Nonnull final RelationalExpression key) {
                        return FindExpressionVisitor.evaluate(interestingExpressionClasses, key);
                    }
                });
    }
}
