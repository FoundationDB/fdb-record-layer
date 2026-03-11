/*
 * RewritingCostModel.java
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
import com.apple.foundationdb.record.query.plan.cascades.FindExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.properties.PredicateCountByLevelProperty;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static com.apple.foundationdb.record.query.plan.cascades.properties.ExpressionCountProperty.selectCount;
import static com.apple.foundationdb.record.query.plan.cascades.properties.ExpressionCountProperty.tableFunctionCount;
import static com.apple.foundationdb.record.query.plan.cascades.properties.NormalizedResidualPredicateProperty.countNormalizedConjuncts;
import static com.apple.foundationdb.record.query.plan.cascades.properties.PredicateCountByLevelProperty.predicateCountByLevel;

/**
 * Cost model for {@link PlannerPhase#REWRITING}. TODO To be fleshed out whe we have actual rules.
 */
@API(API.Status.EXPERIMENTAL)
@SpotBugsSuppressWarnings("SE_COMPARATOR_SHOULD_BE_SERIALIZABLE")
public class RewritingCostModel implements CascadesCostModel<RelationalExpression> {
    @Nonnull
    private static final Set<Class<? extends RelationalExpression>> interestingExpressionClasses =
            ImmutableSet.of();
    @Nonnull
    private static final Tiebreaker<RelationalExpression> tiebreaker =
            Tiebreaker.combineTiebreakers(ImmutableList.of(
                    lowestNumSelectExpressionsTiebreaker(),
                    lowestNumTableFunctionsTiebreaker(),
                    fewestNormalizedConjunctsTiebreaker(),
                    deepestPredicatesTiebreaker(),
                    semanticHashTiebreaker(),
                    PickRightTiebreaker.pickRightTiebreaker()));

    @Nonnull
    private final RecordQueryPlannerConfiguration configuration;

    public RewritingCostModel(@Nonnull final RecordQueryPlannerConfiguration configuration) {
        this.configuration = configuration;
    }

    @Nonnull
    @Override
    public RecordQueryPlannerConfiguration getConfiguration() {
        return configuration;
    }

    @Nonnull
    @Override
    public Optional<RelationalExpression> getBestExpression(@Nonnull final Set<? extends RelationalExpression> expressions,
                                                            @Nonnull final Consumer<RelationalExpression> onRemoveConsumer) {
        return costExpressions(expressions, onRemoveConsumer).getOnlyExpressionMaybe();
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
    @Override
    public Integer compare(@Nonnull final RelationalExpression a,
                           @Nonnull final RelationalExpression b) {
        return tiebreaker.compare(getConfiguration(), FindExpressionVisitor.evaluate(interestingExpressionClasses, a), FindExpressionVisitor.evaluate(interestingExpressionClasses, b), a, b);
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

    @Nonnull
    static LowestNumSelectExpressionsTiebreaker lowestNumSelectExpressionsTiebreaker() {
        return LowestNumSelectExpressionsTiebreaker.INSTANCE;
    }

    @Nonnull
    static LowestNumTableFunctionsTiebreaker lowestNumTableFunctionsTiebreaker() {
        return LowestNumTableFunctionsTiebreaker.INSTANCE;
    }

    @Nonnull
    static NormalizedConjuctsTiebreaker fewestNormalizedConjunctsTiebreaker() {
        return NormalizedConjuctsTiebreaker.INSTANCE;
    }

    @Nonnull
    static PredicateCountByLevelTiebreaker deepestPredicatesTiebreaker() {
        return PredicateCountByLevelTiebreaker.INSTANCE;
    }

    @Nonnull
    static SemanticHashTiebreaker semanticHashTiebreaker() {
        return SemanticHashTiebreaker.INSTANCE;
    }

    static class LowestNumSelectExpressionsTiebreaker implements Tiebreaker<RelationalExpression> {
        private static final LowestNumSelectExpressionsTiebreaker INSTANCE = new LowestNumSelectExpressionsTiebreaker();

        @Override
        public int compare(@Nonnull final RecordQueryPlannerConfiguration configuration,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB,
                           @Nonnull final RelationalExpression a, @Nonnull final RelationalExpression b) {
            //
            // Choose the expression with the fewest select boxes
            //
            final int aSelects = selectCount().evaluate(a);
            final int bSelects = selectCount().evaluate(b);
            return Integer.compare(aSelects, bSelects);
        }
    }

    static class LowestNumTableFunctionsTiebreaker implements Tiebreaker<RelationalExpression> {
        private static final LowestNumTableFunctionsTiebreaker INSTANCE = new LowestNumTableFunctionsTiebreaker();

        @Override
        public int compare(@Nonnull final RecordQueryPlannerConfiguration configuration,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB,
                           @Nonnull final RelationalExpression a, @Nonnull final RelationalExpression b) {
            //
            // Choose the expression with the fewest TableFunction expressions
            //
            final int aTableFunctions = tableFunctionCount().evaluate(a);
            final int bTableFunctions = tableFunctionCount().evaluate(b);
            return Integer.compare(aTableFunctions, bTableFunctions);
        }
    }

    static class NormalizedConjuctsTiebreaker implements Tiebreaker<RelationalExpression> {
        private static final NormalizedConjuctsTiebreaker INSTANCE = new NormalizedConjuctsTiebreaker();

        @Override
        public int compare(@Nonnull final RecordQueryPlannerConfiguration configuration,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB,
                           @Nonnull final RelationalExpression a,
                           @Nonnull final RelationalExpression b) {
            //
            // Pick the expression which has the least number of conjuncts in the normalized form of
            // its combined query predicates.
            //
            final long aNormalizedConjuncts = countNormalizedConjuncts(a);
            final long bNormalizedConjuncts = countNormalizedConjuncts(b);
            return Long.compare(aNormalizedConjuncts, bNormalizedConjuncts);
        }
    }

    static class PredicateCountByLevelTiebreaker implements Tiebreaker<RelationalExpression> {
        private static final PredicateCountByLevelTiebreaker INSTANCE = new PredicateCountByLevelTiebreaker();

        @Override
        public int compare(@Nonnull final RecordQueryPlannerConfiguration configuration,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB,
                           @Nonnull final RelationalExpression a,
                           @Nonnull final RelationalExpression b) {
            //
            // Pick the expression which has a higher number of query predicates at a deeper level of
            // the expression tree.
            //
            final PredicateCountByLevelProperty.PredicateCountByLevelInfo aPredicateCountByLevel = predicateCountByLevel().evaluate(a);
            final PredicateCountByLevelProperty.PredicateCountByLevelInfo bPredicateCountByLevel = predicateCountByLevel().evaluate(b);
            return PredicateCountByLevelProperty.PredicateCountByLevelInfo.compare(bPredicateCountByLevel, aPredicateCountByLevel);
        }
    }

    static class SemanticHashTiebreaker implements Tiebreaker<RelationalExpression> {
        private static final SemanticHashTiebreaker INSTANCE = new SemanticHashTiebreaker();

        @Override
        public int compare(@Nonnull final RecordQueryPlannerConfiguration configuration,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB,
                           @Nonnull final RelationalExpression a, @Nonnull final RelationalExpression b) {
            final int aSemanticHash = a.semanticHashCode();
            final int bSemanticHash = b.semanticHashCode();
            return Integer.compare(aSemanticHash, bSemanticHash);
        }
    }
}
