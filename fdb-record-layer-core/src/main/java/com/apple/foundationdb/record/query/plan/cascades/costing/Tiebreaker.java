/*
 * Tiebreaker.java
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.FindExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * An interface encapsulating comparisons between two expressions. This is used by the {@link CascadesCostModel}
 * to evaluate which plans are more desirable during planner pruning. This is akin to a {@link java.util.Comparator}
 * with two major differences:
 *
 * <ul>
 *     <li>
 *         The {@link #compare(RecordQueryPlannerConfiguration, Map, Map, RelationalExpression, RelationalExpression) compare()}
 *         method is enriched with additional context collected by the cost model and cached to avoid duplicated effort.
 *     </li>
 *     <li>
 *         Objects of this class are intended to be composed together, hence "tiebreaker". Each {@link CascadesCostModel}
 *         implementation strings together a series of these tiebreakers and will favor plans that compare better
 *         given earlier tiebreakers before continuing down the list. See {@link #combineTiebreakers(List)}.
 *     </li>
 * </ul>
 *
 * @param <T> the type of expressions being compared
 */
interface Tiebreaker<T extends RelationalExpression> {

    /**
     * Compare two expressions. Like a {@link java.util.Comparator}, this should return a
     * negative value if the expression {@code a} is more optimal, a positive value if
     * the expression {@code b} is more optimal, or zero if the two expressions cannot
     * be distinguished. To avoid needing to re-calculate the number of sub-expressions
     * of each type, each implementation also has access to a cached {@code opsMap}, which
     * contains all sub-expressions of {@code a} or {@code b} grouped by their class.
     *
     * @param configuration general planner configuration options that may affect some implementations' parameters
     * @param opsMapA a cached set of sub-expressions of {@code a} grouped by each expression's class
     * @param opsMapB a similar map computed for {@code b}
     * @param a the left expression to compare
     * @param b the right expression to compare
     * @return a negative value if {@code a} is preferred, a positive value if {@code b} is preferred, and
     *        zero if they are not distinguished by this implementation
     */
    int compare(@Nonnull RecordQueryPlannerConfiguration configuration,
                @Nonnull Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA,
                @Nonnull Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB,
                @Nonnull T a, @Nonnull T b);

    /**
     * Construct a single {@link Tiebreaker} combining the logic of multiple other tiebreakers.
     * The resulting tiebreaker will return a comparison that is identical to the first non-zero
     * result that would have been returned by the input tiebreakers, and it returns zero if
     * all the input tiebreakers return zero.
     *
     * @param tiebreakers a collection of {@link Tiebreaker}s to combine
     * @param <T> type of expressions being compared by the resulting {@link Tiebreaker}
     * @return a {@link Tiebreaker} based on the given {@code tiebreakers}
     */
    @Nonnull
    static <T extends RelationalExpression> Tiebreaker<T> combineTiebreakers(@Nonnull final List<Tiebreaker<? super T>> tiebreakers) {
        return (configuration, opsMapA, opsMapB, a, b) -> {
            int compareResult = 0;
            for (final Tiebreaker<? super T> tiebreaker : tiebreakers) {
                compareResult = tiebreaker.compare(configuration, opsMapA, opsMapB, a, b);
                if (compareResult != 0) {
                    return compareResult;
                }
            }
            return compareResult;
        };
    }

    @Nonnull
    static <T extends RelationalExpression> TiebreakerResult<T>
            ofContext(@Nonnull final RecordQueryPlannerConfiguration plannerConfiguration,
                      @Nonnull final Set<Class<? extends RelationalExpression>> interestingExpressionClasses,
                      @Nonnull final Set<? extends RelationalExpression> expressions,
                      @Nonnull final Class<T> specificClazz,
                      @Nonnull final Consumer<T> onRemoveConsumer) {
        final var filteredExpressions = new LinkedIdentitySet<T>();
        for (final var expression : expressions) {
            if (specificClazz.isInstance(expression)) {
                filteredExpressions.add(specificClazz.cast(expression));
            }
        }

        if (filteredExpressions.size() <= 1) {
            return new TerminalTiebreakerResult<>(filteredExpressions);
        }

        final var opsCache = createOpsCache(interestingExpressionClasses);
        return new TiebreakerResultWithNext<>(plannerConfiguration, opsCache, filteredExpressions, onRemoveConsumer);
    }

    /**
     * Collect a set of the best elements from a stream of expressions according to a given {@link Tiebreaker}.
     * This uses the {@link Tiebreaker} to compare plans as they are collected. It will return a set which
     * should have the following two properties:
     *
     * <ul>
     *     <li>Using the tiebreaker to compare any pair of expressions in the set should return zero.</li>
     *     <li>Any expression in the returned set should compare as more optimal than any expression discarded by the collector.</li>
     * </ul>
     *
     * @param plannerConfiguration general planner configuration options that may affect some implementations' parameters
     * @param tiebreaker tiebreaker to use to select the best equivalency class of plans
     * @param opsCache a cache to be used to produce the operations maps passed to {@link #compare(RecordQueryPlannerConfiguration, Map, Map, RelationalExpression, RelationalExpression)}
     * @param onRemoveConsumer a callback to be invoked whenever an expression is discarded from the stream
     * @param <T> the type of expressions in the stream
     * @return a collector that returns the most set of expressions considered the most optimal
     */
    @Nonnull
    static <T extends RelationalExpression> Collector<T, LinkedIdentitySet<T>, Set<T>>
             toBestExpressions(@Nonnull final RecordQueryPlannerConfiguration plannerConfiguration,
                               @Nonnull final Tiebreaker<? super T> tiebreaker,
                               @Nonnull final LoadingCache<RelationalExpression, Map<Class<? extends RelationalExpression>, Set<RelationalExpression>>> opsCache,
                               @Nonnull final Consumer<T> onRemoveConsumer) {
        return new BestExpressionsCollector<>(plannerConfiguration, tiebreaker, opsCache, onRemoveConsumer);
    }

    @VisibleForTesting
    @Nonnull
    static LoadingCache<RelationalExpression, Map<Class<? extends RelationalExpression>, Set<RelationalExpression>>> createOpsCache(Set<Class<? extends RelationalExpression>> interestingExpressionClasses) {
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

    /**
     * {@code Collector} implementation for {@link #toBestExpressions(RecordQueryPlannerConfiguration, Tiebreaker, LoadingCache, Consumer)}.
     *
     * @param <T> the type of elements to be collected
     * @see #toBestExpressions(RecordQueryPlannerConfiguration, Tiebreaker, LoadingCache, Consumer)
     */
    final class BestExpressionsCollector<T extends RelationalExpression> implements Collector<T, LinkedIdentitySet<T>, Set<T>> {
        @Nonnull
        private static final Set<Characteristics> characteristics = Collections.unmodifiableSet(
                EnumSet.of(Characteristics.IDENTITY_FINISH));
        @Nonnull
        private final RecordQueryPlannerConfiguration plannerConfiguration;
        @Nonnull
        private final Tiebreaker<? super T> tiebreaker;
        @Nonnull
        private final LoadingCache<RelationalExpression, Map<Class<? extends RelationalExpression>, Set<RelationalExpression>>> opsCache;
        @Nonnull
        private final Consumer<T> onRemoveConsumer;

        private BestExpressionsCollector(@Nonnull final RecordQueryPlannerConfiguration plannerConfiguration,
                                         @Nonnull final Tiebreaker<? super T> tiebreaker,
                                         @Nonnull final LoadingCache<RelationalExpression, Map<Class<? extends RelationalExpression>, Set<RelationalExpression>>> opsCache,
                                         @Nonnull final Consumer<T> onRemoveConsumer) {
            this.plannerConfiguration = plannerConfiguration;
            this.tiebreaker = tiebreaker;
            this.opsCache = opsCache;
            this.onRemoveConsumer = onRemoveConsumer;
        }

        @Override
        public BiConsumer<LinkedIdentitySet<T>, T> accumulator() {
            return (bestExpressions, newExpression) -> {
                // Pick a representative from the best expressions set and cost that against the new expression.
                // All the expressions in the current set of bestExpressions must be equivalent, so just take
                // the first one arbitrarily
                final var aBestExpression = Iterables.getFirst(bestExpressions, null);
                final int compare;
                if (aBestExpression == null) {
                    compare = -1;
                } else {
                    final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA;
                    final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB;
                    try {
                        opsMapA = opsCache.get(newExpression);
                        opsMapB = opsCache.get(aBestExpression);
                    } catch (ExecutionException eE) {
                        throw new RecordCoreException(eE);
                    }

                    compare =
                            tiebreaker.compare(plannerConfiguration, opsMapA, opsMapB,
                                    newExpression, aBestExpression);
                }

                if (compare < 0) {
                    // New expression is preferred to all the existing ones. Drop them
                    bestExpressions.forEach(onRemoveConsumer);
                    bestExpressions.clear();
                    bestExpressions.add(newExpression);
                } else if (compare > 0) {
                    // Existing expressions are preferred. Drop the new one
                    onRemoveConsumer.accept(newExpression);
                } else {
                    // New expression is equivalent to the existing ones. Add it to the set
                    bestExpressions.add(newExpression);
                }
            };
        }

        @Override
        public Supplier<LinkedIdentitySet<T>> supplier() {
            return LinkedIdentitySet::new;
        }

        @Override
        public BinaryOperator<LinkedIdentitySet<T>> combiner() {
            return (left, right) -> {
                // Each set contains an equivalency class of expressions, so take the first
                // one from each as representatives that will be used during the comparison
                final var aLeftBestExpression = Iterables.getFirst(left, null);
                if (aLeftBestExpression == null) {
                    return right;
                }
                final var aRightBestExpression = Iterables.getFirst(right, null);
                if (aRightBestExpression == null) {
                    return left;
                }

                final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> leftMap;
                final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> rightMap;
                try {
                    leftMap = opsCache.get(aLeftBestExpression);
                    rightMap = opsCache.get(aRightBestExpression);
                } catch (ExecutionException eE) {
                    throw new RecordCoreException(eE);
                }
                // Counter-intuitively, we need to put the right expressions on the left and the
                // left expressions on the right. The reason is that the accumulator puts each
                // new expression into the left as it iterates. If every tiebreaker were
                // antisymmetric, then we wouldn't need to care about this, but that's not quite
                // the case.
                // See: https://github.com/FoundationDB/fdb-record-layer/issues/3998
                final int compare =
                        tiebreaker.compare(plannerConfiguration, rightMap, leftMap,
                                aRightBestExpression, aLeftBestExpression);
                // Check if one set is more optimal than the other. Choose the preferred one, and
                // mark all the discarded elements as removed
                if (compare > 0) {
                    right.forEach(onRemoveConsumer);
                    return left;
                } else if (compare < 0) {
                    left.forEach(onRemoveConsumer);
                    return right;
                }
                // Two sets form a larger equivalency class. Combine them
                return new LinkedIdentitySet<>(Iterables.concat(left, right));
            };
        }

        @Override
        public Function<LinkedIdentitySet<T>, Set<T>> finisher() {
            return s -> s;
        }

        @Override
        public Set<Characteristics> characteristics() {
            return characteristics;
        }
    }
}
