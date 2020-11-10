/*
 * Compensation.java
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.temp.expressions.SelectExpression;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import javax.annotation.Nonnull;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * Interface for all kinds of compensation.
 */
public interface Compensation extends Function<ExpressionRef<RelationalExpression>, RelationalExpression> {
    Compensation NO_COMPENSATION = new Compensation() {
        @Override
        public boolean isNeeded() {
            return false;
        }

        @Nonnull
        @Override
        public Compensation and(@Nonnull final Compensation otherCompensation) {
            return this;
        }

        @Override
        public RelationalExpression apply(final ExpressionRef<RelationalExpression> reference) {
            throw new RecordCoreException("this method should not be called");
        }
    };

    default boolean isNeeded() {
        return true;
    }

    @Nonnull
    default Compensation and(@Nonnull Compensation otherCompensation) {
        if (!isNeeded() || !otherCompensation.isNeeded()) {
            return noCompensation();
        }

        return new Compensation() {
            @Nonnull
            @Override
            public RelationalExpression apply(@Nonnull final ExpressionRef<RelationalExpression> reference) {
                if (!otherCompensation.isNeeded()) {
                    return apply(reference);
                }

                if (!isNeeded()) {
                    return otherCompensation.apply(reference);
                }

                return apply(
                        GroupExpressionRef.of(otherCompensation
                                .apply(reference)));
            }
        };
    }

    @Nonnull
    static Compensation noCompensation() {
        return NO_COMPENSATION;
    }

    @Nonnull
    static Compensation of(final Map<QueryPredicate, QueryPredicate> predicateCompensationMap) {
        return predicateCompensationMap.isEmpty() ? noCompensation() : new ForMatch(predicateCompensationMap);
    }

    /**
     * Interface for {@link Compensation}s that map original {@link QueryPredicate}s to compensating {@link QueryPredicate}s.
     */
    interface WithPredicateCompensation extends Compensation {
        @Nonnull
        Map<QueryPredicate, QueryPredicate> getPredicateCompensationMap();

        @Nonnull
        WithPredicateCompensation withPredicateCompensationMap(@Nonnull IdentityHashMap<QueryPredicate, QueryPredicate> predicateCompensationMap);

        @Nonnull
        @Override
        default Compensation and(@Nonnull Compensation otherCompensation) {
            if (!(otherCompensation instanceof WithPredicateCompensation)) {
                return Compensation.super.and(otherCompensation);
            }
            final WithPredicateCompensation otherWithPredicateCompensation = (WithPredicateCompensation)otherCompensation;

            final IdentityHashMap<QueryPredicate, QueryPredicate> combinedMap = Maps.newIdentityHashMap();
            final Map<QueryPredicate, QueryPredicate> otherCompensationMap = otherWithPredicateCompensation.getPredicateCompensationMap();
            for (final Map.Entry<QueryPredicate, QueryPredicate> entry : getPredicateCompensationMap().entrySet()) {
                // if the other side does not have compensation for this key, we don't need compensation
                if (otherCompensationMap.containsKey(entry.getKey())) {
                    // we just pick one side
                    combinedMap.put(entry.getKey(), entry.getValue());
                }
            }

            return combinedMap.isEmpty() ? noCompensation() : withPredicateCompensationMap(combinedMap);
        }
    }

    /**
     * Regular compensation class for matches.
     */
    class ForMatch implements WithPredicateCompensation {
        @Nonnull
        final Map<QueryPredicate, QueryPredicate> predicateCompensationMap;

        public ForMatch(@Nonnull final Map<QueryPredicate, QueryPredicate> predicateCompensationMap) {
            this.predicateCompensationMap = Maps.newIdentityHashMap();
            this.predicateCompensationMap.putAll(predicateCompensationMap);
        }

        @Nonnull
        @Override
        public Map<QueryPredicate, QueryPredicate> getPredicateCompensationMap() {
            return predicateCompensationMap;
        }

        @Nonnull
        @Override
        public WithPredicateCompensation withPredicateCompensationMap(@Nonnull final IdentityHashMap<QueryPredicate, QueryPredicate> predicateCompensationMap) {
            Verify.verify(!predicateCompensationMap.isEmpty());
            return new ForMatch(predicateCompensationMap);
        }

        @Override
        public RelationalExpression apply(final ExpressionRef<RelationalExpression> reference) {
            return new SelectExpression(ImmutableList.of(Quantifier.forEach(reference)), ImmutableList.copyOf(predicateCompensationMap.values()));
        }
    }

    static Collector<Compensation, ?, Compensation> toCompensation() {
        return new Collector<Compensation, ImmutableList.Builder<Compensation>, Compensation>() {
            @Override
            public Supplier<ImmutableList.Builder<Compensation>> supplier() {
                return ImmutableList.Builder::new;
            }

            @Override
            public BiConsumer<ImmutableList.Builder<Compensation>, Compensation> accumulator() {
                return ImmutableList.Builder::add;
            }

            @Override
            public BinaryOperator<ImmutableList.Builder<Compensation>> combiner() {
                return (container1, container2) -> container1.addAll(container2.build());
            }

            @Override
            public Function<ImmutableList.Builder<Compensation>, Compensation> finisher() {
                return container -> {
                    Compensation folded = Compensation.noCompensation();
                    for (final Compensation compensation : container.build()) {
                        folded = folded.and(compensation);
                    }
                    return folded;
                };
            }

            @Override
            public Set<Characteristics> characteristics() {
                return ImmutableSet.of(Characteristics.UNORDERED);
            }
        };
    }
}
