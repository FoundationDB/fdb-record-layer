/*
 * ExpressionPartitionMatchers.java
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

package com.apple.foundationdb.record.query.plan.cascades.matching.structure;

import com.apple.foundationdb.record.query.plan.cascades.ExpressionPartition;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionPartitions;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionProperty;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;
import java.util.function.Predicate;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcher.typed;

public class ExpressionsPartitionMatchers {
    private ExpressionsPartitionMatchers() {
        // do not instantiate
    }

    @SuppressWarnings("unchecked")
    public static BindingMatcher<Reference> expressionPartitions(final BindingMatcher<? extends Iterable<? extends ExpressionPartition<? extends RelationalExpression>>> downstream) {
        return TypedMatcherWithExtractAndDownstream.typedWithDownstream(Reference.class,
                Extractor.of(Reference::toExpressionPartitions, name -> "expressionPartitions(" + name + ")"),
                downstream);
    }

    @SuppressWarnings("unchecked")
    public static BindingMatcher<Collection<? extends ExpressionPartition<? extends RelationalExpression>>> filterPartition(final Predicate<ExpressionPartition<? extends RelationalExpression>> predicate,
                                                                                                                            final BindingMatcher<? extends Iterable<? extends ExpressionPartition<? extends RelationalExpression>>> downstream) {
        return TypedMatcherWithExtractAndDownstream.typedWithDownstream(
                (Class<Collection<? extends ExpressionPartition<? extends RelationalExpression>>>)(Class<?>)Collection.class,
                Extractor.of(planPartitions ->
                        planPartitions.stream()
                                .filter(predicate)
                                .collect(ImmutableList.toImmutableList()),
                        name -> "filtered expressionPartitions(" + name + ")"),
                downstream);
    }

    public static BindingMatcher<Collection<ExpressionPartition<RelationalExpression>>> rollUpPartitions(final BindingMatcher<? extends Iterable<ExpressionPartition<RelationalExpression>>> downstream) {
        return rollUpPartitionsTo(downstream, ImmutableSet.of());
    }

    public static BindingMatcher<Collection<ExpressionPartition<RelationalExpression>>> rollUpPartitionsTo(final BindingMatcher<? extends Iterable<ExpressionPartition<RelationalExpression>>> downstream,
                                                                                                           final ExpressionProperty<?> interestingProperty) {
        return rollUpPartitionsTo(downstream, ImmutableSet.of(interestingProperty));
    }

    @SuppressWarnings("unchecked")
    public static BindingMatcher<Collection<ExpressionPartition<RelationalExpression>>> rollUpPartitionsTo(final BindingMatcher<? extends Iterable<ExpressionPartition<RelationalExpression>>> downstream,
                                                                                                           final Set<ExpressionProperty<?>> interestingProperties) {
        return TypedMatcherWithExtractAndDownstream.typedWithDownstream(
                (Class<Collection<ExpressionPartition<RelationalExpression>>>)(Class<?>)Collection.class,
                Extractor.of(partitions -> ExpressionPartitions.rollUpTo(partitions, interestingProperties),
                        name -> "rolled up planPartitions(" + name + ")"),
                downstream);
    }

    @SuppressWarnings("unchecked")
    public static BindingMatcher<ExpressionPartition<RelationalExpression>> anyExpressionPartition() {
        return typed((Class<ExpressionPartition<RelationalExpression>>)(Class<?>)ExpressionPartition.class);
    }

    @SuppressWarnings("unchecked")
    public static <E extends RelationalExpression, P extends ExpressionPartition<E>> BindingMatcher<P> expressions(final BindingMatcher<? extends Iterable<E>> downstream) {
        return TypedMatcherWithExtractAndDownstream.typedWithDownstream((Class<P>)(Class<?>)ExpressionPartition.class,
                Extractor.of(ExpressionPartition::getExpressions,
                        name -> "expressions(" + name + ")"),
                downstream);
    }
}
