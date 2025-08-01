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
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcher.typed;

public class ExpressionsPartitionMatchers {
    private ExpressionsPartitionMatchers() {
        // do not instantiate
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static BindingMatcher<Reference> expressionPartitions(@Nonnull final BindingMatcher<? extends Iterable<? extends ExpressionPartition<? extends RelationalExpression>>> downstream) {
        return TypedMatcherWithExtractAndDownstream.typedWithDownstream(Reference.class,
                Extractor.of(Reference::toExpressionPartitions, name -> "expressionPartitions(" + name + ")"),
                downstream);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static BindingMatcher<Collection<? extends ExpressionPartition<? extends RelationalExpression>>> filterPartition(@Nonnull final Predicate<ExpressionPartition<? extends RelationalExpression>> predicate,
                                                                                                                            @Nonnull final BindingMatcher<? extends Iterable<? extends ExpressionPartition<? extends RelationalExpression>>> downstream) {
        return TypedMatcherWithExtractAndDownstream.typedWithDownstream(
                (Class<Collection<? extends ExpressionPartition<? extends RelationalExpression>>>)(Class<?>)Collection.class,
                Extractor.of(planPartitions ->
                        planPartitions.stream()
                                .filter(predicate)
                                .collect(ImmutableList.toImmutableList()),
                        name -> "filtered expressionPartitions(" + name + ")"),
                downstream);
    }

    @Nonnull
    public static BindingMatcher<Collection<ExpressionPartition<RelationalExpression>>> rollUpPartitions(@Nonnull final BindingMatcher<? extends Iterable<ExpressionPartition<RelationalExpression>>> downstream) {
        return rollUpPartitionsTo(downstream, ImmutableSet.of());
    }

    @Nonnull
    public static BindingMatcher<Collection<ExpressionPartition<RelationalExpression>>> rollUpPartitionsTo(@Nonnull final BindingMatcher<? extends Iterable<ExpressionPartition<RelationalExpression>>> downstream,
                                                                                                           @Nonnull final ExpressionProperty<?> interestingProperty) {
        return rollUpPartitionsTo(downstream, ImmutableSet.of(interestingProperty));
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static BindingMatcher<Collection<ExpressionPartition<RelationalExpression>>> rollUpPartitionsTo(@Nonnull final BindingMatcher<? extends Iterable<ExpressionPartition<RelationalExpression>>> downstream,
                                                                                                           @Nonnull final Set<ExpressionProperty<?>> interestingProperties) {
        return TypedMatcherWithExtractAndDownstream.typedWithDownstream(
                (Class<Collection<ExpressionPartition<RelationalExpression>>>)(Class<?>)Collection.class,
                Extractor.of(partitions -> ExpressionPartitions.rollUpTo(partitions, interestingProperties),
                        name -> "rolled up planPartitions(" + name + ")"),
                downstream);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static BindingMatcher<ExpressionPartition<RelationalExpression>> anyExpressionPartition() {
        return typed((Class<ExpressionPartition<RelationalExpression>>)(Class<?>)ExpressionPartition.class);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static <E extends RelationalExpression, P extends ExpressionPartition<E>> BindingMatcher<P> filterExpressions(@Nonnull final Predicate<E> predicate,
                                                                                                                         @Nonnull final BindingMatcher<P> downstream) {
        return TypedMatcherWithExtractAndDownstream.typedWithDownstream((Class<P>)(Class<?>)ExpressionPartition.class,
                Extractor.of(expressionPartition -> expressionPartition.filter(predicate),
                        name -> "filtered expressions(" + name + ")"),
                downstream);
    }

    @Nonnull
    public static <C extends Comparable<C>, E extends RelationalExpression> BiFunction<ExpressionPartition<E>, ? super E, C> comparisonByProperty(@Nonnull ExpressionProperty<C> expressionProperty) {
        return (partition, expression) -> partition.getNonPartitioningPropertyValue(expression, expressionProperty);
    }

    @Nonnull
    public static <E extends RelationalExpression> BiFunction<ExpressionPartition<E>, ? super E, Tuple> comparisonByPropertyList(@Nonnull ExpressionProperty<?>... expressionProperties) {
        return (partition, expression) ->
                Tuple.fromItems(Arrays.stream(expressionProperties)
                        .map(property -> partition.getNonPartitioningPropertyValue(expression, property))
                        .collect(Collectors.toList()));
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static <C extends Comparable<C>, E extends RelationalExpression> BindingMatcher<ExpressionPartition<RelationalExpression>> argmin(@Nonnull final ExpressionProperty<C> expressionProperty,
                                                                                                                                             @Nonnull final BindingMatcher<E> downstream) {
        return argmin(ExpressionsPartitionMatchers.<C, E>comparisonByProperty(expressionProperty), downstream);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static <C extends Comparable<C>, E extends RelationalExpression> BindingMatcher<ExpressionPartition<RelationalExpression>> argmin(@Nonnull final BiFunction<ExpressionPartition<E>, ? super E, C> comparisonKeyFunction,
                                                                                                                                             @Nonnull final BindingMatcher<E> downstream) {
        return TypedMatcherWithExtractAndDownstream.typedWithDownstream(
                (Class<ExpressionPartition<RelationalExpression>>)(Class<?>)ExpressionPartition.class,
                Extractor.of(partition ->
                                partition.getExpressions()
                                        .stream()
                                        .min(Comparator.comparing(
                                                expression ->
                                                        comparisonKeyFunction.apply((ExpressionPartition<E>)partition, (E)expression))),
                        name -> "argmin(" + name + ")"),
                OptionalIfPresentMatcher.present(downstream));
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static <P extends Comparable<P>> BindingMatcher<ExpressionPartition<RelationalExpression>> argmax(@Nonnull final ExpressionProperty<P> expressionProperty,
                                                                                                             @Nonnull final BindingMatcher<? extends RelationalExpression> downstream) {
        return TypedMatcherWithExtractAndDownstream.typedWithDownstream(
                (Class<ExpressionPartition<RelationalExpression>>)(Class<?>)Collection.class,
                Extractor.of(partition ->
                                partition.getExpressions()
                                        .stream()
                                        .max(Comparator.comparing(
                                                expression ->
                                                        partition.getNonPartitioningPropertyValue(expression,
                                                                expressionProperty))),
                        name -> "argmax(" + name + ")"),
                OptionalIfPresentMatcher.present(downstream));
    }
}
