/*
 * PlanPartition.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Helpers for collections of {@link ExpressionPartition}.
 */
public class ExpressionPartitions {
    private ExpressionPartitions() {
        // do not instantiate
    }

    @Nonnull
    public static <E extends RelationalExpression> List<ExpressionPartition<E>> rollUpTo(@Nonnull final Collection<ExpressionPartition<E>> expressionPartitions,
                                                                                         @Nonnull final ExpressionProperty<?> property) {
        return rollUpTo(expressionPartitions,
                ImmutableSet.of(property),
                ExpressionPartition::new);
    }

    @Nonnull
    protected static <E extends RelationalExpression, P extends ExpressionPartition<E>> List<P> rollUpTo(@Nonnull final Collection<P> expressionPartitions,
                                                                                                         @Nonnull final ExpressionProperty<?> property,
                                                                                                         @Nonnull final BiFunction<Map<ExpressionProperty<?>, ?>, Collection<E>, P> partitionCreatorFunction) {
        return rollUpTo(expressionPartitions, ImmutableSet.of(property), partitionCreatorFunction);
    }

    @Nonnull
    public static <E extends RelationalExpression> List<ExpressionPartition<E>> rollUpTo(@Nonnull final Collection<ExpressionPartition<E>> expressionPartitions,
                                                                                         @Nonnull final Set<ExpressionProperty<?>> rollupProperties) {
        return rollUpTo(expressionPartitions, rollupProperties, ExpressionPartition::new);
    }

    @Nonnull
    static <E extends RelationalExpression, P extends ExpressionPartition<E>> List<P> rollUpTo(@Nonnull final Collection<P> planPartitions,
                                                                                               @Nonnull final Set<ExpressionProperty<?>> rollupProperties,
                                                                                               @Nonnull final BiFunction<Map<ExpressionProperty<?>, ?>, Collection<E>, P> partitionCreatorFunction) {
        final Map<Map<ExpressionProperty<?>, ?>, ? extends Set<E>> rolledUpAttributesMap =
                planPartitions
                        .stream()
                        .map(planPartition -> {
                            final var attributesMap = planPartition.getPropertyValuesMap();
                            final Map<ExpressionProperty<?>, ?> filteredAttributesMap =
                                    attributesMap
                                            .entrySet()
                                            .stream()
                                            .filter(attributeEntry -> rollupProperties.contains(attributeEntry.getKey()))
                                            .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

                            // create a new partition that uses only the rollup attributes
                            return new ExpressionPartition<>(filteredAttributesMap, planPartition.getExpressions());
                        })
                        // group by the filtered attributes rolling up to form new sets of plans
                        .collect(Collectors.groupingBy(ExpressionPartition::getPropertyValuesMap,
                                LinkedHashMap::new,
                                Collectors.flatMapping(planPartition -> planPartition.getExpressions().stream(), LinkedIdentitySet.toLinkedIdentitySet())));

        return toPartitions(rolledUpAttributesMap, partitionCreatorFunction);
    }

    @Nonnull
    protected static <E extends RelationalExpression> List<ExpressionPartition<E>> toPartitions(@Nonnull final Map<Map<ExpressionProperty<?>, ?>, ? extends Set<E>> propertiesToExpressionsMap) {
        return toPartitions(propertiesToExpressionsMap, ExpressionPartition::new);
    }

    @Nonnull
    protected static <E extends RelationalExpression, P extends ExpressionPartition<E>> List<P> toPartitions(@Nonnull final Map<Map<ExpressionProperty<?>, ?>, ? extends Set<E>> propertiesToExpressionsMap,
                                                                                                             @Nonnull final BiFunction<Map<ExpressionProperty<?>, ?>, Collection<E>, P> partitionCreatorFunction) {
        return propertiesToExpressionsMap
                .entrySet()
                .stream()
                .map(entry -> {
                    final var attributesMap = entry.getKey();
                    final var plans = entry.getValue();
                    return partitionCreatorFunction.apply(attributesMap, plans);
                })
                .collect(ImmutableList.toImmutableList());
    }
}
