/*
 * ExpressionPartitions.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Helpers for collections of {@link ExpressionPartition}s.
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
                (PartitionCreator<E, ExpressionPartition<E>>)ExpressionPartition::new);
    }

    @Nonnull
    protected static <E extends RelationalExpression, P extends ExpressionPartition<E>> List<P> rollUpTo(@Nonnull final Collection<P> expressionPartitions,
                                                                                                         @Nonnull final ExpressionProperty<?> property,
                                                                                                         @Nonnull final PartitionCreator<E, P> partitionCreator) {
        return rollUpTo(expressionPartitions, ImmutableSet.of(property), partitionCreator);
    }

    @Nonnull
    public static <E extends RelationalExpression> List<ExpressionPartition<E>> rollUpTo(@Nonnull final Collection<ExpressionPartition<E>> expressionPartitions,
                                                                                         @Nonnull final Set<ExpressionProperty<?>> rollupProperties) {
        return rollUpTo(expressionPartitions, rollupProperties,
                (PartitionCreator<E, ExpressionPartition<E>>)ExpressionPartition::new);
    }

    /**
     * Merges (rolls up) a collection of partitions into fewer, coarser partitions by retaining only the
     * properties in {@code rollupProperties} as grouping keys.
     *
     * <p>Each incoming partition carries a map of <em>partitioning properties</em> (the key that defines the
     * partition) and a map of <em>non-partitioning properties</em> (per-expression property values within the
     * partition). This method projects the partitioning-property map down to the subset specified by
     * {@code rollupProperties}. Partitions whose projected keys are equal are merged: their non-partitioning
     * property maps are combined so that the resulting partition contains the union of all expressions from the
     * merged input partitions.
     *
     * <p>Passing an empty {@code rollupProperties} set causes <em>all</em> partitions to merge into one
     * (a full roll-up), since every partition projects to the same empty key.
     *
     * @param <E>              the expression type
     * @param <P>              the partition type
     * @param partitions       the input partitions to roll up
     * @param rollupProperties the properties to keep as grouping keys; all others are discarded from the key
     * @param partitionCreator factory for constructing result partitions
     * @return a list of rolled-up partitions, one per distinct projected key
     */
    @Nonnull
    static <E extends RelationalExpression, P extends ExpressionPartition<E>> List<P> rollUpTo(@Nonnull final Collection<P> partitions,
                                                                                               @Nonnull final Set<ExpressionProperty<?>> rollupProperties,
                                                                                               @Nonnull final PartitionCreator<E, P> partitionCreator) {
        final Map<Map<ExpressionProperty<?>, ?>, Map<E, Map<ExpressionProperty<?>, ?>>> rolledUpMap =
                new LinkedHashMap<>();
        for (final P partition : partitions) {
            final var groupingPropertyMap = partition.getPartitionPropertiesMap();
            final Map<ExpressionProperty<?>, ?> filteredPropertiesMap =
                    groupingPropertyMap
                            .entrySet()
                            .stream()
                            .filter(attributeEntry ->
                                    rollupProperties.contains(attributeEntry.getKey()))
                            .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

            final var nonTrackedPartitioningProperties = rollupProperties.stream()
                    .filter(property -> !groupingPropertyMap.containsKey(property))
                    .collect(Collectors.toSet());

            for (final E expression : partition.getExpressions()) {
                final Map<ExpressionProperty<?>, ?> partitioningPropertiesForExpression = Streams.concat(
                        filteredPropertiesMap.entrySet().stream(),
                        nonTrackedPartitioningProperties.stream().map(
                                expressionProperty -> Map.entry(
                                        expressionProperty, expressionProperty.createVisitor().visit(expression)))
                        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                final var nonPartitioningPropertiesMapForExpression = partition.getNonPartitioningPropertiesMap().get(expression);
                rolledUpMap.compute(partitioningPropertiesForExpression, (key, oldValue) -> {
                    if (oldValue == null) {
                        oldValue = new LinkedHashMap<>();
                    }
                    oldValue.put(expression, nonPartitioningPropertiesMapForExpression);
                    return oldValue;
                });
            }
        }

        final var resultsBuilder = ImmutableList.<P>builder();
        for (final var entry : rolledUpMap.entrySet()) {
            resultsBuilder.add(partitionCreator.create(entry.getKey(), entry.getValue()));
        }
        return resultsBuilder.build();
    }

    @Nonnull
    protected static <E extends RelationalExpression> List<ExpressionPartition<E>> toPartitions(@Nonnull final ExpressionPropertiesMap<E> propertiesMap) {
        return toPartitions(propertiesMap, (PartitionCreator<E, ExpressionPartition<E>>)ExpressionPartition::new);
    }

    @Nonnull
    protected static <E extends RelationalExpression, P extends ExpressionPartition<E>> List<P> toPartitions(@Nonnull final ExpressionPropertiesMap<E> propertiesMap,
                                                                                                             @Nonnull final PartitionCreator<E, P> partitionCreator) {
        return toPartitions(propertiesMap.getPartitioningPropertiesExpressionsMap(),
                propertiesMap.computeNonPartitioningPropertiesMap(), partitionCreator);
    }

    @Nonnull
    private static <E extends RelationalExpression, P extends ExpressionPartition<E>> List<P> toPartitions(@Nonnull final Map<Map<ExpressionProperty<?>, ?>, ? extends Set<E>> partitioningPropertiesMap,
                                                                                                           @Nonnull Map<E, Map<ExpressionProperty<?>, ?>> nonPartitioningPropertiesMap,
                                                                                                           @Nonnull final PartitionCreator<E, P> partitionCreator) {
        return partitioningPropertiesMap
                .entrySet()
                .stream()
                .map(entry -> {
                    final var partitioningPropertyMap = entry.getKey();
                    final var expressions = entry.getValue();
                    final var nonPartitioningPropertyMap = new LinkedIdentityMap<E, Map<ExpressionProperty<?>, ?>>();
                    for (final var expression : expressions) {
                        final var propertiesMapForExpression =
                                nonPartitioningPropertiesMap.get(expression);
                        nonPartitioningPropertyMap.put(expression, ImmutableMap.copyOf(propertiesMapForExpression));
                    }

                    //
                    // Note that the creator is not expected to blindly copy the maps handed in, however, the partition
                    // needs to own these maps. The grouping property map is not necessarily immutable nor is it owned
                    // by the partition. We need to defensively copy that map.
                    //
                    return partitionCreator.create(ImmutableMap.copyOf(partitioningPropertyMap), nonPartitioningPropertyMap);
                })
                .collect(ImmutableList.toImmutableList());
    }

    @FunctionalInterface
    protected interface PartitionCreator<E extends RelationalExpression, P extends ExpressionPartition<E>> {
        P create(@Nonnull Map<ExpressionProperty<?>, ?> groupingPropertyMap,
                 @Nonnull Map<E, Map<ExpressionProperty<?>, ?>> groupedPropertyMap);
    }
}
