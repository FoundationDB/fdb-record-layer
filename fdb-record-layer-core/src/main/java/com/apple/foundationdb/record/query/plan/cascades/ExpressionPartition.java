/*
 * ExpressionPartition.java
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
import com.google.common.collect.Maps;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

/**
 * A partition used for matching holding a set of {@link RelationalExpression}s.
 * @param <E> type parameter to indicate the type of expression this partition holds.
 */
public class ExpressionPartition<E extends RelationalExpression> {
    /**
     * Grouping property map which holds all properties and their values that are common to all plans and therefore
     * defining the partition.
     */
    @Nonnull
    private final Map<ExpressionProperty<?>, ?> partitionPropertiesMap;
    /**
     * Grouped property map which is a map from each individual plan in the partition to properties and their values
     * for properties that are not defining the partition.
     */
    @Nonnull
    private final Map<E, Map<ExpressionProperty<?>, ?>> nonPartitioningPropertiesMap;

    /**
     * Constructor. Note that we do not defensively copy here as it is dependent on the use case if we need a copy or
     * not. The caller needs to ensure that the maps being passed in are either immutable or owned.
     * @param partitionPropertiesMap property map common to the entire partition
     * @param nonPartitioningPropertiesMap expression property map for expression-specific properties
     */
    public ExpressionPartition(@Nonnull final Map<ExpressionProperty<?>, ?> partitionPropertiesMap,
                               @Nonnull final Map<E, Map<ExpressionProperty<?>, ?>> nonPartitioningPropertiesMap) {
        this.partitionPropertiesMap = partitionPropertiesMap;
        this.nonPartitioningPropertiesMap = nonPartitioningPropertiesMap;
    }

    @Nonnull
    public Map<ExpressionProperty<?>, ?> getPartitionPropertiesMap() {
        return partitionPropertiesMap;
    }

    @Nonnull
    public Map<E, Map<ExpressionProperty<?>, ?>> getNonPartitioningPropertiesMap() {
        return nonPartitioningPropertiesMap;
    }

    @Nonnull
    public <A> A getPartitionPropertyValue(@Nonnull final ExpressionProperty<A> expressionProperty) {
        return expressionProperty.narrowAttribute(Objects.requireNonNull(partitionPropertiesMap.get(expressionProperty)));
    }

    @Nonnull
    public <A> A getNonPartitioningPropertyValue(@Nonnull final E expression,
                                                 @Nonnull final ExpressionProperty<A> expressionProperty) {
        final var propertyMapForExpression = nonPartitioningPropertiesMap.get(expression);
        return expressionProperty.narrowAttribute(Objects.requireNonNull(propertyMapForExpression.get(expressionProperty)));
    }

    @Nonnull
    public Set<E> getExpressions() {
        return nonPartitioningPropertiesMap.keySet();
    }

    @Nonnull
    public ExpressionPartition<E> filter(@Nonnull final Predicate<E> expressionPredicate) {
        return with(partitionPropertiesMap, filterGroupedPropertyMap(expressionPredicate));
    }

    @Nonnull
    protected Map<E, Map<ExpressionProperty<?>, ?>> filterGroupedPropertyMap(@Nonnull final Predicate<E> expressionPredicate) {
        return Maps.filterKeys(nonPartitioningPropertiesMap, expressionPredicate::test);
    }

    @Nonnull
    protected ExpressionPartition<E> with(@Nonnull final Map<ExpressionProperty<?>, ?> groupingPropertyMap,
                                          @Nonnull final Map<E, Map<ExpressionProperty<?>, ?>> groupedPropertyMap) {
        return new ExpressionPartition<>(groupingPropertyMap, groupedPropertyMap);
    }
}
