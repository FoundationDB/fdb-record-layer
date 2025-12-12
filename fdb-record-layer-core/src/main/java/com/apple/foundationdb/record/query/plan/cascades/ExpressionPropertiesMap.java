/*
 * ExpressionPropertiesMap.java
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
import com.apple.foundationdb.record.query.plan.cascades.properties.ExpressionCountProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.PredicateComplexityProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.PredicateCountByLevelProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.PredicateCountProperty;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.Map;
import java.util.Set;

/**
 * Class to manage properties for expressions of some type {@code E} that extends {@link RelationalExpression}.
 * A properties map is part of an expression reference ({@link Reference}).
 * <br>
 * Properties for expressions managed by this map are computed lazily when a caller attempts to retrieve the value of a
 * property. The reason for that is twofold. First, we want to avoid unnecessary computation of a property if it is not
 * retrieved at a later point in time. Second, the basic planner
 * {@link com.apple.foundationdb.record.query.plan.RecordQueryPlanner} uses a simplified way of creating a dag of
 * {@link RecordQueryPlan}s that lacks some fundamental information (e.g. no type system) that some properties
 * computations depend on meaning that these properties cannot be computed if the plan was created by
 * {@link com.apple.foundationdb.record.query.plan.RecordQueryPlanner}.
 * In order to still allow that planner to create the same structures as the {@link CascadesPlanner} we need these
 * property computations to be lazy as {@link com.apple.foundationdb.record.query.plan.RecordQueryPlanner} never
 * accesses the properties.
 * @param <E> type parameter to capture the kind of expression property map
 */
public class ExpressionPropertiesMap<E extends RelationalExpression> {
    /**
     * Class object to do runtime type checks against as this is not Scala.
     */
    @Nonnull
    private final Class<E> expressionClass;

    /**
     * This set works a bit like an enumeration; it defines the domain of {@link ExpressionProperty}s that are being
     * maintained by the properties map as part of the partitioning properties, i.e. the property value that together
     * define the finest level of {@link ExpressionPartition}s.
     */
    private final Set<ExpressionProperty<?>> trackedPartitioningProperties;

    /**
     * This set works a bit like an enumeration; it defines the domain of {@link ExpressionProperty}s that are being
     * maintained by the properties map as part of the individual properties whose values do not partake in defining
     * partitions.
     */
    private final Set<ExpressionProperty<?>> trackedNonPartitioningProperties;

    /**
     * A queue with expressions whose properties have not been computed yet.
     */
    @Nonnull
    private final Deque<E> toBeInsertedExpressions;

    /**
     * Map from each expression to its associated map of computed property values.
     */
    @Nonnull
    private final Map<E, Map<ExpressionProperty<?>, ?>> propertiesMap;

    /**
     * {@link SetMultimap} from a map of computed properties to {@code E}s.
     */
    @Nonnull
    private final SetMultimap<Map<ExpressionProperty<?>, ?>, E> partitioningPropertiesExpressionsMap;

    public ExpressionPropertiesMap(@Nonnull final Class<E> expressionClass,
                                   @Nonnull final Set<ExpressionProperty<?>> trackedPartitioningProperties,
                                   @Nonnull final Set<ExpressionProperty<?>> trackedNonPartitioningProperties,
                                   @Nonnull final Collection<? extends RelationalExpression> expressions) {
        this.expressionClass = expressionClass;
        this.trackedPartitioningProperties = ImmutableSet.copyOf(trackedPartitioningProperties);
        this.trackedNonPartitioningProperties = ImmutableSet.copyOf(trackedNonPartitioningProperties);
        this.toBeInsertedExpressions = new ArrayDeque<>();
        this.propertiesMap = new LinkedIdentityMap<>();
        this.partitioningPropertiesExpressionsMap = Multimaps.newSetMultimap(Maps.newLinkedHashMap(), LinkedIdentitySet::new);
        expressions.forEach(this::add);
    }

    @Nonnull
    private E narrow(@Nonnull final RelationalExpression expression) {
        Verify.verify(expressionClass.isInstance(expression),
                "unable to cast property value to its declared type");
        return expressionClass.cast(expression);
    }

    /**
     * Method to compute the properties of the plans residing in the queue of to-be-inserted plans. Plans and their
     * computed properties are then used to update the internal structures. Every retrieve operation to this class
     * must call this method to ensure that the internals of this object are up-to-date.
     */
    protected void update() {
        while (!toBeInsertedExpressions.isEmpty()) {
            final var expression = toBeInsertedExpressions.pop();
            final var groupingPropertyMapBuilder = ImmutableMap.<ExpressionProperty<?>, Object>builder();
            for (final var expressionProperty : trackedPartitioningProperties) {
                groupingPropertyMapBuilder.put(expressionProperty, computePropertyValue(expressionProperty, expression));
            }
            final var groupingPropertyMap = groupingPropertyMapBuilder.build();
            final var groupedPropertyMapBuilder = ImmutableMap.<ExpressionProperty<?>, Object>builder();
            for (final var expressionProperty : trackedNonPartitioningProperties) {
                groupedPropertyMapBuilder.put(expressionProperty, computePropertyValue(expressionProperty, expression));
            }
            final var groupedPropertyMap = groupedPropertyMapBuilder.build();
            add(expression, groupingPropertyMap, groupedPropertyMap);
        }
    }

    @Nonnull
    public Map<E, Map<ExpressionProperty<?>, ?>> getPropertiesMap() {
        return propertiesMap;
    }

    @Nonnull
    public Map<E, Map<ExpressionProperty<?>, ?>> computeNonPartitioningPropertiesMap() {
        return Maps.transformValues(propertiesMap,
                propertyMap ->
                        Maps.filterKeys(propertyMap, trackedNonPartitioningProperties::contains));
    }

    /**
     * Returns the properties currently stored in the properties map for the given expression. Note that
     * {@link #update()} is called prior to retrieving the properties.
     * @param expression the expression
     * @return a map of properties for the given expression, or {@code null} if the expression passed in is
     *         not stored in the properties map.
     */
    @Nullable
    public Map<ExpressionProperty<?>, ?> getProperties(@Nonnull final RelationalExpression expression) {
        update();
        return getCurrentProperties(expression);
    }

    /**
     * Returns the properties currently stored in the properties map for the given expression. Note that
     * {@link #update()} is not called prior to retrieving the properties.
     * @param expression the expression
     * @return a map of properties for the given expression, or {@code null} if the expression passed in is
     *         either not stored in the properties map or not yet stored in the map (it may be in the queue but is
     *         not yet processed).
     */
    @Nullable
    public Map<ExpressionProperty<?>, ?> getCurrentProperties(@Nonnull final RelationalExpression expression) {
        return propertiesMap.get(narrow(expression));
    }

    /**
     * Method to add a new {@link RelationalExpression} to this properties map. The plan is added to a queue that is
     * consumed upon read to lazily compute the properties of the plan passed in.
     * @param expression new expression to be added
     */
    public void add(@Nonnull final RelationalExpression expression) {
        toBeInsertedExpressions.add(narrow(expression));
    }

    /**
     * Method to add a new {@link RecordQueryPlan} to this property map using precomputed properties. That is
     * useful when the caller retrieved the plan from some other reference.
     * @param expression new record query plan to be added
     * @param propertyMap a map containing all properties for the expression passed in
     */
    public void add(@Nonnull final RelationalExpression expression,
                    @Nonnull final Map<ExpressionProperty<?>, ?> propertyMap) {
        final var partitioningPropertyMapBuilder = ImmutableMap.<ExpressionProperty<?>, Object>builder();
        for (final var expressionProperty : trackedPartitioningProperties) {
            final var propertyValue = propertyMap.get(expressionProperty);
            Verify.verify(propertyValue != null);
            partitioningPropertyMapBuilder.put(expressionProperty, propertyMap.get(expressionProperty));
        }
        final var partitioningPropertyMap = partitioningPropertyMapBuilder.build();
        final E typedExpression = narrow(expression);
        Verify.verify(!propertiesMap.containsKey(typedExpression));
        propertiesMap.put(typedExpression, propertyMap);
        partitioningPropertiesExpressionsMap.put(partitioningPropertyMap, typedExpression);
    }

    /**
     * Method to add a new {@link RecordQueryPlan} to this property map using precomputed properties. That is
     * useful when the caller retrieved the plan from some other reference.
     * @param expression new record query plan to be added
     * @param partitioningPropertyMap a map containing all partitioning properties for the expression passed in
     * @param nonPartitioningPropertyMap a map containing all non-partitioning properties for the expression passed in
     */
    public void add(@Nonnull final RelationalExpression expression,
                    @Nonnull final Map<ExpressionProperty<?>, ?> partitioningPropertyMap,
                    @Nonnull final Map<ExpressionProperty<?>, ?> nonPartitioningPropertyMap) {
        final E typedExpression = narrow(expression);
        Verify.verify(!propertiesMap.containsKey(typedExpression));
        final var combinedPropertyMap =
                ImmutableMap.<ExpressionProperty<?>, Object>builder()
                        .putAll(partitioningPropertyMap)
                        .putAll(nonPartitioningPropertyMap)
                        .build();
        propertiesMap.put(typedExpression, combinedPropertyMap);
        partitioningPropertiesExpressionsMap.put(partitioningPropertyMap, typedExpression);
    }

    @Nonnull
    private <P> P computePropertyValue(@Nonnull final ExpressionProperty<P> expressionProperty,
                                       @Nonnull final RelationalExpression expression) {
        final var propertyVisitor = expressionProperty.createVisitor();
        return propertyVisitor.visit(expression);
    }

    public void clear() {
        toBeInsertedExpressions.clear();
        propertiesMap.clear();
        partitioningPropertiesExpressionsMap.clear();
    }

    /**
     * Returns a map from expression to a computed specific property value for a {@link ExpressionProperty}
     * passed in.
     * @param <P> the type parameter of the {@link  ExpressionProperty}
     * @param expressionProperty the property the caller is interested in
     * @return a new map that holds a key/value for each plan that is currently being managed by this property map
     *         to its {@link ExpressionProperty}'s value
     */
    @Nonnull
    public <P> Map<E, P> propertyValueForExpressions(@Nonnull final ExpressionProperty<P> expressionProperty) {
        update();
        final var resultMap = new LinkedIdentityMap<E, P>();
        for (final var entry : propertiesMap.entrySet()) {
            resultMap.put(entry.getKey(), expressionProperty.narrowAttribute(entry.getValue().get(expressionProperty)));
        }
        return resultMap;
    }

    @Nonnull
    public Map<Map<ExpressionProperty<?>, ?>, Set<E>> getPartitioningPropertiesExpressionsMap() {
        update();
        return Multimaps.asMap(partitioningPropertiesExpressionsMap);
    }

    @Nonnull
    public Map<Map<ExpressionProperty<?>, ?>, Set<RecordQueryPlan>> getGroupingPropertiesPlansMap() {
        throw new UnsupportedOperationException("method should not be called");
    }

    /**
     * Returns a map from expression to a computed specific property value for a {@link ExpressionProperty}
     * passed in.
     * @param <P> the type parameter of the {@link  ExpressionProperty}
     * @param expressionProperty the property the caller is interested in
     * @return a new map that holds a key/value for each plan that is currently being managed by this property map
     *         to its {@link ExpressionProperty}'s value
     */
    @Nonnull
    public <P> Map<RecordQueryPlan, P> propertyValueForPlans(@Nonnull final ExpressionProperty<P> expressionProperty) {
        throw new UnsupportedOperationException("method cannot provide plans");
    }

    @Nonnull
    public static ExpressionPropertiesMap<RelationalExpression> defaultForRewritePhase() {
        return new ExpressionPropertiesMap<>(RelationalExpression.class,
                ImmutableSet.of(),
                ImmutableSet.of(
                        ExpressionCountProperty.selectCount(),
                        ExpressionCountProperty.tableFunctionCount(),
                        PredicateComplexityProperty.predicateComplexity(),
                        PredicateCountProperty.predicateCount(),
                        PredicateCountByLevelProperty.predicateCountByLevel()
                ),
                ImmutableList.of());
    }
}
