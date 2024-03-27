/*
 * PropertiesMap.java
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
import com.apple.foundationdb.record.query.plan.cascades.properties.DistinctRecordsProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.OrderingProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.PrimaryKeyProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.StoredRecordProperty;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class to manage properties for plans. A properties map is part of an expression reference ({@link Reference}).
 * <br>
 * Properties for plans managed by this map are computed lazily when a caller attempts to retrieve the value of a property.
 * The reason for that is twofold. First, we want to avoid unnecessary computation of a property if it is not retrieved
 * at a later point in time. Second, the basic planner {@link RecordQueryPlan} uses a simplified way of creating a dag of
 * {@link RecordQueryPlan}s that lacks some fundamental information (e.g. no type system) that some properties computations
 * depend on meaning that these properties cannot be computed if the plan was created by {@link RecordQueryPlan}.
 * In order to still allow that planner to create the same structures as the {@link CascadesPlanner} we need these property
 * computations to be lazy as {@link RecordQueryPlan} never accesses the properties afterwards.
 */
public class PropertiesMap {
    /**
     * This set works a bit like an enumeration; it defines the domain of {@link PlanProperty}s that are being maintained
     * by the properties map.
     */
    private static final Set<PlanProperty<?>> planProperties =
            ImmutableSet.<PlanProperty<?>>builder()
                    .add(OrderingProperty.ORDERING)
                    .add(DistinctRecordsProperty.DISTINCT_RECORDS)
                    .add(StoredRecordProperty.STORED_RECORD)
                    .add(PrimaryKeyProperty.PRIMARY_KEY)
                    .build();

    /**
     * A queue with plans whose properties have not been computed yet.
     */
    @Nonnull
    private final Deque<RecordQueryPlan> toBeInsertedPlans;

    /**
     * Map from {@link RecordQueryPlan} to a map of {@link PlanProperty} to a computed property value.
     */
    @Nonnull
    private final Map<RecordQueryPlan, Map<PlanProperty<?>, ?>> planPropertiesMap;

    /**
     * {@link SetMultimap} from a map of computed properties to a set of {@link RecordQueryPlan}.
     */
    @Nonnull
    private final SetMultimap<Map<PlanProperty<?>, ?>, RecordQueryPlan> attributeGroupedPlansMap;

    public PropertiesMap(@Nonnull Collection<? extends RelationalExpression> relationalExpressions) {
        this.toBeInsertedPlans = new ArrayDeque<>();
        this.planPropertiesMap = new LinkedIdentityMap<>();
        this.attributeGroupedPlansMap = Multimaps.newSetMultimap(Maps.newLinkedHashMap(), LinkedIdentitySet::new);
        relationalExpressions
                .stream()
                .filter(relationalExpression -> relationalExpression instanceof RecordQueryPlan)
                .map(relationalExpression -> (RecordQueryPlan)relationalExpression)
                .forEach(this::add);
    }

    /**
     * Method to compute the properties of the plans residing in the queue of to-be-inserted plans. Plans and their
     * computed properties are then used to update the internal structures. Every retrieve operation to this class
     * must call this method to ensure that the internals of this object are up-to-date.
     */
    private void update() {
        while (!toBeInsertedPlans.isEmpty()) {
            final var recordQueryPlan = toBeInsertedPlans.pop();
            final var attributeMapBuilder = ImmutableMap.<PlanProperty<?>, Object>builder();
            for (final var planProperty : planProperties) {
                attributeMapBuilder.put(planProperty, computePropertyValue(planProperty, recordQueryPlan));
            }
            final var propertiesForPlanMap = attributeMapBuilder.build();
            add(recordQueryPlan, propertiesForPlanMap);
        }
    }

    /**
     * Returns the properties currently stored in the properties map for the given plan. Note that
     * {@link #update()} is not called prior to retrieving the properties.
     * @param recordQueryPlan the plan
     * @return a map of properties for the given plan, or {@code null} if the {@link  RecordQueryPlan} passed in is
     *         either stored in the properties map.
     */
    @Nullable
    public Map<PlanProperty<?>, ?> getPropertiesForPlan(@Nonnull final RecordQueryPlan recordQueryPlan) {
        update();
        return getCurrentPropertiesForPlan(recordQueryPlan);
    }

    /**
     * Returns the properties currently stored in the properties map for the given plan. Note that
     * {@link #update()} is not called prior to retrieving the properties.
     * @param recordQueryPlan the plan
     * @return a map of properties for the given plan, or {@code null} if the {@link  RecordQueryPlan} passed in is
     *         either not stored in the properties map or not yet stored in the map (it may be in the queue but is
     *         not yet processed).
     */
    @Nullable
    public Map<PlanProperty<?>, ?> getCurrentPropertiesForPlan(@Nonnull final RecordQueryPlan recordQueryPlan) {
        return planPropertiesMap.get(recordQueryPlan);
    }

    /**
     * Method to add a new {@link RecordQueryPlan} to this properties map. The plan is added to a queue that is
     * consumed upon read to lazily compute the properties of the plan passed in.
     * @param recordQueryPlan new record query plan to be added
     */
    public void add(@Nonnull final RecordQueryPlan recordQueryPlan) {
        toBeInsertedPlans.add(recordQueryPlan);
    }

    /**
     * Method to add a new {@link RecordQueryPlan} to this properties map using precomputed properties. That is
     * useful when the caller retrieved the plan from some other reference.
     * @param recordQueryPlan new record query plan to be added
     * @param propertiesForPlanMap a map containing all managed properties for the {@link RecordQueryPlan} passed in
     */
    public void add(@Nonnull final RecordQueryPlan recordQueryPlan, @Nonnull final Map<PlanProperty<?>, ?> propertiesForPlanMap) {
        Verify.verify(!planPropertiesMap.containsKey(recordQueryPlan));
        planPropertiesMap.put(recordQueryPlan, propertiesForPlanMap);
        attributeGroupedPlansMap.put(propertiesForPlanMap, recordQueryPlan);
    }

    @Nonnull
    private <P> P computePropertyValue(@Nonnull final PlanProperty<P> planProperty,
                                       @Nonnull final RecordQueryPlan recordQueryPlan) {
        final var propertyVisitor = planProperty.createVisitor();
        return propertyVisitor.visit(recordQueryPlan);
    }

    public void clear() {
        toBeInsertedPlans.clear();
        planPropertiesMap.clear();
        attributeGroupedPlansMap.clear();
    }

    /**
     * Returns a map from {@link RecordQueryPlan} to a computed specific property value for a {@link PlanProperty}
     * passed in.
     * @param <P> the type parameter of the {@link  PlanProperty}
     * @param planProperty the property the caller is interested in
     * @return a new map that holds a key/value for each plan that is currently being managed by this property map
     *         to its {@link PlanProperty}'s value
     */
    @Nonnull
    public <P> Map<RecordQueryPlan, P> getPlannerAttributeForAllPlans(@Nonnull final PlanProperty<P> planProperty) {
        update();
        final var resultMap = new LinkedIdentityMap<RecordQueryPlan, P>();
        for (final var entry : planPropertiesMap.entrySet()) {
            resultMap.put(entry.getKey(), planProperty.narrowAttribute(entry.getValue().get(planProperty)));
        }

        return resultMap;
    }

    @SuppressWarnings("UnstableApiUsage")
    @Nonnull
    public List<PlanPartition> getPlanPartitions() {
        update();
        return PlanPartition.toPlanPartitions(Multimaps.asMap(attributeGroupedPlansMap));
    }

    @Nonnull
    public static Set<PlanProperty<?>> allAttributesExcept(final PlanProperty<?>... exceptAttributes) {
        final var exceptAttributesSet = ImmutableSet.copyOf(Arrays.asList(exceptAttributes));
        return ImmutableSet.copyOf(Sets.difference(planProperties, exceptAttributesSet));
    }
}
