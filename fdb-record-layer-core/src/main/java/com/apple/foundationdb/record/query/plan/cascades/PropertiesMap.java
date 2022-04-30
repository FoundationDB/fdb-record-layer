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
import com.apple.foundationdb.record.query.plan.cascades.properties.StoredRecordProperty;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class to manage properties for plans.
 */
public class PropertiesMap {
    private static final Set<PlanProperty<?>> plannerAttributes =
            ImmutableSet.<PlanProperty<?>>builder()
                    .add(OrderingProperty.ORDERING)
                    .add(DistinctRecordsProperty.DISTINCT_RECORDS)
                    .add(StoredRecordProperty.STORED_RECORD)
                    .build();

    @Nonnull
    private final Map<RecordQueryPlan, Map<PlanProperty<?>, ?>> planAttributesMap;

    @Nonnull
    private final SetMultimap<Map<PlanProperty<?>, ?>, RecordQueryPlan> attributeGroupedPlansMap;

    public PropertiesMap(@Nonnull Collection<? extends RelationalExpression> relationalExpressions) {
        this.planAttributesMap = new LinkedIdentityMap<>();
        this.attributeGroupedPlansMap = Multimaps.newSetMultimap(Maps.newLinkedHashMap(), LinkedIdentitySet::new);
        relationalExpressions
                .stream()
                .filter(relationalExpression -> relationalExpression instanceof RecordQueryPlan)
                .forEach(this::insert);
    }

    public boolean insert(@Nonnull final RelationalExpression relationalExpression) {
        if (!(relationalExpression instanceof RecordQueryPlan) || planAttributesMap.containsKey(relationalExpression)) {
            return false;
        }

        final var recordQueryPlan = (RecordQueryPlan)relationalExpression;
        final var attributeMapBuilder = ImmutableMap.<PlanProperty<?>, Object>builder();

        for (final var plannerAttribute : plannerAttributes) {
            final var attributeVisitor = plannerAttribute.createVisitor();
            final var attribute = attributeVisitor.visit(recordQueryPlan);

            attributeMapBuilder.put(plannerAttribute, attribute);
        }

        final var attributeForPlanMap = attributeMapBuilder.build();

        planAttributesMap.put(recordQueryPlan, attributeForPlanMap);
        attributeGroupedPlansMap.put(attributeForPlanMap, recordQueryPlan);

        return true;
    }

    public void clear() {
        planAttributesMap.clear();
        attributeGroupedPlansMap.clear();
    }

    @Nonnull
    public <A> Map<RecordQueryPlan, A> getPlannerAttributeForAllPlans(@Nonnull final PlanProperty<A> planProperty) {
        final var resultMap = new LinkedIdentityMap<RecordQueryPlan, A>();
        for (final var entry : planAttributesMap.entrySet()) {
            resultMap.put(entry.getKey(), planProperty.narrowAttribute(entry.getValue().get(planProperty)));
        }

        return resultMap;
    }

    @SuppressWarnings("UnstableApiUsage")
    @Nonnull
    public List<PlanPartition> getPlanPartitions() {
        return PlanPartition.toPlanPartitions(Multimaps.asMap(attributeGroupedPlansMap));
    }

    @Nonnull
    public static Set<PlanProperty<?>> allAttributesExcept(final PlanProperty<?>... exceptAttributes) {
        final var exceptAttributesSet = ImmutableSet.copyOf(Arrays.asList(exceptAttributes));
        return ImmutableSet.copyOf(Sets.difference(plannerAttributes, exceptAttributesSet));
    }
}
