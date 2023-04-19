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

import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A plan partition used for matching.
 */
public class PlanPartition {
    private final Map<PlanProperty<?>, ?> attributesMap;
    private final Set<RecordQueryPlan> plans;

    public PlanPartition(final Map<PlanProperty<?>, ?> attributesMap, final Collection<RecordQueryPlan> plans) {
        this.attributesMap = ImmutableMap.copyOf(attributesMap);
        this.plans = new LinkedIdentitySet<>(plans);
    }

    public Map<PlanProperty<?>, ?> getAttributesMap() {
        return attributesMap;
    }

    public <A> A getAttributeValue(@Nonnull final PlanProperty<A> planProperty) {
        return planProperty.narrowAttribute(Objects.requireNonNull(attributesMap.get(planProperty)));
    }

    public Set<RecordQueryPlan> getPlans() {
        return plans;
    }

    @Nonnull
    public static List<PlanPartition> rollUpTo(@Nonnull Collection<PlanPartition> planPartitions, @Nonnull final PlanProperty<?> rollupAttributes) {
        return rollUpTo(planPartitions, ImmutableSet.of(rollupAttributes));
    }

    @Nonnull
    public static List<PlanPartition> rollUpTo(@Nonnull Collection<PlanPartition> planPartitions, @Nonnull final Set<PlanProperty<?>> rollupAttributes) {
        final Map<Map<PlanProperty<?>, ?>, ? extends Set<RecordQueryPlan>> rolledUpAttributesMap =
                planPartitions
                        .stream()
                        .map(planPartition -> {
                            final var attributesMap = planPartition.getAttributesMap();
                            final Map<PlanProperty<?>, ?> filteredAttributesMap =
                                    attributesMap
                                            .entrySet()
                                            .stream()
                                            .filter(attributeEntry -> rollupAttributes.contains(attributeEntry.getKey()))
                                            .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));

                            // create a new partition that uses only the rollup attributes
                            return new PlanPartition(filteredAttributesMap, planPartition.getPlans());
                        })
                        // group by the filtered attributes rolling up to form new sets of plans
                        .collect(Collectors.groupingBy(PlanPartition::getAttributesMap,
                                LinkedHashMap::new,
                                Collectors.flatMapping(planPartition -> planPartition.getPlans().stream(), LinkedIdentitySet.toLinkedIdentitySet())));

        return toPlanPartitions(rolledUpAttributesMap);
    }

    @Nonnull
    public static List<PlanPartition> toPlanPartitions(@Nonnull Map<Map<PlanProperty<?>, ?>, ? extends Set<RecordQueryPlan>> attributesToPlansMap) {
        return attributesToPlansMap
                .entrySet()
                .stream()
                .map(entry -> {
                    final var attributesMap = entry.getKey();
                    final var plans = entry.getValue();
                    return new PlanPartition(attributesMap, plans);
                })
                .collect(ImmutableList.toImmutableList());
    }
}
