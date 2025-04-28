/*
 * PlanPartitions.java
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

import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Plan partition helpers.
 */
public class PlanPartitions {

    private PlanPartitions() {
        // do not instantiate
    }

    @Nonnull
    public static List<PlanPartition> rollUpTo(@Nonnull Collection<PlanPartition> planPartitions, @Nonnull final ExpressionProperty<?> rollupAttributes) {
        return rollUpTo(planPartitions, ImmutableSet.of(rollupAttributes));
    }

    @Nonnull
    public static List<PlanPartition> rollUpTo(@Nonnull Collection<PlanPartition> planPartitions, @Nonnull final Set<ExpressionProperty<?>> rollupAttributes) {
        return ExpressionPartitions.rollUpTo(planPartitions, rollupAttributes, PlanPartition::new);
    }

    @Nonnull
    public static List<PlanPartition> toPartitions(@Nonnull Map<Map<ExpressionProperty<?>, ?>, ? extends Set<RecordQueryPlan>> attributesToPlansMap) {
        return ExpressionPartitions.toPartitions(attributesToPlansMap, PlanPartition::new);
    }
}
