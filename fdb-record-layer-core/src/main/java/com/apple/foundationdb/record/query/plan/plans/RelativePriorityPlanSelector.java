/*
 * RelativePriority.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.function.Function;

public class RelativePriorityPlanSelector implements RecordQuerySelectorPlan.PlanSelector {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Relative-Priority-Plan-Selector");
    @Nonnull
    List<Double> priorities;
    @Nonnull
    Random random;

    public RelativePriorityPlanSelector(@Nonnull final List<Double> priorities) {
        this(priorities, new Random());
    }

    @VisibleForTesting
    RelativePriorityPlanSelector(@Nonnull final List<Double> priorities, @Nonnull final Random random) {
        Verify.verify((priorities != null) && (!priorities.isEmpty()));
        this.priorities = ImmutableList.copyOf(priorities);
        this.random = random;
        double sumAll = this.priorities.stream().mapToDouble(Double::doubleValue).sum();
        Verify.verify(sumAll == 1.0D);
    }

    @Override
    public int selectPlan(@Nonnull final List<RecordQueryPlan> plans) {
        double rand = random.nextDouble();
        double sum = priorities.get(0);
        int index = 0;
        while (sum <= rand) {
            sum += priorities.get(++index);
        }
        return index;
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, priorities);
    }

    @Override
    public String toString() {
        return "RelativePriorityPlanSelector{" +
               "priorities=" + priorities +
               '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final RelativePriorityPlanSelector that = (RelativePriorityPlanSelector)o;
        return priorities.equals(that.priorities);
    }

    @Override
    public int hashCode() {
        return Objects.hash(priorities);
    }
}
