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
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A utility class to implement an algorithm to make a random selection based off of relative priorities.
 * The priorities are given in the list of priorities (to the constructor) and the selector chooses a number (between
 * 0 and the priorities.size - 1), where the chance for a given selection to be picked being relative to its value.
 * The relative priorities are given as % points: they should all add up to 100.
 * For example, with the given list having {50, 35, 15}, at 50% the selector will pick #0, at 35% #1 and at 15%, #2.
 */
public class RelativePriorityPlanSelector implements PlanSelector {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Relative-Priority-Plan-Selector");
    @Nonnull
    List<Integer> priorities;
    @Nonnull
    Random random;

    /**
     * Create a new priority selector with the given priority list.
     * @param priorities the list of priorities. These should all add up to 100.
     */
    public RelativePriorityPlanSelector(@Nonnull final List<Integer> priorities) {
        this(priorities, ThreadLocalRandom.current());
    }

    @VisibleForTesting
    RelativePriorityPlanSelector(@Nonnull final List<Integer> priorities, @Nonnull final Random random) {
        if (priorities.isEmpty()) {
            throw new RecordCoreArgumentException("Priority selector should have at least one priority");
        }
        this.priorities = List.copyOf(priorities);
        this.random = random;
        int sumAll = this.priorities.stream().mapToInt(Integer::intValue).sum();
        if (sumAll != 100) {
            throw new RecordCoreArgumentException("Priorities should all add up to 100");
        }
    }

    @Override
    public int selectPlan(@Nonnull final List<RecordQueryPlan> plans) {
        int rand = random.nextInt(100) + 1;
        int sum = priorities.get(0);
        int index = 0;
        while (sum < rand) {
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
