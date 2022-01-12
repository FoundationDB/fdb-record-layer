/*
 * RelativeProbabilityPlanSelector.java
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
 * A utility class to implement an algorithm to make a random selection based off of relative probabilities.
 * The probabilities are given in the list to the constructor, and the selector chooses a number (between
 * 0 and the probabilities.size - 1), where the chance for a given selection to be picked being relative to its value.
 * The relative probabilities are given as % points: they should all add up to 100.
 * For example, with the given list having {50, 35, 15}, at 50% the selector will pick #0, at 35% #1 and at 15%, #2.
 */
public class RelativeProbabilityPlanSelector implements PlanSelector {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Relative-Probability-Plan-Selector");
    @Nonnull
    List<Integer> probabilities;
    @Nonnull
    Random random;

    /**
     * Create a new probability selector with the given probabilities list.
     * @param probabilities the list of probabilities. These should all add up to 100.
     */
    public RelativeProbabilityPlanSelector(@Nonnull final List<Integer> probabilities) {
        this(probabilities, ThreadLocalRandom.current());
    }

    @VisibleForTesting
    RelativeProbabilityPlanSelector(@Nonnull final List<Integer> probabilities, @Nonnull final Random random) {
        if (probabilities.isEmpty()) {
            throw new RecordCoreArgumentException("Probability selector should have at least one probability");
        }
        this.probabilities = List.copyOf(probabilities);
        this.random = random;
        int sumAll = this.probabilities.stream().mapToInt(Integer::intValue).sum();
        if (sumAll != 100) {
            throw new RecordCoreArgumentException("Probabilities should all add up to 100");
        }
    }

    @Override
    public int selectPlan(@Nonnull final List<RecordQueryPlan> plans) {
        int rand = random.nextInt(100) + 1;
        int sum = probabilities.get(0);
        int index = 0;
        while (sum < rand) {
            sum += probabilities.get(++index);
        }
        return index;
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, probabilities);
    }

    @Override
    public String toString() {
        return "RelativeProbabilityPlanSelector{" +
               "probabilities=" + probabilities +
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
        final RelativeProbabilityPlanSelector that = (RelativeProbabilityPlanSelector)o;
        return probabilities.equals(that.probabilities);
    }

    @Override
    public int hashCode() {
        return Objects.hash(probabilities);
    }
}
