/*
 * RecordQueryPlanWithComparisons.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRanges;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Set;

/**
 * A query plan that uses {@link ScanComparisons} or {@link Comparisons.Comparison} to drive some scan of
 * the record store.
 */
@API(API.Status.EXPERIMENTAL)
public interface RecordQueryPlanWithComparisons extends RecordQueryPlan {

    default boolean hasComparisons() {
        return hasComparisonRanges() || hasScanComparisons();
    }

    default Set<Comparisons.Comparison> getComparisons() {
        // TODO find a way to memoize this
        Preconditions.checkArgument(hasComparisonRanges() || hasScanComparisons());

        if (hasComparisonRanges()) {
            final ComparisonRanges comparisonRanges = getComparisonRanges();
            final ImmutableSet.Builder<Comparisons.Comparison> resultBuilder = ImmutableSet.builder();
            comparisonRanges.getRanges()
                    .forEach(comparisonRange -> {
                        if (comparisonRange.isEquality()) {
                            resultBuilder.add(comparisonRange.getEqualityComparison());
                        } else if (comparisonRange.isInequality()) {
                            resultBuilder.addAll(comparisonRange.getInequalityComparisons());
                        }
                    });
            return resultBuilder.build();
        }

        final var scanComparisons = getScanComparisons();
        return ImmutableSet.<Comparisons.Comparison>builder()
                .addAll(scanComparisons.getEqualityComparisons())
                .addAll(scanComparisons.getInequalityComparisons())
                .build();
    }

    @Nonnull
    ScanComparisons getScanComparisons();

    default boolean hasScanComparisons() {
        return true;
    }

    @Nonnull
    ComparisonRanges getComparisonRanges();

    default boolean hasComparisonRanges() {
        return true;
    }
}
