/*
 * PlanPartitionMatchers.java
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

package com.apple.foundationdb.record.query.plan.cascades.matching.structure;

import com.apple.foundationdb.record.query.plan.cascades.ExpressionProperty;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartition;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartitions;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Set;
import java.util.function.Predicate;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcher.typed;

public class PlanPartitionMatchers {
    private PlanPartitionMatchers() {
        // do not instantiate
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static BindingMatcher<Reference> planPartitions(@Nonnull final BindingMatcher<? extends Iterable<PlanPartition>> downstream) {
        return TypedMatcherWithExtractAndDownstream.typedWithDownstream(Reference.class,
                Extractor.of(Reference::toPlanPartitions, name -> "planPartitions(" + name + ")"),
                downstream);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static BindingMatcher<Collection<PlanPartition>> filterPlanPartitions(@Nonnull final Predicate<PlanPartition> predicate,
                                                                                 @Nonnull final BindingMatcher<? extends Iterable<PlanPartition>> downstream) {
        return TypedMatcherWithExtractAndDownstream.typedWithDownstream((Class<Collection<PlanPartition>>)(Class<?>)Collection.class,
                Extractor.of(planPartitions ->
                        planPartitions.stream()
                                .filter(predicate)
                                .collect(ImmutableList.toImmutableList()),
                        name -> "filtered planPartitions(" + name + ")"),
                downstream);
    }

    @Nonnull
    public static BindingMatcher<Collection<PlanPartition>> rollUpPartitions(@Nonnull final BindingMatcher<? extends Iterable<PlanPartition>> downstream) {
        return rollUpPartitionsTo(downstream, ImmutableSet.of());
    }

    @Nonnull
    public static BindingMatcher<Collection<PlanPartition>> rollUpPartitionsTo(@Nonnull final BindingMatcher<? extends Iterable<PlanPartition>> downstream,
                                                                               @Nonnull final ExpressionProperty<?> interestingProperty) {
        return rollUpPartitionsTo(downstream, ImmutableSet.of(interestingProperty));
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static BindingMatcher<Collection<PlanPartition>> rollUpPartitionsTo(@Nonnull final BindingMatcher<? extends Iterable<PlanPartition>> downstream,
                                                                               @Nonnull final Set<ExpressionProperty<?>> interestingProperties) {
        return TypedMatcherWithExtractAndDownstream.typedWithDownstream((Class<Collection<PlanPartition>>)(Class<?>)Collection.class,
                Extractor.of(partitions -> PlanPartitions.rollUpTo(partitions, interestingProperties),
                        name -> "rolled up planPartitions(" + name + ")"),
                downstream);
    }

    @Nonnull
    public static BindingMatcher<PlanPartition> anyPlanPartition() {
        return typed(PlanPartition.class);
    }

    @Nonnull
    public static BindingMatcher<PlanPartition> planPartitionWhere(@Nonnull Predicate<PlanPartition> predicate) {
        return TypedMatcherWithPredicate.typedMatcherWithPredicate(PlanPartition.class, predicate);
    }
}
