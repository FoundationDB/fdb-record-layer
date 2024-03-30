/*
 * ReferenceMatchers.java
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

package com.apple.foundationdb.record.query.plan.cascades.matching.structure;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.PlanPartition;
import com.apple.foundationdb.record.query.plan.cascades.PlanProperty;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Set;
import java.util.function.Predicate;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcher.typed;

/**
 * Matchers for {@link Reference}s.
 */
@API(API.Status.EXPERIMENTAL)
public class ReferenceMatchers {
    @Nonnull
    private static final BindingMatcher<Reference> topReferenceMatcher = BindingMatcher.instance();

    private ReferenceMatchers() {
        // do not instantiate
    }


    @Nonnull
    public static BindingMatcher<Reference> getTopReferenceMatcher() {
        return topReferenceMatcher;
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static <R extends Reference> BindingMatcher<R> anyRef() {
        return typed((Class<R>)(Class<?>)Reference.class);
    }

    @Nonnull
    public static BindingMatcher<? extends Reference> anyRefOverOnlyPlans() {
        return members(all(RelationalExpressionMatchers.ofType(RecordQueryPlan.class)));
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static <R extends Reference, E extends RelationalExpression> BindingMatcher<R> members(@Nonnull final CollectionMatcher<E> downstream) {
        return TypedMatcherWithExtractAndDownstream.typedWithDownstream((Class<R>)(Class<?>)Reference.class,
                Extractor.of(Reference::getMembers, name -> "members(" + name + ")"),
                downstream);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static <R extends Reference> BindingMatcher<R> planPartitions(@Nonnull final BindingMatcher<? extends Iterable<? extends PlanPartition>> downstream) {
        return TypedMatcherWithExtractAndDownstream.typedWithDownstream((Class<R>)(Class<?>)Reference.class,
                Extractor.of(Reference::getPlanPartitions, name -> "planPartitions(" + name + ")"),
                downstream);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static <R extends Reference> BindingMatcher<R> exploratoryMembers(@Nonnull final BindingMatcher<? extends Iterable<? extends RelationalExpression>> downstream) {
        return TypedMatcherWithExtractAndDownstream.typedWithDownstream((Class<R>)(Class<?>)Reference.class,
                Extractor.of(Reference::getExploratoryMembers, name -> "exploratoryMembers(" + name + ")"),
                downstream);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static BindingMatcher<Collection<PlanPartition>> where(@Nonnull final Predicate<PlanPartition> predicate,
                                                                  @Nonnull final BindingMatcher<? extends Iterable<? extends PlanPartition>> downstream) {
        return TypedMatcherWithExtractAndDownstream.typedWithDownstream((Class<Collection<PlanPartition>>)(Class<?>)Collection.class,
                Extractor.of(planPartitions -> planPartitions.stream().filter(predicate).collect(ImmutableList.toImmutableList()), name -> "filtered planPartitions(" + name + ")"),
                downstream);
    }

    public static BindingMatcher<Collection<PlanPartition>> rollUp(@Nonnull final BindingMatcher<? extends Iterable<? extends PlanPartition>> downstream) {
        return rollUpTo(downstream, ImmutableSet.of());
    }

    public static BindingMatcher<Collection<PlanPartition>> rollUpTo(@Nonnull final BindingMatcher<? extends Iterable<? extends PlanPartition>> downstream, @Nonnull final PlanProperty<?> interestingAttribute) {
        return rollUpTo(downstream, ImmutableSet.of(interestingAttribute));
    }

    @SuppressWarnings("unchecked")
    public static BindingMatcher<Collection<PlanPartition>> rollUpTo(@Nonnull final BindingMatcher<? extends Iterable<? extends PlanPartition>> downstream, @Nonnull final Set<PlanProperty<?>> interestingAttributes) {
        return TypedMatcherWithExtractAndDownstream.typedWithDownstream((Class<Collection<PlanPartition>>)(Class<?>)Collection.class,
                Extractor.of(planPartitions -> PlanPartition.rollUpTo(planPartitions, interestingAttributes), name -> "rolled up planPartitions(" + name + ")"),
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
