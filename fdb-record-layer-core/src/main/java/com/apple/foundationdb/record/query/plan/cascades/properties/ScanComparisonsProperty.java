/*
 * ScanComparisonsProperty.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.properties;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionProperty;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionOnKeyExpressionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionOnValuesPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithComparisons;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;

/**
 * A property for collecting all {@link ScanComparisons} for the sub tree the property is evaluated on.
 */
@API(API.Status.EXPERIMENTAL)
public class ScanComparisonsProperty implements ExpressionProperty<Set<ScanComparisons>>, RelationalExpressionVisitorWithDefaults<Set<ScanComparisons>> {
    @Nonnull
    @Override
    public Set<ScanComparisons> evaluateAtExpression(@Nonnull RelationalExpression expression, @Nonnull List<Set<ScanComparisons>> childResults) {
        final ImmutableSet.Builder<ScanComparisons> resultBuilder = ImmutableSet.builder();
        for (Set<ScanComparisons> childResult : childResults) {
            if (childResult != null) {
                resultBuilder.addAll(childResult);
            }
        }

        if (expression instanceof RecordQueryCoveringIndexPlan) {
            expression = ((RecordQueryCoveringIndexPlan)expression).getIndexPlan();
        }

        if (expression instanceof RecordQueryPlanWithComparisons) {
            final var planWithComparisons = (RecordQueryPlanWithComparisons)expression;
            if (planWithComparisons.hasScanComparisons()) {
                resultBuilder.add(planWithComparisons.getScanComparisons());
            }
        }

        return resultBuilder.build();
    }

    @Nonnull
    @Override
    public Set<ScanComparisons> visitRecordQueryIntersectionOnKeyExpressionPlan(@Nonnull final RecordQueryIntersectionOnKeyExpressionPlan intersectionOnKeyExpressionPlan) {
        return visitRecordQueryIntersectionPlan(intersectionOnKeyExpressionPlan);
    }

    @Nonnull
    @Override
    public Set<ScanComparisons> visitRecordQueryIntersectionOnValuesPlan(@Nonnull final RecordQueryIntersectionOnValuesPlan intersectionOnValuesPlan) {
        return visitRecordQueryIntersectionPlan(intersectionOnValuesPlan);
    }

    @Nonnull
    private Set<ScanComparisons> visitRecordQueryIntersectionPlan(@Nonnull final RecordQueryIntersectionPlan intersectionPlan) {
        //TODO revisit this logic if we ever implement skipping intersections
        final var scanComparisonsFromQuantifiers = visitQuantifiers(intersectionPlan);
        Verify.verify(!scanComparisonsFromQuantifiers.isEmpty());

        if (scanComparisonsFromQuantifiers.size() == 1) {
            return Iterables.getOnlyElement(scanComparisonsFromQuantifiers);
        }

        final var it = scanComparisonsFromQuantifiers.iterator();
        final var intersected = Sets.newHashSet(it.next());
        while (it.hasNext()) {
            intersected.retainAll(it.next());
        }
        return intersected;
    }

    @Nonnull
    @Override
    public Set<ScanComparisons> evaluateAtRef(@Nonnull ExpressionRef<? extends RelationalExpression> ref, @Nonnull List<Set<ScanComparisons>> memberResults) {
        final ImmutableSet.Builder<ScanComparisons> resultBuilder = ImmutableSet.builder();
        for (Set<ScanComparisons> memberResult : memberResults) {
            if (memberResult != null) {
                resultBuilder.addAll(memberResult);
            }
        }
        return resultBuilder.build();
    }

    public static Set<ScanComparisons> evaluate(@Nonnull RelationalExpression expression) {
        Set<ScanComparisons> result = expression.acceptPropertyVisitor(new ScanComparisonsProperty());
        if (result == null) {
            return ImmutableSet.of();
        }
        return result;
    }
}
