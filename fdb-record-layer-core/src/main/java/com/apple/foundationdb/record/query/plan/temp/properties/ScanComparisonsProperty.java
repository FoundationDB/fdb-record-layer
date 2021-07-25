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

package com.apple.foundationdb.record.query.plan.temp.properties;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerProperty;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.IndexScanExpression;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;

/**
 * A property for counting the total number of {@link KeyExpression} columns (i.e., field-like {@code KeyExpression}s
 * such as {@link com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression} and
 * {@link com.apple.foundationdb.record.metadata.expressions.RecordTypeKeyExpression}) that are not matched with at
 * least one {@link com.apple.foundationdb.record.query.expressions.Comparisons.Comparison} in a planner expression
 * tree. This is computed over all {@link KeyExpression}s in various scan expressions over indexes
 * (e.g. {@link IndexScanExpression}, {@link RecordQueryScanPlan}), and also primary scans
 * (e.g. {@link RecordQueryScanPlan}).
 *
 * <p>
 * For example, suppose that a planner expression scans two indexes:
 * <ul>
 *     <li>{@code concat(field1, field2)}, with a comparison {@code field1 = 'foo'}</li>
 *     <li>{@code concat(field1, field3, field4)}, with a comparison {@code field1 = 'foo', field3 < 5}</li>
 * </ul>
 * The {@code UnmatchedFieldsCountProperty} on such a planner expression is 2.
 */
@API(API.Status.EXPERIMENTAL)
public class ScanComparisonsProperty implements PlannerProperty<Set<ScanComparisons>> {
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
            resultBuilder.add(((RecordQueryPlanWithComparisons)expression).getComparisons());
        }

        return resultBuilder.build();
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
