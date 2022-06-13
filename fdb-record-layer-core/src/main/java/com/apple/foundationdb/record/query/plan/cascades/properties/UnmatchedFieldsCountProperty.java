/*
 * UnmatchedFieldsCountProperty.java
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.PlanContext;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionProperty;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.IndexScanExpression;

import javax.annotation.Nonnull;
import java.util.List;

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
public class UnmatchedFieldsCountProperty implements ExpressionProperty<Integer>, RelationalExpressionVisitorWithDefaults<Integer> {
    @Nonnull
    private final PlanContext planContext;

    public UnmatchedFieldsCountProperty(@Nonnull PlanContext context) {
        this.planContext = context;
    }

    @Nonnull
    @Override
    public Integer evaluateAtExpression(@Nonnull RelationalExpression expression, @Nonnull List<Integer> childResults) {
        int total = 0;
        for (Integer result : childResults) {
            if (result != null) {
                total += result;
            }
        }

        if (expression instanceof RecordQueryCoveringIndexPlan) {
            expression = ((RecordQueryCoveringIndexPlan)expression).getIndexPlan();
        }

        final int columnSize;
        if (expression instanceof RecordQueryPlanWithComparisons) {
            final ScanComparisons comparisons = ((RecordQueryPlanWithComparisons)expression).getComparisons();
            if (expression instanceof RecordQueryPlanWithIndex) {
                final String indexName = ((RecordQueryPlanWithIndex)expression).getIndexName();
                columnSize = planContext.getIndexByName(indexName).getRootExpression().getColumnSize();
            } else if (expression instanceof RecordQueryScanPlan) {
                columnSize = planContext.getGreatestPrimaryKeyWidth();
            } else {
                throw new RecordCoreException("unhandled plan with comparisons: can't find key expression");
            }
            return total + columnSize - (comparisons.getEqualitySize() +
                                                            (comparisons.isEquality() ? 0 : 1));
        } else {
            return total;
        }
    }

    @Nonnull
    @Override
    public Integer evaluateAtRef(@Nonnull ExpressionRef<? extends RelationalExpression> ref, @Nonnull List<Integer> memberResults) {
        int min = Integer.MAX_VALUE;
        for (Integer memberResult : memberResults) {
            if (memberResult != null && memberResult < min) {
                min = memberResult;
            }
        }
        return min;
    }

    public static int evaluate(@Nonnull PlanContext context, @Nonnull RelationalExpression expression) {
        Integer result = expression.acceptPropertyVisitor(new UnmatchedFieldsCountProperty(context));
        if (result == null) {
            return Integer.MAX_VALUE;
        }
        return result;
    }
}
