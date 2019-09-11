/*
 * RelationalPlannerExpression.java
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

package com.apple.foundationdb.record.query.plan.temp.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.NestedContext;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;

import java.util.HashSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A relational expression is a {@link PlannerExpression} that represents a stream of records. At all times, the root
 * expression being planned must be relational. This interface acts as a common tag interface for
 * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan}s, which can actually produce a stream of records,
 * and various logical relational expressions (not yet introduced), which represent an abstract stream of records but can't
 * be executed directly (such as an unimplemented sort). Other planner expressions such as {@link com.apple.foundationdb.record.query.expressions.QueryComponent}
 * and {@link com.apple.foundationdb.record.metadata.expressions.KeyExpression} do not represent streams of records.
 */
@API(API.Status.EXPERIMENTAL)
public interface RelationalPlannerExpression extends PlannerExpression {
    @Nonnull
    static PlannerExpression fromRecordQuery(@Nonnull RecordQuery query) {
        RelationalPlannerExpression expression = new FullUnorderedScanExpression();
        if (query.getSort() != null) {
            expression = new LogicalSortExpression(query.getSort(), query.isSortReverse(), expression);
        }
        if (query.getFilter() != null) {
            expression = new LogicalFilterExpression(query.getFilter(), expression);
        }
        if (!query.getRecordTypes().isEmpty()) {
            expression = new LogicalTypeFilterExpression(new HashSet<>(query.getRecordTypes()), expression);
        }
        if (query.removesDuplicates()) {
            expression = new LogicalDistinctExpression(expression);
        }
        return expression;
    }

    /**
     * Produce an exactly equivalent version of the {@link PlannerExpression} tree rooted at this
     * {@code RelationalPlannerExpression} as if all operations were nested inside the given {@link NestedContext}. That
     * is, transform all predicates, index scans, and other operations to the form that they would have if they were
     * nested within the field given by the parent field of the given nested context. If it is not possible to produce
     * such an expression, return {@code null}.
     *
     * <p>
     * With {@link #asUnnestedWith(NestedContext, ExpressionRef)}, this method should obey the contract that, for any
     * {@code expression} and {@code nestedContext},
     * {@code expression.asNestedWith(nestedContext, ref).asUnnestedWith(nestedContext, ref)} is either equal to
     * {@code expression} (according to the {@code equals()} comparison) or {@code null}.
     * </p>
     *
     * <p>
     * For example, if this expression is a filter on the predicate {@code field("a").matches(field("b).equals(3))},
     * with an inner {@link FullUnorderedScanExpression}, and the {@code nestedContext} is built around
     * {@code field("a")}, then this method would return a reference containing a logical filter on the predicate
     * {@code field("b").equals(3}) with an inner {@link FullUnorderedScanExpression}.
     * </p>
     *
     * <p>
     * The {@code thisRef} parameter has two uses. For some implementations of {@code asNestedWith()}, the expression
     * does not need to be changed, and so it is more efficient to return the containing reference than to build a
     * new one. Additionally, it is used to generate a reference of the appropriate type using the
     * {@link ExpressionRef#getNewRefWith(PlannerExpression)}.
     * </p>
     * @param nestedContext a context describing the field to use for nesting
     * @param thisRef the reference that contains this relational planner expression
     * @return a nested version of this expression with respect to the given context, or null if no such expression exists
     */
    @Nullable
    @API(API.Status.EXPERIMENTAL)
    ExpressionRef<RelationalPlannerExpression> asNestedWith(@Nonnull NestedContext nestedContext,
                                                            @Nonnull ExpressionRef<RelationalPlannerExpression> thisRef);

    /**
     * Produce an exactly equivalent version of the {@link PlannerExpression} tree rooted at this
     * {@code RelationalPlannerExpression} with all operations placed inside the field given by the {@link NestedContext}.
     * That is, put all predicates, index scans, and other operations within the given nested field. If it is not possible
     * to produce such an expression, return {@code null}.
     *
     * <p>
     * With {@link #asNestedWith(NestedContext, ExpressionRef)}, this method should obey the contract that, for any
     * {@code expression} and {@code nestedContext},
     * {@code expression.asNestedWith(nestedContext, ref).asUnnestedWith(nestedContext, ref)} is either equal to
     * {@code expression} (according to the {@code equals()} comparison) or {@code null}.
     * </p>
     *
     * <p>
     * For example, if this expression is a logical filter on the predicate {@code field("b).equals(3)}, with an inner
     * {@link FullUnorderedScanExpression}, and the {@code nestedContext} is built around {@code field("a")}, then this
     * method would return a reference containing a logical filter on the predicate
     * {@code field("a").matches(field("b").equals(3))} with an inner {@link FullUnorderedScanExpression}.
     * </p>k
     *
     * <p>
     * The {@code thisRef} parameter has two uses. For some implementations of {@code asUnnestedWith()}, the expression
     * does not need to be changed, and so it is more efficient to return the containing reference than to build a
     * new one. Additionally, it is used to generate a reference of the appropriate type using the
     * {@link ExpressionRef#getNewRefWith(PlannerExpression)}.
     * </p>
     * @param nestedContext a context describing the field to use for unnesting
     * @param thisRef the reference that contains this expression
     * @return a unnested version of this expression with respect to the given context, or null if no such expression exists
     */
    @Nullable
    @API(API.Status.EXPERIMENTAL)
    ExpressionRef<RelationalPlannerExpression> asUnnestedWith(@Nonnull NestedContext nestedContext,
                                                              @Nonnull ExpressionRef<RelationalPlannerExpression> thisRef);
}
