/*
 * NestedContextExpression.java
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

package com.apple.foundationdb.record.query.plan.temp.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.NestedContext;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.SingleExpressionRef;
import com.google.common.collect.Iterators;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;

/**
 * A planner expression that indicates that all of its descendants are nested relative to the {@link NestedContext} that
 * it holds. A {@code NestedContextExpression} allows the planner to view various sub-parts of a {@link PlannerExpression}
 * inside of a nested record as unnested, thereby allowing most rules to be written without handling the complexities
 * of nested record types.
 *
 * <p>
 * For example, suppose that we have a book record, which contains a nested author field of
 * author records and an index {@code by_author} defined by the {@code KeyExpression}:
 * <pre>
 *     field("author").nest("name")
 * </pre>
 * Consider the following planner expression with a predicate on the library records:
 * <pre>
 *     LogicalFilterExpression(
 *         field("author").nest("name").equalsValue("James Joyce"),
 *         IndexEntrySourceScanExpression("by_author", NO COMPARISONS))
 * </pre>
 * If we executed a rule (such as {@link com.apple.foundationdb.record.query.plan.temp.rules.FilterWithNestedToNestingContextRule})
 * to convert this into a form relative to the {@link NestedContext} for the non-repeated field {@code author}, we
 * would obtain the following planner expression:
 * <pre>
 *     NestedContextExpression(SingleFieldNestingContext(field("author")),
 *         LogicalFilterExpression(
 *             field("name).equalsValue("James Joyce"),
 *             IndexEntrySourceScanExpression("by_author", NO COMPARISONS))
 * </pre>
 * where the {@code IndexEntrySourceScanExpression} now has a
 * {@link com.apple.foundationdb.record.query.plan.temp.KeyExpressionComparisons} based on the {@code KeyExpression}
 * <pre>
 *     field("name")
 * </pre>
 * Similarly, unnesting the latter expression would produce the former expression.
 *
 * @see NestedContext
 * @see com.apple.foundationdb.record.query.plan.temp.rules.FilterWithNestedToNestingContextRule
 * @see com.apple.foundationdb.record.query.plan.temp.rules.RemoveNestedContextRule
 */
@API(API.Status.EXPERIMENTAL)
public class NestedContextExpression implements RelationalPlannerExpression {
    @Nonnull
    private final NestedContext nestedContext;
    @Nonnull
    private final ExpressionRef<RelationalPlannerExpression> inner;

    public NestedContextExpression(@Nonnull NestedContext nestedContext, @Nonnull RelationalPlannerExpression inner) {
        this(nestedContext, SingleExpressionRef.of(inner));
    }

    public NestedContextExpression(@Nonnull NestedContext nestedContext, @Nonnull ExpressionRef<RelationalPlannerExpression> inner) {
        this.nestedContext = nestedContext;
        this.inner = inner;
    }

    @Nonnull
    public NestedContext getNestedContext() {
        return nestedContext;
    }

    @Nullable
    @Override
    public ExpressionRef<RelationalPlannerExpression> asNestedWith(@Nonnull NestedContext nestedContext,
                                                                   @Nonnull ExpressionRef<RelationalPlannerExpression> thisRef) {
        return null; // not nestable
    }

    @Nullable
    @Override
    public ExpressionRef<RelationalPlannerExpression> asUnnestedWith(@Nonnull NestedContext nestedContext,
                                                                     @Nonnull ExpressionRef<RelationalPlannerExpression> thisRef) {
        return null;
    }

    @Nonnull
    @Override
    public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
        return Iterators.singletonIterator(inner);
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull PlannerExpression otherExpression) {
        return otherExpression instanceof NestedContextExpression &&
               nestedContext.equals(((NestedContextExpression)otherExpression).getNestedContext());
    }
}
