/*
 * NestedContext.java
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.temp.expressions.RelationalPlannerExpression;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A {@code NestedContext} is defined by a single {@link FieldKeyExpression} and represents a relative nesting within
 * the internal state of the query planner. Various objects that the query planner uses, such as
 * {@link PlannerExpression}s and {@link KeyExpressionComparisons}, can be <em>nested</em>and <em>unnested</em> relative
 * to a {@code NestedContext}, allowing most query planner rules to ignore the complexities of dealing with queries on
 * nested record structures. The {@code NestedContext} itself tracks the properties of the nested field and provides
 * a variety of convenience methods for simplifying the implementation of the {@code asNestedWith()} and
 * {@code asUnnestedWith()} on various planner-related classes.
 *
 * @see com.apple.foundationdb.record.query.plan.temp.expressions.NestedContextExpression for more details
 * @see com.apple.foundationdb.record.query.plan.temp.rules.FilterWithNestedToNestingContextRule
 * @see com.apple.foundationdb.record.query.plan.temp.rules.RemoveNestedContextRule
 */
@API(API.Status.EXPERIMENTAL)
public interface NestedContext {
    @Nonnull
    FieldKeyExpression getParentField();

    boolean isParentFieldRepeated();

    @Nullable
    default ExpressionRef<RelationalPlannerExpression> getNestedRelationalPlannerExpression(@Nonnull ExpressionRef<RelationalPlannerExpression> ref) {
        return ref.flatMapNullable(expression -> expression.asNestedWith(this, ref));
    }

    @Nullable
    default ExpressionRef<QueryComponent> getNestedQueryComponent(@Nonnull ExpressionRef<QueryComponent> ref) {
        return ref.flatMapNullable(expression -> expression.asNestedWith(this, ref));
    }

    @Nullable
    default ExpressionRef<RelationalPlannerExpression> getUnnestedRelationalPlannerExpression(
            @Nonnull ExpressionRef<RelationalPlannerExpression> ref) {
        return ref.flatMapNullable(expression -> expression.asUnnestedWith(this, ref));
    }

    @Nullable
    default ExpressionRef<QueryComponent> getUnnestedQueryComponent(@Nonnull ExpressionRef<QueryComponent> ref) {
        return ref.flatMapNullable(expression -> expression.asUnnestedWith(this, ref));
    }
}
