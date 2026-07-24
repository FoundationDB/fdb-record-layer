/*
 * SelectMergeableProperty.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.query.plan.cascades.ExpressionProperty;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionWithPredicates;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * A property indicating whether the top expression of a subgraph is one that
 * {@link com.apple.foundationdb.record.query.plan.cascades.rules.SelectMergeRule} could merge into a referencing parent
 * {@link com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression}. This captures only the
 * child-side condition of mergeability, namely that the expression carries predicates (i.e. it is a
 * {@link com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression} or a
 * {@link com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression}). The remaining
 * conditions (the referencing quantifier being a plain {@code ForEach} without a null-on-empty) are properties of the
 * parent quantifier and are therefore not visible to a property computed on the expression itself; they continue to be
 * checked at rule match time.
 */
public class SelectMergeableProperty implements ExpressionProperty<Boolean> {
    private static final SelectMergeableProperty SELECT_MERGEABLE = new SelectMergeableProperty();

    private SelectMergeableProperty() {
        // prevent outside instantiation
    }

    @Nonnull
    @Override
    public RelationalExpressionVisitorWithDefaults<Boolean> createVisitor() {
        return new SelectMergeableVisitor();
    }

    public boolean evaluate(@Nonnull final Reference reference) {
        return evaluate(reference.get());
    }

    public boolean evaluate(@Nonnull final RelationalExpression expression) {
        return Objects.requireNonNull(createVisitor().visit(expression));
    }

    @Nonnull
    public static SelectMergeableProperty selectMergeable() {
        return SELECT_MERGEABLE;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    /**
     * A visitor that inspects only the top expression: it returns {@code true} for any
     * {@link RelationalExpressionWithPredicates} (a {@code SelectExpression} or {@code LogicalFilterExpression}) and
     * {@code false} for everything else. It does not recurse into children.
     */
    public static class SelectMergeableVisitor implements RelationalExpressionVisitorWithDefaults<Boolean> {
        @Nonnull
        @Override
        public Boolean visitDefault(@Nonnull final RelationalExpression expression) {
            return expression instanceof RelationalExpressionWithPredicates;
        }
    }
}
