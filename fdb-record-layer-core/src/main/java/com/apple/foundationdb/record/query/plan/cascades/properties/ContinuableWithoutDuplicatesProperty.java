/*
 * ContinuableWithoutDuplicatesProperty.java
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
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitor;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedDistinctPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;

import javax.annotation.Nonnull;
import java.util.List;


/**
 * A property that determines whether a {@link RecordQueryPlan} can be safely continued (resumed from a continuation)
 * without producing duplicate records.
 *
 * <p>
 * Certain plan operators maintain in-memory state (e.g. {@link RecordQueryUnorderedDistinctPlan}) that is
 * lost across continuations. If such a plan appears anywhere in the plan tree, resuming from a
 * continuation may result in a record that was already processed to be produced again.
 * This property traverses the plan tree and returns {@code false} if any such operator is found,
 * and {@code true} otherwise.
 * </p>
 *
 * <p>
 * This class is a singleton; use {@link #continuableWithoutDuplicates()} to obtain the instance.
 * </p>
 */
public final class ContinuableWithoutDuplicatesProperty implements ExpressionProperty<Boolean> {
    private static final ContinuableWithoutDuplicatesProperty CONTINUABLE_WITHOUT_DUPLICATES_PROPERTY =
            new ContinuableWithoutDuplicatesProperty();

    private ContinuableWithoutDuplicatesProperty() {
        // prevent outside instantiation
    }

    /**
     * Creates a new {@link RelationalExpressionVisitor} that traverses a plan tree and evaluates whether the plan
     * can be continued without producing duplicates.
     *
     * @return a visitor that returns {@code true} if the plan can be continued without producing duplicate records,
     *         {@code false} otherwise.
     */
    @Nonnull
    @Override
    public RelationalExpressionVisitor<Boolean> createVisitor() {
        return ExpressionProperty.toExpressionVisitor(new ContinuableWithoutDuplicatesPropertyVisitor());
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    /**
     * Evaluates whether the given {@link RecordQueryPlan} can be safely continued without producing duplicate records.
     *
     * @param recordQueryPlan the plan to evaluate.
     * @return {@code true} if the plan can be continued without producing duplicate records, {@code false} otherwise.
     */
    public boolean evaluate(@Nonnull final RecordQueryPlan recordQueryPlan) {
        return createVisitor().visit(recordQueryPlan);
    }

    /**
     * Returns the singleton instance of this property.
     *
     * @return the singleton {@link ContinuableWithoutDuplicatesProperty} instance.
     */
    @Nonnull
    public static ContinuableWithoutDuplicatesProperty continuableWithoutDuplicates() {
        return CONTINUABLE_WITHOUT_DUPLICATES_PROPERTY;
    }

    /**
     * A {@link RecordQueryPlanVisitorWithDefaults} that traverses the {@link RecordQueryPlan} tree to detect plans
     * whose in-memory state, which is not included in the serialized continuation, makes the plan unsafe to continue
     * without producing duplicates records.
     *
     * <p>
     * By default, all plan operators are considered safe (the visitor recurses into children and returns {@code true}
     * if all children are also safe). Specific plans that maintain ephemeral in-memory state override this to return
     * {@code false}.
     * </p>
     */
    public static class ContinuableWithoutDuplicatesPropertyVisitor implements RecordQueryPlanVisitorWithDefaults<Boolean> {
        /**
         * {@link RecordQueryUnorderedPrimaryKeyDistinctPlan} plans maintain an in-memory hash set of seen primary keys,
         * which is lost across continuations and could lead to duplicate records when continued.
         *
         * @param unorderedPrimaryKeyDistinctPlan the plan to visit
         * @return {@code false} always
         */
        @Nonnull
        @Override
        public Boolean visitUnorderedPrimaryKeyDistinctPlan(@Nonnull final RecordQueryUnorderedPrimaryKeyDistinctPlan unorderedPrimaryKeyDistinctPlan) {
            return false;
        }

        /**
         * {@link RecordQueryUnorderedDistinctPlan} plans maintain an in-memory hash set of seen records based on a
         * comparison key, which is lost across continuations and could lead to duplicate records when continued.
         *
         * @param unorderedDistinctPlan the plan to visit
         * @return {@code false} always
         */
        @Nonnull
        @Override
        public Boolean visitUnorderedDistinctPlan(@Nonnull final RecordQueryUnorderedDistinctPlan unorderedDistinctPlan) {
            return false;
        }

        /**
         * Default visit method for all other plan types. Recursively checks all children of the plan and returns
         * {@code true} only if every child is also continuable without duplicates.
         *
         * @param recordQueryPlan the plan to visit
         * @return {@code true} if all children are continuable without duplicates, {@code false} otherwise
         */
        @Nonnull
        @Override
        public Boolean visitDefault(@Nonnull final RecordQueryPlan recordQueryPlan) {
            return fromChildren(recordQueryPlan.getChildren());
        }

        private boolean fromChildren(@Nonnull final List<RecordQueryPlan> children) {
            return children.stream().allMatch(this::visit);
        }
    }
}
