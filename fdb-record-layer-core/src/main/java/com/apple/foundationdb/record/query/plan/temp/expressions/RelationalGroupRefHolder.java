/*
 * RelationalGroupRefHolder.java
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

import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.FixedCollectionExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.google.common.collect.Iterators;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Iterator;

/**
 * A special relational planner expression that holds a {@link FixedCollectionExpressionRef} which contains a fixed
 * number of possible implementations of a relational planner expression. For example, the child reference might have
 * a member for each of the indexes that could implement a part of a filter. Rules should be <em>extremely careful</em>
 * when binding to or otherwise manipulating a {@link RelationalGroupRefHolder} as anything other than a reference to a
 * relational planner expression; in general, rules other than the
 * {@link com.apple.foundationdb.record.query.plan.temp.rules.PickFromPossibilitiesRule} should not bind to it at all.
 *
 * <p>
 * A {@code RelationalGroupRefHolder} acts as a special marker or "barrier" for a rewrite-style planner that usually
 * only considers a single expression at once but needs the ability to compare multiple planner expressions in a limited
 * sense. For example, if several indexes could implement part of a filter, we can try each of them and pick the one
 * that satisfies the greatest number of filters. The group reference holder exists primarily to prevent rules that are
 * not aware of the unusual semantics from binding through a {@link FixedCollectionExpressionRef}, which can create
 * a variety of complex problems that a rewrite planner is not equipped to handle and which would require a full
 * Cascades-style planner to perform efficiently.
 * </p>
 *
 * <p>
 * Unlike a Cascades-style Memo data structure, the {@code RelationalGroupRefHolder} does not attempt to memoize shared
 * results across different parts of a planner expression tree. As a result, care should be taken to minimize the number
 * of members in a collection reference. For an example of this, see the implementation of the
 * {@link com.apple.foundationdb.record.query.plan.temp.rules.FindPossibleIndexForAndComponentRule}.
 * </p>
 *
 * @see FixedCollectionExpressionRef
 */
public class RelationalGroupRefHolder implements RelationalExpressionWithChildren {
    @Nonnull
    private final FixedCollectionExpressionRef<RelationalPlannerExpression> innerGroup;

    public RelationalGroupRefHolder(@Nonnull Collection<RelationalPlannerExpression> groupMembers) {
        this.innerGroup = new FixedCollectionExpressionRef<>(groupMembers);
    }

    @Nonnull
    public FixedCollectionExpressionRef<RelationalPlannerExpression> getInnerGroup() {
        return innerGroup;
    }

    @Nonnull
    @Override
    public Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren() {
        return Iterators.singletonIterator(innerGroup);
    }

    @Override
    public int getRelationalChildCount() {
        return 1;
    }
}
