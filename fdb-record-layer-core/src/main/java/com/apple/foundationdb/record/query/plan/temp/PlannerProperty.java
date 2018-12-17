/*
 * PlannerProperty.java
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

package com.apple.foundationdb.record.query.plan.temp;

import javax.annotation.Nonnull;

/**
 * An interface for hierarchical visitors that represent Cascades properties. In Cascades, properties are measurable
 * features of an expression other than the structure of the expression tree. For example, the sort order and set of
 * record types produced by a {@link com.apple.foundationdb.record.query.plan.temp.expressions.RelationalPlannerExpression}
 * could be represented by a property.
 *
 * To avoid littering {@link PlannerExpression} classes with methods for various properties, properties are implemented
 * as visitors on the tree of {@code PlannerExpression}s and {@link ExpressionRef}s. A property can be evaluated against
 * an expression tree by having the visitor traverse the tree.
 */
public interface PlannerProperty {
    /**
     * Called on nodes in the expression tree in visit pre-order of the depth-first traversal of the tree.
     * That is, as each node is visited for the first time, <code>visitEnter()</code> is called on that node.
     * @param expression the planner expression to visit
     * @return <code>true</code> if the children of <code>cursor</code> should be visited, and <code>false</code> if they should not be visited
     */
    boolean visitEnter(@Nonnull PlannerExpression expression);

    /**
     * Called on nodes in the expression tree in visit pre-order of the depth-first traversal of the tree.
     * That is, as each node is visited for the first time, <code>visitEnter()</code> is called on that node.
     * @param ref the expression reference to visit
     * @return <code>true</code> if the children of <code>cursor</code> should be visited, and <code>false</code> if they should not be visited
     */
    boolean visitEnter(@Nonnull ExpressionRef<? extends PlannerExpression> ref);

    /**
     * Called on nodes in the expression tree in visit post-order of the depth-first traversal of the tree.
     * That is, as each node is visited for the last time (after all of its children have been visited, if applicable),
     * <code>visitLeave()</code> is called on that node.
     * @param expression the cursor to visit
     * @return <code>true</code> if the subsequent siblings of the <code>cursor</code> should be visited, and <code>false</code> otherwise
     */
    boolean visitLeave(@Nonnull PlannerExpression expression);

    /**
     * Called on nodes in the expression tree in visit post-order of the depth-first traversal of the tree.
     * That is, as each node is visited for the last time (after all of its children have been visited, if applicable),
     * <code>visitLeave()</code> is called on that node.
     * @param ref the expression reference to visit
     * @return <code>true</code> if the subsequent siblings of the <code>cursor</code> should be visited, and <code>false</code> otherwise
     */
    boolean visitLeave(@Nonnull ExpressionRef<? extends PlannerExpression> ref);
}
