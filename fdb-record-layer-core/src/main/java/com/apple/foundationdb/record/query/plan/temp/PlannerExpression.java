/*
 * PlannerExpression.java
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

import com.apple.foundationdb.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Optional;

/**
 * The basic type that represents a part of the planner expression tree. An expression is generally an immutable
 * object with two different kinds of fields: regular Java fields and reference fields. The regular fields represent
 * "node information", which pertains only to this specific node in the tree. In contrast, the reference fields represent
 * this expression's children in the tree, such as its inputs and filter/sort expressions, and are always hidden behind
 * an {@link ExpressionRef}.
 *
 * Deciding whether certain fields constitute "node information" (and should therefore be a regular field) or
 * "hierarchical information" (and therefore should not be) is subtle and more of an art than a science. There are two
 * reasonable tests that can help make this decision:
 * <ol>
 *     <li>When writing a planner rule to manipulate this field, does it make sense to match it separately
 *     or access it as a getter on the matched operator? Will you ever want to match to just this field?</li>
 *     <li>Should the planner memoize (and therefore optimize) this field separately from its parent?</li>
 * </ol>
 *
 * For example, {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan} has only regular fields, including the
 * index name and the comparisons to use when scanning it.
 * Applying the first rule, it wouldn't really make sense to match the index name or the comparisons being performed on
 * their own: they're what define an index scan, after all!
 * Applying the second rule, they're relatively small immutable objects that don't need to be memoized.
 *
 * In contrast, {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan} has no regular fields.
 * A filter plan has two important fields: the <code>Query.Component</code> used for the filter and a child plan that
 * provides input. Both of these might be matched by rules directly, in order to optimize them without regard for the
 * fact that there's a filter. Similarly, they should both be memoized separately, since there might be many possible
 * implementations of each.
 */
@API(API.Status.EXPERIMENTAL)
public interface PlannerExpression extends Bindable {
    /**
     * Matches a matcher expression to an expression tree rooted at this node, adding to some existing bindings.
     * @param binding the binding to match against
     * @param existing an existing map of bindings
     * @return the existing bindings extended with some new ones if the match was successful or <code>Optional.empty()</code> otherwise
     */
    @Nonnull
    default Optional<PlannerBindings> bindWithExisting(@Nonnull ExpressionMatcher<? extends Bindable> binding, @Nonnull PlannerBindings existing) {
        if (existing.containsKey(binding)) {
            throw new RecordCoreException("tried to bind to a matcher that is already bound");
        }

        if (!binding.matches(this).equals(ExpressionMatcher.Result.MATCHES)) {
            return Optional.empty();
        }
        Iterator<? extends ExpressionRef<? extends PlannerExpression>> childIterator = getPlannerExpressionChildren();
        Iterator<ExpressionMatcher<? extends Bindable>> bindingIterator = binding.getChildren().iterator();

        while (childIterator.hasNext() && bindingIterator.hasNext()) {
            Optional<PlannerBindings> possible = childIterator.next().bindWithExisting(bindingIterator.next(), existing);
            if (!possible.isPresent()) {
                return possible;
            }
            existing = possible.get();
        }

        if (childIterator.hasNext() || bindingIterator.hasNext()) { // unable to match completely
            return Optional.empty();
        }
        existing.put(binding, this);
        return Optional.of(existing);
    }

    /**
     * Return an iterator of references to the children of this planner expression. The iterators returned by different
     * calls are guaranteed to be independent (i.e., advancing one will not advance another). However, they might point
     * to the same object, as when <code>Collections.emptyIterator()</code> is returned. The returned iterator should
     * be treated as an immutable object and may throw an exception if {@link Iterator#remove} is called.
     * The iterator must return its elements in a consistent order.
     * @return an iterator of references to the children of this planner expression
     */
    @Nonnull
    Iterator<? extends ExpressionRef<? extends PlannerExpression>> getPlannerExpressionChildren();

    default boolean acceptPropertyVisitor(@Nonnull PlannerProperty visitor) {
        if (visitor.visitEnter(this)) {
            Iterator<? extends ExpressionRef<? extends PlannerExpression>> children = getPlannerExpressionChildren();
            while (children.hasNext()) {
                if (!children.next().acceptPropertyVisitor(visitor)) {
                    break;
                }
            }
        }
        return visitor.visitLeave(this);
    }
}

