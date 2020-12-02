/*
 * FlattenNestedAndPredicateRule.java
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

package com.apple.foundationdb.record.query.plan.temp.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.MultiChildrenMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.AnyChildWithRestMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.AnyChildrenMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeWithPredicateMatcher;
import com.apple.foundationdb.record.query.predicates.AndPredicate;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

/**
 * A simple rule that performs some basic Boolean normalization by flattening a nested {@link AndPredicate} into a single,
 * wider AND. This rule only attempts to remove a single {@code AndComponent}; it may be repeated if necessary.
 * For example, it would transform:
 * <code>
 *     Query.and(
 *         Query.and(Query.field("a").equals("foo"), Query.field("b").equals("bar")),
 *         Query.field(c").equals("baz"),
 *         Query.and(Query.field("d").equals("food"), Query.field("e").equals("bare"))
 * </code>
 * to
 * <code>
 *     Query.and(
 *         Query.field("a").equals("foo"),
 *         Query.field("b").equals("bar"),
 *         Query.field("c").equals("baz")),
 *         Query.and(Query.field("d").equals("food"), Query.field("e").equals("bare"))
 * </code>
 */
@API(API.Status.EXPERIMENTAL)
public class FlattenNestedAndPredicateRule extends PlannerRule<LogicalFilterExpression> {
    private static final ExpressionMatcher<QueryPredicate> andChildrenMatcher = TypeMatcher.of(QueryPredicate.class, AnyChildrenMatcher.ANY);
    private static final ExpressionMatcher<QueryPredicate> otherInnerComponentsMatcher = TypeMatcher.of(QueryPredicate.class, AnyChildrenMatcher.ANY);
    private static final ExpressionMatcher<Quantifier.ForEach> innerQuantifierMatcher = QuantifierMatcher.forEach(ReferenceMatcher.anyRef());
    private static final ExpressionMatcher<LogicalFilterExpression> root = TypeWithPredicateMatcher.ofPredicate(LogicalFilterExpression.class,
            TypeMatcher.of(AndPredicate.class,
                    AnyChildWithRestMatcher.anyMatchingWithRest(
                            TypeMatcher.of(AndPredicate.class, MultiChildrenMatcher.allMatching(andChildrenMatcher)),
                    otherInnerComponentsMatcher)),
            innerQuantifierMatcher);


    public FlattenNestedAndPredicateRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final List<QueryPredicate> innerAndChildren = call.getBindings().getAll(andChildrenMatcher);
        final List<QueryPredicate> otherOuterAndChildren = call.getBindings().getAll(otherInnerComponentsMatcher);
        final Quantifier.ForEach innerQuantifier = call.get(innerQuantifierMatcher);
        List<QueryPredicate> allConjuncts = new ArrayList<>(innerAndChildren);
        allConjuncts.addAll(otherOuterAndChildren);
        call.yield(call.ref(new LogicalFilterExpression(new AndPredicate(allConjuncts),
                innerQuantifier)));
    }
}
