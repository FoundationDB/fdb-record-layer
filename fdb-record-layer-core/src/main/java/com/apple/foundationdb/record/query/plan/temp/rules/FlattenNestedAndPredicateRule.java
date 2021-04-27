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
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.MatchOneAndRestMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.apple.foundationdb.record.query.plan.temp.matchers.QueryPredicateMatchers;
import com.apple.foundationdb.record.query.plan.temp.matchers.RelationalExpressionMatchers;
import com.apple.foundationdb.record.query.predicates.AndPredicate;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

import static com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatchers.forEachQuantifier;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QueryPredicateMatchers.anyPredicate;
import static com.apple.foundationdb.record.query.plan.temp.matchers.MultiMatcher.all;

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
    private static final BindingMatcher<QueryPredicate> nestedPredicateMatcher = anyPredicate();
    private static final BindingMatcher<QueryPredicate> otherPredicateMatcher = anyPredicate();
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifier();

    private static final BindingMatcher<LogicalFilterExpression> root =
            RelationalExpressionMatchers.logicalFilterExpression(
                    MatchOneAndRestMatcher.matchOneAndRest(
                            QueryPredicateMatchers.andPredicate(all(nestedPredicateMatcher)), all(otherPredicateMatcher)),
                    all(innerQuantifierMatcher));

    public FlattenNestedAndPredicateRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final PlannerBindings bindings = call.getBindings();
        final List<? extends QueryPredicate> innerAndChildren = bindings.getAll(nestedPredicateMatcher);
        final List<? extends QueryPredicate> otherOuterAndChildren = bindings.getAll(otherPredicateMatcher);
        final Quantifier.ForEach innerQuantifier = call.get(innerQuantifierMatcher);
        List<QueryPredicate> allConjuncts = new ArrayList<>(innerAndChildren);
        allConjuncts.addAll(otherOuterAndChildren);
        call.yield(call.ref(new LogicalFilterExpression(allConjuncts,
                innerQuantifier)));
    }
}
