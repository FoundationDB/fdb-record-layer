/*
 * CombineFilterRule.java
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

package com.apple.foundationdb.record.query.plan.temp.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.Quantifiers;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatchers;
import com.apple.foundationdb.record.query.plan.temp.matchers.QueryPredicateMatchers;
import com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatchers;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;

import static com.apple.foundationdb.record.query.plan.temp.expressions.LogicalFilterExpression.logicalFilterExpression;
import static com.apple.foundationdb.record.query.plan.temp.matchers.TListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.temp.matchers.TMultiMatcher.all;

/**
 * A simple rule that combines two nested filter plans and combines them into a single filter plan with a conjunction
 * of the two filters.
 *
 * <pre>
 * {@code
 *     +----------------------------+                               +-------------------------------------+
 *     |                            |                               |                                     |
 *     |  LogicalFilterExpression   |                               |  LogicalFilterExpression            |
 *     |                 upperPred  |                               |             lowerPred' ^ upperPred  |
 *     |                            |                               |                                     |
 *     +-------------+--------------+                               +------------------+------------------+
 *                   |                                                                 |
 *                   |  upperQun                                                       |
 *                   |                    +------------------->                        |
 *     +-------------+--------------+                                                  |
 *     |                            |                                                  |
 *     |  LogicalFilterExpression   |                                                  |
 *     |                 lowerPred  |                                                  |
 *     |                            |                                                  |
 *     +-------------+--------------+                                                  |
 *                   |                                                                 | upperQun'
 *                   |  lowerQun                                                       |
 *                   |                                                                 |
 *             +-----+-----+                                                           |
 *             |           |                                                           |
 *             |  anyRef   |   +-------------------------------------------------------+
 *             |           |
 *             +-----------+
 * }
 * </pre>
 *
 * where lowerPred has been rebased (pulled up through upperQun). upperQun' on the right side is still a duplicated
 * quantifier but it shares the same name.
 */
@API(API.Status.EXPERIMENTAL)
public class CombineFilterRule extends PlannerRule<LogicalFilterExpression> {
    private static final BindingMatcher<? extends ExpressionRef<? extends RelationalExpression>> innerMatcher = ReferenceMatchers.anyRef();
    private static final BindingMatcher<Quantifier.ForEach> lowerQunMatcher = QuantifierMatchers.forEachQuantifierOverRef(innerMatcher);
    private static final BindingMatcher<QueryPredicate> lowerMatcher = QueryPredicateMatchers.anyPredicate();
    private static final BindingMatcher<Quantifier.ForEach> upperQunMatcher =
            QuantifierMatchers.forEachQuantifier(
                    logicalFilterExpression(
                            all(lowerMatcher),
                            exactly(lowerQunMatcher)));

    private static final BindingMatcher<QueryPredicate> upperMatcher = QueryPredicateMatchers.anyPredicate();
    private static final BindingMatcher<LogicalFilterExpression> root =
            logicalFilterExpression(all(upperMatcher),
                    exactly(upperQunMatcher));

    public CombineFilterRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final PlannerBindings bindings = call.getBindings();
        final ExpressionRef<?> inner = bindings.get(innerMatcher);
        final Quantifier.ForEach lowerQun = bindings.get(lowerQunMatcher);
        final List<? extends QueryPredicate> lowerPreds = bindings.getAll(lowerMatcher);
        final Quantifier.ForEach upperQun = call.get(upperQunMatcher);
        final List<? extends QueryPredicate> upperPreds = bindings.getAll(upperMatcher);
        
        final Quantifier.ForEach newUpperQun =
                Quantifier.forEach(inner, upperQun.getAlias());
                        
        final List<? extends QueryPredicate> newLowerPred =
                lowerPreds.stream()
                        .map(lowerPred -> lowerPred.rebase(Quantifiers.translate(lowerQun, newUpperQun)))
                        .collect(ImmutableList.toImmutableList());
        call.yield(call.ref(
                new LogicalFilterExpression(Iterables.concat(upperPreds, newLowerPred),
                        newUpperQun)));
    }
}
