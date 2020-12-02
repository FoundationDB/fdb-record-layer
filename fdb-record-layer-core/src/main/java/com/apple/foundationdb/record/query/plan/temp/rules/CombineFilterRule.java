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
import com.apple.foundationdb.record.query.plan.temp.matchers.AnyChildrenMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeWithPredicateMatcher;
import com.apple.foundationdb.record.query.predicates.AndPredicate;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;

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
    private static final ExpressionMatcher<ExpressionRef<? extends RelationalExpression>> innerMatcher = ReferenceMatcher.anyRef();
    private static final ExpressionMatcher<Quantifier.ForEach> lowerQunMatcher = QuantifierMatcher.forEach(innerMatcher);
    private static final ExpressionMatcher<QueryPredicate> lowerMatcher = TypeMatcher.of(QueryPredicate.class, AnyChildrenMatcher.ANY);
    private static final ExpressionMatcher<QueryPredicate> upperMatcher = TypeMatcher.of(QueryPredicate.class, AnyChildrenMatcher.ANY);
    private static final ExpressionMatcher<Quantifier.ForEach> upperQunMatcher =
            QuantifierMatcher.forEach(
                    TypeWithPredicateMatcher.ofPredicate(LogicalFilterExpression.class,
                            lowerMatcher,
                            lowerQunMatcher));

    private static final ExpressionMatcher<LogicalFilterExpression> root = TypeWithPredicateMatcher.ofPredicate(
            LogicalFilterExpression.class,
            upperMatcher,
            upperQunMatcher);

    public CombineFilterRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final ExpressionRef<?> inner = call.get(innerMatcher);
        final Quantifier.ForEach lowerQun = call.get(lowerQunMatcher);
        final QueryPredicate lowerPred = call.get(lowerMatcher);
        final Quantifier.ForEach upperQun = call.get(upperQunMatcher);
        final QueryPredicate upperPred = call.get(upperMatcher);

        final Quantifier.ForEach newUpperQun =
                Quantifier.forEach(inner, upperQun.getAlias());
                        
        final QueryPredicate newLowerPred = lowerPred.rebase(Quantifiers.translate(lowerQun, newUpperQun));
        final QueryPredicate combinedPred = new AndPredicate(ImmutableList.of(upperPred, newLowerPred));
        call.yield(call.ref(
                new LogicalFilterExpression(combinedPred,
                        newUpperQun)));
    }
}
