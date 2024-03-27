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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifierOverRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.anyPredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers.anyRef;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.logicalFilterExpression;

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
 * quantifier, but it shares the same name.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class CombineFilterRule extends CascadesRule<LogicalFilterExpression> {
    private static final BindingMatcher<? extends Reference> innerMatcher = anyRef();
    private static final BindingMatcher<Quantifier.ForEach> lowerQunMatcher = forEachQuantifierOverRef(innerMatcher);
    private static final BindingMatcher<QueryPredicate> lowerMatcher = anyPredicate();
    private static final BindingMatcher<Quantifier.ForEach> upperQunMatcher =
            forEachQuantifier(
                    logicalFilterExpression(
                            all(lowerMatcher),
                            exactly(lowerQunMatcher)));

    private static final BindingMatcher<QueryPredicate> upperMatcher = anyPredicate();
    private static final BindingMatcher<LogicalFilterExpression> root =
            logicalFilterExpression(all(upperMatcher),
                    exactly(upperQunMatcher));

    public CombineFilterRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final PlannerBindings bindings = call.getBindings();
        final Reference inner = bindings.get(innerMatcher);
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
        call.yieldExpression(new LogicalFilterExpression(Iterables.concat(upperPreds, newLowerPred), newUpperQun));
    }
}
