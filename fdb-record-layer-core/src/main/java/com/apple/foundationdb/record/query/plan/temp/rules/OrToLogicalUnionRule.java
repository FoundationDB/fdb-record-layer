/*
 * OrToLogicalUnionRule.java
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
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.Quantifiers;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalUnionExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.QueryPredicateMatchers;
import com.apple.foundationdb.record.query.plan.temp.matchers.RelationalExpressionMatchers;
import com.apple.foundationdb.record.query.predicates.OrPredicate;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.temp.matchers.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.temp.matchers.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatchers.anyQuantifier;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QueryPredicateMatchers.anyPredicate;

/**
 * Convert a filter on an {@linkplain OrPredicate or} expression into a plan on the union. In particular, this will
 * produce a {@link LogicalUnionExpression} with simple filter plans on each child.
 *
 * <pre>
 * {@code
 *     +----------------------------+                 +-----------------------------------+
 *     |                            |                 |                                   |
 *     |  SelectExpression          |                 |  LogicalUnionExpression           |
 *     |       p1 v p2 v ... v pn   |                 |                                   |
 *     |                            |                 +-----------------------------------+
 *     +-------------+--------------+                        /        |               \
 *                   |                    +-->              /         |                \
 *                   | qun                                 /          |                 \
 *                   |                                    /           |                  \
 *                   |                                   /            |                   \
 *                   |                             +--------+    +--------+          +--------+
 *                   |                             |        |    |        |          |        |
 *                   |                             |  SEL   |    |  SEL   |          |  SEL   |
 *                   |                             |    p1' |    |    p2' |   ....   |    pn' |
 *                   |                             |        |    |        |          |        |
 *                   |                             +--------+    +--------+          +--------+
 *                   |                                /              /                   /
 *                   |                               / qun          / qun               / qun
 *            +------+------+  ---------------------+              /                   /
 *            |             |                                     /                   /
 *            |   any ref   |  ----------------------------------+                   /
 *            |             |                                                       /
 *            +-------------+  ----------------------------------------------------+
 * }
 * </pre>
 * Where p1, p2, ..., pn are the or terms of the predicate in the original {@link SelectExpression}.
 *        
 */
@API(API.Status.EXPERIMENTAL)
public class OrToLogicalUnionRule extends PlannerRule<SelectExpression> {
    @Nonnull
    private static final BindingMatcher<Quantifier> qunMatcher = anyQuantifier();
    @Nonnull
    private static final BindingMatcher<QueryPredicate> orTermPredicateMatcher = anyPredicate();
    @Nonnull
    private static final BindingMatcher<OrPredicate> orMatcher = QueryPredicateMatchers.ofTypeWithChildren(OrPredicate.class, all(orTermPredicateMatcher));
    @Nonnull
    private static final BindingMatcher<SelectExpression> root = RelationalExpressionMatchers.selectExpression(exactly(orMatcher), all(qunMatcher)); // TODO make this better to include other predicates

    public OrToLogicalUnionRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final var bindings = call.getBindings();
        final var selectExpression = bindings.get(root);
        final var resultValues = selectExpression.getResultValue();
        final var quantifiers = bindings.getAll(qunMatcher);
        final var orTermPredicates = bindings.getAll(orTermPredicateMatcher);
        final var relationalExpressionRefs = Lists.<ExpressionRef<RelationalExpression>>newArrayListWithCapacity(orTermPredicates.size());
        for (final var orPredicate : orTermPredicates) {
            final var orCorrelatedTo = orPredicate.getCorrelatedTo();

            //
            // Subset the quantifiers to only those that are actually needed by this or term. Needed quantifiers are
            // quantifiers that contribute (in positive or negative ways to the cardinality, i.e. all for-each quantifiers)
            // and existential quantifiers that are predicated by means of an exists() predicate. As existential
            // quantifier by itself just creates a true or false but never removes a record or contributes in a meaningful
            // way to the result set.
            // TODO This optimization can done for all quantifiers that are not referred to by the term that also have a
            //      cardinality of one.
            //
            final ImmutableList<? extends Quantifier> neededQuantifiers =
                    quantifiers
                            .stream()
                            .filter(quantifier -> quantifier instanceof Quantifier.ForEach ||
                                                  (quantifier instanceof Quantifier.Existential && orCorrelatedTo.contains(quantifier.getAlias())))
                            .collect(ImmutableList.toImmutableList());
            relationalExpressionRefs.add(call.ref(new SelectExpression(resultValues, neededQuantifiers, ImmutableList.of(orPredicate))));
        }
        call.yield(GroupExpressionRef.of(new LogicalUnionExpression(Quantifiers.forEachQuantifiers(relationalExpressionRefs))));
    }
}
