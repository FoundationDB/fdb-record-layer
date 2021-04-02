/*
 * OrToUnorderedUnionRule.java
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
import com.apple.foundationdb.record.query.plan.temp.matchers.AnyChildrenMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.MultiChildrenMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeWithPredicateMatcher;
import com.apple.foundationdb.record.query.predicates.OrPredicate;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

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
public class OrToUnorderedUnionRule extends PlannerRule<SelectExpression> {
    @Nonnull
    private static final ReferenceMatcher<? extends RelationalExpression> innerMatcher = ReferenceMatcher.anyRef();
    @Nonnull
    private static final QuantifierMatcher<Quantifier> qunMatcher = QuantifierMatcher.any(innerMatcher);
    @Nonnull
    private static final ExpressionMatcher<QueryPredicate> orTermPredicateMatcher = TypeMatcher.of(QueryPredicate.class, AnyChildrenMatcher.ANY);
    @Nonnull
    private static final ExpressionMatcher<OrPredicate> orMatcher = TypeMatcher.of(OrPredicate.class, MultiChildrenMatcher.allMatching(orTermPredicateMatcher));
    @Nonnull
    private static final ExpressionMatcher<SelectExpression> root =
            TypeWithPredicateMatcher.ofPredicate(SelectExpression.class,
                    orMatcher,
                    MultiChildrenMatcher.allMatching(qunMatcher));

    public OrToUnorderedUnionRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final PlannerBindings bindings = call.getBindings();
        final SelectExpression selectExpression = bindings.get(root);
        final List<? extends Value> resultValues = selectExpression.getResultValues();
        final List<Quantifier> quantifiers = bindings.getAll(qunMatcher);
        final List<QueryPredicate> orTermPredicates = bindings.getAll(orTermPredicateMatcher);
        final List<ExpressionRef<RelationalExpression>> relationalExpressionRefs = new ArrayList<>(orTermPredicates.size());
        for (final QueryPredicate orTermPredicate : orTermPredicates) {
            relationalExpressionRefs.add(call.ref(new SelectExpression(resultValues, quantifiers, ImmutableList.of(orTermPredicate))));
        }
        call.yield(GroupExpressionRef.of(new LogicalUnionExpression(Quantifiers.forEachQuantifiers(relationalExpressionRefs))));
    }
}
