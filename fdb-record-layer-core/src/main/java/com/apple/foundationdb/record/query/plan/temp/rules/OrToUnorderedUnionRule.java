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
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalUnorderedUnionExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.MultiChildrenMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.AnyChildrenMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeWithPredicateMatcher;
import com.apple.foundationdb.record.query.predicates.OrPredicate;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

/**
 * Convert a filter on an {@linkplain OrPredicate or} expression into a plan on the union. In particular, this will
 * produce a {@link LogicalUnorderedUnionExpression} with simple filter plans on each child.
 *
 * <pre>
 * {@code
 *     +----------------------------+                 +-----------------------------------+
 *     |                            |                 |                                   |
 *     |  LogicalFilterExpression   |                 |  LogicalUnorderedUnionExpression  |
 *     |       p1 v p2 v ... v pn   |                 |                                   |
 *     |                            |                 +-----------------------------------+
 *     +-------------+--------------+                        /        |               \
 *                   |                    +-->              /         |                \
 *                   | qun                                 /          |                 \
 *                   |                                    /           |                  \
 *                   |                                   /            |                   \
 *                   |                             +--------+    +--------+          +--------+
 *                   |                             |        |    |        |          |        |
 *                   |                             |  LFE   |    |  LFE   |          |  LFE   |
 *                   |                             |    p1' |    |    p2' |   ....   |    pn' |
 *                   |                             |        |    |        |          |        |
 *                   |                             +--------+    +--------+          +--------+
 *                   |                                /              /                   /
 *                   |                               / newQun1      / newQun2           / newQunn
 *            +------+------+  ---------------------+              /                   /
 *            |             |                                     /                   /
 *            |   any ref   |  ----------------------------------+                   /
 *            |             |                                                       /
 *            +-------------+  ----------------------------------------------------+
 * }
 * </pre>
 * Where p1', p2', ..., pn' are rebased from p1, p2, ..., pn (from qun to newQun).
 *        
 */
@API(API.Status.EXPERIMENTAL)
public class OrToUnorderedUnionRule extends PlannerRule<SelectExpression> {
    @Nonnull
    private static final ReferenceMatcher innerMatcher = ReferenceMatcher.anyRef();
    @Nonnull
    private static final QuantifierMatcher<Quantifier.ForEach> qunMatcher = QuantifierMatcher.forEach(innerMatcher);
    @Nonnull
    private static final ExpressionMatcher<QueryPredicate> childMatcher = TypeMatcher.of(QueryPredicate.class, AnyChildrenMatcher.ANY);
    @Nonnull
    private static final ExpressionMatcher<OrPredicate> orMatcher = TypeMatcher.of(OrPredicate.class, MultiChildrenMatcher.allMatching(childMatcher));
    @Nonnull
    private static final ExpressionMatcher<SelectExpression> root =
            TypeWithPredicateMatcher.ofPredicate(SelectExpression.class,
                    orMatcher,
                    qunMatcher);

    public OrToUnorderedUnionRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final ExpressionRef<? extends RelationalExpression> inner = call.get(innerMatcher);
        final Quantifier.ForEach qun = call.get(qunMatcher);
        final List<QueryPredicate> children = call.getBindings().getAll(childMatcher);
        final List<ExpressionRef<RelationalExpression>> relationalExpressionRefs = new ArrayList<>(children.size());
        for (final QueryPredicate child : children) {
            final Quantifier.ForEach newQun = Quantifier.forEach(inner);
            final QueryPredicate rebasedChild = child.rebase(Quantifiers.translate(qun, newQun));
            relationalExpressionRefs.add(call.ref(new SelectExpression(ImmutableList.of(newQun), ImmutableList.of(rebasedChild))));
        }
        call.yield(GroupExpressionRef.of(new LogicalUnorderedUnionExpression(Quantifiers.forEachQuantifiers(relationalExpressionRefs))));
    }
}
