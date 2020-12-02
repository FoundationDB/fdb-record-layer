/*
 * ImplementDistinctRule.java
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
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.temp.KeyPart;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.AnyChildrenMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;
import com.apple.foundationdb.record.query.plan.temp.properties.OrderingProperty;
import com.apple.foundationdb.record.query.plan.temp.properties.OrderingProperty.OrderingInfo;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * A rule that implements a sort expression by removing this expression if appropriate.
 */
@API(API.Status.EXPERIMENTAL)
public class ImplementSortRule extends PlannerRule<LogicalSortExpression> {
    @Nonnull
    private static final ExpressionMatcher<RecordQueryPlan> innerPlanMatcher = TypeMatcher.of(RecordQueryPlan.class, AnyChildrenMatcher.ANY);
    @Nonnull
    private static final ExpressionMatcher<Quantifier.ForEach> innerQuantifierMatcher = QuantifierMatcher.forEach(innerPlanMatcher);
    @Nonnull
    private static final ExpressionMatcher<LogicalSortExpression> root =
            TypeMatcher.of(LogicalSortExpression.class, innerQuantifierMatcher);

    public ImplementSortRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final LogicalSortExpression sortExpression = call.get(root);

        final RecordQueryPlan innerPlan = call.get(innerPlanMatcher);
        final Optional<OrderingInfo> orderingInfoOptional = OrderingProperty.evaluate(innerPlan, call.getContext());
        if (!orderingInfoOptional.isPresent()) {
            return;
        }

        final OrderingInfo orderingInfo = orderingInfoOptional.get();
        final Set<KeyExpression> equalityBoundKeys = orderingInfo.getEqualityBoundKeys();
        final List<KeyPart> orderingKeys = orderingInfo.getOrderingKeyParts();
        final Iterator<KeyPart> orderingKeysIterator = orderingKeys.iterator();

        final List<KeyExpression> normalizedSortExpressions = sortExpression.getSort().normalizeKeyForPositions();
        for (final KeyExpression normalizedSortExpression : normalizedSortExpressions) {
            //
            // A top level sort (which is the only sort supported at this point should not be able to express
            // and order based on a repeated field).
            //
            if (normalizedSortExpression.createsDuplicates()) {
                return;
            }

            if (equalityBoundKeys.contains(normalizedSortExpression)) {
                continue;
            }
            if (!orderingKeysIterator.hasNext()) {
                return;
            }

            final KeyPart currentOrderingKeyPart = orderingKeysIterator.next();

            if (!normalizedSortExpression.equals(currentOrderingKeyPart.getNormalizedKeyExpression())) {
                return;
            }
        }

        call.yield(call.ref(innerPlan));
    }
}
