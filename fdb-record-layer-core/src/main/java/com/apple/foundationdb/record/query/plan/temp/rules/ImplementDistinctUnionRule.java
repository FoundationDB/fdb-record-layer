/*
 * ImplementUnorderedUnionRule.java
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
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.apple.foundationdb.record.query.plan.temp.KeyPart;
import com.apple.foundationdb.record.query.plan.temp.PlanContext;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalDistinctExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalUnorderedUnionExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.MultiChildrenMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;
import com.apple.foundationdb.record.query.plan.temp.properties.OrderingProperty;
import com.apple.foundationdb.record.query.plan.temp.properties.OrderingProperty.OrderingInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * A rule that implements an unordered union of its (already implemented) children. This will extract the
 * {@link RecordQueryPlan} from each child of a {@link LogicalUnorderedUnionExpression} and create a
 * {@link RecordQueryUnorderedUnionPlan} with those plans as children.
 */
@API(API.Status.EXPERIMENTAL)
public class ImplementDistinctUnionRule extends PlannerRule<LogicalDistinctExpression> {
    @Nonnull
    private static final ExpressionMatcher<RecordQueryPlan> unionLegExpressionMatcher = TypeMatcher.of(RecordQueryPlan.class, MultiChildrenMatcher.allMatching(ReferenceMatcher.anyRef()));
    @Nonnull
    private static final ExpressionMatcher<LogicalUnorderedUnionExpression> unionExpressionMatcher =
            TypeMatcher.of(LogicalUnorderedUnionExpression.class,
                    MultiChildrenMatcher.allMatching(QuantifierMatcher.forEach(unionLegExpressionMatcher)));

    @Nonnull
    private static final ExpressionMatcher<LogicalDistinctExpression> root =
            TypeMatcher.of(LogicalDistinctExpression.class,
                    QuantifierMatcher.forEach(unionExpressionMatcher));

    public ImplementDistinctUnionRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final PlanContext context = call.getContext();
        final PlannerBindings bindings = call.getBindings();
        final List<RecordQueryPlan> unionLegExpressions = bindings.getAll(unionLegExpressionMatcher);
        final LogicalUnorderedUnionExpression logicalUnorderedUnionExpression = bindings.get(unionExpressionMatcher);
        final Optional<OrderingInfo> orderingInfoOptional = OrderingProperty.evaluate(logicalUnorderedUnionExpression, context);
        if (!orderingInfoOptional.isPresent()) {
            return;
        }

        final OrderingInfo orderingInfo = orderingInfoOptional.get();

        final Set<KeyExpression> equalityBoundKeys = orderingInfo.getEqualityBoundKeys();
        final List<KeyExpression> orderingKeys =
                orderingInfo.getOrderingKeyParts()
                        .stream()
                        .map(KeyPart::getNormalizedKeyExpression)
                        .collect(ImmutableList.toImmutableList());

        final KeyExpression commonPrimaryKey = context.getCommonPrimaryKey();
        if (commonPrimaryKey == null) {
            return;
        }

        final List<KeyExpression> commonPrimaryKeyParts = commonPrimaryKey.normalizeKeyForPositions();

        // make sure the common primary key parts are either bound through equality or they are part of the ordering
        for (final KeyExpression commonPrimaryKeyPart : commonPrimaryKeyParts) {
            if (!equalityBoundKeys.contains(commonPrimaryKeyPart) && !orderingKeys.contains(commonPrimaryKeyPart)) {
                return;
            }
        }

        final KeyExpression comparisonKey =
                orderingKeys.size() == 1
                ? Iterables.getOnlyElement(orderingKeys) : Key.Expressions.concat(orderingKeys);
        call.yield(call.ref(RecordQueryUnionPlan.from(unionLegExpressions,
                comparisonKey,
                true)));
    }
}
