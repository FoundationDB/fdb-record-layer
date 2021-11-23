/*
 * RemoveSortRule.java
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
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.temp.KeyPart;
import com.apple.foundationdb.record.query.plan.temp.Ordering;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.apple.foundationdb.record.query.plan.temp.properties.OrderingProperty;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.apple.foundationdb.record.query.plan.temp.matchers.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.temp.matchers.QuantifierMatchers.forEachQuantifier;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RecordQueryPlanMatchers.anyPlan;
import static com.apple.foundationdb.record.query.plan.temp.matchers.RelationalExpressionMatchers.logicalSortExpression;

/**
 * A rule that implements a sort expression by removing this expression if appropriate.
 */
@API(API.Status.EXPERIMENTAL)
public class RemoveSortRule extends PlannerRule<LogicalSortExpression> {
    @Nonnull
    private static final BindingMatcher<RecordQueryPlan> innerPlanMatcher = anyPlan();
    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifier(innerPlanMatcher);
    @Nonnull
    private static final BindingMatcher<LogicalSortExpression> root = logicalSortExpression(exactly(innerQuantifierMatcher));

    public RemoveSortRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final LogicalSortExpression sortExpression = call.get(root);
        final RecordQueryPlan innerPlan = call.get(innerPlanMatcher);

        final KeyExpression sortKeyExpression = sortExpression.getSort();
        if (sortKeyExpression == null) {
            call.yield(call.ref(innerPlan));
            return;
        }

        final Optional<Ordering> orderingOptional = OrderingProperty.evaluate(innerPlan, call.getContext());
        if (orderingOptional.isEmpty()) {
            return;
        }

        final Ordering ordering = orderingOptional.get();
        final Set<KeyExpression> equalityBoundKeys = ordering.getEqualityBoundKeys();
        int equalityBoundUnsorted = equalityBoundKeys.size();
        final List<KeyPart> orderingKeys = ordering.getOrderingKeyParts();
        final Iterator<KeyPart> orderingKeysIterator = orderingKeys.iterator();

        final List<KeyExpression> normalizedSortExpressions = sortKeyExpression.normalizeKeyForPositions();
        for (final KeyExpression normalizedSortExpression : normalizedSortExpressions) {
            if (equalityBoundKeys.contains(normalizedSortExpression)) {
                equalityBoundUnsorted--;
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

        final boolean strictOrdered =
                // If we have exhausted the ordering info's keys, too, then its constituents are strictly ordered.
                !orderingKeysIterator.hasNext() ||
                // Also a unique index if have gone through declared fields.
                strictlyOrderedIfUnique(innerPlan, call.getContext()::getIndexByName, normalizedSortExpressions.size() + equalityBoundUnsorted);

        call.yield(call.ref(strictOrdered ? innerPlan.strictlySorted() : innerPlan));
    }

    // TODO: This suggests that ordering and distinct should be tracked together.
    public static boolean strictlyOrderedIfUnique(@Nonnull RecordQueryPlan orderedPlan, @Nonnull final Function<String, Index> getIndex, final int nkeys) {
        if (orderedPlan instanceof RecordQueryCoveringIndexPlan) {
            orderedPlan = ((RecordQueryCoveringIndexPlan)orderedPlan).getIndexPlan();
        }
        if (orderedPlan instanceof RecordQueryIndexPlan) {
            RecordQueryIndexPlan indexPlan = (RecordQueryIndexPlan)orderedPlan;
            Index index = getIndex.apply(indexPlan.getIndexName());
            return index.isUnique() && nkeys >= index.getColumnSize();
        }
        return false;
    }
}
