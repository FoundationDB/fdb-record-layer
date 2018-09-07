/*
 * RewritePlanner.java
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.temp.expressions.RelationalPlannerExpression;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Optional;

/**
 * A simple planner that applies rewrite rules until it can't apply any more of them, then returns the resulting plan.
 * This planner is greedy (applies rule immediately) and doesn't respect any kind of ordering of the rules (such as "try
 * this rule before this rule") and so cannot implement all of the behavior in the current planner.
 *
 * TODO this planner might have bugs since we don't currently have enough rules to write good tests for it.
 */
public class RewritePlanner {
    @Nonnull
    private static final PlannerRuleSet rules = PlannerRuleSet.DEFAULT_REWRITE;
    @Nonnull
    private final RecordMetaData metaData;
    @Nonnull
    private final RecordStoreState recordStoreState;
    @Nullable
    private SingleExpressionRef<PlannerExpression> current;
    @Nullable
    private PlanContext context;

    public RewritePlanner(@Nonnull RecordMetaData metaData, @Nonnull RecordStoreState recordStoreState) {
        this.metaData = metaData;
        this.recordStoreState = recordStoreState;
    }

    /**
     * Plan the given record query by attempting to match rules in a greedy fashion until no rules can be applied.
     * If the final expression is a {@link RecordQueryPlan} then the planning was successful and that plan is returned.
     * Otherwise, an exception is thrown.
     * @param query a record query to plan
     * @return a plan implementing the given query
     * @throws RecordCoreException if the planner could not plan the given query
     */
    @Nonnull
    public RecordQueryPlan plan(@Nonnull RecordQuery query) {
        context = new MetaDataPlanContext(metaData, recordStoreState, query);
        current = SingleExpressionRef.of(RelationalPlannerExpression.fromRecordQuery(query));
        RewriteRuleCall nextRuleCall = getNextRuleCall();
        while (nextRuleCall != null) {
            nextRuleCall.run();
            nextRuleCall = getNextRuleCall();
        }

        context = null;
        if (current.get() instanceof RecordQueryPlan) { // turned into a concrete plan
            return (RecordQueryPlan)current.get();
        } else {
            throw new RecordCoreException("rewrite planner could not plan query")
                    .addLogInfo("query", query)
                    .addLogInfo("finalExpression", current.get());
        }
    }

    @Nullable
    @SuppressWarnings("unchecked")
    private RewriteRuleCall getNextRuleCall() {
        Deque<SingleExpressionRef<PlannerExpression>> toTry = new ArrayDeque<>();
        toTry.add(current);

        while (!toTry.isEmpty()) {
            SingleExpressionRef<PlannerExpression> root = toTry.remove();
            Iterator<PlannerRule<? extends PlannerExpression>> possibleRules = rules.getRulesMatching(root.get());

            while (possibleRules.hasNext()) {
                Optional<RewriteRuleCall> attemptedCall = RewriteRuleCall.tryMatchRule(context, possibleRules.next(), root);

                if (attemptedCall.isPresent()) {
                    return attemptedCall.get();
                }
            }

            PlannerExpression basicRoot = root.get();
            Iterator<? extends ExpressionRef<? extends PlannerExpression>> childrenIterator = basicRoot.getPlannerExpressionChildren();
            while (childrenIterator.hasNext()) {
                ExpressionRef<? extends PlannerExpression> child = childrenIterator.next();
                if (child instanceof SingleExpressionRef) {
                    toTry.add((SingleExpressionRef<PlannerExpression>) child);
                }
            }
        }
        return null;
    }
}
