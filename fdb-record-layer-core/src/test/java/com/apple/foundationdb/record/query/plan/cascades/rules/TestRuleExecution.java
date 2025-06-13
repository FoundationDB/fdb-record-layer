/*
 * TestRuleExecution.java
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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.PlanContext;
import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.Traversal;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ReferenceMatchers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Iterator;

/**
 * A helper class for executing a rule during a unit test, without using one of the tasks from the
 * {@link com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner}.
 */
public class TestRuleExecution {
    private final boolean ruleMatched;
    private final boolean hasYielded;
    @Nonnull
    private final Reference result;

    private TestRuleExecution(boolean ruleMatched, boolean hasYielded, @Nonnull Reference result) {
        this.ruleMatched = ruleMatched;
        this.hasYielded = hasYielded;
        this.result = result;
    }

    public boolean isRuleMatched() {
        return ruleMatched;
    }

    public boolean hasYielded() {
        return hasYielded;
    }

    @Nonnull
    public Reference getResult() {
        return result;
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public <T> T getResultMemberWithClass(@Nonnull Class<T> clazz) {
        for (RelationalExpression member : result.getAllMemberExpressions()) {
            if (clazz.isInstance(member)) {
                return (T) member;
            }
        }
        return null;
    }

    public static TestRuleExecution applyRule(@Nonnull PlanContext context,
                                              @Nonnull CascadesRule<? extends RelationalExpression> rule,
                                              @Nonnull Reference group,
                                              @Nonnull final EvaluationContext evaluationContext) {
        boolean ruleMatched = false;
        boolean hasYielded = false;
        for (RelationalExpression expression : group.getAllMemberExpressions()) {
            final Iterator<CascadesRuleCall> ruleCalls =
                    rule.getMatcher()
                            .bindMatches(context.getPlannerConfiguration(),
                                    PlannerBindings.newBuilder()
                                            .put(ReferenceMatchers.getCurrentReferenceMatcher(), group)
                                            .build(),
                                    expression)
                            .map(bindings -> new CascadesRuleCall(PlannerPhase.REWRITING, context, rule, group,
                                    Traversal.withRoot(group), new ArrayDeque<>(), bindings, evaluationContext))
                            .iterator();
            while (ruleCalls.hasNext()) {
                final var ruleCall = ruleCalls.next();
                ruleCall.run();
                hasYielded |= !ruleCall.getNewExploratoryExpressions().isEmpty() ||
                        !ruleCall.getNewFinalExpressions().isEmpty();
                ruleMatched = true;
            }
        }
        return new TestRuleExecution(ruleMatched, hasYielded, group);
    }
}
