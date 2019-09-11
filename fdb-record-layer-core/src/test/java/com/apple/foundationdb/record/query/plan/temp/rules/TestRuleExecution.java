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

package com.apple.foundationdb.record.query.plan.temp.rules;

import com.apple.foundationdb.record.query.plan.temp.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlanContext;
import com.apple.foundationdb.record.query.plan.temp.PlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;

/**
 * A helper class for executing a rule during a unit test, without using one of the tasks from the
 * {@link com.apple.foundationdb.record.query.plan.temp.CascadesPlanner}.
 */
public class TestRuleExecution {
    private final boolean ruleMatched;
    @Nonnull
    private final GroupExpressionRef<PlannerExpression> result;

    private TestRuleExecution(boolean ruleMatched, @Nonnull GroupExpressionRef<PlannerExpression> result) {
        this.ruleMatched = ruleMatched;
        this.result = result;
    }

    public boolean isRuleMatched() {
        return ruleMatched;
    }

    @Nonnull
    public GroupExpressionRef<PlannerExpression> getResult() {
        return result;
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public <T extends PlannerExpression> T getResultMemberWithClass(@Nonnull Class<T> clazz) {
        for (PlannerExpression member : result.getMembers()) {
            if (clazz.isInstance(member)) {
                return (T) member;
            }
        }
        return null;
    }

    public static TestRuleExecution applyRule(@Nonnull PlanContext context,
                                              @Nonnull PlannerRule<? extends PlannerExpression> rule,
                                              @Nonnull GroupExpressionRef<PlannerExpression> group) {
        boolean ruleMatched = false;
        for (PlannerExpression expression : group.getMembers()) {
            final Iterator<CascadesRuleCall> ruleCalls = expression.bindTo(rule.getMatcher())
                    .map(bindings -> new CascadesRuleCall(context, rule, group, bindings))
                    .iterator();
            while (ruleCalls.hasNext()) {
                ruleCalls.next().run();
                ruleMatched = true;
            }
        }
        return new TestRuleExecution(ruleMatched, group);
    }
}
