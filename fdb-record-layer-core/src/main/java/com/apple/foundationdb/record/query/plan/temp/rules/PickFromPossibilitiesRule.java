/*
 * PickFromPossibilitiesRule.java
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.temp.FixedCollectionExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.SingleExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.expressions.RelationalGroupRefHolder;
import com.apple.foundationdb.record.query.plan.temp.expressions.RelationalPlannerExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;
import com.apple.foundationdb.record.query.plan.temp.properties.FieldWithComparisonCountProperty;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A rule that iterates through the expressions in a {@link RelationalGroupRefHolder}'s {@link FixedCollectionExpressionRef}
 * and selects the "best" one using some embedded heuristic. Currently, it selects the one that contains the fewest
 * {@link com.apple.foundationdb.record.query.expressions.FieldWithComparison}s, as a proxy for the number of
 * unimplemented filters.
 */
@API(API.Status.EXPERIMENTAL)
public class PickFromPossibilitiesRule extends PlannerRule<RelationalGroupRefHolder> {
    private static ExpressionMatcher<RelationalGroupRefHolder> root = TypeMatcher.of(RelationalGroupRefHolder.class, ReferenceMatcher.anyRef());

    public PickFromPossibilitiesRule() {
        super(root);
    }

    @Nonnull
    @Override
    public ChangesMade onMatch(@Nonnull PlannerRuleCall call) {
        final FixedCollectionExpressionRef<RelationalPlannerExpression> holder = call.get(root).getInnerGroup();
        final List<SingleExpressionRef<RelationalPlannerExpression>> groupMembers = holder.getMembers();

        SingleExpressionRef<RelationalPlannerExpression> bestMember = null;
        int bestUnsatisfiedFilterCount = Integer.MAX_VALUE;
        for (SingleExpressionRef<RelationalPlannerExpression> member : groupMembers) {
            int unsatisfiedFilterCount = FieldWithComparisonCountProperty.evaluate(member);
            if (bestMember == null || unsatisfiedFilterCount < bestUnsatisfiedFilterCount) {
                bestMember = member;
                bestUnsatisfiedFilterCount = unsatisfiedFilterCount;
            }
        }

        if (bestMember == null) {
            throw new RecordCoreException("tried to collapse a group expression holder with no members");
        }
        call.yield(bestMember);
        return ChangesMade.MADE_CHANGES;
    }
}
