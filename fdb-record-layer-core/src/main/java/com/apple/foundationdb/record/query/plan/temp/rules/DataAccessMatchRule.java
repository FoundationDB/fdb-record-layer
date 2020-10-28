/*
 * DataAccessMatchRule.java
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
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.MatchCandidate;
import com.apple.foundationdb.record.query.plan.temp.MatchWithCompensation;
import com.apple.foundationdb.record.query.plan.temp.PartialMatch;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatcher;

import javax.annotation.Nonnull;

/**
 * A rule that converts a logical index scan expression to a {@link RecordQueryIndexPlan}. This rule simply converts
 * the logical index scan's {@link com.apple.foundationdb.record.query.plan.temp.view.ViewExpressionComparisons} to a
 * {@link com.apple.foundationdb.record.query.plan.ScanComparisons} to be used during query execution.
 */
@API(API.Status.EXPERIMENTAL)
public class DataAccessMatchRule extends PlannerRule<ExpressionRef<RelationalExpression>> {
    private static final ReferenceMatcher<RelationalExpression> root = ReferenceMatcher.anyRef();

    public DataAccessMatchRule() {
        super(root);
    }

    @Override
    @SuppressWarnings("java:S135")
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final ExpressionRef<RelationalExpression> ref = call.get(root);

        // go through all the match candidates
        for (final MatchCandidate matchCandidate : ref.getMatchCandidates()) {
            for (final PartialMatch partialMatch : ref.getPartialMatchesForCandidate(matchCandidate)) {
                if (partialMatch.getCandidateRef() == matchCandidate.getTraversal().getRootReference()) {
                    // this match is complete
                    final MatchWithCompensation matchWithCompensation = partialMatch.getMatchWithCompensation();
                    call.yield(call.ref(matchCandidate.toScanExpression(matchWithCompensation)));
                }
            }
        }
    }
}
