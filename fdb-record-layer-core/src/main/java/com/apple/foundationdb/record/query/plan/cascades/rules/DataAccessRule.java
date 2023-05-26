/*
 * DataAccessRule.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.MatchPartition;
import com.apple.foundationdb.record.query.plan.cascades.Memoizer;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.PlanContext;
import com.apple.foundationdb.record.query.plan.cascades.PrimaryScanMatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrderingConstraint;
import com.apple.foundationdb.record.query.plan.cascades.ValueIndexScanMatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalIntersectionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.PrimaryScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MatchPartitionMatchers.ofExpressionAndMatches;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.some;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.NotMatcher.not;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PartialMatchMatchers.completeMatch;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.anyExpression;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.ofType;

/**
 * A rule that utilizes index matching information compiled by {@link CascadesPlanner} to create one or more
 * expressions for data access. While this rule delegates specifics to the {@link MatchCandidate}s, the following are
 * possible outcomes of the application of this transformation rule. Based on the match info, we may create for a single match:
 *
 * <ul>
 *     <li>a {@link PrimaryScanExpression} for a single {@link PrimaryScanMatchCandidate},</li>
 *     <li>an index scan/index scan + fetch for a single {@link ValueIndexScanMatchCandidate}</li>
 *     <li>an intersection ({@link LogicalIntersectionExpression}) of data accesses </li>
 * </ul>
 *
 * The logic that this rules delegates to actually create the expressions can be found in
 * {@link MatchCandidate#toEquivalentPlan(PartialMatch, PlanContext, Memoizer)}
 *
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class DataAccessRule extends AbstractDataAccessRule<RelationalExpression> {
    private static final BindingMatcher<PartialMatch> completeMatchMatcher = completeMatch();
    private static final BindingMatcher<RelationalExpression> expressionMatcher = anyExpression().where(not(ofType(SelectExpression.class)));

    private static final BindingMatcher<MatchPartition> rootMatcher =
            ofExpressionAndMatches(expressionMatcher, some(completeMatchMatcher));
    
    public DataAccessRule() {
        super(rootMatcher, completeMatchMatcher, expressionMatcher);
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final var bindings = call.getBindings();
        final var completeMatches = bindings.getAll(getCompleteMatchMatcher());
        if (completeMatches.isEmpty()) {
            return;
        }

        final var expression = bindings.get(getExpressionMatcher());

        if (expression.getQuantifiers().size() != 1) {
            return;
        }

        //
        // return if there is no pre-determined interesting ordering
        //
        final var requestedOrderingsOptional = call.getPlannerConstraint(RequestedOrderingConstraint.REQUESTED_ORDERING);
        if (requestedOrderingsOptional.isEmpty()) {
            return;
        }

        final var requestedOrderings = requestedOrderingsOptional.get();

        final var matchPartition =
                completeMatches
                        .stream()
                        .filter(match -> {
                            final var matchedQuantifiers =
                                    expression.getMatchedQuantifiers(match);
                            return matchedQuantifiers.size() == 1;
                        })
                        .collect(ImmutableList.toImmutableList());

        call.yield(dataAccessForMatchPartition(call,
                requestedOrderings,
                matchPartition));
    }
}
