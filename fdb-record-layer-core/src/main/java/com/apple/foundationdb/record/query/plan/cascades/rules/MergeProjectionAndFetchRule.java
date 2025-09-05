/*
 * MergeProjectionAndFetchRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalProjectionExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.forEachQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.anyPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.fetchFromPartialRecordPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.logicalProjectionExpression;

/**
 * A rule that removes a {@link LogicalProjectionExpression} and a {@link RecordQueryFetchFromPartialRecordPlan}
 * if all fields needed by the projection are already available prior to the fetch.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class MergeProjectionAndFetchRule extends ImplementationCascadesRule<LogicalProjectionExpression> {
    @Nonnull
    private static final BindingMatcher<RecordQueryFetchFromPartialRecordPlan> innerPlanMatcher = fetchFromPartialRecordPlan(anyPlan());
    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = forEachQuantifier(innerPlanMatcher);
    @Nonnull
    private static final BindingMatcher<LogicalProjectionExpression> root = logicalProjectionExpression(exactly(innerQuantifierMatcher));

    public MergeProjectionAndFetchRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final ImplementationCascadesRuleCall call) {
        final LogicalProjectionExpression projectionExpression = call.get(root);

        // if the fetch is able to push all values we can eliminate the fetch as well
        final RecordQueryFetchFromPartialRecordPlan fetchPlan = call.get(innerPlanMatcher);
        final CorrelationIdentifier oldInnerAlias = Iterables.getOnlyElement(projectionExpression.getQuantifiers()).getAlias();
        final CorrelationIdentifier newInnerAlias = Quantifier.uniqueId();
        final List<? extends Value> projectedValues = projectionExpression.getProjectedValues();
        final boolean allPushable = projectedValues
                .stream()
                .allMatch(value -> fetchPlan.pushValue(value, oldInnerAlias, newInnerAlias).isPresent());
        if (allPushable) {
            // all fields in the projection are already available underneath the fetch
            // we don't need the projection nor the fetch
            call.yieldPlan(fetchPlan.getChild());
        }
    }
}
