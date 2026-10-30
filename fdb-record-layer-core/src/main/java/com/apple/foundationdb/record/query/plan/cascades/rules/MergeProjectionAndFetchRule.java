/*
 * MergeProjectionAndFetchRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.AbstractCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ImplementationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalProjectionExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.values.IndexEntryColumnValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QuantifierMatchers.plainForEachQuantifier;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.anyPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RecordQueryPlanMatchers.fetchFromPartialRecordPlan;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.logicalProjectionExpression;

/**
 * Removes a {@link LogicalProjectionExpression} and a {@link RecordQueryFetchFromPartialRecordPlan} directly
 * underneath if all values required by the projection are already available in the partial record prior to the fetch.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class MergeProjectionAndFetchRule extends AbstractCascadesRule<LogicalProjectionExpression> implements ImplementationCascadesRule<LogicalProjectionExpression> {
    @Nonnull
    private static final BindingMatcher<RecordQueryFetchFromPartialRecordPlan> innerPlanMatcher = fetchFromPartialRecordPlan(anyPlan());
    @Nonnull
    private static final BindingMatcher<Quantifier.ForEach> innerQuantifierMatcher = plainForEachQuantifier(innerPlanMatcher);
    @Nonnull
    private static final BindingMatcher<LogicalProjectionExpression> root = logicalProjectionExpression(exactly(innerQuantifierMatcher));

    public MergeProjectionAndFetchRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final ImplementationCascadesRuleCall call) {
        final LogicalProjectionExpression projectionExpression = call.get(root);
        final RecordQueryFetchFromPartialRecordPlan fetchPlan = call.get(innerPlanMatcher);

        // Ask the FETCH to push each projected value underneath itself, that is, to rewrite the value so that it reads
        // the partial record produced below the FETCH instead of the fetched record. If this is possible, then the
        // partial record holds everything the value needs, and we can eliminate both the projection and the FETCH.
        final CorrelationIdentifier oldInnerAlias = Iterables.getOnlyElement(projectionExpression.getQuantifiers()).getAlias();
        final CorrelationIdentifier newInnerAlias = Quantifier.uniqueId();
        final boolean canPushAllProjectedValues = projectionExpression.getProjectedValues().stream()
                .allMatch(value -> canPushToPartialRecord(fetchPlan, value, oldInnerAlias, newInnerAlias));
        if (canPushAllProjectedValues) {
            call.yieldPlan(fetchPlan.getChild());
        }
    }

    /**
     * Checks whether {@code value} can be pushed underneath {@code fetchPlan}, meaning that the pushed value reads
     * nothing but the partial record. This is the case if {@link RecordQueryFetchFromPartialRecordPlan#pushValue}
     * succeeds and the pushed value does not contain any {@link IndexEntryColumnValue}.
     *
     * <p>An {@code IndexEntryColumnValue} may get substituted by the push for a computed index column. It extracts an
     * element from the index entry that is <i>not</i> a field of the partial record. After this rule discards the
     * projection and the FETCH, the plan returns the partial records themselves from the child of the FETCH and every
     * required value therefore <i>must</i> be a field of the partial record. Since the pushed values are discarded
     * after this check, nothing in the plan would evaluate the {@code IndexEntryColumnValue}, and the returned records
     * would lack the required value.
     *
     * @param fetchPlan the fetch to push {@code value} through
     * @param value the value to push, correlated to {@code sourceAlias}
     * @param sourceAlias the alias that {@code value} reads the fetched record through
     * @param targetAlias the alias that the pushed value is to read the partial record through; as the pushed value is
     *        discarded, this can be any unique id
     */
    private static boolean canPushToPartialRecord(@Nonnull final RecordQueryFetchFromPartialRecordPlan fetchPlan,
                                                  @Nonnull final Value value,
                                                  @Nonnull final CorrelationIdentifier sourceAlias,
                                                  @Nonnull final CorrelationIdentifier targetAlias) {
        return fetchPlan.pushValue(value, sourceAlias, targetAlias)
                .filter(pushedValue -> !IndexEntryColumnValue.occursIn(pushedValue))
                .isPresent();
    }
}
