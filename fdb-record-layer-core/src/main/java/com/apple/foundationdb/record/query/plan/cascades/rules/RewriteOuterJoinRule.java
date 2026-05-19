/*
 * RewriteOuterJoinRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.ExplorationCascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.ExplorationCascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.OuterJoinExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.isExploratoryExpression;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.outerJoinExpression;

/**
 * An exploration rule that reduces an {@link OuterJoinExpression} to two nested {@link SelectExpression} boxes.
 * This canonicalization happens during the rewriting phase so that the subsequent
 * planning phase, which includes all matching and implementation rules, does not need to explicitly deal with
 * {@code OuterJoinExpression}.
 *
 * <p>The rewrite produces the following structure:
 * <pre>{@code
 *   Outer SelectExpression                              ← no predicates, combines both sides
 *   ├── ForEach quantifier → «preserved side»           ← normal quantifier
 *   └── ForEach(nullOnEmpty) quantifier →               ← emits NULL row when inner is empty
 *       Inner SelectExpression                          ← carries the ON predicates
 *       └── ForEach quantifier → «null-supplying side»
 * }</pre>
 *
 * <p>The {@code nullOnEmpty} flag on the quantifier connecting the inner select to the outer one provides the
 * outer-join semantics: When the inner select produces no rows (no match for the current preserved-side row), the
 * quantifier emits a single row with all columns set to {@code NULL}.
 *
 * <p>Only {@code LEFT} and {@code RIGHT} outer joins are supported. {@code FULL} outer joins are skipped.
 */
@API(API.Status.EXPERIMENTAL)
public class RewriteOuterJoinRule extends ExplorationCascadesRule<OuterJoinExpression> {

    @Nonnull
    private static final BindingMatcher<OuterJoinExpression> root =
            outerJoinExpression().where(isExploratoryExpression());

    public RewriteOuterJoinRule() {
        super(root);
    }

    @Override
    public void onMatch(@Nonnull final ExplorationCascadesRuleCall call) {
        final PlannerBindings bindings = call.getBindings();
        final OuterJoinExpression outerJoinExpression = bindings.get(root);

        if (outerJoinExpression.getJoinType() == OuterJoinExpression.JoinType.FULL) {
            return;
        }

        final Quantifier.ForEach preservedQun = outerJoinExpression.getPreservedQuantifier();
        final Quantifier.ForEach nullSupplyingQun = outerJoinExpression.getNullSupplyingQuantifier();

        // The rewrite reuses both child quantifiers directly. That is only valid because the `OuterJoinExpression`
        // children are plain `ForEach` quantifiers. The null-on-empty behavior is introduced here, on the
        // `outerNullOnEmptyQun` wrapping the new inner `SelectExpression`. If a null-on-empty quantifier ever appears
        // as a direct child of `OuterJoinExpression`, this rewrite would silently change semantics.
        Verify.verify(!preservedQun.isNullOnEmpty());
        Verify.verify(!nullSupplyingQun.isNullOnEmpty());

        // Build the inner `SelectExpression` carrying the ON predicates. The original null-supplying quantifier is
        // already a plain `ForEach` (no null-on-empty), so we can use it directly without copying.
        final SelectExpression innerSelect = new SelectExpression(
                nullSupplyingQun.getFlowedObjectValue(),
                ImmutableList.of(nullSupplyingQun),
                ImmutableList.copyOf(outerJoinExpression.getJoinPredicates()));

        // Memoize the inner `SelectExpression` so it becomes part of the planner’s memo structure, then build a
        // null-on-empty quantifier over it. This quantifier emits a single NULL row when the inner SELECT is empty.
        final Reference innerRef = call.memoizeExploratoryExpression(innerSelect);
        final Quantifier.ForEach outerNullOnEmptyQun =
                Quantifier.forEachWithNullOnEmpty(innerRef, nullSupplyingQun.getAlias());

        // Build the outer `SelectExpression`. It combines the preserved side (reused directly, as it is already a plain
        // `ForEach`) and the null-on-empty inner side. Note that the order of quantifiers in the list does not matter.
        // The result value resolves correlations by alias, the implementation rules explore both join orderings, and
        // the null-on-empty constraint plus the ON-predicate correlations force the correct NLJ orientation regardless.
        // The result value here is reused directly from the original `OuterJoinExpression` since its correlations refer
        // to the same aliases as the original left and right quantifiers, which we have preserved.
        final SelectExpression outerSelect = new SelectExpression(
                outerJoinExpression.getResultValue(),
                ImmutableList.of(preservedQun, outerNullOnEmptyQun),
                ImmutableList.of());

        call.yieldExploratoryExpression(outerSelect);
    }
}
