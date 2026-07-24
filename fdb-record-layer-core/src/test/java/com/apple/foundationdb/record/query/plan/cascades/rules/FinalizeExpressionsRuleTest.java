/*
 * FinalizeExpressionsRuleTest.java
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

import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.apple.foundationdb.record.query.plan.cascades.PlannerStage;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper;
import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.debug.DebuggerWithSymbolTables;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.properties.SelectMergeableProperty;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.column;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.selectWithPredicates;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.TYPE_T;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.TYPE_TAU;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.baseT;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.baseTau;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.fuseQun;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.join;

/**
 * Tests of the {@link FinalizeExpressionsRule}.
 */
class FinalizeExpressionsRuleTest {
    @Nonnull
    private static final RuleTestHelper testHelper = new RuleTestHelper(new FinalizeExpressionsRule(), PlannerPhase.REWRITING);

    @BeforeEach
    void setUp() {
        Debugger.setDebugger(DebuggerWithSymbolTables.withSanityChecks());
        Debugger.setup();
    }

    /**
     * Test that finalizing an expression whose children each have multiple candidate expressions spanning more
     * than one {@link com.apple.foundationdb.record.query.plan.cascades.ExpressionPartition} produces the cross
     * product of the choices across all of its children, rather than just one combination per child. Here, each of
     * two children is a reference with two candidates that fall into distinct partitions under
     * {@link SelectMergeableProperty}: one that is select-mergeable (a {@link SelectExpression}) and one that is not
     * (a {@link LogicalTypeFilterExpression}). The two children range over different record types (T and TAU) so
     * that the four combinations are not accidentally structurally identical to one another. Finalizing the join on
     * top of these two children should yield one final expression for each of the 2 x 2 = 4 combinations of
     * partition choices.
     */
    @Test
    void crossProductOfChildPartitions() {
        final RelationalExpression nonMergeableA = LogicalTypeFilterExpression.of(ImmutableSet.of("T"), fuseQun(), TYPE_T);
        final SelectExpression mergeableA = selectWithPredicates(baseT());
        final Quantifier childQunA = Quantifier.forEach(
                Reference.ofFinalExpressions(PlannerStage.INITIAL, ImmutableSet.of(nonMergeableA, mergeableA)));

        final RelationalExpression nonMergeableB = LogicalTypeFilterExpression.of(ImmutableSet.of("TAU"), fuseQun(), TYPE_TAU);
        final SelectExpression mergeableB = selectWithPredicates(baseTau());
        final Quantifier childQunB = Quantifier.forEach(
                Reference.ofFinalExpressions(PlannerStage.INITIAL, ImmutableSet.of(nonMergeableB, mergeableB)));

        final SelectExpression original = join(childQunA, childQunB)
                .addResultColumn(column(childQunA, "a", "a1"))
                .addResultColumn(column(childQunB, "alpha", "a2"))
                .build().buildSelect();

        testHelper.assertYields(original,
                joinedOn(nonMergeableA, nonMergeableB),
                joinedOn(nonMergeableA, mergeableB),
                joinedOn(mergeableA, nonMergeableB),
                joinedOn(mergeableA, mergeableB));
    }

    @Nonnull
    private static SelectExpression joinedOn(@Nonnull final RelationalExpression childA, @Nonnull final RelationalExpression childB) {
        final Quantifier qunA = Quantifier.forEach(Reference.ofFinalExpressions(PlannerStage.CANONICAL, ImmutableSet.of(childA)));
        final Quantifier qunB = Quantifier.forEach(Reference.ofFinalExpressions(PlannerStage.CANONICAL, ImmutableSet.of(childB)));
        return join(qunA, qunB)
                .addResultColumn(column(qunA, "a", "a1"))
                .addResultColumn(column(qunB, "alpha", "a2"))
                .build().buildSelect();
    }

    /**
     * Test the degenerate case of a single quantifier whose child reference has multiple candidates spanning two
     * partitions. The "cross product" over a single dimension is just that dimension's own choices, so finalizing
     * should yield one final expression per partition (2 in this case), rather than collapsing to a single choice.
     */
    @Test
    void singleQuantifierYieldsOneFinalExpressionPerPartition() {
        final RelationalExpression nonMergeable = LogicalTypeFilterExpression.of(ImmutableSet.of("T"), fuseQun(), TYPE_T);
        final SelectExpression mergeable = selectWithPredicates(baseT());
        final Quantifier childQun = Quantifier.forEach(
                Reference.ofFinalExpressions(PlannerStage.INITIAL, ImmutableSet.of(nonMergeable, mergeable)));

        final SelectExpression original = join(childQun)
                .addResultColumn(column(childQun, "a", "a1"))
                .build().buildSelect();

        testHelper.assertYields(original,
                joinedOnSingle(nonMergeable),
                joinedOnSingle(mergeable));
    }

    @Nonnull
    private static SelectExpression joinedOnSingle(@Nonnull final RelationalExpression child) {
        final Quantifier qun = Quantifier.forEach(Reference.ofFinalExpressions(PlannerStage.CANONICAL, ImmutableSet.of(child)));
        return join(qun)
                .addResultColumn(column(qun, "a", "a1"))
                .build().buildSelect();
    }

    /**
     * Test the other degenerate case: a leaf expression with no quantifiers at all. There are no children to
     * enumerate partitions over, so the cross product is over zero dimensions, which by convention has exactly one
     * (empty) combination. Finalizing should therefore yield exactly one final expression -- the finalized leaf
     * itself -- rather than yielding nothing.
     */
    @Test
    void leafExpressionYieldsExactlyOneFinalExpression() {
        final RelationalExpression original =
                new FullUnorderedScanExpression(ImmutableSet.of("T", "TAU"), Type.Record.fromFields(ImmutableList.of()), new AccessHints());

        testHelper.assertYields(original, original);
    }
}
