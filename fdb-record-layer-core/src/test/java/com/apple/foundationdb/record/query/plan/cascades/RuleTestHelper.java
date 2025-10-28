/*
 * RuleTestHelper.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TableFunctionExpression;
import com.apple.foundationdb.record.query.plan.cascades.rules.FinalizeExpressionsRule;
import com.apple.foundationdb.record.query.plan.cascades.rules.TestRuleExecution;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RangeValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.ToUniqueAliasesTranslationMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.assertj.core.api.AutoCloseableSoftAssertions;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fieldValue;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.forEach;

public class RuleTestHelper {
    @Nonnull
    public static final Comparisons.Comparison EQUALS_42 = new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 42L);
    @Nonnull
    public static final Comparisons.Comparison GREATER_THAN_HELLO = new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, "hello");
    @Nonnull
    public static final Comparisons.Comparison EQUALS_PARAM = new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, "p");

    @Nonnull
    public static final Type.Record TYPE_S = Type.Record.fromFields(ImmutableList.of(
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, true), Optional.of("one")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("two")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.BYTES, true), Optional.of("three"))
    ));

    @Nonnull
    public static final Type.Record TYPE_T = Type.Record.fromFields(ImmutableList.of(
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, true), Optional.of("a")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("b")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.BYTES, true), Optional.of("c")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("d")),
            Type.Record.Field.of(TYPE_S, Optional.of("e")),
            Type.Record.Field.of(new Type.Array(true, Type.primitiveType(Type.TypeCode.LONG, true)), Optional.of("f")),
            Type.Record.Field.of(new Type.Array(true, TYPE_S), Optional.of("g"))
    ));

    @Nonnull
    public static final Type.Record TYPE_TAU = Type.Record.fromFields(ImmutableList.of(
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, true), Optional.of("alpha")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("beta")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.BYTES, true), Optional.of("gamma")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("delta")),
            Type.Record.Field.of(TYPE_S, Optional.of("epsilon")),
            Type.Record.Field.of(new Type.Array(true, Type.primitiveType(Type.TypeCode.LONG, true)), Optional.of("zeta")),
            Type.Record.Field.of(new Type.Array(true, TYPE_S), Optional.of("eta"))
    ));

    @Nonnull
    public static Quantifier fuseQun() {
        return forEach(new FullUnorderedScanExpression(ImmutableSet.of("T", "TAU"), Type.Record.fromFields(ImmutableList.of()), new AccessHints()));
    }

    @Nonnull
    public static Quantifier baseT() {
        return forEach(new LogicalTypeFilterExpression(ImmutableSet.of("T"), fuseQun(), TYPE_T));
    }

    @Nonnull
    public static Quantifier baseTau() {
        return forEach(new LogicalTypeFilterExpression(ImmutableSet.of("TAU"), fuseQun(), TYPE_TAU));
    }

    @Nonnull
    public static GraphExpansion.Builder join(Quantifier... quns) {
        return GraphExpansion.builder().addAllQuantifiers(List.of(quns));
    }

    @Nonnull
    public static Quantifier rangeOneQun() {
        var rangeValue = (RangeValue) new RangeValue.RangeFn().encapsulate(ImmutableList.of(LiteralValue.ofScalar(1L)));
        TableFunctionExpression tvf = new TableFunctionExpression(rangeValue);
        return Quantifier.forEach(Reference.initialOf(tvf));
    }

    @Nonnull
    public static Quantifier valuesQun(@Nonnull Map<String, Value> valueMap) {
        var graphBuilder = GraphExpansion.builder()
                .addQuantifier(rangeOneQun());
        for (Map.Entry<String, Value> entry : valueMap.entrySet()) {
            graphBuilder.addResultColumn(Column.of(Optional.of(entry.getKey()), entry.getValue()));
        }
        return Quantifier.forEach(Reference.initialOf(graphBuilder.build().buildSelect()));
    }

    @Nonnull
    public static Quantifier valuesQun(@Nonnull Value value) {
        return Quantifier.forEach(Reference.initialOf(new SelectExpression(value, ImmutableList.of(rangeOneQun()), ImmutableList.of())));
    }

    @Nonnull
    public static Quantifier explodeField(@Nonnull final Quantifier t, @Nonnull final String fieldName) {
        return forEach(new ExplodeExpression(fieldValue(t, fieldName)));
    }

    @Nonnull
    private final CascadesRule<? extends RelationalExpression> rule;

    public RuleTestHelper(@Nonnull CascadesRule<? extends RelationalExpression> rule) {
        this.rule = rule;
    }

    @Nonnull
    private TestRuleExecution run(RelationalExpression original, EvaluationContext evaluationContext) {
        // copy the graph handed in so the caller can modify it at will afterwards and won't see the effects of
        // rewriting and planning
        final var copiedOriginal =
                Iterables.getOnlyElement(References.rebaseGraphs(ImmutableList.of(Reference.initialOf(original)),
                        Memoizer.noMemoization(PlannerStage.INITIAL), ToUniqueAliasesTranslationMap.newInstance(), false)).get();
        ensureStage(PlannerStage.CANONICAL, copiedOriginal);
        if (rule instanceof ImplementationCascadesRule) {
            //
            // Descend the copied original to:
            // 1. apply FinalizeExpressionsRule for all children recursively bottom up
            // 2. run the cost model for the children of the finalized expressions
            // This simulates exactly what the planner does fo the children of this expression before the current
            // rule is applied to the current expression. Note that this is only required for implementation rules
            // as they depend on finalized children to work properly. In fact, we cannot call preExploreForRule(...)
            // here for exploration rules as they attempt to share sub graphs which would then make the verification
            // of the result of the rule difficult.
            //
            preExploreForRule(copiedOriginal, false);
        }
        Reference ref = Reference.ofExploratoryExpression(PlannerStage.CANONICAL, copiedOriginal);
        PlanContext planContext = new FakePlanContext();
        return TestRuleExecution.applyRule(planContext, rule, ref, evaluationContext);
    }

    public void ensureStage(@Nonnull PlannerStage plannerStage, @Nonnull RelationalExpression expression) {
        for (Quantifier qun : expression.getQuantifiers()) {
            Reference ref = qun.getRangesOver();
            if (ref.getPlannerStage() != plannerStage) {
                ref.advancePlannerStageUnchecked(plannerStage);
            }
            for (RelationalExpression refMember : ref.getAllMemberExpressions()) {
                ensureStage(plannerStage, refMember);
            }
        }
    }

    public void preExploreForRule(@Nonnull final RelationalExpression expression,
                                  final boolean isClearExploratoryExpressions) {
        for (Quantifier qun : expression.getQuantifiers()) {
            Reference ref = qun.getRangesOver();
            for (RelationalExpression refMember : ref.getAllMemberExpressions()) {
                preExploreForRule(refMember, isClearExploratoryExpressions);
            }
            ref.setExplored();
            PlanContext planContext = new FakePlanContext();
            TestRuleExecution.applyRule(planContext, new FinalizeExpressionsRule(), ref, EvaluationContext.EMPTY);
            pruneInputs(ref.getFinalExpressions(), isClearExploratoryExpressions);
        }
    }

    public void pruneInputs(@Nonnull final Collection<? extends RelationalExpression> finalExpressions,
                            final boolean isClearExploratoryExpressions) {
        for (final var finalExpression : finalExpressions) {
            for (final var quantifier : finalExpression.getQuantifiers()) {
                pruneWithBest(quantifier.getRangesOver(), isClearExploratoryExpressions);
            }
        }
    }

    private static void pruneWithBest(final Reference reference,
                                      final boolean isClearExploratoryExpressions) {
        final var bestFinalExpression = costModel(reference);
        reference.pruneWith(Objects.requireNonNull(bestFinalExpression));
        if (isClearExploratoryExpressions) {
            reference.clearExploratoryExpressions();
        }
    }

    @Nonnull
    private static RelationalExpression costModel(final Reference reference) {
        reference.setExplored();
        final var costModel =
                PlannerPhase.REWRITING.createCostModel(RecordQueryPlannerConfiguration.defaultPlannerConfiguration());
        RelationalExpression bestFinalExpression = null;
        for (final var finalExpression : reference.getFinalExpressions()) {
            if (bestFinalExpression == null || costModel.compare(finalExpression, bestFinalExpression) < 0) {
                bestFinalExpression = finalExpression;
            }
        }
        return Objects.requireNonNull(bestFinalExpression);
    }

    @CanIgnoreReturnValue
    @Nonnull
    public TestRuleExecution assertYields(RelationalExpression original, RelationalExpression... expected) {
        return assertYields(original, EvaluationContext.EMPTY, expected);
    }

    @CanIgnoreReturnValue
    @Nonnull
    public TestRuleExecution assertYields(RelationalExpression original, EvaluationContext evaluationContext,
                                          RelationalExpression... expected) {
        final ImmutableList.Builder<RelationalExpression> expectedListBuilder = ImmutableList.builder();
        for (RelationalExpression expression : expected) {
            //
            // Copy the expected as the caller can construct the expected from parts of the original.
            // we just want to have our own unshared version of it.
            //
            final var copiedExpected =
                    Iterables.getOnlyElement(References.rebaseGraphs(ImmutableList.of(Reference.initialOf(expression)),
                            Memoizer.noMemoization(PlannerStage.INITIAL), ToUniqueAliasesTranslationMap.newInstance(), false)).get();
            ensureStage(PlannerStage.CANONICAL, copiedExpected);
            expectedListBuilder.add(copiedExpected);
        }
        final var expectedList = expectedListBuilder.build();
        if (rule instanceof ImplementationCascadesRule) {
            for (RelationalExpression expression : expectedList) {
                preExploreForRule(expression, true);
                pruneInputs(ImmutableList.of(expression), true);
            }
        }

        TestRuleExecution execution = run(original, evaluationContext);
        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            softly.assertThat(execution.getResult().getAllMemberExpressions())
                    .hasSize(1 + expectedList.size())
                    .containsAll(expectedList);
        }
        return execution;
    }

    @CanIgnoreReturnValue
    @Nonnull
    public TestRuleExecution assertYieldsNothing(RelationalExpression original, boolean matched) {
        return assertYieldsNothing(original, EvaluationContext.empty(), matched);
    }

    @CanIgnoreReturnValue
    @Nonnull
    public TestRuleExecution assertYieldsNothing(RelationalExpression original, EvaluationContext evaluationContext, boolean matched) {
        TestRuleExecution execution = run(original, evaluationContext);
        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            softly.assertThat(execution.hasYielded())
                    .as("rule should not have yielded new expressions")
                    .isFalse();
            softly.assertThat(execution.isRuleMatched())
                    .as("rule should %shave been matched", matched ? "" : "not ")
                    .isEqualTo(matched);
        }
        return execution;
    }
}
