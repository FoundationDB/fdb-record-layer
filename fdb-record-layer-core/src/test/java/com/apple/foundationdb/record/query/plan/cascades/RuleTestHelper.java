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
import com.google.common.base.Verify;
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
    @SuppressWarnings("PMD.CompareObjectsWithEquals")

    private TestRuleExecution run(Reference root, Reference group, RelationalExpression original) {
        // copy the graph handed in so the caller can modify it at will afterward and won't see the effects of
        // rewriting and planning
        final var copiedOriginals =
                References.rebaseGraphs(ImmutableList.of(root, group),
                        Memoizer.noMemoization(PlannerStage.INITIAL), new ToUniqueAliasesTranslationMap(), false);

        Verify.verify(copiedOriginals.size() == 2);
        final var copiedRoot = copiedOriginals.get(0);
        final var copiedGroup = copiedOriginals.get(1);
        final var copiedExpression = copiedGroup.get();

        ensureStage(PlannerStage.CANONICAL, copiedRoot);

        final var traversal = Traversal.withRoot(copiedRoot);
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
            preExploreForRule(copiedExpression, traversal, false);
        }
        PlanContext planContext = new FakePlanContext();
        return TestRuleExecution.applyRule(planContext, rule, copiedGroup, true,
                traversal, EvaluationContext.empty());
    }

    public void ensureStage(@Nonnull PlannerStage plannerStage, @Nonnull Reference reference) {
        if (reference.getPlannerStage() != plannerStage) {
            reference.advancePlannerStageUnchecked(plannerStage);
        }
        for (RelationalExpression expression : reference.getAllMemberExpressions()) {
            for (Quantifier qun : expression.getQuantifiers()) {
                Reference childReference = qun.getRangesOver();
                ensureStage(plannerStage, childReference);
            }
        }
    }

    public void preExploreForRule(@Nonnull final RelationalExpression expression,
                                  @Nonnull final Traversal traversal,
                                  final boolean isClearExploratoryExpressions) {
        for (Quantifier qun : expression.getQuantifiers()) {
            Reference ref = qun.getRangesOver();
            for (RelationalExpression refMember : ref.getAllMemberExpressions()) {
                preExploreForRule(refMember, traversal, isClearExploratoryExpressions);
            }
            ref.setExplored();
            PlanContext planContext = new FakePlanContext();
            TestRuleExecution.applyRule(planContext, new FinalizeExpressionsRule(), ref, false,
                    traversal, EvaluationContext.empty());
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
        final var root = Reference.initialOf(original);
        return assertYields(root, root, original, expected);
    }

    @CanIgnoreReturnValue
    @Nonnull
    public TestRuleExecution assertYields(Reference root, Reference group, RelationalExpression original,
                                          RelationalExpression... expected) {
        final ImmutableList.Builder<RelationalExpression> expectedListBuilder = ImmutableList.builder();
        for (RelationalExpression expression : expected) {
            //
            // Copy the expected as the caller can construct the expected from parts of the original.
            // We just want to have our own unshared version of it.
            //
            final var copiedExpected =
                    Iterables.getOnlyElement(References.rebaseGraphs(ImmutableList.of(Reference.initialOf(expression)),
                            Memoizer.noMemoization(PlannerStage.INITIAL), new ToUniqueAliasesTranslationMap(), false));

            //
            // Advance the copied expected expression to the CANONICAL phase
            //
            ensureStage(PlannerStage.CANONICAL, copiedExpected);
            expectedListBuilder.add(copiedExpected.get());
        }
        final var expectedList = expectedListBuilder.build();

        //
        // If the rule that is being tested is an implementation rule, we need to prepare the subgraph underneath
        // the group to be tested in a way that the implementation rule can find finalized expressions.
        //
        if (rule instanceof ImplementationCascadesRule) {
            for (RelationalExpression expression : expectedList) {
                preExploreForRule(expression, Traversal.withRoot(Reference.initialOf(expression)),
                        true);
                pruneInputs(ImmutableList.of(expression), true);
            }
        }

        TestRuleExecution execution = run(root, group, original);
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
        final var root = Reference.initialOf(original);
        return assertYieldsNothing(root, root, original, matched);
    }

    @CanIgnoreReturnValue
    @Nonnull
    public TestRuleExecution assertYieldsNothing(Reference root, Reference group, RelationalExpression original,
                                                 boolean matched) {
        TestRuleExecution execution = run(root, group, original);
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
