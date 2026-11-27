/*
 * PartitionBinarySelectRuleAutoQATest.java
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

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValueAndRanges;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.RangeConstraints;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.column;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fieldPredicate;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fieldValue;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.forEach;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.projectColumn;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.selectWithPredicates;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.baseT;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.baseTau;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.explodeField;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.join;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.valuesQun;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class PartitionBinarySelectRuleAutoQATest {
    private static final List<Comparisons.Type> SWAPPABLE_TYPES = ImmutableList.of(
            Comparisons.Type.EQUALS,
            Comparisons.Type.NOT_EQUALS,
            Comparisons.Type.LESS_THAN,
            Comparisons.Type.LESS_THAN_OR_EQUALS,
            Comparisons.Type.GREATER_THAN,
            Comparisons.Type.GREATER_THAN_OR_EQUALS,
            Comparisons.Type.NOT_DISTINCT_FROM,
            Comparisons.Type.IS_DISTINCT_FROM);

    private static final List<Long> OPERANDS = Arrays.asList(null, 1L, 2L);

    private final RuleTestHelper testHelper = new RuleTestHelper(new PartitionBinarySelectRule(), PlannerPhase.PLANNING);

    @Nonnull
    static Stream<Arguments> typesAndOrientations() {
        return SWAPPABLE_TYPES.stream()
                .flatMap(type -> Stream.of(Arguments.of(type, true), Arguments.of(type, false)));
    }

    @ParameterizedTest(name = "{0} firstOnLeft={1}")
    @MethodSource("typesAndOrientations")
    void reorientedValuePredicateIsEquivalent(@Nonnull final Comparisons.Type type, final boolean firstOnLeft) {
        // Every predicate the rule pushes down for a plain ValuePredicate join criterion must evaluate exactly like
        // the original one (three-valued, including NULL operands), for every comparison type that can be swapped
        final Quantifier first = valuesQun(LiteralValue.ofScalar(1L));
        final Quantifier second = valuesQun(LiteralValue.ofScalar(2L));
        final Value firstValue = QuantifiedObjectValue.of(first);
        final Value secondValue = QuantifiedObjectValue.of(second);
        final QueryPredicate original = firstOnLeft
                ? new ValuePredicate(firstValue, new Comparisons.ValueComparison(type, secondValue))
                : new ValuePredicate(secondValue, new Comparisons.ValueComparison(type, firstValue));

        assertPushedPredicatesEquivalent(first, second, original);
    }

    @Nonnull
    static Stream<Arguments> rangeTypesAndOrientations() {
        return typesAndOrientations()
                .filter(args -> RangeConstraints.ofComparisonMaybe(new Comparisons.ValueComparison(
                        (Comparisons.Type)args.get()[0], LiteralValue.ofScalar(1L))).isPresent());
    }

    @ParameterizedTest(name = "{0} firstOnLeft={1}")
    @MethodSource("rangeTypesAndOrientations")
    void reorientedRangePredicateIsEquivalent(@Nonnull final Comparisons.Type type, final boolean firstOnLeft) {
        // Same as above for a sargable PredicateWithValueAndRanges with a single range holding a single comparison;
        // the pushed predicate's residual form must be equivalent to the original's residual form
        final Quantifier first = valuesQun(LiteralValue.ofScalar(1L));
        final Quantifier second = valuesQun(LiteralValue.ofScalar(2L));
        final Value firstValue = QuantifiedObjectValue.of(first);
        final Value secondValue = QuantifiedObjectValue.of(second);
        final Value value = firstOnLeft ? firstValue : secondValue;
        final Value comparand = firstOnLeft ? secondValue : firstValue;
        final PredicateWithValueAndRanges original = PredicateWithValueAndRanges.sargable(value,
                RangeConstraints.ofComparisonMaybe(new Comparisons.ValueComparison(type, comparand)).orElseThrow());

        assertPushedPredicatesEquivalent(first, second, original);
    }

    @Test
    void pushMultiComparisonRangeNotReoriented() {
        // A range with two comparisons against the other side, t.b [> tau.beta AND < tau.delta], cannot be reoriented
        // and must be pushed to either side unchanged
        final Quantifier t = baseT();
        final Quantifier tau = baseTau();
        final RangeConstraints.Builder rangeBuilder = RangeConstraints.newBuilder();
        assertThat(rangeBuilder.addComparisonMaybe(
                new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, fieldValue(tau, "beta")))).isTrue();
        assertThat(rangeBuilder.addComparisonMaybe(
                new Comparisons.ValueComparison(Comparisons.Type.LESS_THAN, fieldValue(tau, "delta")))).isTrue();
        final QueryPredicate joinPredicate =
                PredicateWithValueAndRanges.sargable(fieldValue(t, "b"), rangeBuilder.build().orElseThrow());
        final SelectExpression join = join(t, tau)
                .addResultColumn(projectColumn(t, "a"))
                .addResultColumn(projectColumn(tau, "alpha"))
                .addPredicate(joinPredicate)
                .build().buildSelect();

        final Quantifier newT = forEach(selectWithPredicates(t, joinPredicate));
        final SelectExpression newJoinOnT = join(tau, newT)
                .addResultColumn(projectColumn(newT, "a"))
                .addResultColumn(projectColumn(tau, "alpha"))
                .build().buildSelect();

        final Quantifier newTau = forEach(selectWithPredicates(tau, joinPredicate));
        final SelectExpression newJoinOnTau = join(t, newTau)
                .addResultColumn(projectColumn(t, "a"))
                .addResultColumn(projectColumn(newTau, "alpha"))
                .build().buildSelect();

        testHelper.assertYields(join, newJoinOnT, newJoinOnTau);
    }

    @Test
    void pushInequalityToCorrelatedExplodeReoriented() {
        // With g exploding t, the inequality t.b > g._0.two can only be pushed to g, and is reoriented to
        // g._0.two < t.b
        final Quantifier t = baseT();
        final Quantifier g = explodeField(t, "g");
        final SelectExpression join = join(t, g)
                .addResultColumn(projectColumn(t, "a"))
                .addResultColumn(column(g, "_0.one", "one"))
                .addPredicate(fieldPredicate(t, "b",
                        new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, fieldValue(g, "_0.two"))))
                .build().buildSelect();

        final Quantifier newG = forEach(selectWithPredicates(g,
                fieldPredicate(g, "_0.two", new Comparisons.ValueComparison(Comparisons.Type.LESS_THAN, fieldValue(t, "b")))));
        final SelectExpression newJoin = join(t, newG)
                .addResultColumn(projectColumn(t, "a"))
                .addResultColumn(column(newG, "_0.one", "one"))
                .build().buildSelect();

        testHelper.assertYields(join, newJoin);
    }

    private void assertPushedPredicatesEquivalent(@Nonnull final Quantifier first,
                                                  @Nonnull final Quantifier second,
                                                  @Nonnull final QueryPredicate original) {
        final SelectExpression join = join(first, second)
                .addResultValue(RecordConstructorValue.ofUnnamed(ImmutableList.of(
                        QuantifiedObjectValue.of(first), QuantifiedObjectValue.of(second))))
                .addPredicate(original)
                .build().buildSelect();

        final TestRuleExecution execution = testHelper.run(join, EvaluationContext.empty());
        assertThat(execution.hasYielded()).isTrue();

        // The helper copies the graph (with new aliases), so take the original predicate and aliases from the copy
        QueryPredicate copiedOriginal = null;
        CorrelationIdentifier firstAlias = null;
        CorrelationIdentifier secondAlias = null;
        final List<List<QueryPredicate>> pushedPerYield = new ArrayList<>();
        for (final RelationalExpression member : execution.getResult().getExploratoryExpressions()) {
            assertThat(member).isInstanceOf(SelectExpression.class);
            final SelectExpression select = (SelectExpression)member;
            if (!select.getPredicates().isEmpty()) {
                assertThat(select.getPredicates()).hasSize(1);
                copiedOriginal = select.getPredicates().get(0);
                final List<CorrelationIdentifier> resultAliases = select.getResultValue().preOrderStream()
                        .filter(QuantifiedObjectValue.class::isInstance)
                        .map(value -> ((QuantifiedObjectValue)value).getAlias())
                        .collect(ImmutableList.toImmutableList());
                assertThat(resultAliases).hasSize(2);
                firstAlias = resultAliases.get(0);
                secondAlias = resultAliases.get(1);
                continue;
            }
            final List<QueryPredicate> pushed = new ArrayList<>();
            for (final Quantifier quantifier : select.getQuantifiers()) {
                for (final RelationalExpression lower : quantifier.getRangesOver().getAllMemberExpressions()) {
                    if (lower instanceof SelectExpression) {
                        pushed.addAll(((SelectExpression)lower).getPredicates());
                    }
                }
            }
            pushedPerYield.add(pushed);
        }
        assertThat(copiedOriginal).isNotNull();
        // Both assignments of the quantifiers to left and right yield a partitioning
        assertThat(pushedPerYield).hasSize(2);

        final QueryPredicate evaluableOriginal = toEvaluable(copiedOriginal);
        boolean sawTrue = false;
        boolean sawFalse = false;
        for (final List<QueryPredicate> pushed : pushedPerYield) {
            assertThat(pushed).hasSize(1);
            final QueryPredicate pushedPredicate = pushed.get(0);
            assertThat(pushedPredicate.getCorrelatedTo()).isEqualTo(ImmutableSet.of(firstAlias, secondAlias));
            final QueryPredicate evaluablePushed = toEvaluable(pushedPredicate);
            for (final Long firstOperand : OPERANDS) {
                for (final Long secondOperand : OPERANDS) {
                    final EvaluationContext context = EvaluationContext.empty()
                            .withBinding(Bindings.Internal.CORRELATION, firstAlias, QueryResult.ofComputed(firstOperand))
                            .withBinding(Bindings.Internal.CORRELATION, secondAlias, QueryResult.ofComputed(secondOperand));
                    assertThat(evaluablePushed.eval(null, context))
                            .as("original %s, pushed %s, first=%s, second=%s",
                                    copiedOriginal, pushedPredicate, firstOperand, secondOperand)
                            .isEqualTo(evaluableOriginal.eval(null, context));
                    sawTrue |= Boolean.TRUE.equals(evaluableOriginal.eval(null, context));
                    sawFalse |= Boolean.FALSE.equals(evaluableOriginal.eval(null, context));
                }
            }
        }

        // guard against a vacuous check where nothing evaluates to a definite result
        assertThat(sawTrue).isTrue();
        assertThat(sawFalse).isTrue();
    }

    @Nonnull
    private static QueryPredicate toEvaluable(@Nonnull final QueryPredicate predicate) {
        return predicate instanceof PredicateWithValueAndRanges
                ? ((PredicateWithValueAndRanges)predicate).toResidualPredicate()
                : predicate;
    }
}
