/*
 * ExpandWindowExpressionsRuleTest.java
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

import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper;
import com.apple.foundationdb.record.query.plan.cascades.WindowOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.debug.DebuggerWithSymbolTables;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.WindowExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValue;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.CompileTimeWindowValue;
import com.apple.foundationdb.record.query.plan.cascades.values.CompileTimeWindowValue.WindowFunction;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RankWindowValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.apple.foundationdb.record.query.plan.cascades.values.WindowFrameSpecification;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ExpandWindowExpressionsRule}: a {@link SelectExpression} computing a window function is rewritten
 * into a {@link WindowExpression} with a select on top that references the window expression's columns, and the
 * compile-time placeholder is gone afterwards.
 */
class ExpandWindowExpressionsRuleTest {
    @Nonnull
    private static final ExpandWindowExpressionsRule rule = new ExpandWindowExpressionsRule();
    @Nonnull
    private static final RuleTestHelper testHelper = new RuleTestHelper(rule, PlannerPhase.REWRITING);

    @Nonnull
    private static Value field(@Nonnull final Quantifier quantifier, @Nonnull final String fieldName) {
        return FieldValue.ofFieldName(quantifier.getFlowedObjectValue(), fieldName);
    }

    @Nonnull
    private static Value column(@Nonnull final Quantifier quantifier, final int ordinalNumber) {
        return FieldValue.ofOrdinalNumber(QuantifiedObjectValue.of(quantifier), ordinalNumber);
    }

    @Nonnull
    private static CompileTimeWindowValue rankOverPartitionByB(@Nonnull final Quantifier quantifier) {
        return new CompileTimeWindowValue(WindowFunction.RANK,
                ImmutableList.of(field(quantifier, "a")),
                ImmutableList.of(field(quantifier, "b")));
    }

    /**
     * Applies the rule and returns the select it yielded. The result reference also still holds the original select, so
     * the one with a window expression underneath is the one to assert on.
     */
    @Nonnull
    private static SelectExpression yieldedSelect(@Nonnull final SelectExpression original) {
        final var execution = testHelper.apply(original);
        assertThat(execution.hasYielded()).isTrue();
        return execution.getResult().getAllMemberExpressions()
                .stream()
                .filter(SelectExpression.class::isInstance)
                .map(SelectExpression.class::cast)
                .filter(select -> windowExpressionUnderMaybe(select).isPresent())
                .findFirst()
                .orElseThrow(() -> new AssertionError("no select over a window expression was yielded"));
    }

    @Nonnull
    private static Optional<WindowExpression> windowExpressionUnderMaybe(@Nonnull final SelectExpression select) {
        return select.getQuantifiers()
                .stream()
                .flatMap(quantifier -> quantifier.getRangesOver().getAllMemberExpressions().stream())
                .filter(WindowExpression.class::isInstance)
                .map(WindowExpression.class::cast)
                .findFirst();
    }

    @Nonnull
    private static WindowExpression windowExpressionUnder(@Nonnull final SelectExpression select) {
        return windowExpressionUnderMaybe(select)
                .orElseThrow(() -> new AssertionError("no window expression underneath the yielded select"));
    }

    /**
     * The select the window expression ranges over. Fails if the window expression ranges over something else, which is
     * the shape the rewrite must not produce.
     */
    @Nonnull
    private static SelectExpression lowerSelectUnder(@Nonnull final WindowExpression windowExpression) {
        return windowExpression.getInnerQuantifier().getRangesOver().getAllMemberExpressions()
                .stream()
                .filter(SelectExpression.class::isInstance)
                .map(SelectExpression.class::cast)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no select underneath the window expression"));
    }

    @Nonnull
    private static List<Value> flowedColumnsOf(@Nonnull final SelectExpression select) {
        return ImmutableList.copyOf(select.getResultValues());
    }

    @Nonnull
    private static List<QueryPredicate> predicatesOf(@Nonnull final SelectExpression select) {
        return ImmutableList.copyOf(select.getPredicates());
    }

    @Nonnull
    private static Stream<Value> allValues(@Nonnull final RelationalExpression expression) {
        return Stream.concat(
                expression.getResultValue().preOrderStream(),
                expression.getQuantifiers().stream()
                        .flatMap(quantifier -> quantifier.getRangesOver().getAllMemberExpressions().stream())
                        .flatMap(ExpandWindowExpressionsRuleTest::allValues));
    }

    @Nonnull
    private static SelectExpression select(@Nonnull final Quantifier quantifier,
                                          @Nonnull final ImmutableList<Value> resultValues) {
        return new SelectExpression(RecordConstructorValue.ofUnnamed(resultValues),
                ImmutableList.of(quantifier), ImmutableList.of());
    }

    @BeforeEach
    void setUpDebugger() {
        Debugger.setDebugger(DebuggerWithSymbolTables.withSanityChecks());
        Debugger.setup();
    }

    @Test
    void windowingSelectIsRewrittenIntoAWindowExpressionWithASelectOnTop() {
        final var base = RuleTestHelper.baseT();
        final var original = select(base, ImmutableList.of(rankOverPartitionByB(base), field(base, "b")));

        final var yielded = yieldedSelect(original);

        // the select on top reads the window expression positionally rather than computing anything itself
        assertThat(yielded.getResultValue().preOrderStream())
                .noneMatch(CompileTimeWindowValue.class::isInstance);

        final var windowExpression = windowExpressionUnder(yielded);
        assertThat(windowExpression.getWindowValue()).isInstanceOf(RankWindowValue.class);
        assertThat(windowExpression.getWindowValue().getArgumentValues()).hasSize(1);
        assertThat(windowExpression.getPartitioningValues()).hasSize(1);
        // the query selects "b", which is also the PARTITION BY column. The expression flows its partitioning columns
        // itself, so there is nothing left to list as an additional result
        assertThat(windowExpression.getAdditionalResultValues()).isEmpty();
        // the window needs its input grouped by the partitioning column; this window has no ORDER BY of its own
        assertThat(windowExpression.getRequestedOrdering().getOrderingValues()).hasSize(1);
    }

    @Test
    void theCompileTimePlaceholderDoesNotSurviveAnywhereInTheRewrite() {
        final var base = RuleTestHelper.baseT();
        final var original = select(base, ImmutableList.of(rankOverPartitionByB(base), field(base, "b")));

        final var yielded = yieldedSelect(original);

        assertThat(allValues(yielded)).noneMatch(CompileTimeWindowValue.class::isInstance);
    }

    @Test
    void columnThatMerelyContainsAWindowValueIsNotPassedThroughVerbatim() {
        // the flow filter has to look at the whole value tree: this column is not a window value itself, but it
        // is not a plain column either, so passing it through would carry the placeholder into the window expression
        final var base = RuleTestHelper.baseT();
        final var nested = RecordConstructorValue.ofUnnamed(ImmutableList.of(rankOverPartitionByB(base)));
        final var original = select(base, ImmutableList.of(nested, field(base, "b")));

        final var yielded = yieldedSelect(original);

        assertThat(allValues(yielded)).noneMatch(CompileTimeWindowValue.class::isInstance);
        // the one wrapping the window value is not flowed; the plain column here is the partitioning column, which the
        // expression flows itself rather than as an additional result
        final var windowExpression = windowExpressionUnder(yielded);
        assertThat(windowExpression.getAdditionalResultValues()).isEmpty();
    }

    @Test
    void columnSelectedTwiceIsFlowedOnce() {
        // two identical flowed columns would make pulling that column up ambiguous
        final var base = RuleTestHelper.baseT();
        final var original = select(base,
                ImmutableList.of(rankOverPartitionByB(base), field(base, "a"), field(base, "a")));

        final var windowExpression = windowExpressionUnder(yieldedSelect(original));

        // the window expression reads the lower select's columns positionally: column 0 is the partitioning column "b"
        // and column 1 is "a", flowed once even though the query selects it twice
        assertThat(windowExpression.getAdditionalResultValues())
                .containsExactly(column(windowExpression.getInnerQuantifier(), 1));
    }

    @Test
    void partitioningColumnIsFlowedEvenWhenTheQueryDoesNotSelectIt() {
        final var base = RuleTestHelper.baseT();
        // "b" is partitioned by but not selected; the expression flows it anyway, and the select on top ignores it
        final var original = select(base, ImmutableList.of(rankOverPartitionByB(base), field(base, "d")));

        final var windowExpression = windowExpressionUnder(yieldedSelect(original));
        final var lowerSelect = lowerSelectUnder(windowExpression);
        final var lowerQuantifier = Iterables.getOnlyElement(lowerSelect.getQuantifiers());

        // the lower select still has to flow it -- PARTITION BY reads it from there
        assertThat(flowedColumnsOf(lowerSelect)).containsExactly(field(lowerQuantifier, "b"),
                field(lowerQuantifier, "a"), field(lowerQuantifier, "d"));
        assertThat(windowExpression.getPartitioningValues())
                .containsExactly(column(windowExpression.getInnerQuantifier(), 0));
        // "d" is the only additional result, but the expression flows the partitioning column before it and the window
        // value after it
        assertThat(windowExpression.getAdditionalResultValues())
                .containsExactly(column(windowExpression.getInnerQuantifier(), 2));
        assertThat(Values.deconstructRecord(windowExpression.getResultValue())).hasSize(3);
    }

    @Test
    void selectWithoutAWindowValueIsLeftAlone() {
        final var base = RuleTestHelper.baseT();
        final var original = select(base, ImmutableList.of(field(base, "a"), field(base, "b")));

        testHelper.assertYieldsNothing(original, true);
    }

    @Test
    void selectComputingTwoWindowFunctionsIsLeftAlone() {
        final var base = RuleTestHelper.baseT();
        final var otherRank = new CompileTimeWindowValue(WindowFunction.RANK,
                ImmutableList.of(field(base, "b")), ImmutableList.of(field(base, "a")),
                ImmutableList.of(), WindowFrameSpecification.defaultSpecification());
        final var original = select(base, ImmutableList.of(rankOverPartitionByB(base), otherRank));

        testHelper.assertYieldsNothing(original, true);
    }

    @Test
    void selectOverAJoinIsRewritten() {
        final var t = RuleTestHelper.baseT();
        final var tau = RuleTestHelper.baseTau();
        final var joinPredicate = new ValuePredicate(field(t, "a"),
                new Comparisons.ValueComparison(Comparisons.Type.EQUALS, field(tau, "alpha")));
        final var original = new SelectExpression(
                RecordConstructorValue.ofUnnamed(ImmutableList.of(rankOverPartitionByB(t), field(tau, "alpha"))),
                ImmutableList.of(t, tau), ImmutableList.of(joinPredicate));

        final var windowExpression = windowExpressionUnder(yieldedSelect(original));
        final var lowerSelect = lowerSelectUnder(windowExpression);

        // the lower select takes over both quantifiers and the join, so the window expression keeps exactly one input
        // no matter how many the query joined
        assertThat(windowExpression.getQuantifiers()).hasSize(1);
        assertThat(lowerSelect.getQuantifiers()).hasSize(2);
        // a join predicate spans two quantifiers, so nothing else can push it down; it has to be evaluated before the
        // window function is computed, and it stays correlated to both of them
        final var lowerPredicate = Iterables.getOnlyElement(predicatesOf(lowerSelect));
        assertThat(lowerPredicate.getCorrelatedTo()).isEqualTo(lowerSelect.getQuantifiers()
                .stream()
                .map(Quantifier::getAlias)
                .collect(ImmutableSet.toImmutableSet()));
    }

    @Test
    void predicateOnTheWindowValueStaysOnTopOfTheWindowExpression() {
        final var base = RuleTestHelper.baseT();
        final var rank = rankOverPartitionByB(base);
        final var original = new SelectExpression(
                RecordConstructorValue.ofUnnamed(ImmutableList.of(rank, field(base, "b"))),
                ImmutableList.of(base), ImmutableList.of(new ValuePredicate(rank, RuleTestHelper.EQUALS_42)));

        final var yielded = yieldedSelect(original);
        final var windowExpression = windowExpressionUnder(yielded);

        // the predicate cannot be evaluated before the value it reads exists, so it stays on top -- and reads the window
        // expression's column rather than the placeholder
        assertThat(predicatesOf(lowerSelectUnder(windowExpression))).isEmpty();
        final var upperPredicate = (PredicateWithValue)Iterables.getOnlyElement(predicatesOf(yielded));
        assertThat(Objects.requireNonNull(upperPredicate.getValue()).preOrderStream())
                .noneMatch(CompileTimeWindowValue.class::isInstance);
    }

    @Test
    void columnThatIsBothArgumentAndPartitionedByFlowsOnce() {
        final var base = RuleTestHelper.baseT();
        final var rankOverA = new CompileTimeWindowValue(WindowFunction.RANK,
                ImmutableList.of(field(base, "a")), ImmutableList.of(field(base, "a")));
        final var original = select(base, ImmutableList.of(rankOverA));

        final var windowExpression = windowExpressionUnder(yieldedSelect(original));

        final var lowerSelect = lowerSelectUnder(windowExpression);
        assertThat(flowedColumnsOf(lowerSelect))
                .containsExactly(field(Iterables.getOnlyElement(lowerSelect.getQuantifiers()), "a"));
        assertThat(windowExpression.getAdditionalResultValues()).isEmpty();
    }

    @Test
    void theRequestedOrderingIsExpressedOverTheLowerSelect() {
        final var base = RuleTestHelper.baseT();
        final var rankOrderedByC = new CompileTimeWindowValue(WindowFunction.RANK,
                ImmutableList.of(field(base, "a")), ImmutableList.of(field(base, "b")),
                ImmutableList.of(new WindowOrderingPart(field(base, "c"),
                        OrderingPart.RequestedSortOrder.ASCENDING)),
                WindowFrameSpecification.defaultSpecification());
        final var original = select(base, ImmutableList.of(rankOrderedByC));

        final var windowExpression = windowExpressionUnder(yieldedSelect(original));
        final var lowerSelect = lowerSelectUnder(windowExpression);
        final var lowerQuantifier = Iterables.getOnlyElement(lowerSelect.getQuantifiers());
        final var innerQuantifier = windowExpression.getInnerQuantifier();

        // the ordering value travels up with everything else -- it is the lower select's third column here, not a field
        // of the base record. Left over the original quantifier it would correlate the requested ordering to an alias
        // that is not in scope for the window expression, and a window over a join would trip toRequestedOrdering's
        // "at most one correlated alias" check outright. A requested ordering states its values over the current alias.
        final var overCurrent = AliasMap.ofAliases(innerQuantifier.getAlias(), Quantifier.current());
        assertThat(windowExpression.getRequestedOrdering().getOrderingValues())
                .containsExactly(column(innerQuantifier, 0).rebase(overCurrent),
                        column(innerQuantifier, 2).rebase(overCurrent));
        // an ordering column the query does not select still has to be flowed, or the window could not be sorted by it
        assertThat(flowedColumnsOf(lowerSelect)).containsExactly(field(lowerQuantifier, "b"),
                field(lowerQuantifier, "a"), field(lowerQuantifier, "c"));
    }

    /**
     * The rewrite has to produce the same three-level shape the match candidate is built as: a select underneath the
     * window expression, the window expression, and a select on top. Without the lower select the window expression
     * ranges over whatever the original select ranged over, and the query graph cannot line up with the candidate.
     */
    @Test
    void theRewriteProducesASelectUnderneathTheWindowExpression() {
        final var base = RuleTestHelper.baseT();
        final var original = select(base, ImmutableList.of(rankOverPartitionByB(base), field(base, "b")));

        final var windowExpression = windowExpressionUnder(yieldedSelect(original));
        final var lowerSelect = lowerSelectUnder(windowExpression);
        // the harness rebases the graph to fresh aliases before the rule runs, so the columns are asserted over the
        // lower select's own quantifier rather than over the one this test built
        final var lowerQuantifier = Iterables.getOnlyElement(lowerSelect.getQuantifiers());

        // the partitioning column comes first, which lines the ordinals up with the way the match candidate expands a
        // windowed index key, then what the window function reads
        assertThat(flowedColumnsOf(lowerSelect))
                .containsExactly(field(lowerQuantifier, "b"), field(lowerQuantifier, "a"));
        // the lower select is interposed directly over what the original select ranged over
        assertThat(lowerQuantifier.getRangesOver().getAllMemberExpressions())
                .allMatch(LogicalTypeFilterExpression.class::isInstance);
    }
}
