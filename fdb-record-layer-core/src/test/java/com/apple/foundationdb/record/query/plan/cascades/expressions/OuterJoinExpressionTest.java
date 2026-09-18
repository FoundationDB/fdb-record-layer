/*
 * OuterJoinExpressionTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.expressions;

import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link OuterJoinExpression}.
 */
class OuterJoinExpressionTest {

    private static final Type.Record TYPE_T = Type.Record.fromFields(ImmutableList.of(
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, true), Optional.of("a")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("b"))
    ));

    private static final Type.Record TYPE_TAU = Type.Record.fromFields(ImmutableList.of(
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, true), Optional.of("alpha")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("beta"))
    ));

    private static Quantifier.ForEach baseQuantifier(@Nonnull final String typeName, @Nonnull final Type.Record type) {
        final FullUnorderedScanExpression scan =
                new FullUnorderedScanExpression(ImmutableSet.of(typeName), Type.Record.fromFields(ImmutableList.of()), new AccessHints());
        final LogicalTypeFilterExpression filter =
                LogicalTypeFilterExpression.of(ImmutableSet.of(typeName), Quantifier.forEach(Reference.initialOf(scan)), type);
        return Quantifier.forEach(Reference.initialOf(filter));
    }

    @Nonnull
    private static OuterJoinExpression buildSimpleOuterJoin(@Nonnull final Quantifier.ForEach preserved,
                                                            @Nonnull final Quantifier.ForEach nullSupplying,
                                                            @Nonnull final ImmutableList<? extends QueryPredicate> joinPredicates) {
        final Value resultValue = preserved.getFlowedObjectValue();
        return new OuterJoinExpression(preserved, nullSupplying, joinPredicates, resultValue);
    }

    @Test
    void canCorrelateAlwaysReturnsTrue() {
        final Quantifier.ForEach preserved = baseQuantifier("T", TYPE_T);
        final Quantifier.ForEach nullSupplying = baseQuantifier("TAU", TYPE_TAU);
        final OuterJoinExpression expression = buildSimpleOuterJoin(preserved, nullSupplying, ImmutableList.of());

        assertThat(expression.canCorrelate()).isTrue();
    }

    @Test
    void getQuantifiersReturnsPreservedThenNullSupplying() {
        final Quantifier.ForEach preserved = baseQuantifier("T", TYPE_T);
        final Quantifier.ForEach nullSupplying = baseQuantifier("TAU", TYPE_TAU);
        final OuterJoinExpression expression = buildSimpleOuterJoin(preserved, nullSupplying, ImmutableList.of());

        assertThat(expression.getQuantifiers().size()).isEqualTo(2);
        assertThat(expression.getQuantifiers().get(0)).isSameAs(preserved);
        assertThat(expression.getQuantifiers().get(1)).isSameAs(nullSupplying);
        assertThat(expression.getRelationalChildCount()).isEqualTo(2);
    }

    @Test
    void getAliasToQuantifierMapMapsBothQuantifiers() {
        final Quantifier.ForEach preserved = baseQuantifier("T", TYPE_T);
        final Quantifier.ForEach nullSupplying = baseQuantifier("TAU", TYPE_TAU);
        final OuterJoinExpression expression = buildSimpleOuterJoin(preserved, nullSupplying, ImmutableList.of());

        assertThat(expression.getAliasToQuantifierMap()).hasSize(2);
        assertThat(expression.getAliasToQuantifierMap().get(preserved.getAlias())).isSameAs(preserved);
        assertThat(expression.getAliasToQuantifierMap().get(nullSupplying.getAlias())).isSameAs(nullSupplying);
    }

    @Test
    void getAliasToQuantifierMapIsMemoized() {
        final Quantifier.ForEach preserved = baseQuantifier("T", TYPE_T);
        final Quantifier.ForEach nullSupplying = baseQuantifier("TAU", TYPE_TAU);
        final OuterJoinExpression expression = buildSimpleOuterJoin(preserved, nullSupplying, ImmutableList.of());

        // Two consecutive calls should return the exact same instance because of memoization.
        assertThat(expression.getAliasToQuantifierMap()).isSameAs(expression.getAliasToQuantifierMap());
    }

    @Test
    void getCorrelationOrderForIndependentQuantifiersHasNoDependencies() {
        final Quantifier.ForEach preserved = baseQuantifier("T", TYPE_T);
        final Quantifier.ForEach nullSupplying = baseQuantifier("TAU", TYPE_TAU);
        final OuterJoinExpression expression = buildSimpleOuterJoin(preserved, nullSupplying, ImmutableList.of());

        assertThat(expression.getCorrelationOrder().getSet())
                .containsExactlyInAnyOrder(preserved.getAlias(), nullSupplying.getAlias());
        assertThat(expression.getCorrelationOrder().getDependencyMap().isEmpty()).isTrue();
    }

    @Test
    void getCorrelationOrderTracksCorrelationFromNullSupplyingToPreserved() {
        final Quantifier.ForEach preserved = baseQuantifier("T", TYPE_T);
        // null-supplying side filters on a value flowing from the preserved side, introducing a correlation.
        final FieldValue preservedField = FieldValue.ofFieldNameAndFuseIfPossible(preserved.getFlowedObjectValue(), "a");
        final QueryPredicate correlatedPredicate = new ValuePredicate(preservedField,
                new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 42L));
        final Quantifier.ForEach baseTau = baseQuantifier("TAU", TYPE_TAU);
        final SelectExpression filteredTau = new SelectExpression(
                baseTau.getFlowedObjectValue(),
                ImmutableList.of(baseTau),
                ImmutableList.of(correlatedPredicate));
        final Quantifier.ForEach nullSupplying = Quantifier.forEach(Reference.initialOf(filteredTau));
        final OuterJoinExpression expression = buildSimpleOuterJoin(preserved, nullSupplying, ImmutableList.of());

        // The dependency map points each alias to its correlations.
        assertThat(expression.getCorrelationOrder().getDependencyMap().get(nullSupplying.getAlias()))
                .containsExactly(preserved.getAlias());
        assertThat(expression.getCorrelationOrder().getDependencyMap().get(preserved.getAlias()))
                .isEmpty();
    }

    @Test
    void getCorrelationOrderIsMemoized() {
        final Quantifier.ForEach preserved = baseQuantifier("T", TYPE_T);
        final Quantifier.ForEach nullSupplying = baseQuantifier("TAU", TYPE_TAU);
        final OuterJoinExpression expression = buildSimpleOuterJoin(preserved, nullSupplying, ImmutableList.of());

        // Memoization should hand back the same partial-order instance every time.
        assertThat(expression.getCorrelationOrder()).isSameAs(expression.getCorrelationOrder());
    }

    @Test
    void equalsIsReflexive() {
        final Quantifier.ForEach preserved = baseQuantifier("T", TYPE_T);
        final Quantifier.ForEach nullSupplying = baseQuantifier("TAU", TYPE_TAU);
        final OuterJoinExpression expression = buildSimpleOuterJoin(preserved, nullSupplying, ImmutableList.of());

        assertThat(expression.equals(expression)).isTrue();
        assertThat(expression.hashCode()).isEqualTo(expression.hashCode());
    }

    @Test
    void equalsHoldsForSemanticallyEqualButDistinctInstances() {
        final Quantifier.ForEach preservedA = baseQuantifier("T", TYPE_T);
        final Quantifier.ForEach nullSupplyingA = baseQuantifier("TAU", TYPE_TAU);
        final OuterJoinExpression expressionA = buildSimpleOuterJoin(preservedA, nullSupplyingA, ImmutableList.of());

        final Quantifier.ForEach preservedB = baseQuantifier("T", TYPE_T);
        final Quantifier.ForEach nullSupplyingB = baseQuantifier("TAU", TYPE_TAU);
        final OuterJoinExpression expressionB = new OuterJoinExpression(
                preservedB, nullSupplyingB, ImmutableList.of(), preservedB.getFlowedObjectValue());

        assertThat(expressionA.equals(expressionB)).isTrue();
    }

    @Test
    void equalsReturnsFalseWhenJoinPredicatesDiffer() {
        final Quantifier.ForEach preserved = baseQuantifier("T", TYPE_T);
        final Quantifier.ForEach nullSupplying = baseQuantifier("TAU", TYPE_TAU);
        final OuterJoinExpression withoutPredicate =
                buildSimpleOuterJoin(preserved, nullSupplying, ImmutableList.of());
        final OuterJoinExpression withPredicate =
                buildSimpleOuterJoin(preserved, nullSupplying, ImmutableList.of(ConstantPredicate.TRUE));

        assertThat(withoutPredicate.equals(withPredicate)).isFalse();
        assertThat(withPredicate.equals(withoutPredicate)).isFalse();
    }

    @Test
    void equalsReturnsFalseForOtherClass() {
        final Quantifier.ForEach preserved = baseQuantifier("T", TYPE_T);
        final Quantifier.ForEach nullSupplying = baseQuantifier("TAU", TYPE_TAU);
        final OuterJoinExpression expression = buildSimpleOuterJoin(preserved, nullSupplying, ImmutableList.of());

        assertThat(expression.equals("not an expression")).isFalse();
        assertThat(expression.equals(null)).isFalse();
    }

    @Test
    void equalsWithoutChildrenReturnsTrueForSameInstance() {
        final Quantifier.ForEach preserved = baseQuantifier("T", TYPE_T);
        final Quantifier.ForEach nullSupplying = baseQuantifier("TAU", TYPE_TAU);
        final OuterJoinExpression expression = buildSimpleOuterJoin(preserved, nullSupplying, ImmutableList.of());

        assertThat(expression.equalsWithoutChildren(expression, AliasMap.emptyMap())).isTrue();
    }

    @Test
    void equalsWithoutChildrenReturnsFalseForDifferentClass() {
        final Quantifier.ForEach preserved = baseQuantifier("T", TYPE_T);
        final Quantifier.ForEach nullSupplying = baseQuantifier("TAU", TYPE_TAU);
        final OuterJoinExpression expression = buildSimpleOuterJoin(preserved, nullSupplying, ImmutableList.of());

        // A `SelectExpression` carrying the same quantifiers and result value is structurally similar but is a
        // different class, so `equalsWithoutChildren()` must reject it.
        final SelectExpression otherExpression = new SelectExpression(
                preserved.getFlowedObjectValue(),
                ImmutableList.of(preserved, nullSupplying),
                ImmutableList.of());
        assertThat(expression.equalsWithoutChildren(otherExpression, AliasMap.emptyMap())).isFalse();
    }

    @Test
    void equalsWithoutChildrenChecksJoinPredicateCount() {
        final Quantifier.ForEach preserved = baseQuantifier("T", TYPE_T);
        final Quantifier.ForEach nullSupplying = baseQuantifier("TAU", TYPE_TAU);
        final OuterJoinExpression onePredicate = buildSimpleOuterJoin(preserved, nullSupplying,
                ImmutableList.of(ConstantPredicate.TRUE));
        final OuterJoinExpression twoPredicates = buildSimpleOuterJoin(preserved, nullSupplying,
                ImmutableList.of(ConstantPredicate.TRUE, ConstantPredicate.FALSE));

        assertThat(onePredicate.equalsWithoutChildren(twoPredicates, AliasMap.emptyMap())).isFalse();
    }

    @Test
    void rewriteInternalPlannerGraphProducesOuterJoinNodeWithoutDetailsWhenNoPredicates() {
        final Quantifier.ForEach preserved = baseQuantifier("T", TYPE_T);
        final Quantifier.ForEach nullSupplying = baseQuantifier("TAU", TYPE_TAU);
        final OuterJoinExpression expression = buildSimpleOuterJoin(preserved, nullSupplying, ImmutableList.of());

        final PlannerGraph graph = expression.rewriteInternalPlannerGraph(ImmutableList.of());
        assertThat(graph.getRoot().getName()).isEqualTo("OUTER JOIN");
        assertThat(graph.getRoot().getDetails()).isEmpty();
    }

    @Test
    void rewriteInternalPlannerGraphRendersJoinPredicatesAsDetail() {
        final Quantifier.ForEach preserved = baseQuantifier("T", TYPE_T);
        final Quantifier.ForEach nullSupplying = baseQuantifier("TAU", TYPE_TAU);
        final OuterJoinExpression expression = buildSimpleOuterJoin(preserved, nullSupplying,
                ImmutableList.of(ConstantPredicate.TRUE));

        final PlannerGraph graph = expression.rewriteInternalPlannerGraph(ImmutableList.of());
        assertThat(graph.getRoot().getName()).isEqualTo("OUTER JOIN");
        assertThat(graph.getRoot().getDetails()).hasSize(1);
        assertThat(graph.getRoot().getDetails().get(0)).startsWith("ON ");
    }

    @Test
    void computeCorrelatedToWithoutChildrenIncludesOuterCorrelations() {
        final Quantifier.ForEach preserved = baseQuantifier("T", TYPE_T);
        final Quantifier.ForEach nullSupplying = baseQuantifier("TAU", TYPE_TAU);
        // A predicate referencing an outer alias not present among the children: that alias must surface as
        // “correlated to without children”.
        final CorrelationIdentifier outerAlias = CorrelationIdentifier.of("outer");
        final QueryPredicate joinPredicate = new ValuePredicate(
                FieldValue.ofFieldNameAndFuseIfPossible(QuantifiedObjectValue.of(outerAlias, TYPE_T), "a"),
                new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 42L));
        final OuterJoinExpression expression =
                buildSimpleOuterJoin(preserved, nullSupplying, ImmutableList.of(joinPredicate));

        assertThat(expression.computeCorrelatedToWithoutChildren()).contains(outerAlias);
    }
}
