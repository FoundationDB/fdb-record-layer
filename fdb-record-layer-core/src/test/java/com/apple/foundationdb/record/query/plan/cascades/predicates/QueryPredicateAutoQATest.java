/*
 * QueryPredicateAutoQATest.java
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

package com.apple.foundationdb.record.query.plan.cascades.predicates;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.values.ValueTestHelpers.field;
import static com.apple.foundationdb.record.query.plan.cascades.values.ValueTestHelpers.rcv;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Additional tests for the {@link QueryPredicate} implementations.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class QueryPredicateAutoQATest {

    @Nonnull
    private static ValuePredicate stringEquals(@Nonnull final Value value, @Nonnull final String comparand) {
        return new ValuePredicate(value, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, comparand));
    }

    @Test
    void predicateWithValueAndRangesIsNullRangeOnNullValueIsTrue() {
        // BUG AQA-23: PredicateWithValueAndRanges.eval returns NULL for a NULL value before checking an IS NULL range.
        // Spec: SQL three-valued logic; ConstantFoldingPredicateWithRangesRule javadoc (NULL IS NULL -> TRUE).
        // A NULL value lies in the IS NULL range: eval must agree with the residual predicate (NULL IS NULL is TRUE).
        final Value nullLiteral = new LiteralValue<>(Type.primitiveType(Type.TypeCode.INT, true), null);
        final RangeConstraints.Builder rangeBuilder = RangeConstraints.newBuilder();
        assertThat(rangeBuilder.addComparisonMaybe(new Comparisons.NullComparison(Comparisons.Type.IS_NULL))).isTrue();
        final PredicateWithValueAndRanges predicate =
                PredicateWithValueAndRanges.ofRanges(nullLiteral, ImmutableSet.of(rangeBuilder.build().orElseThrow()));

        final EvaluationContext context = EvaluationContext.empty();
        assertThat(predicate.toResidualPredicate().eval(null, context)).isTrue();
        assertThat(predicate.eval(null, context)).isTrue();
    }

    @Test
    void andSemanticEqualsUnderAliasMapRequiresAllChildrenOfOtherMatched() {
        // BUG AQA-24: ValueEquivalence.semanticEquals(Set, Set) checks sizes and that each left element has some match,
        // not a bijection, so AND semanticEquals is asymmetric. Spec: Correlated#semanticEquals javadoc.
        // AND(q1.a = 'x', q2.a = 'x') must not equal AND(q2.a = 'x', q2.b = 'y') under q1 -> q2: nothing matches q2.b = 'y'.
        final Type recordType = rcv().getResultType();
        final CorrelationIdentifier q1 = CorrelationIdentifier.of("q1");
        final CorrelationIdentifier q2 = CorrelationIdentifier.of("q2");
        final Value q1Value = QuantifiedObjectValue.of(q1, recordType);
        final Value q2Value = QuantifiedObjectValue.of(q2, recordType);

        final QueryPredicate left = AndPredicate.and(stringEquals(field(q1Value, "a"), "x"),
                stringEquals(field(q2Value, "a"), "x"));
        final QueryPredicate right = AndPredicate.and(stringEquals(field(q2Value, "a"), "x"),
                stringEquals(field(q2Value, "b"), "y"));
        final AliasMap aliasMap = AliasMap.ofAliases(q1, q2);

        assertThat(right.semanticEquals(left, aliasMap.inverse())).isFalse();
        assertThat(left.semanticEquals(right, aliasMap)).isFalse();
    }

    @Nonnull
    private static RangeConstraints range(@Nonnull final Comparisons.Comparison... comparisons) {
        final RangeConstraints.Builder builder = RangeConstraints.newBuilder();
        for (final Comparisons.Comparison comparison : comparisons) {
            assertThat(builder.addComparisonMaybe(comparison)).isTrue();
        }
        return builder.build().orElseThrow();
    }

    @Nonnull
    private static Comparisons.Comparison simple(@Nonnull final Comparisons.Type type, @Nonnull final Object comparand) {
        return new Comparisons.SimpleComparison(type, comparand);
    }

    @Test
    void rangeConstraintsSemanticHashCodeConsistentWithSemanticEquals() {
        // BUG AQA-32: RangeConstraints.semanticHashCode hashes CompilableRange, which has no equals/hashCode, so two
        // semantically equal ranges get different (identity, per-run) hash codes. Spec: Correlated#semanticHashCode javadoc.
        // Two RangeConstraints built from the same comparisons are semantically equal, so their semantic hash codes
        // (Correlated#semanticHashCode: consistent with semanticEquals, no identity hashing) must be equal.
        final RangeConstraints first = range(simple(Comparisons.Type.GREATER_THAN, 5));
        final RangeConstraints second = range(simple(Comparisons.Type.GREATER_THAN, 5));
        assertThat(first.semanticEquals(second, AliasMap.emptyMap())).isTrue();
        assertThat(first.semanticHashCode()).isEqualTo(second.semanticHashCode());
    }
}
