/*
 * ConstantPredicateFoldingUtilTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.predicates.simplification;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.simplification.ConstantPredicateFoldingUtil.EffectiveConstant;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.and;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.areEqual;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.fieldValue;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.isNotNull;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.isNull;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.litInt;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.litTrue;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.lowerType;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.nullableBoolean;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.or;
import static com.apple.foundationdb.record.query.plan.cascades.ConstantFoldingTestUtils.qov;

/**
 * Tests for {@link ConstantPredicateFoldingUtil}.
 */
class ConstantPredicateFoldingUtilTest {

    private static final CorrelationIdentifier Q = Quantifier.current();

    //
    // `foldPredicateAtNull` outcomes
    //

    @Test
    void isNullAcceptsNull() {
        // q IS NULL → NULL IS NULL → TRUE
        final QueryPredicate predicate = isNull(nullableBoolean().value());
        Assertions.assertThat(ConstantPredicateFoldingUtil.foldPredicateAtNull(predicate, Q, EvaluationContext.empty()))
                .isEqualTo(EffectiveConstant.TRUE);
    }

    @Test
    void isNotNullRejectsNull() {
        // q IS NOT NULL → NULL IS NOT NULL → FALSE
        final QueryPredicate predicate = isNotNull(nullableBoolean().value());
        Assertions.assertThat(ConstantPredicateFoldingUtil.foldPredicateAtNull(predicate, Q, EvaluationContext.empty()))
                .isEqualTo(EffectiveConstant.FALSE);
    }

    @Test
    void equalsConstantBecomesNull() {
        // q = TRUE → NULL = TRUE → NULL
        final QueryPredicate predicate = areEqual(nullableBoolean().value(), litTrue().value());
        Assertions.assertThat(ConstantPredicateFoldingUtil.foldPredicateAtNull(predicate, Q, EvaluationContext.empty()))
                .isEqualTo(EffectiveConstant.NULL);
    }

    @Test
    void andOfTrueAndFalseRejectsNull() {
        // (q IS NULL) AND (q IS NOT NULL) → TRUE AND FALSE → FALSE
        final Value q = nullableBoolean().value();
        final QueryPredicate predicate = and(isNull(q), isNotNull(q));
        Assertions.assertThat(ConstantPredicateFoldingUtil.foldPredicateAtNull(predicate, Q, EvaluationContext.empty()))
                .isEqualTo(EffectiveConstant.FALSE);
    }

    @Test
    void orOfTrueAndNullAcceptsNull() {
        // (q IS NULL) OR (q = TRUE) → TRUE OR NULL → TRUE
        final Value q = nullableBoolean().value();
        final QueryPredicate predicate = or(isNull(q), areEqual(q, litTrue().value()));
        Assertions.assertThat(ConstantPredicateFoldingUtil.foldPredicateAtNull(predicate, Q, EvaluationContext.empty()))
                .isEqualTo(EffectiveConstant.TRUE);
    }

    @Test
    void unrelatedAliasIsUnknown() {
        // The predicate references Q but `foldPredicateAtNull()` is asked about a different alias, so no substitution
        // happens and the simplifier cannot fold a non-constant qov.
        final QueryPredicate predicate = areEqual(nullableBoolean().value(), litTrue().value());
        final CorrelationIdentifier unrelatedAlias = CorrelationIdentifier.of("not_q");
        Assertions.assertThat(ConstantPredicateFoldingUtil.foldPredicateAtNull(predicate, unrelatedAlias, EvaluationContext.empty()))
                .isEqualTo(EffectiveConstant.UNKNOWN);
    }

    @Test
    void fieldValueOverSubstitutedLeafCollapsesToNull() {
        // q.b_nullable IS NOT NULL → FieldValue(NullValue, "b_nullable") IS NOT NULL
        //                          → (via CollapseNullStrictValueOverNullValueRule) NullValue IS NOT NULL
        //                          → FALSE.
        final QueryPredicate predicate = isNotNull(fieldValue(qov(lowerType), "b_nullable").value());
        Assertions.assertThat(ConstantPredicateFoldingUtil.foldPredicateAtNull(predicate, Q, EvaluationContext.empty()))
                .isEqualTo(EffectiveConstant.FALSE);
    }

    @Test
    void mixedAndWithUnsubstitutedAliasIsUnknown() {
        // q = 42 AND r = 10 → NULL = 42 AND r = 10 → NULL AND (r = 10).
        // TODO Issue #4222: The simplifier cannot fold `NULL AND <non-constant>` without 3VL filter-context awareness.
        final Value q = QuantifiedObjectValue.of(Q, Type.primitiveType(Type.TypeCode.INT, true));
        final Value r = QuantifiedObjectValue.of(CorrelationIdentifier.of("r"),
                Type.primitiveType(Type.TypeCode.INT, true));
        final QueryPredicate predicate = and(areEqual(q, litInt(42).value()), areEqual(r, litInt(10).value()));
        Assertions.assertThat(ConstantPredicateFoldingUtil.foldPredicateAtNull(predicate, Q, EvaluationContext.empty()))
                .isEqualTo(EffectiveConstant.UNKNOWN);
    }

    //
    // `rejectsNull` outcomes
    //

    @Test
    void rejectsNullTrueOnHardFalse() {
        // FALSE outcome (provably rejects null): q IS NOT NULL.
        final QueryPredicate predicate = isNotNull(nullableBoolean().value());
        Assertions.assertThat(ConstantPredicateFoldingUtil.rejectsNull(predicate, Q, EvaluationContext.empty()))
                .isTrue();
    }

    @Test
    void rejectsNullTrueOnNullOutcome() {
        // NULL outcome (which counts as rejecting): q = TRUE.
        final QueryPredicate predicate = areEqual(nullableBoolean().value(), litTrue().value());
        Assertions.assertThat(ConstantPredicateFoldingUtil.rejectsNull(predicate, Q, EvaluationContext.empty()))
                .isTrue();
    }

    @Test
    void rejectsNullFalseOnAccepting() {
        // TRUE outcome (provably accepts null).
        final QueryPredicate predicate = isNull(nullableBoolean().value());
        Assertions.assertThat(ConstantPredicateFoldingUtil.rejectsNull(predicate, Q, EvaluationContext.empty()))
                .isFalse();
    }

    @Test
    void rejectsNullFalseOnUnknown() {
        // The UNKNOWN outcome must not be reported as “rejecting”.
        final QueryPredicate predicate = areEqual(nullableBoolean().value(), litTrue().value());
        final CorrelationIdentifier unrelatedAlias = CorrelationIdentifier.of("not_q");
        Assertions.assertThat(ConstantPredicateFoldingUtil.rejectsNull(predicate, unrelatedAlias, EvaluationContext.empty()))
                .isFalse();
    }
}
