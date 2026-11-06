/*
 * LogicalFilterExpressionTest.java
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
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link LogicalFilterExpression}.
 */
class LogicalFilterExpressionTest {
    private static final Type.Record FLOWED_TYPE = Type.Record.fromFields(List.of(
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("a"))));

    private static Quantifier.ForEach scanQuantifierOver(final Type flowedType) {
        final var scan = new FullUnorderedScanExpression(ImmutableSet.of("R"), flowedType, new AccessHints());
        return Quantifier.forEach(Reference.initialOf(scan));
    }

    @Test
    void getDynamicTypesWithoutPredicatesReturnsResultValueTypes() {
        final var filter = new LogicalFilterExpression(List.of(), scanQuantifierOver(FLOWED_TYPE));

        assertThat(filter.getDynamicTypes())
                .containsExactlyInAnyOrderElementsOf(filter.getResultValue().getDynamicTypes())
                .containsExactly(FLOWED_TYPE);
    }

    @Test
    void getDynamicTypesWithPredicatesReturnsResultValueAndPredicateTypes() {
        final var firstPredicate = new ValuePredicate(
                RecordConstructorValue.ofUnnamed(List.of(LiteralValue.ofScalar(1L))),
                new Comparisons.NullComparison(Comparisons.Type.NOT_NULL));
        final var secondPredicate = new ValuePredicate(
                RecordConstructorValue.ofUnnamed(List.of(LiteralValue.ofScalar("2"))),
                new Comparisons.NullComparison(Comparisons.Type.NOT_NULL));

        // Nest the predicates under an OR to verify that the predicate tree is traversed.
        final var filter = new LogicalFilterExpression(
                List.of(OrPredicate.or(firstPredicate, secondPredicate)),
                scanQuantifierOver(FLOWED_TYPE));

        assertThat(filter.getDynamicTypes())
                .containsExactlyInAnyOrder(
                        FLOWED_TYPE,
                        firstPredicate.getValue().getResultType(),
                        secondPredicate.getValue().getResultType());
    }
}
