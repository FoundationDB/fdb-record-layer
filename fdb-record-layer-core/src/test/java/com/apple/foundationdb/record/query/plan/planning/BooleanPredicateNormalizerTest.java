/*
 * BooleanPredicateNormalizerTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.planning;

import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.Type;
import com.apple.foundationdb.record.query.predicates.FieldValue;
import com.apple.foundationdb.record.query.predicates.OrPredicate;
import com.apple.foundationdb.record.query.predicates.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.predicates.ValuePredicate;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.record.query.plan.planning.BooleanPredicateNormalizer.Mode.DNF;
import static com.apple.foundationdb.record.query.predicates.AndPredicate.and;
import static com.apple.foundationdb.record.query.predicates.NotPredicate.not;
import static com.apple.foundationdb.record.query.predicates.OrPredicate.or;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link BooleanPredicateNormalizer}.
 */
class BooleanPredicateNormalizerTest {
    private static final FieldValue F = new FieldValue(QuantifiedObjectValue.of(CorrelationIdentifier.UNGROUNDED, Type.Record.fromFields(ImmutableList.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("f"))))), ImmutableList.of("f"));
    private static final QueryPredicate P1 = new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 1));
    private static final QueryPredicate P2 = new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 2));
    private static final QueryPredicate P3 = new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 3));
    private static final QueryPredicate P4 = new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 4));
    private static final QueryPredicate P5 = new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 5));
    private static final QueryPredicate P6 = new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 6));
    private static final QueryPredicate P7 = new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 7));
    private static final QueryPredicate P8 = new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 8));
    private static final QueryPredicate P9 = new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 9));

    @Test
    void atomic() {
        assertExpectedDnf(P1, P1);
        assertExpectedDnf(not(P1), not(P1));
        assertExpectedCnf(P1, P1);
        assertExpectedCnf(not(P1), not(P1));
    }
    
    @Test
    void flattenDnf() {
        assertExpectedDnf(and(P1, P2, P3),
                and(and(P1, P2), P3));
        assertExpectedDnf(and(P1, P2, P3),
                and(P1, and(P2, P3)));
        assertExpectedDnf(and(P1, P2, P3, P4, P5),
                and(P1, and(P2, and(P3, and(P4, P5)))));
        assertExpectedDnf(and(P1, P2, P3, P4, P5),
                and(and(and(and(P1, P2), P3), P4), P5));
        assertExpectedDnf(or(P1, P2, P3),
                or(or(P1, P2), P3));
        assertExpectedDnf(or(P1, P2, P3),
                or(P1, or(P2, P3)));
    }

    @Test
    void flattenCnf() {
        assertExpectedCnf(and(P1, P2, P3),
                and(and(P1, P2), P3));
        assertExpectedCnf(and(P1, P2, P3),
                and(P1, and(P2, P3)));
        assertExpectedCnf(and(P1, P2, P3, P4, P5),
                and(P1, and(P2, and(P3, and(P4, P5)))));
        assertExpectedCnf(and(P1, P2, P3, P4, P5),
                and(and(and(and(P1, P2), P3), P4), P5));
        assertExpectedCnf(or(P1, P2, P3),
                or(or(P1, P2), P3));
        assertExpectedCnf(or(P1, P2, P3),
                or(P1, or(P2, P3)));
    }

    @Test
    void distributeDnf() {
        assertExpectedDnf(or(P1, and(P2, P3)),
                and(or(P1, P2), or(P1, P3)));
        assertExpectedDnf(or(and(P1, P2), and(P3, P4)),
                or(and(P1, P2), and(P3, P4)));
        assertExpectedDnf(or(and(P1, P2), and(P1, P3)),
                and(P1, or(P2, P3)));
        assertExpectedDnf(or(and(P1, P3), and(P2, P3), and(P1, P4), and(P2, P4)),
                and(or(P1, P2), or(P3, P4)));
    }

    @Test
    void distributeCnf() {
        assertExpectedCnf(and(or(P1, P2), or(P3, P4)),
                and(or(P1, P2), or(P3, P4)));
        assertExpectedCnf(and(P1, or(P2, P3)),
                or(and(P1, P2), and(P1, P3)));
        assertExpectedCnf(and(or(P1, P2), or(P3, P4)),
                or(and(P1, P3), and(P2, P3), and(P1, P4), and(P2, P4)));
    }

    @Test
    void deMorgan() {
        assertExpectedDnf(or(not(P1), not(P2)),
                not(and(P1, P2)));
        assertExpectedDnf(and(not(P1), not(P2)),
                not(or(P1, P2)));
        assertExpectedCnf(or(not(P1), not(P2)),
                not(and(P1, P2)));
        assertExpectedCnf(and(not(P1), not(P2)),
                not(or(P1, P2)));
    }

    @Test
    void complexDnf() {
        assertExpectedDnf(or(and(P1, not(P2)), and(P1, not(P3), not(P4))),
                and(P1, not(and(P2, or(P3, P4)))));
    }

    @Test
    void complexCnf() {
        assertExpectedCnf(and(P1, or(not(P2), not(P3)), or(not(P2), not(P4))),
                or(and(P1, not(P2)), and(P1, not(P3), not(P4))));
    }

    @Test
    void complexRoundTrip() {
        assertExpectedDnf(or(and(P1, P2), and(P1, P3), and(P1, P4), and(P1, P5)), and(P1, or(P2, P3, P4, P5)));
        assertExpectedCnf(and(P1, or(P2, P3, P4, P5)), or(and(P1, P2), and(P1, P3), and(P1, P4), and(P1, P5)));

        // take an original predicate and DNF it, then CNF it, and DNF its result again
        assertExpectedDnf(or(and(P1, P2, P3), and(P1, P4, P5)), and(P1, or(and(P2, P3), and(P4, P5))));
        assertExpectedCnf(and(P1, or(P2, P4), or(P3, P4), or(P2, P5), or(P3, P5)), or(and(P1, P2, P3), and(P1, P4, P5)));
        assertExpectedDnf(or(and(P1, P2, P3), and(P1, P4, P5)), and(P1, or(P2, P4), or(P3, P4), or(P2, P5), or(P3, P5)));

        final BooleanPredicateNormalizer forCnf = BooleanPredicateNormalizer.getDefaultInstanceForCnf();
        final BooleanPredicateNormalizer forDnf = BooleanPredicateNormalizer.getDefaultInstanceForDnf();

        final QueryPredicate original = and(P1, or(and(P2, P3), and(P4, P5), and(P6, P7)));

        final QueryPredicate expectedDnf = or(and(P1, P2, P3), and(P1, P4, P5), and(P1, P6, P7));
        assertEquals(expectedDnf, forDnf.normalize(original).orElse(original));

        // original -> cnf -> dnf
        assertEquals(expectedDnf, forDnf.normalize(forCnf.normalize(original).orElse(original)).orElse(original));

        // expected dnf -> cnf -> dnf
        assertEquals(expectedDnf, forDnf.normalize(forCnf.normalize(expectedDnf).orElse(expectedDnf)).orElse(expectedDnf));
    }

    @Test
    void redundant() {
        assertExpectedDnf(and(P1, P2, P3),
                and(P1, or(and(P2, P3), and(P2, P4, P3))));

        assertExpectedDnf(and(P1, P2, P3), and(P1, or(and(P2, P3), and(P2, P4, P3))));
    }

    @Test
    void cnf() {
        final List<QueryPredicate> conjuncts = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            final List<QueryPredicate> disjuncts = new ArrayList<>();
            for (int j = 0; j < 5; j++) {
                disjuncts.add(new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, i * 4 + j)));
            }
            conjuncts.add(or(disjuncts));
        }
        final QueryPredicate cnf = and(conjuncts);

        final BooleanPredicateNormalizer normalizer = BooleanPredicateNormalizer.getDefaultInstanceForDnf();
        assertNotEquals(cnf, normalizer.normalize(cnf).orElse(cnf));
        assertTrue(numberOfOrTerms(Objects.requireNonNull(normalizer.normalize(cnf).orElse(cnf))) <= normalizer.getNormalizedSize(cnf));

        final BooleanPredicateNormalizer lowLimitNormalizer = BooleanPredicateNormalizer.withLimit(DNF, 2);
        assertThrows(BooleanPredicateNormalizer.NormalFormTooLargeException.class, () -> lowLimitNormalizer.normalize(cnf));
        assertEquals(cnf, lowLimitNormalizer.normalizeIfPossible(cnf).orElse(cnf));
    }

    @Test
    void bigNonCnf() {
        final QueryPredicate cnf = and(
                IntStream.rangeClosed(1, 9).boxed().map(i ->
                        or(IntStream.rangeClosed(1, 9).boxed()
                                .map(j -> and(
                                        new ValuePredicate(new FieldValue(
                                                QuantifiedObjectValue.of(CorrelationIdentifier.UNGROUNDED,
                                                        Type.Record.fromFields(ImmutableList.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("num_value_3_indexed"))))),
                                                ImmutableList.of("num_value_3_indexed")),
                                                new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, i * 9 + j)),
                                        new ValuePredicate(new FieldValue(
                                                QuantifiedObjectValue.of(CorrelationIdentifier.UNGROUNDED,
                                                        Type.Record.fromFields(ImmutableList.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("str_value_indexed"))))),
                                                ImmutableList.of("str_value_indexed")),
                                                new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "foo"))))
                                .collect(Collectors.toList())))
                        .collect(Collectors.toList()));
        final BooleanPredicateNormalizer plannerNormalizer = BooleanPredicateNormalizer.forConfiguration(DNF, RecordQueryPlannerConfiguration.builder().build());
        assertThrows(BooleanPredicateNormalizer.NormalFormTooLargeException.class, () -> plannerNormalizer.normalize(cnf));
        assertEquals(cnf, plannerNormalizer.normalizeIfPossible(cnf).orElse(cnf));
    }

    @Test
    void bigCnfThatWouldOverflow() {
        // A CNF who's DNF size doesn't fit in an int.
        final List<QueryPredicate> conjuncts = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            final List<QueryPredicate> disjuncts = new ArrayList<>();
            for (int j = 0; j < 2; j++) {
                disjuncts.add(new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, i * 100 + j)));
            }
            conjuncts.add(or(disjuncts));
        }
        final QueryPredicate cnf = and(conjuncts);
        final BooleanPredicateNormalizer normalizer = BooleanPredicateNormalizer.getDefaultInstanceForDnf();
        assertThrows(ArithmeticException.class, () -> normalizer.getNormalizedSize(cnf));
        assertThrows(BooleanPredicateNormalizer.NormalFormTooLargeException.class, () -> normalizer.normalize(cnf));
        assertEquals(cnf, normalizer.normalizeIfPossible(cnf).orElse(cnf));
    }

    protected static void assertExpectedCnf(@Nonnull final QueryPredicate expected, @Nonnull final QueryPredicate given) {
        assertExpectedNormalization(BooleanPredicateNormalizer.getDefaultInstanceForCnf(), expected, given);
    }

    protected static void assertExpectedDnf(@Nonnull final QueryPredicate expected, @Nonnull final QueryPredicate given) {
        assertExpectedNormalization(BooleanPredicateNormalizer.getDefaultInstanceForDnf(), expected, given);
    }

    protected static void assertExpectedNormalization(@Nonnull final BooleanPredicateNormalizer normalizer,
                                                      @Nonnull final QueryPredicate expected, @Nonnull final QueryPredicate given) {
        final QueryPredicate normalized = normalizer.normalize(given).orElse(given);
        assertFilterEquals(expected, Objects.requireNonNull(normalized));
        assertEquals(normalized, normalizer.normalize(normalized).orElse(normalized), "Normalized form should be stable");
    }

    // Query components do not implement equals, but they have distinctive enough printed representations.
    protected static void assertFilterEquals(@Nonnull final QueryPredicate expected, @Nonnull final QueryPredicate actual) {
        assertEquals(expected.toString(), actual.toString());
    }

    private static int numberOfOrTerms(@Nonnull final QueryPredicate predicate) {
        if (predicate instanceof OrPredicate) {
            return ((OrPredicate)predicate).getChildren().size();
        } else {
            return 1;
        }
    }
}
