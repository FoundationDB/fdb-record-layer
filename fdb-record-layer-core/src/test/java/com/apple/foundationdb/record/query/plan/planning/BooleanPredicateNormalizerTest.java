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
import com.apple.foundationdb.record.query.predicates.FieldValue;
import com.apple.foundationdb.record.query.predicates.OrPredicate;
import com.apple.foundationdb.record.query.predicates.QuantifiedColumnValue;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.predicates.ValuePredicate;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.record.query.predicates.AndPredicate.and;
import static com.apple.foundationdb.record.query.predicates.NotPredicate.not;
import static com.apple.foundationdb.record.query.predicates.OrPredicate.or;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link BooleanPredicateNormalizer}.
 */
class BooleanPredicateNormalizerTest {
    private static final FieldValue F = new FieldValue(QuantifiedColumnValue.of(CorrelationIdentifier.UNGROUNDED, 0), ImmutableList.of("f"));
    private static final QueryPredicate P1 = new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 1));
    private static final QueryPredicate P2 = new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 2));
    private static final QueryPredicate P3 = new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 3));
    private static final QueryPredicate P4 = new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 4));
    private static final QueryPredicate P5 = new ValuePredicate(F, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 5));

    @Test
    void atomic() {
        assertExpectedNormalization(P1, P1);
        assertExpectedNormalization(not(P1), not(P1));
    }
    
    @Test
    void flatten() {
        assertExpectedNormalization(and(P1, P2, P3),
                and(and(P1, P2), P3));
        assertExpectedNormalization(and(P1, P2, P3),
                and(P1, and(P2, P3)));
        assertExpectedNormalization(and(P1, P2, P3, P4, P5),
                and(P1, and(P2, and(P3, and(P4, P5)))));
        assertExpectedNormalization(and(P1, P2, P3, P4, P5),
                and(and(and(and(P1, P2), P3), P4), P5));
        assertExpectedNormalization(or(P1, P2, P3),
                or(or(P1, P2), P3));
        assertExpectedNormalization(or(P1, P2, P3),
                or(P1, or(P2, P3)));
    }

    @Test
    void distribute() {
        assertExpectedNormalization(or(and(P1, P2), and(P3, P4)),
                or(and(P1, P2), and(P3, P4)));
        assertExpectedNormalization(or(and(P1, P2), and(P1, P3)),
                and(P1, or(P2, P3)));
        assertExpectedNormalization(or(and(P1, P3), and(P2, P3), and(P1, P4), and(P2, P4)),
                and(or(P1, P2), or(P3, P4)));
    }

    @Test
    void deMorgan() {
        assertExpectedNormalization(or(not(P1), not(P2)),
                not(and(P1, P2)));
        assertExpectedNormalization(and(not(P1), not(P2)),
                not(or(P1, P2)));
    }

    @Test
    void complex() {
        assertExpectedNormalization(or(and(P1, not(P2)), and(P1, not(P3), not(P4))),
                and(P1, not(and(P2, or(P3, P4)))));
    }

    @Test
    void redundant() {
        final BooleanPredicateNormalizer eliminator = BooleanPredicateNormalizer.forConfiguration(
                RecordQueryPlannerConfiguration.builder().setCheckForDuplicateConditions(true).build());

        assertExpectedNormalization(or(and(P1, P2, P3), and(P1, P2, P4, P3)),
                and(P1, or(and(P2, P3), and(P2, P4, P3))));

        assertExpectedNormalization(eliminator, and(P1, P2, P3),
                and(P1, or(and(P2, P3), and(P2, P4, P3))));
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

        final BooleanPredicateNormalizer normalizer = BooleanPredicateNormalizer.getDefaultInstance();
        assertNotEquals(cnf, normalizer.normalize(cnf));
        assertEquals(numberOfTerms(Objects.requireNonNull(normalizer.normalize(cnf))), normalizer.getNormalizedSize(cnf));

        final BooleanPredicateNormalizer lowLimitNormalizer = BooleanPredicateNormalizer.withLimit(2);
        assertThrows(BooleanPredicateNormalizer.DNFTooLargeException.class, () -> lowLimitNormalizer.normalize(cnf));
        assertEquals(cnf, lowLimitNormalizer.normalizeIfPossible(cnf));
    }

    @Test
    void bigNonCnf() {
        final QueryPredicate cnf = and(
                IntStream.rangeClosed(1, 9).boxed().map(i ->
                        or(IntStream.rangeClosed(1, 9).boxed()
                                .map(j -> and(
                                        new ValuePredicate(new FieldValue(QuantifiedColumnValue.of(CorrelationIdentifier.UNGROUNDED, 0), ImmutableList.of("num_value_3_indexed")),
                                                new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, i * 9 + j)),
                                        new ValuePredicate(new FieldValue(QuantifiedColumnValue.of(CorrelationIdentifier.UNGROUNDED, 0), ImmutableList.of("str_value_indexed")),
                                                new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "foo"))))
                                .collect(Collectors.toList())))
                        .collect(Collectors.toList()));
        final BooleanPredicateNormalizer plannerNormalizer = BooleanPredicateNormalizer.forConfiguration(RecordQueryPlannerConfiguration.builder().build());
        assertThrows(BooleanPredicateNormalizer.DNFTooLargeException.class, () -> plannerNormalizer.normalize(cnf));
        assertEquals(cnf, plannerNormalizer.normalizeIfPossible(cnf));
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
        final BooleanPredicateNormalizer normalizer = BooleanPredicateNormalizer.getDefaultInstance();
        assertThrows(ArithmeticException.class, () -> normalizer.getNormalizedSize(cnf));
        assertThrows(BooleanPredicateNormalizer.DNFTooLargeException.class, () -> normalizer.normalize(cnf));
        assertEquals(cnf, normalizer.normalizeIfPossible(cnf));
    }

    protected static void assertExpectedNormalization(@Nonnull final QueryPredicate expected, @Nonnull final QueryPredicate given) {
        assertExpectedNormalization(BooleanPredicateNormalizer.getDefaultInstance(), expected, given);
    }

    protected static void assertExpectedNormalization(@Nonnull final BooleanPredicateNormalizer normalizer,
                                                      @Nonnull final QueryPredicate expected, @Nonnull final QueryPredicate given) {
        final QueryPredicate normalized = normalizer.normalize(given);
        assertFilterEquals(expected, Objects.requireNonNull(normalized));
        if (!normalizer.isCheckForDuplicateConditions()) {
            assertEquals(numberOfTerms(expected), normalizer.getNormalizedSize(given));
        }

        assertEquals(normalized, normalizer.normalize(normalized), "Normalized form should be stable");
    }

    // Query components do not implement equals, but they have distinctive enough printed representations.
    protected static void assertFilterEquals(@Nonnull final QueryPredicate expected, @Nonnull final QueryPredicate actual) {
        assertEquals(expected.toString(), actual.toString());
    }

    private static int numberOfTerms(@Nonnull final QueryPredicate predicate) {
        if (predicate instanceof OrPredicate) {
            return ((OrPredicate)predicate).getChildren().size();
        } else {
            return 1;
        }
    }
}
