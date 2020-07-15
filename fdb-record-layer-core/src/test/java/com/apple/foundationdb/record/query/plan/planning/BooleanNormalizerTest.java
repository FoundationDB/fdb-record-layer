/*
 * BooleanNormalizerTest.java
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

import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.query.expressions.OrComponent;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link BooleanNormalizer}.
 */
public class BooleanNormalizerTest {

    static final QueryComponent P1 = Query.field("f").equalsValue(1);
    static final QueryComponent P2 = Query.field("f").equalsValue(2);
    static final QueryComponent P3 = Query.field("f").equalsValue(3);
    static final QueryComponent P4 = Query.field("f").equalsValue(4);
    static final QueryComponent P5 = Query.field("f").equalsValue(5);

    static final QueryComponent PNested = Query.field("p").matches(Query.field("c").equalsValue("x"));
    static final QueryComponent POneOf = Query.field("r").oneOfThem().equalsValue("x");
    static final QueryComponent PRank = Query.rank(Key.Expressions.field("score").groupBy(Key.Expressions.field("game"))).lessThan(100);

    @Test
    public void atomic() {
        assertExpectedNormalization(P1, P1);
        assertExpectedNormalization(Query.not(P1), Query.not(P1));

        assertExpectedNormalization(PNested, PNested);
        assertExpectedNormalization(POneOf, POneOf);
        assertExpectedNormalization(PRank, PRank);
    }
    
    @Test
    public void flatten() {
        assertExpectedNormalization(Query.and(P1, P2, P3),
                Query.and(Query.and(P1, P2), P3));
        assertExpectedNormalization(Query.and(P1, P2, P3),
                Query.and(P1, Query.and(P2, P3)));
        assertExpectedNormalization(Query.and(P1, P2, P3, P4, P5),
                Query.and(P1, Query.and(P2, Query.and(P3, Query.and(P4, P5)))));
        assertExpectedNormalization(Query.and(P1, P2, P3, P4, P5),
                Query.and(Query.and(Query.and(Query.and(P1, P2), P3), P4), P5));
        assertExpectedNormalization(Query.or(P1, P2, P3),
                Query.or(Query.or(P1, P2), P3));
        assertExpectedNormalization(Query.or(P1, P2, P3),
                Query.or(P1, Query.or(P2, P3)));
    }

    @Test
    public void distribute() {
        assertExpectedNormalization(Query.or(Query.and(P1, P2), Query.and(P3, P4)),
                Query.or(Query.and(P1, P2), Query.and(P3, P4)));
        assertExpectedNormalization(Query.or(Query.and(P1, P2), Query.and(P1, P3)),
                Query.and(P1, Query.or(P2, P3)));
        assertExpectedNormalization(Query.or(Query.and(P1, P3), Query.and(P2, P3), Query.and(P1, P4), Query.and(P2, P4)),
                Query.and(Query.or(P1, P2), Query.or(P3, P4)));
    }

    @Test
    public void deMorgan() {
        assertExpectedNormalization(Query.or(Query.not(P1), Query.not(P2)),
                Query.not(Query.and(P1, P2)));
        assertExpectedNormalization(Query.and(Query.not(P1), Query.not(P2)),
                Query.not(Query.or(P1, P2)));
    }

    @Test
    public void complex() {
        assertExpectedNormalization(Query.or(Query.and(P1, Query.not(P2)), Query.and(P1, Query.not(P3), Query.not(P4))),
                Query.and(P1, Query.not(Query.and(P2, Query.or(P3, P4)))));
    }

    @Test
    public void redundant() {
        final BooleanNormalizer eliminator = BooleanNormalizer.forConfiguration(
                RecordQueryPlannerConfiguration.builder().setCheckForDuplicateConditions(true).build());

        assertExpectedNormalization(Query.or(Query.and(P1, P2, P3), Query.and(P1, P2, P4, P3)),
                Query.and(P1, Query.or(Query.and(P2, P3), Query.and(P2, P4, P3))));

        assertExpectedNormalization(eliminator, Query.and(P1, P2, P3),
                Query.and(P1, Query.or(Query.and(P2, P3), Query.and(P2, P4, P3))));
    }

    @Test
    public void cnf() {
        List<QueryComponent> conjuncts = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            List<QueryComponent> disjuncts = new ArrayList<>();
            for (int j = 0; j < 5; j++) {
                disjuncts.add(Query.field("f").equalsValue(i * 4 + j));
            }
            conjuncts.add(Query.or(disjuncts));
        }
        final QueryComponent cnf = Query.and(conjuncts);

        final BooleanNormalizer normalizer = BooleanNormalizer.getDefaultInstance();
        assertNotEquals(cnf, normalizer.normalize(cnf));
        assertEquals(numberOfTerms(normalizer.normalize(cnf)), normalizer.getNormalizedSize(cnf));

        final BooleanNormalizer lowLimitNormalizer = BooleanNormalizer.withLimit(2);
        assertThrows(BooleanNormalizer.DNFTooLargeException.class, () -> lowLimitNormalizer.normalize(cnf));
        assertEquals(cnf, lowLimitNormalizer.normalizeIfPossible(cnf));
    }

    @Test
    public void bigNonCnf() {
        final QueryComponent cnf = Query.and(
                IntStream.rangeClosed(1, 9).boxed().map(i ->
                        Query.or(IntStream.rangeClosed(1, 9).boxed()
                                .map(j -> Query.and(
                                        Query.field("num_value_3_indexed").equalsValue(i * 9 + j),
                                        Query.field("str_value_indexed").equalsValue("foo")))
                                .collect(Collectors.toList())))
                        .collect(Collectors.toList()));
        final BooleanNormalizer plannerNormalizer = BooleanNormalizer.forConfiguration(RecordQueryPlannerConfiguration.builder().build());
        assertThrows(BooleanNormalizer.DNFTooLargeException.class, () -> plannerNormalizer.normalize(cnf));
        assertEquals(cnf, plannerNormalizer.normalizeIfPossible(cnf));
    }

    @Test
    public void bigCnfThatWouldOverflow() {
        // A CNF who's DNF size doesn't fit in an int.
        List<QueryComponent> conjuncts = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            List<QueryComponent> disjuncts = new ArrayList<>();
            for (int j = 0; j < 2; j++) {
                disjuncts.add(Query.field("f").equalsValue(i * 100 + j));
            }
            conjuncts.add(Query.or(disjuncts));
        }
        final QueryComponent cnf = Query.and(conjuncts);
        final BooleanNormalizer normalizer = BooleanNormalizer.getDefaultInstance();
        assertThrows(ArithmeticException.class, () -> normalizer.getNormalizedSize(cnf));
        assertThrows(BooleanNormalizer.DNFTooLargeException.class, () -> normalizer.normalize(cnf));
        assertEquals(cnf, normalizer.normalizeIfPossible(cnf));
    }

    protected static void assertExpectedNormalization(@Nonnull final QueryComponent expected, @Nonnull final QueryComponent given) {
        assertExpectedNormalization(BooleanNormalizer.getDefaultInstance(), expected, given);
    }

    protected static void assertExpectedNormalization(@Nonnull final BooleanNormalizer normalizer,
                                                      @Nonnull final QueryComponent expected, @Nonnull final QueryComponent given) {
        final QueryComponent normalized = normalizer.normalize(given);
        assertFilterEquals(expected, normalized);
        if (!normalizer.isCheckForDuplicateConditions()) {
            assertEquals(numberOfTerms(expected), normalizer.getNormalizedSize(given));
        }
    }

    // Query components do not implement equals, but they have distinctive enough printed representations.
    protected static void assertFilterEquals(@Nonnull final QueryComponent expected, @Nonnull final QueryComponent actual) {
        assertEquals(expected.toString(), actual.toString());
    }

    private static int numberOfTerms(@Nonnull final QueryComponent predicate) {
        if (predicate instanceof OrComponent) {
            return ((OrComponent)predicate).getChildren().size();
        } else {
            return 1;
        }
    }
}
