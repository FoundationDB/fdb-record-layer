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
import com.apple.foundationdb.record.query.expressions.AndComponent;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.NestedField;
import com.apple.foundationdb.record.query.expressions.OrComponent;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link BooleanNormalizer}.
 */
public class BooleanNormalizerTest {
    @Nonnull
    private static final BooleanNormalizer REDUNDANCY_ELIMINATOR = BooleanNormalizer.forConfiguration(
            RecordQueryPlannerConfiguration.builder().setCheckForDuplicateConditions(true).build());

    static final QueryComponent P1 = Query.field("f").equalsValue(1);
    static final QueryComponent P2 = Query.field("f").equalsValue(2);
    static final QueryComponent P3 = Query.field("f").equalsValue(3);
    static final QueryComponent P4 = Query.field("f").equalsValue(4);
    static final QueryComponent P5 = Query.field("f").equalsValue(5);

    static final QueryComponent PNested = Query.field("p").matches(Query.field("c").equalsValue("x"));
    static final QueryComponent POneOf = Query.field("r").oneOfThem().equalsValue("x");
    static final QueryComponent PRank = Query.rank(Key.Expressions.field("score").groupBy(Key.Expressions.field("game"))).lessThan(100);

    @Nonnull
    private static QueryComponent nest(@Nonnull QueryComponent p) {
        return nest("p", p);
    }

    @Nonnull
    private static QueryComponent nest(@Nonnull String parent, @Nonnull QueryComponent p) {
        return Query.field(parent).matches(p);
    }

    @SuppressWarnings("unused") // argument source for parameterized test
    static Stream<QueryComponent> atomic() {
        return Stream.of(
                P1,
                Query.not(P1),
                nest(P1),
                Query.not(nest(P2)),
                Query.and(P1, P2, P3),
                Query.or(P1, P2, P3),
                PNested,
                POneOf,
                PRank
        );
    }

    @ParameterizedTest(name = "atomic[{0}]")
    @MethodSource
    void atomic(QueryComponent starting) {
        assertExpectedNormalization(starting, starting);
    }

    @SuppressWarnings("unused") // argument source for parameterized test
    static Stream<Arguments> flatten() {
        return Stream.of(
                Arguments.of(Query.and(P1, P2, P3),
                        Query.and(Query.and(P1, P2), P3)),
                Arguments.of(Query.and(P1, P2, P3),
                        Query.and(P1, Query.and(P2, P3))),
                Arguments.of(Query.and(P1, P2, P3, P4, P5),
                        Query.and(P1, Query.and(P2, Query.and(P3, Query.and(P4, P5))))),
                Arguments.of(Query.and(P1, P2, P3, P4, P5),
                        Query.and(Query.and(Query.and(Query.and(P1, P2), P3), P4), P5)),
                Arguments.of(Query.or(P1, P2, P3),
                        Query.or(Query.or(P1, P2), P3)),
                Arguments.of(Query.or(P1, P2, P3),
                        Query.or(P1, Query.or(P2, P3)))
        );
    }
    
    @ParameterizedTest(name = "flatten[{1}]")
    @MethodSource
    void flatten(QueryComponent expected, QueryComponent starting) {
        assertExpectedNormalization(expected, starting);
    }

    @SuppressWarnings("unused") // argument source for parameterized test
    static Stream<Arguments> distribute() {
        return Stream.of(
            Arguments.of(Query.or(Query.and(P1, P2), Query.and(P3, P4)),
                    Query.or(Query.and(P1, P2), Query.and(P3, P4))),
            Arguments.of(Query.or(Query.and(P1, P2), Query.and(P1, P3)),
                    Query.and(P1, Query.or(P2, P3))),
            Arguments.of(Query.or(Query.and(P1, P3), Query.and(P2, P3), Query.and(P1, P4), Query.and(P2, P4)),
                    Query.and(Query.or(P1, P2), Query.or(P3, P4)))
        );
    }

    @ParameterizedTest(name = "distribute[{1}]")
    @MethodSource
    void distribute(QueryComponent expected, QueryComponent starting) {
        assertExpectedNormalization(expected, starting);
    }

    @SuppressWarnings("unused") // argument source for parameterized test
    static Stream<Arguments> deMorgan() {
        return Stream.of(
            Arguments.of(Query.or(Query.not(P1), Query.not(P2)),
                    Query.not(Query.and(P1, P2))),
            Arguments.of(Query.and(Query.not(P1), Query.not(P2)),
                    Query.not(Query.or(P1, P2)))
        );
    }

    @ParameterizedTest(name = "deMorgan[{1}]")
    @MethodSource
    void deMorgan(QueryComponent expected, QueryComponent starting) {
        assertExpectedNormalization(expected, starting);
    }

    @Test
    void complex() {
        assertExpectedNormalization(Query.or(Query.and(P1, Query.not(P2)), Query.and(P1, Query.not(P3), Query.not(P4))),
                Query.and(P1, Query.not(Query.and(P2, Query.or(P3, P4)))));
    }

    @Test
    void redundant() {
        assertExpectedNormalization(Query.or(Query.and(P1, P2, P3), Query.and(P1, P2, P4, P3)),
                Query.and(P1, Query.or(Query.and(P2, P3), Query.and(P2, P4, P3))));

        assertExpectedNormalization(REDUNDANCY_ELIMINATOR, Query.and(P1, P2, P3),
                Query.and(P1, Query.or(Query.and(P2, P3), Query.and(P2, P4, P3))));
    }

    @SuppressWarnings("unused") // arguments source for parameterized test
    static Stream<Arguments> unnest() {
        return Stream.of(
                Arguments.of(
                        Query.not(nest("a", nest("b", nest("c", P1)))),
                        nest("a", nest("b", Query.not(nest("c", P1))))
                ),
                Arguments.of(
                        Query.and(nest(P1), nest(P2), nest(P3)),
                        nest(Query.and(P1, P2, P3))
                ),
                Arguments.of(
                        Query.or(nest(P1), nest(P2), nest(P3)),
                        nest(Query.or(P1, P2, P3))
                ),
                Arguments.of(
                        Query.and(P1, nest(P2), nest(P3)),
                        Query.and(P1, nest(Query.and(P2, P3)))
                ),
                Arguments.of(
                        Query.or(Query.and(P1, nest(P2)), Query.and(P1, nest(P3))),
                        Query.and(P1, nest(Query.or(P2, P3)))
                ),
                Arguments.of(
                        Query.or(Query.and(P1, Query.not(nest(P2))), Query.and(P1, Query.not(nest(P3)))),
                        Query.and(P1, Query.not(nest(Query.and(P2, P3))))
                ),
                Arguments.of(
                        Query.or(Query.and(P1, Query.not(nest(P2))), Query.and(P1, Query.not(nest(P3)))),
                        Query.and(P1, nest(Query.not(Query.and(P2, P3))))
                ),
                Arguments.of(
                        nest("a", nest("b", nest("c", nest("d", P1)))),
                        Query.not(nest("a", Query.not(nest("b", Query.not(nest("c", Query.not(nest("d", P1))))))))
                ),
                Arguments.of(
                        Query.not(nest("a", nest("b", nest("c", nest("d", nest("e", P1)))))),
                        Query.not(nest("a", Query.not(nest("b", Query.not(nest("c", Query.not(nest("d", Query.not(nest("e", P1))))))))))
                ),
                Arguments.of(
                        Query.or(
                                Query.and(Query.not(nest("a", nest("b", P1))), nest("a", nest("b", P2))),
                                Query.and(nest("a", nest("c", P3)), Query.not(nest("a", nest("c", nest("d", P4)))))
                        ),
                        nest("a", Query.or(nest("b", Query.and(Query.not(P1), P2)), Query.not(nest("c", Query.or(Query.not(P3), nest("d", P4))))))
                )
        );
    }

    @ParameterizedTest(name = "unnest[{1}]")
    @MethodSource
    void unnest(QueryComponent expected, QueryComponent starting) {
        assertExpectedNormalization(expected, starting);
    }

    /**
     * Make sure that if we have the same predicates on different parent fields that those predicates
     * don't incorrectly get marked as duplicates.
     */
    @Test
    void parentsTakenIntoAccountForRedundancyCheck() {
        // Validate the redundancy behavior on flat predicates
        assertExpectedNormalization(REDUNDANCY_ELIMINATOR,
                Query.and(P1, P2, P3),
                Query.or(Query.and(P1, P2, P3), Query.and(P1, P2, P3, P4)));
        // Add predicates with different parents. Here, the parent differences should result in all predicates sticking around
        assertExpectedNormalization(REDUNDANCY_ELIMINATOR,
                Query.or(Query.and(nest("a", P1), nest("a", P2), nest("a", P3)), Query.and(nest("b", P1), nest("b", P2), nest("b", P3), nest("b", P4))),
                Query.or(nest("a", Query.and(P1, P2, P3)), nest("b", Query.and(P1, P2, P3, P4))));
        // Nest both of the original predicates under the same parent. Here, the redundancy check should be performed like with the original case
        assertExpectedNormalization(REDUNDANCY_ELIMINATOR,
                Query.and(nest(P1), nest(P2), nest(P3)),
                Query.or(nest(Query.and(P1, P2, P3)), nest(Query.and(P1, P2, P3, P4))));
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
        final QueryComponent dnf = assertNormalizeCnf(cnf, 5, 5);

        // Assert that the DNF is an OR of ANDs of FieldWithComparisons
        assertThat(dnf, Matchers.instanceOf(OrComponent.class));
        for (QueryComponent level0 : ((OrComponent)dnf).getChildren()) {
            assertThat(level0, Matchers.instanceOf(AndComponent.class));
            for (QueryComponent level1 : ((AndComponent)level0).getChildren()) {
                assertThat(level1, Matchers.instanceOf(FieldWithComparison.class));
            }
        }
    }

    @Test
    public void cnfWithNesteds() {
        int parent = 0;
        List<QueryComponent> conjuncts = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            List<QueryComponent> disjuncts = new ArrayList<>();
            for (int j = 0; j < 3; j++) {
                disjuncts.add(nest("p" + parent, Query.field("f" + parent).equalsValue(0)));
                parent++;
            }
            conjuncts.add(nest("p" + parent, Query.or(disjuncts)));
            parent++;
        }
        final QueryComponent cnf = nest("p" + parent, Query.and(conjuncts));
        final QueryComponent dnf = assertNormalizeCnf(cnf, 4, 3);

        // Assert that the DNF is an OR of ANDS of thrice nested field comparisons
        assertThat(dnf, Matchers.instanceOf(OrComponent.class));
        for (QueryComponent level0 : ((OrComponent)dnf).getChildren()) {
            assertThat(level0, Matchers.instanceOf(AndComponent.class));
            for (QueryComponent level1 : ((AndComponent)level0).getChildren()) {
                assertThat(level1, Matchers.instanceOf(NestedField.class));
                QueryComponent level2 = ((NestedField)level1).getChild();
                assertThat(level2, Matchers.instanceOf(NestedField.class));
                QueryComponent level3 = ((NestedField)level2).getChild();
                assertThat(level3, Matchers.instanceOf(NestedField.class));
                QueryComponent level4 = ((NestedField)level3).getChild();
                assertThat(level4, Matchers.instanceOf(FieldWithComparison.class));
            }
        }
    }

    private static QueryComponent assertNormalizeCnf(QueryComponent cnf, int conjuncts, int disjuncts) {
        final BooleanNormalizer normalizer = BooleanNormalizer.getDefaultInstance();
        final QueryComponent normalized = normalizer.normalize(cnf);
        assertNotNull(normalized);
        assertNotEquals(cnf, normalized);
        assertEquals((int)Math.pow(disjuncts, conjuncts), normalizer.getNormalizedSize(cnf));
        assertEquals(numberOfTerms(normalized), normalizer.getNormalizedSize(cnf));

        final BooleanNormalizer lowLimitNormalizer = BooleanNormalizer.withLimit(2);
        assertThrows(BooleanNormalizer.DNFTooLargeException.class, () -> lowLimitNormalizer.normalize(cnf));
        assertEquals(cnf, lowLimitNormalizer.normalizeIfPossible(cnf));

        return normalized;
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

        assertEquals(normalized, normalizer.normalize(normalized), "Normalized form should be stable");
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
