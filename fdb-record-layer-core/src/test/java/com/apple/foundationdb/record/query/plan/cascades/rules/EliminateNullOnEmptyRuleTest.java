/*
 * EliminateNullOnEmptyRuleTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.debug.DebuggerWithSymbolTables;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.column;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.exists;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fieldPredicate;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.forEachWithNullOnEmpty;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.selectWithPredicates;
import static com.apple.foundationdb.record.query.plan.cascades.Reference.initialOf;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.EQUALS_42;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.GREATER_THAN_HELLO;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.baseT;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.baseTau;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.join;

/**
 * Tests for the {@link EliminateNullOnEmptyRule}.
 */
class EliminateNullOnEmptyRuleTest {
    @Nonnull
    private static final RuleTestHelper testHelper = new RuleTestHelper(new EliminateNullOnEmptyRule(), PlannerPhase.PLANNING);

    @BeforeEach
    void setUp() {
        Debugger.setDebugger(DebuggerWithSymbolTables.withSanityChecks());
        Debugger.setup();
    }

    /**
     * Tests the rule against the following QGM:
     * <pre>{@code
     *   Select [q.a = 42]
     *     └── q: ƒ (nullOnEmpty)
     *          └── Select [b > 'hello']
     *                └── ƒ ── BaseScan(T)
     * }</pre>
     * <p>The predicate {@code q.a = 42} rejects the null tuple injected by the null-on-empty quantifier, so the rule
     * replaces {@code q} with a normal {@code ForEach}.
     */
    @Test
    void eliminatesSingleNullOnEmpty() {
        final Quantifier baseQun = baseT();
        final SelectExpression innerSelect = selectWithPredicates(baseQun,
                ImmutableList.of("a", "b", "c"),
                fieldPredicate(baseQun, "b", GREATER_THAN_HELLO));
        final Quantifier noeQun = forEachWithNullOnEmpty(innerSelect);
        final SelectExpression outer = selectWithPredicates(noeQun,
                ImmutableList.of("b", "c"),
                fieldPredicate(noeQun, "a", EQUALS_42));

        // Build the expected rewrite: A single select with a normal ƒ over the same inner reference, with the
        // alias and result value of the original outer select preserved.
        final Quantifier baseQun2 = baseT();
        final SelectExpression innerSelect2 = selectWithPredicates(baseQun2,
                ImmutableList.of("a", "b", "c"),
                fieldPredicate(baseQun2, "b", GREATER_THAN_HELLO));
        final Quantifier.ForEach expectedQun = Quantifier.forEachBuilder()
                .withAlias(noeQun.getAlias())
                .build(initialOf(innerSelect2));
        final SelectExpression expected = GraphExpansion.builder()
                .addQuantifier(expectedQun)
                .addPredicate(fieldPredicate(expectedQun, "a", EQUALS_42))
                .build()
                .buildSelectWithResultValue(outer.getResultValue());

        testHelper.assertYields(outer, expected);
    }

    /**
     * Tests that the rule eliminates only the null-on-empty quantifier whose null tuple is rejected. Initial QGM:
     * <pre>{@code
     *   Select [q1.a = 42]
     *     ├── q1: ƒ (nullOnEmpty)
     *     │     └── Select [b > 'hello'] → (a, b, c)
     *     │           └── ƒ ── BaseScan(T)
     *     ├── q2: ƒ (nullOnEmpty)
     *     │     └── Select [b > 'hello'] → (a, b, c)
     *     │           └── ƒ ── BaseScan(T)
     *     └── q3: ƒ
     *           └── BaseScan(T)
     * }</pre>
     * <p>The predicate references {@code q1.a} only; so the rule fires once with {@code q1} bound and replaces it with
     * a normal {@code ForEach}, while leaving {@code q2} and {@code q3} unchanged.
     */
    @Test
    void eliminatesTheRelevantNullOnEmptyQuantifier() {
        final Quantifier baseQunQ1 = baseT();
        final SelectExpression innerSelectQ1 = selectWithPredicates(baseQunQ1,
                ImmutableList.of("a", "b", "c"),
                fieldPredicate(baseQunQ1, "b", GREATER_THAN_HELLO));
        final Quantifier q1 = forEachWithNullOnEmpty(innerSelectQ1);
        final Quantifier baseQunQ2 = baseT();
        final SelectExpression innerSelectQ2 = selectWithPredicates(baseQunQ2,
                ImmutableList.of("a", "b", "c"),
                fieldPredicate(baseQunQ2, "b", GREATER_THAN_HELLO));
        final Quantifier q2 = forEachWithNullOnEmpty(innerSelectQ2);
        final Quantifier q3 = baseT();
        final SelectExpression outer = join(q1, q2, q3)
                .addResultColumn(column(q1, "b", "b"))
                .addPredicate(fieldPredicate(q1, "a", EQUALS_42))
                .build()
                .buildSelect();

        // Build the expected rewrite.
        final Quantifier baseQunQ1Expected = baseT();
        final SelectExpression innerSelectQ1Expected = selectWithPredicates(baseQunQ1Expected,
                ImmutableList.of("a", "b", "c"),
                fieldPredicate(baseQunQ1Expected, "b", GREATER_THAN_HELLO));
        final Quantifier.ForEach q1Expected = Quantifier.forEachBuilder()
                .withAlias(q1.getAlias())
                .build(initialOf(innerSelectQ1Expected));
        final Quantifier baseQunQ2Expected = baseT();
        final SelectExpression innerSelectQ2Expected = selectWithPredicates(baseQunQ2Expected,
                ImmutableList.of("a", "b", "c"),
                fieldPredicate(baseQunQ2Expected, "b", GREATER_THAN_HELLO));
        final Quantifier q2Expected = forEachWithNullOnEmpty(innerSelectQ2Expected);
        final Quantifier q3Expected = baseT();
        final SelectExpression expected = join(q1Expected, q2Expected, q3Expected)
                .addResultColumn(column(q1Expected, "b", "b"))
                .addPredicate(fieldPredicate(q1Expected, "a", EQUALS_42))
                .build()
                .buildSelect();

        testHelper.assertYields(outer, expected);
    }

    /**
     * Tests that the rule eliminates all eligible null-on-empty quantifiers in a single rule application. Initial QGM:
     * <pre>{@code
     *   Select [q1.a = 42 AND q2.a = 42]
     *     ├── q1: ƒ (nullOnEmpty)
     *     │     └── Select [b > 'hello'] → (a, b, c)
     *     │           └── ƒ ── BaseScan(T)
     *     └── q2: ƒ (nullOnEmpty)
     *           └── Select [b > 'hello'] → (a, b, c)
     *                 └── ƒ ── BaseScan(T)
     * }</pre>
     * <p>Both predicates reject the null tuple at their respective alias, so the rule fires once and replaces both
     * {@code q1} and {@code q2} with normal {@code ForEach} quantifiers in a single yielded expression.
     */
    @Test
    void eliminatesAllEligibleNullOnEmptyQuantifiers() {
        final Quantifier baseQunQ1 = baseT();
        final SelectExpression innerSelectQ1 = selectWithPredicates(baseQunQ1,
                ImmutableList.of("a", "b", "c"),
                fieldPredicate(baseQunQ1, "b", GREATER_THAN_HELLO));
        final Quantifier q1 = forEachWithNullOnEmpty(innerSelectQ1);
        final Quantifier baseQunQ2 = baseT();
        final SelectExpression innerSelectQ2 = selectWithPredicates(baseQunQ2,
                ImmutableList.of("a", "b", "c"),
                fieldPredicate(baseQunQ2, "b", GREATER_THAN_HELLO));
        final Quantifier q2 = forEachWithNullOnEmpty(innerSelectQ2);
        final SelectExpression outer = join(q1, q2)
                .addResultColumn(column(q1, "b", "b"))
                .addPredicate(fieldPredicate(q1, "a", EQUALS_42))
                .addPredicate(fieldPredicate(q2, "a", EQUALS_42))
                .build()
                .buildSelect();

        // Build the expected rewrite: both null-on-empty quantifiers replaced in a single application.
        final Quantifier baseQunQ1Expected = baseT();
        final SelectExpression innerSelectQ1Expected = selectWithPredicates(baseQunQ1Expected,
                ImmutableList.of("a", "b", "c"),
                fieldPredicate(baseQunQ1Expected, "b", GREATER_THAN_HELLO));
        final Quantifier.ForEach q1Expected = Quantifier.forEachBuilder()
                .withAlias(q1.getAlias())
                .build(initialOf(innerSelectQ1Expected));
        final Quantifier baseQunQ2Expected = baseT();
        final SelectExpression innerSelectQ2Expected = selectWithPredicates(baseQunQ2Expected,
                ImmutableList.of("a", "b", "c"),
                fieldPredicate(baseQunQ2Expected, "b", GREATER_THAN_HELLO));
        final Quantifier.ForEach q2Expected = Quantifier.forEachBuilder()
                .withAlias(q2.getAlias())
                .build(initialOf(innerSelectQ2Expected));
        final SelectExpression expected = join(q1Expected, q2Expected)
                .addResultColumn(column(q1Expected, "b", "b"))
                .addPredicate(fieldPredicate(q1Expected, "a", EQUALS_42))
                .addPredicate(fieldPredicate(q2Expected, "a", EQUALS_42))
                .build()
                .buildSelect();

        testHelper.assertYields(outer, expected);
    }

    /**
     * Tests that the rule does not fire when the predicate, here {@code q.a IS NULL}, accepts the null tuple.
     */
    @Test
    void doesNotFireWhenPredicateAcceptsNull() {
        final Quantifier baseQun = baseT();
        final SelectExpression innerSelect = selectWithPredicates(baseQun,
                ImmutableList.of("a", "b", "c"),
                fieldPredicate(baseQun, "b", GREATER_THAN_HELLO));
        final Quantifier noeQun = forEachWithNullOnEmpty(innerSelect);
        final SelectExpression outer = selectWithPredicates(noeQun,
                ImmutableList.of("b", "c"),
                fieldPredicate(noeQun, "a", new Comparisons.NullComparison(Comparisons.Type.IS_NULL)));

        testHelper.assertYieldsNothing(outer, true);
    }

    /**
     * Tests that the rule does not fire when an {@code OR} predicate mixes a null-rejecting and a null-accepting
     * disjunct. The {@code q.a IS NULL} disjunct accepts the null tuple, so the overall {@code OR} cannot reject null
     * and the null-on-empty quantifier must stay in place.
     */
    @Test
    void doesNotFireWhenOrMixesAcceptingDisjunct() {
        final Quantifier baseQun = baseT();
        final SelectExpression innerSelect = selectWithPredicates(baseQun,
                ImmutableList.of("a", "b", "c"),
                fieldPredicate(baseQun, "b", GREATER_THAN_HELLO));
        final Quantifier noeQun = forEachWithNullOnEmpty(innerSelect);
        final QueryPredicate orPredicate = OrPredicate.or(
                fieldPredicate(noeQun, "a", EQUALS_42),
                fieldPredicate(noeQun, "a", new Comparisons.NullComparison(Comparisons.Type.IS_NULL)));
        final SelectExpression outer = selectWithPredicates(noeQun,
                ImmutableList.of("b", "c"),
                orPredicate);

        testHelper.assertYieldsNothing(outer, true);
    }

    /**
     * Tests that the rule fires for an {@code AND} predicate that mixes correlations to a null-on-empty quantifier and
     * a sibling. Initial QGM:
     * <pre>{@code
     *   Select [q.a = 42 AND r.b > 'hello']
     *     ├── q: ƒ (nullOnEmpty)
     *     │     └── Select [b > 'hello'] → (a, b, c)
     *     │           └── ƒ ── BaseScan(T)
     *     └── r: ƒ ── BaseScan(T)
     * }</pre>
     * <p>{@link SelectExpression} flattens the non-atomic top-level {@code AND} into separate conjuncts at
     * construction, so the rule sees {@code q.a = 42} on its own and, since it provably rejects null, eliminates the
     * null-on-empty.
     */
    @Test
    void eliminatesWhenFlattenedAndMixesCorrelations() {
        final Quantifier baseQun = baseT();
        final SelectExpression innerSelect = selectWithPredicates(baseQun,
                ImmutableList.of("a", "b", "c"),
                fieldPredicate(baseQun, "b", GREATER_THAN_HELLO));
        final Quantifier qNoeQun = forEachWithNullOnEmpty(innerSelect);
        final Quantifier rQun = baseT();
        final SelectExpression outer = join(qNoeQun, rQun)
                .addResultColumn(column(qNoeQun, "b", "b"))
                .addPredicate(AndPredicate.and(
                        fieldPredicate(qNoeQun, "a", EQUALS_42),
                        fieldPredicate(rQun, "b", GREATER_THAN_HELLO)))
                .build()
                .buildSelect();

        // Build the expected rewrite: `q` is replaced with a normal ƒ over the same inner reference.
        final Quantifier baseQunExpected = baseT();
        final SelectExpression innerSelectExpected = selectWithPredicates(baseQunExpected,
                ImmutableList.of("a", "b", "c"),
                fieldPredicate(baseQunExpected, "b", GREATER_THAN_HELLO));
        final Quantifier.ForEach qExpected = Quantifier.forEachBuilder()
                .withAlias(qNoeQun.getAlias())
                .build(initialOf(innerSelectExpected));
        final Quantifier rExpected = baseT();
        final SelectExpression expected = join(qExpected, rExpected)
                .addResultColumn(column(qExpected, "b", "b"))
                .addPredicate(AndPredicate.and(
                        fieldPredicate(qExpected, "a", EQUALS_42),
                        fieldPredicate(rExpected, "b", GREATER_THAN_HELLO)))
                .build()
                .buildSelect();

        testHelper.assertYields(outer, expected);
    }

    /**
     * Tests that the rule eliminates a null-on-empty {@code ForEach} sibling and leaves an existential sibling
     * untouched. Initial QGM:
     * <pre>{@code
     *   Select [q.a = 42]
     *     ├── q: ƒ (nullOnEmpty)
     *     │     └── Select [b > 'hello'] → (a, b, c)
     *     │           └── ƒ ── BaseScan(T)
     *     └── e: ∃
     *           └── Select → (a) ── ƒ ── BaseScan(TAU)
     * }</pre>
     */
    @Test
    void leavesExistentialSiblingUntouched() {
        final Quantifier baseQun = baseT();
        final SelectExpression innerSelect = selectWithPredicates(baseQun,
                ImmutableList.of("a", "b", "c"),
                fieldPredicate(baseQun, "b", GREATER_THAN_HELLO));
        final Quantifier qNoeQun = forEachWithNullOnEmpty(innerSelect);
        final Quantifier eQun = exists(selectWithPredicates(baseTau(), ImmutableList.of("alpha")));
        final SelectExpression outer = join(qNoeQun, eQun)
                .addResultColumn(column(qNoeQun, "b", "b"))
                .addPredicate(fieldPredicate(qNoeQun, "a", EQUALS_42))
                .build()
                .buildSelect();

        // Build the expected rewrite: `q` is replaced with a normal ƒ over the same inner reference; the existential
        // `e` is reconstructed in parallel so that semantic comparison succeeds.
        final Quantifier baseQunExpected = baseT();
        final SelectExpression innerSelectExpected = selectWithPredicates(baseQunExpected,
                ImmutableList.of("a", "b", "c"),
                fieldPredicate(baseQunExpected, "b", GREATER_THAN_HELLO));
        final Quantifier.ForEach qExpected = Quantifier.forEachBuilder()
                .withAlias(qNoeQun.getAlias())
                .build(initialOf(innerSelectExpected));
        final Quantifier eExpected = exists(selectWithPredicates(baseTau(), ImmutableList.of("alpha")));
        final SelectExpression expected = join(qExpected, eExpected)
                .addResultColumn(column(qExpected, "b", "b"))
                .addPredicate(fieldPredicate(qExpected, "a", EQUALS_42))
                .build()
                .buildSelect();

        testHelper.assertYields(outer, expected);
    }

    /**
     * Tests that the rule does not fire when the select carries no predicates.
     */
    @Test
    void doesNotFireWithNoPredicates() {
        final Quantifier baseQun = baseT();
        final SelectExpression innerSelect = selectWithPredicates(baseQun,
                ImmutableList.of("a", "b", "c"),
                fieldPredicate(baseQun, "b", GREATER_THAN_HELLO));
        final Quantifier noeQun = forEachWithNullOnEmpty(innerSelect);
        final SelectExpression outer = selectWithPredicates(noeQun, ImmutableList.of("b", "c"));

        testHelper.assertYieldsNothing(outer, true);
    }
}
