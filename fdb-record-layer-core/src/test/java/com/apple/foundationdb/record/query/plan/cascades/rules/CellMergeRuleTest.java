/*
 * CellMergeRuleTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.debug.DebuggerWithSymbolTables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.column;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fieldPredicate;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fieldValue;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.forEach;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.selectWithPredicates;
import static com.apple.foundationdb.record.query.plan.cascades.rules.RuleTestHelper.EQUALS_42;
import static com.apple.foundationdb.record.query.plan.cascades.rules.RuleTestHelper.EQUALS_PARAM;
import static com.apple.foundationdb.record.query.plan.cascades.rules.RuleTestHelper.baseT;
import static com.apple.foundationdb.record.query.plan.cascades.rules.RuleTestHelper.baseTau;
import static com.apple.foundationdb.record.query.plan.cascades.rules.RuleTestHelper.join;

/**
 * Tests of the {@link CellMergeRule}.
 */
class CellMergeRuleTest {
    @Nonnull
    private static final RuleTestHelper testHelper = new RuleTestHelper(new CellMergeRule());

    @BeforeEach
    void setUp() {
        Debugger.setDebugger(DebuggerWithSymbolTables.withSanityChecks());
        Debugger.setup();
    }

    /**
     * Tests that we do not try to do anything when we already have a simple query. In this
     * case, the query looks something like:
     * <pre>{@code
     * SELECT a, b
     *   FROM t
     *   WHERE d = $param
     * }</pre>
     * <p>
     * There aren't any cells that can be merged there. The rule may still visit the expression,
     * but it will find there's nothing to do.
     * </p>
     */
    @Test
    void doNotRewriteSimple() {
        Quantifier baseQun = baseT();
        SelectExpression select = selectWithPredicates(
                baseQun, ImmutableList.of("a", "b"),
                fieldPredicate(baseQun, "d", EQUALS_PARAM));
        testHelper.assertYieldsNothing(select, true);
    }

    /**
     * Tests that we merge two simple selects. In this case, we start
     * with something like:
     * <pre>{@code
     * SELECT b, c
     *   FROM (SELECT a, b, c WHERE d = $param)
     *   WHERE a = 42
     * }</pre>
     * <p>
     * And we end with a combined query:
     * </p>
     * <pre>{@code
     * SELECT b, c
     *   FROM t
     *   WHERE d = $param AND a = 42
     * }</pre>
     */
    @Test
    void mergeTwoSimpleSelects() {
        Quantifier baseQun = baseT();
        Quantifier lowerQun = forEach(selectWithPredicates(
                baseQun, ImmutableList.of("a", "b", "c"),
                fieldPredicate(baseQun, "d", EQUALS_PARAM)
        ));
        SelectExpression upper = selectWithPredicates(
                lowerQun, ImmutableList.of("b", "c"),
                fieldPredicate(lowerQun, "a", EQUALS_42)
        );

        SelectExpression combined = selectWithPredicates(
                baseQun, ImmutableList.of("b", "c"),
                fieldPredicate(baseQun, "d", EQUALS_PARAM),
                fieldPredicate(baseQun, "a", EQUALS_42)
        );

        testHelper.assertYields(upper, combined);
    }

    /**
     * Merge a select with a filter expression underneath. This folds in all of the filter's predicates into the
     * upper select statement's predicates.
     */
    @Test
    void mergeSelectWithFilter() {
        final Value constantBytes = ConstantObjectValue.of(Quantifier.constant(), "1", Type.primitiveType(Type.TypeCode.BYTES, false));
        Quantifier baseQun = baseT();
        Quantifier lowerQun = forEach(new LogicalFilterExpression(
                ImmutableList.of(
                        fieldPredicate(baseQun, "a", EQUALS_42),
                        fieldPredicate(baseQun, "b", EQUALS_PARAM)
                ),
                baseQun));
        SelectExpression upper = selectWithPredicates(
                lowerQun, ImmutableList.of("d"),
                fieldPredicate(lowerQun, "c", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, constantBytes))
        );

        SelectExpression newUpper = selectWithPredicates(
                baseQun, ImmutableList.of("d"),
                fieldPredicate(baseQun, "a", EQUALS_42),
                fieldPredicate(baseQun, "b", EQUALS_PARAM),
                fieldPredicate(baseQun, "c", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, constantBytes))
        );

        testHelper.assertYields(upper, newUpper);
    }

    /**
     * Cell merge that pulls a join up. In this case, the original query had a smaller select on top of a join:
     * <pre>{@code
     * SELECT a1 AS x, a2 AS y, a3 AS z
     *  FROM (
     *    SELECT t.a AS a1, tPrime.a AS a2, tau.alpha AS a3
     *    FROM t, t AS tPrime, tau
     *    WHERE t.d = tPrime.d AND t.d = tau.delta
     *  )
     *  WHERE b1 = $param AND b2 IS NULL AND b3 = b1
     * }</pre>
     * <p>
     * The upper select can be merged with the join below, but the upper predicates and final projection must
     * be rewritten to apply to the base join components:
     * </p>
     * <pre>{@code
     * SELECT t.a AS x, tPrime.a AS y, tau.alpha AS z
     *   FROM t, t AS tPrime, tau
     *   WHERE t.d = tPrime.d AND t.d = tau.delta
     *     AND t.b = $param AND tPrime.b IS NULL AND tau.beta = t.b
     * }</pre>
     */
    @Test
    void mergeProjectionOntoJoin() {
        Quantifier tQun = baseT();
        Quantifier tPrimeQun = baseT();
        Quantifier tauQun = baseTau();

        Quantifier lowerSelectQun = forEach(join(tQun, tPrimeQun, tauQun)
                .addResultColumn(column(tQun, "a", "a1"))
                .addResultColumn(column(tPrimeQun, "a", "a2"))
                .addResultColumn(column(tauQun, "alpha", "a3"))
                .addResultColumn(column(tQun, "b", "b1"))
                .addResultColumn(column(tPrimeQun, "b", "b2"))
                .addResultColumn(column(tauQun, "beta", "b3"))
                .addPredicate(fieldPredicate(tQun, "d", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tPrimeQun, "d"))))
                .addPredicate(fieldPredicate(tQun, "d", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tauQun, "delta"))))
                .build().buildSelect());

        SelectExpression upper = selectWithPredicates(lowerSelectQun,
                ImmutableMap.of("a1", "x", "a2", "y", "a3", "z"),
                fieldPredicate(lowerSelectQun, "b1", EQUALS_PARAM),
                fieldPredicate(lowerSelectQun, "b2", new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                fieldPredicate(lowerSelectQun, "b3", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(lowerSelectQun, "b1")))
        );

        SelectExpression combined = join(tQun, tPrimeQun, tauQun)
                // Final projection list is from upper select, but expressed directly on join components
                .addResultColumn(column(tQun, "a", "x"))
                .addResultColumn(column(tPrimeQun, "a", "y"))
                .addResultColumn(column(tauQun, "alpha", "z"))
                // Copy the original predicates (i.e., join criteria)
                .addPredicate(fieldPredicate(tQun, "d", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tPrimeQun, "d"))))
                .addPredicate(fieldPredicate(tQun, "d", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tauQun, "delta"))))
                // Translate the higher level predicates
                .addPredicate(fieldPredicate(tQun, "b", EQUALS_PARAM))
                .addPredicate(fieldPredicate(tPrimeQun, "b", new Comparisons.NullComparison(Comparisons.Type.IS_NULL)))
                .addPredicate(fieldPredicate(tauQun, "beta", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tQun, "b"))))
                .build().buildSelect();

        testHelper.assertYields(upper, combined);
    }
}
