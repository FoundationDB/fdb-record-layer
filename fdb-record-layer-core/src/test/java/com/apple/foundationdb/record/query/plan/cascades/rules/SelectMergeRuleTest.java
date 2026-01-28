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
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.PlannerStage;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.debug.DebuggerWithSymbolTables;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalDistinctExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ExistsPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Optional;

import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.column;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.exists;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fieldPredicate;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.fieldValue;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.forEach;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.forEachWithNullOnEmpty;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.projectColumn;
import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.selectWithPredicates;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.EQUALS_42;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.EQUALS_PARAM;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.GREATER_THAN_HELLO;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.baseT;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.baseTau;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.explodeField;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.join;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.valuesQun;

/**
 * Tests of the {@link SelectMergeRule}.
 */
class SelectMergeRuleTest {
    @Nonnull
    private static final RuleTestHelper testHelper = new RuleTestHelper(new SelectMergeRule());

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
     * There aren't any select expressions that can be merged there. The rule may still visit the expression,
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

    /**
     * Test merging a select on a repeated primitive value. This takes an expression like:
     * <pre>{@code
     *  SELECT t.b, f
     *    FROM t, (SELECT f FROM t.f WHERE f > 42)
     *    WHERE t.a = 42
     * }</pre>
     * <p>
     * And it pulls the filter into the top level:
     * </p>
     * <pre>{@code
     *  SELECT t.b, f
     *    FROM t, t.f AS f
     *    WHERE f > 42 AND t.a = 42
     * }</pre>
     * <p>
     * Here, {@code f} is an array of longs, and so the syntax here is a little iffy, but the
     * typing in terms of the lower level objects is sensible.
     * </p>
     */
    @Test
    void mergeFilterOnPrimitiveExplode() {
        Quantifier baseQun = baseT();

        Quantifier explodeFQun = forEach(new ExplodeExpression(fieldValue(baseQun, "f")));
        Quantifier higherFValuesQun = forEach(new LogicalFilterExpression(
                ImmutableList.of(new ValuePredicate(explodeFQun.getFlowedObjectValue(), new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 42L))),
                explodeFQun));

        SelectExpression upper = join(baseQun, higherFValuesQun)
                .addResultColumn(projectColumn(baseQun, "b"))
                .addResultColumn(Column.of(Optional.of("f"), higherFValuesQun.getFlowedObjectValue()))
                .addPredicate(fieldPredicate(baseQun, "a", EQUALS_42))
                .build().buildSelect();

        SelectExpression merged = join(baseQun, explodeFQun)
                .addResultColumn(projectColumn(baseQun, "b"))
                .addResultColumn(Column.of(Optional.of("f"), explodeFQun.getFlowedObjectValue()))
                .addPredicate(new ValuePredicate(explodeFQun.getFlowedObjectValue(), new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 42L)))
                .addPredicate(fieldPredicate(baseQun, "a", EQUALS_42))
                .build().buildSelect();

        testHelper.assertYields(upper, merged);
    }

    /**
     * Validate that we do not attempt to merge in an existential predicate, here on a repeated primitive value. The
     * query is something like:
     * <pre>{@code
     *  SELECT a, b
     *    FROM t
     *    WHERE EXISTS (SELECT * FROM t.f WHERE f > 42)
     * }</pre>
     * <p>
     * Here, {@code f} is an array of longs, and so the syntax here is a little iffy, but the
     * typing in terms of the lower level objects is sensible. In any case, there is no way to
     * merge the existential into the upper select.
     * </p>
     */
    @Test
    void doNotMergeExistentials() {
        Quantifier baseQun = baseT();

        Quantifier explodeFQun = forEach(new ExplodeExpression(fieldValue(baseQun, "f")));
        Quantifier existsHigherFQun = exists(new LogicalFilterExpression(
                ImmutableList.of(new ValuePredicate(explodeFQun.getFlowedObjectValue(), new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 42L))),
                explodeFQun));

        SelectExpression upper = join(baseQun, existsHigherFQun)
                .addResultColumn(projectColumn(baseQun, "a"))
                .addResultColumn(projectColumn(baseQun, "b"))
                .addPredicate(new ExistsPredicate(existsHigherFQun.getAlias()))
                .build().buildSelect();

        testHelper.assertYieldsNothing(upper, true);
    }

    /**
     * Test merging a select on a nested repeated. This takes an expression like:
     * <pre>{@code
     *  SELECT t.b, q.one
     *    FROM t, (SELECT one, three FROM t.g WHERE two > 'hello') AS q
     *    WHERE t.d = q.three
     * }</pre>
     * <p>
     * And it pulls the sub-select on the repeated field into the top level:
     * </p>
     * <pre>{@code
     *  SELECT t.b, q.one
     *    FROM t, t.g AS q
     *    WHERE q.two > 'hello' AND t.d = q.three
     * }</pre>
     * <p>
     * Here, {@code g} is an array of a nested message.
     * </p>
     */
    @Test
    void mergeFilterOnNestedExplode() {
        Quantifier baseQun = baseT();
        Quantifier explodeGQun = explodeField(baseQun, "g");
        Quantifier higherTwoValuesQun = forEach(selectWithPredicates(
                explodeGQun, ImmutableList.of("one", "three"),
                fieldPredicate(explodeGQun, "two", GREATER_THAN_HELLO)));

        SelectExpression upper = join(baseQun, higherTwoValuesQun)
                .addResultColumn(projectColumn(baseQun, "b"))
                .addResultColumn(projectColumn(higherTwoValuesQun, "one"))
                .addPredicate(fieldPredicate(baseQun, "d", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(higherTwoValuesQun, "three"))))
                .build().buildSelect();

        SelectExpression merged = join(baseQun, explodeGQun)
                .addResultColumn(projectColumn(baseQun, "b"))
                .addResultColumn(projectColumn(explodeGQun, "one"))
                .addPredicate(fieldPredicate(explodeGQun, "two", GREATER_THAN_HELLO))
                .addPredicate(fieldPredicate(baseQun, "d", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(explodeGQun, "three"))))
                .build().buildSelect();

        testHelper.assertYields(upper, merged);
    }

    /**
     * Validate that we do not attempt to merge in an existential predicate, here on a repeated nested value. The
     * query is something like:
     * <pre>{@code
     *  SELECT a, b
     *    FROM t
     *    WHERE EXISTS (SELECT * FROM t.g WHERE two > 'hello')
     * }</pre>
     * <p>
     * There is no way to merge the existential into the upper select.
     * </p>
     */
    @Test
    void doNotMergeExistentialOnNested() {
        Quantifier baseQun = baseT();

        Quantifier explodeGQun = explodeField(baseQun, "g");
        Quantifier existsHigherTwoQun = exists(selectWithPredicates(
                explodeGQun, fieldPredicate(explodeGQun, "two", GREATER_THAN_HELLO)));

        SelectExpression upper = join(baseQun, existsHigherTwoQun)
                .addResultColumn(projectColumn(baseQun, "a"))
                .addResultColumn(projectColumn(baseQun, "b"))
                .addPredicate(new ExistsPredicate(existsHigherTwoQun.getAlias()))
                .build().buildSelect();

        testHelper.assertYieldsNothing(upper, true);
    }

    /**
     * Validate that if we have a default on empty, we do not attempt to merge it with an upper select.
     * For example, a query like:
     * <pre>{@code
     * SELECT b, c
     *   FROM (SELECT a, b, c FROM t WHERE b > 'hello') OR ELSE NULL
     *   WHERE a = 42
     * }</pre>
     * <p>
     * Then we can't merge the statements. In this case, this is because the inner select
     * may introduce an extra {@code null}. In theory, if we could prove that the predicates on the
     * upper level select will always prevent us from actually returning {@code null}. If we start
     * to allow that, we'll need to modify this test.
     * </p>
     */
    @Test
    void doNotMergeDefaultOnEmpty() {
        Quantifier baseQun = baseT();

        Quantifier lowerQun = forEachWithNullOnEmpty(selectWithPredicates(
                baseQun, ImmutableList.of("a", "b", "c"),
                fieldPredicate(baseQun, "b", GREATER_THAN_HELLO)));

        SelectExpression upper = selectWithPredicates(
                lowerQun, ImmutableList.of("b", "c"),
                fieldPredicate(lowerQun, "a", EQUALS_42));

        // This rule doesn't even get matched if the child quantifier has null-on-empty
        testHelper.assertYieldsNothing(upper, true);
    }

    /**
     * Combines two join queries into one larger join. In this case, we're dealing with something like:
     * <pre>{@code
     * SELECT j1.a AS a0, j1.alpha AS alpha0, j2.a1, j2.a2, j2.alpha AS alpha1
     *   FROM
     *     (
     *       SELECT t.a, tau.alpha, t.b, tau.beta, t.c, tau.gamma
     *        FROM t, tau
     *        WHERE t.c >= tau.gamma
     *     ) as j1,
     *     (
     *       SELECT t1.a AS a1, t2.a AS a2, tau.alpha, t1.b AS b1, t2.b AS b2, tau.beta, t1.c AS c1, t2.c AS c2, tau.gamma
     *         FROM t AS t1, t AS t2, tau
     *         WHERE t1.d = t2.d AND t1.d = tau.delta
     *     ) as j2
     *   WHERE j1.b = j2.b1 AND j1.b = j2.b2 AND j1.beta = j2.beta
     * }</pre>
     * <p>
     * This gets merged into one larger five-way join:
     * </p>
     * <pre>{@code
     * SELECT t0.a AS a0, tau0.alpha AS alpha0, t1.a AS a1, t2.a AS a2, tau1.alpha AS alpha1
     *   FROM t AS t0, tau AS tau0, t AS t1, t AS t2, tau AS tau1
     *   WHERE t0.c >= tau0.gamma AND t1.d = t2.d AND t1.d = tau1.delta
     *     AND t0.b = t1.b AND t0.b = t2.b AND tau0.beta = tau1.beta
     * }</pre>
     * <p>
     * The rule will do this in one shot because it merges together all eligible candidates at
     * once.
     * </p>
     */
    @Test
    void combineTwoJoins() {
        //
        // Join 1
        //  SELECT t.a, tau.alpha, t.b, tau.beta, t.c, tau.gamma
        //    FROM t, tau
        //    WHERE t.c >= tau.gamma
        //
        final Quantifier tQun = baseT();
        final Quantifier tauQun = baseTau();
        final Quantifier join1Qun = forEach(join(tQun, tauQun)
                .addResultColumn(projectColumn(tQun, "a"))
                .addResultColumn(projectColumn(tauQun, "alpha"))
                .addResultColumn(projectColumn(tQun, "b"))
                .addResultColumn(projectColumn(tauQun, "beta"))
                .addResultColumn(projectColumn(tQun, "c"))
                .addResultColumn(projectColumn(tauQun, "gamma"))
                .addPredicate(fieldPredicate(tQun, "c", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, fieldValue(tauQun, "gamma"))))
                .build()
                .buildSelect());

        //
        // Join 2
        //  SELECT t2.a AS a1, t3.a AS a2, tau.alpha, t2.b AS b1, t3.b AS b2, tau.beta, t2.c AS c1, t3.c AS c2, tau.gamma
        //   FROM t AS t2, t AS t3, tau
        //   WHERE t2.d = t3.d AND t2.d = tau.delta
        //
        final Quantifier tQun2 = baseT();
        final Quantifier tQun3 = baseT();
        final Quantifier tauQun2 = baseTau();
        final Quantifier join2Qun = forEach(join(tQun2, tQun3, tauQun2)
                .addResultColumn(column(tQun2, "a", "a1"))
                .addResultColumn(column(tQun3, "a", "a2"))
                .addResultColumn(projectColumn(tauQun2, "alpha"))
                .addResultColumn(column(tQun2, "b", "b1"))
                .addResultColumn(column(tQun3, "b", "b2"))
                .addResultColumn(projectColumn(tauQun2, "beta"))
                .addResultColumn(column(tQun2, "c", "c1"))
                .addResultColumn(column(tQun3, "c", "c2"))
                .addResultColumn(projectColumn(tauQun2, "gamma"))
                .addPredicate(fieldPredicate(tQun2, "d", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tQun3, "d"))))
                .addPredicate(fieldPredicate(tQun2, "d", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tauQun2, "delta"))))
                .build()
                .buildSelect());

        //
        // Join the first two joins together into a larger join
        //  SELECT j1.a AS a0, j1.alpha AS alpha0, j2.a1, j2.a2, j2.alpha AS alpha1
        //   FROM join1 AS j1, join2 AS j2
        //   WHERE j1.b = b2.b1 AND j1.b = j2.b2 AND j1.beta = j2.beta
        //
        final SelectExpression upperJoin = join(join1Qun, join2Qun)
                .addResultColumn(column(join1Qun, "a", "a0"))
                .addResultColumn(column(join1Qun, "alpha", "alpha0"))
                .addResultColumn(projectColumn(join2Qun, "a1"))
                .addResultColumn(projectColumn(join2Qun, "a2"))
                .addResultColumn(column(join2Qun, "alpha", "alpha1"))
                .addPredicate(fieldPredicate(join1Qun, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(join2Qun, "b1"))))
                .addPredicate(fieldPredicate(join1Qun, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(join2Qun, "b2"))))
                .addPredicate(fieldPredicate(join1Qun, "beta", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(join2Qun, "beta"))))
                .build().buildSelect();

        //
        // Merge both lower joins together into a single five-way join
        //
        final SelectExpression mergedJoin = join(tQun, tauQun, tQun2, tQun3, tauQun2)
                .addResultColumn(column(tQun, "a", "a0"))
                .addResultColumn(column(tauQun, "alpha", "alpha0"))
                .addResultColumn(column(tQun2, "a", "a1"))
                .addResultColumn(column(tQun3, "a", "a2"))
                .addResultColumn(column(tauQun2, "alpha", "alpha1"))
                .addPredicate(fieldPredicate(tQun, "c", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, fieldValue(tauQun, "gamma"))))
                .addPredicate(fieldPredicate(tQun2, "d", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tQun3, "d"))))
                .addPredicate(fieldPredicate(tQun2, "d", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tauQun2, "delta"))))
                .addPredicate(fieldPredicate(tQun, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tQun2, "b"))))
                .addPredicate(fieldPredicate(tQun, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tQun3, "b"))))
                .addPredicate(fieldPredicate(tauQun, "beta", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tauQun2, "beta"))))
                .build().buildSelect();

        testHelper.assertYields(upperJoin, mergedJoin);
    }

    /**
     * Validate that if some children are mergeable while others are not, any non-mergeable children are retained.
     * In this case, we consider a query like:
     * <pre>{@code
     * SELECT u.a, u.b, u.c, x.two, x.three
     *   FROM
     *     (SELECT a, b, c FROM T WHERE b > 'hello'
     *      UNION ALL
     *      SELECT a, b, c FROM T WHERE a = 42
     *     ) u,
     *     (SELECT two, three FROM T.g WHERE one IS NOT NULL) x
     *   WHERE u.b = x.two
     * }</pre>
     * <p>
     * Here, the union cannot be merged up (at least not without further transformations like converting it to an
     * OR, which would require proving that there are no duplicates between the legs of the union, or proving that
     * one side of the union is empty). The select over the explode however can be, so we get:
     * </p>
     * <pre>{@code
     * SELECT u.a, u.b, u.c, x.two, x.three
     *   FROM
     *     (SELECT a, b, c FROM T WHERE b > 'hello'
     *      UNION ALL
     *      SELECT a, b, c FROM T WHERE a = 42
     *     ) u,
     *     T.g AS x
     *   WHERE x.one IS NOT NULL AND u.b = x.two
     * }</pre>
     */
    @Test
    void retainNonMergeableChildren() {
        final Quantifier t1 = baseT();
        final Quantifier t2 = baseT();

        final Quantifier unionQun = forEach(new LogicalUnionExpression(ImmutableList.of(
                forEach(selectWithPredicates(t1, ImmutableList.of("a", "b", "c"), fieldPredicate(t1, "b", GREATER_THAN_HELLO))),
                forEach(selectWithPredicates(t2, ImmutableList.of("a", "b", "c"), fieldPredicate(t1, "a", EQUALS_42)))
        )));

        final Quantifier t3 = baseT();
        final Quantifier explodeGQun = forEach(new ExplodeExpression(fieldValue(t3, "g")));
        final Quantifier filterGQun = forEach(selectWithPredicates(explodeGQun, ImmutableList.of("two", "three"),
                fieldPredicate(explodeGQun, "one", new Comparisons.NullComparison(Comparisons.Type.NOT_NULL))));

        final SelectExpression select = join(unionQun, filterGQun)
                .addResultColumn(projectColumn(unionQun, "a"))
                .addResultColumn(projectColumn(unionQun, "b"))
                .addResultColumn(projectColumn(unionQun, "c"))
                .addResultColumn(projectColumn(filterGQun, "two"))
                .addResultColumn(projectColumn(filterGQun, "three"))
                .addPredicate(fieldPredicate(unionQun, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(filterGQun, "two"))))
                .build().buildSelect();

        final SelectExpression merged = join(unionQun, explodeGQun)
                .addResultColumn(projectColumn(unionQun, "a"))
                .addResultColumn(projectColumn(unionQun, "b"))
                .addResultColumn(projectColumn(unionQun, "c"))
                .addResultColumn(projectColumn(explodeGQun, "two"))
                .addResultColumn(projectColumn(explodeGQun, "three"))
                .addPredicate(fieldPredicate(explodeGQun, "one", new Comparisons.NullComparison(Comparisons.Type.NOT_NULL)))
                .addPredicate(fieldPredicate(unionQun, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(explodeGQun, "two"))))
                .build().buildSelect();

        testHelper.assertYields(select, merged);
    }

    /**
     * Merge two expressions where there are correlations between the different legs of the join.
     * In this case, we have something like:
     * <pre>{@code
     * SELECT x.c, y.gamma
     *   FROM (SELECT b, c, d FROM t WHERE a = 42) x,
     *        (SELECT alpha, beta, gamma, delta FROM tau WHERE beta > x.b) y
     * }</pre>
     * <p>
     * Note that the predicate {@code beta > x.b} makes a reference to the other leg of the join.
     * So, in this case, the logic works by merging the independent leg and then consequently rewriting the dependant
     * leg and merge that. We get the following in a single step:
     * </p>
     * <pre>{@code
     * SELECT t.c, tau.gamma
     *   FROM t, tau
     *   WHERE t.a = 42 AND tau.beta > t.b
     * }</pre>
     */
    @Test
    void mergeWithCorrelationsBetweenSiblings() {
        final Quantifier tQun = baseT();
        final Quantifier tauQun = baseTau();

        final Quantifier leftQun = forEach(selectWithPredicates(tQun,
                ImmutableList.of("b", "c", "d"),
                fieldPredicate(tQun, "a", EQUALS_42)));

        final Quantifier rightQun = forEach(selectWithPredicates(tauQun,
                ImmutableList.of("alpha", "beta", "gamma", "delta"),
                fieldPredicate(tauQun, "beta", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, fieldValue(leftQun, "b")))));

        final SelectExpression joined = join(leftQun, rightQun)
                .addResultColumn(projectColumn(leftQun, "c"))
                .addResultColumn(projectColumn(rightQun, "gamma"))
                .build().buildSelect();

        final SelectExpression bothMerged = join(tQun, tauQun)
                .addResultColumn(projectColumn(tQun, "c"))
                .addResultColumn(projectColumn(tauQun, "gamma"))
                .addPredicate(fieldPredicate(tQun, "a", EQUALS_42))
                .addPredicate(fieldPredicate(tauQun, "beta", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, fieldValue(tQun, "b"))))
                .build().buildSelect();

        testHelper.assertYields(joined, bothMerged);
    }

    /**
     * Similar to {@link #mergeWithCorrelationsBetweenSiblings()}, but in this case, the dependant leg cannot be
     * merged, being a {@link LogicalDistinctExpression}. Hence, the independent leg is merged and the dependant leg
     * is appropriately rewritten.
     */
    @Test
    void cannotMergeDueToCorrelationsBetweenSiblings() {
        final Quantifier tQun = baseT();
        final Quantifier tauQun = baseTau();

        final Quantifier leftQun = forEach(selectWithPredicates(tQun,
                ImmutableList.of("b", "c", "d"),
                fieldPredicate(tQun, "a", EQUALS_42)));

        final Quantifier rightQun = forEach(new LogicalDistinctExpression(
                forEach(selectWithPredicates(tauQun,
                        ImmutableList.of("alpha", "beta", "gamma", "delta"),
                        fieldPredicate(tauQun, "beta", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, fieldValue(leftQun, "b")))
                ))
        ));

        final SelectExpression joined = join(leftQun, rightQun)
                .addResultColumn(projectColumn(leftQun, "c"))
                .addResultColumn(projectColumn(rightQun, "gamma"))
                .build().buildSelect();

        final Quantifier rebasedRightQun = forEach(new LogicalDistinctExpression(
                forEach(selectWithPredicates(tauQun,
                        ImmutableList.of("alpha", "beta", "gamma", "delta"),
                        fieldPredicate(tauQun, "beta", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, fieldValue(tQun, "b")))
                ))
        ));

        final SelectExpression expected = join(tQun, rebasedRightQun)
                .addResultColumn(projectColumn(tQun, "c"))
                .addResultColumn(projectColumn(rebasedRightQun, "gamma"))
                .addPredicate(fieldPredicate(tQun, "a", EQUALS_42))
                .build().buildSelect();

        testHelper.assertYields(joined, expected);
    }

    /**
     * Validate a rather complicated join merges up values as referencing correlations are merged up.
     * In particular, we begin with a complicated tree of joins with correlations between the legs.
     * At each step, running the {@link SelectMergeRule} will work with all the quantifiers and merge
     * their select expressions in to the top-level select.
     */
    @Test
    void shaveOffConnectedComponents() {
        final ConstantObjectValue cov1 = ConstantObjectValue.of(Quantifier.constant(), "c1", Type.primitiveType(Type.TypeCode.STRING, false));
        final ConstantObjectValue cov2 = ConstantObjectValue.of(Quantifier.constant(), "c2", Type.primitiveType(Type.TypeCode.STRING, false));

        final Quantifier t1 = baseT();
        final Quantifier t2 = baseT();
        final Quantifier t3 = baseT();

        final Quantifier s3 = forEach(selectWithPredicates(t3,
                ImmutableList.of("a", "c"),
                fieldPredicate(t3, "a", EQUALS_42)));

        final Quantifier l1 = forEach(join(t1, t2)
                .addResultColumn(column(t1, "a", "a1"))
                .addResultColumn(column(t2, "a", "a2"))
                .addResultColumn(column(t1, "c", "c1"))
                .addResultColumn(column(t2, "c", "c2"))
                .addResultColumn(column(t1, "d", "d1"))
                .addResultColumn(column(t2, "d", "d2"))
                .addPredicate(fieldPredicate(t1, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(t2, "a"))))
                .addPredicate(fieldPredicate(t1, "b", EQUALS_PARAM))
                .addPredicate(fieldPredicate(t2, "b", EQUALS_PARAM))
                .addPredicate(fieldPredicate(t1, "c", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(s3, "c"))))
                .addPredicate(fieldPredicate(t2, "c", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(s3, "c"))))
                .build().buildSelect());

        final Quantifier l2 = forEach(join(l1, s3)
                .addResultColumn(projectColumn(l1, "a1"))
                .addResultColumn(projectColumn(l1, "a2"))
                .addResultColumn(column(s3, "a", "a3"))
                .addResultColumn(column(s3, "c", "c123"))
                .addResultColumn(projectColumn(l1, "d1"))
                .addResultColumn(projectColumn(l1, "d2"))
                .addPredicate(fieldPredicate(l1, "d1", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(l1, "d2"))))
                .build().buildSelect());

        final Quantifier tau1 = baseTau();
        final Quantifier tau2 = baseTau();
        final Quantifier tau3 = baseTau();

        final Quantifier sigma2 = forEach(selectWithPredicates(tau2,
                fieldPredicate(tau2, "beta", EQUALS_PARAM)));

        final Quantifier sigma1 = forEach(selectWithPredicates(tau1,
                fieldPredicate(tau1, "alpha", EQUALS_42),
                fieldPredicate(tau1, "beta", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(sigma2, "delta")))));

        final Quantifier sigma3 = forEach(selectWithPredicates(tau3,
                fieldPredicate(tau3, "alpha", new Comparisons.NullComparison(Comparisons.Type.IS_NULL)),
                fieldPredicate(sigma2, "gamma", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, fieldValue(tau3, "gamma")))));

        final Quantifier lambda1 = forEach(join(sigma1, sigma2, sigma3)
                .addResultColumn(column(sigma1, "alpha", "alpha1"))
                .addResultColumn(column(sigma2, "alpha", "alpha2"))
                .addResultColumn(column(sigma3, "alpha", "alpha3"))
                .addResultColumn(column(sigma1, "beta", "beta1"))
                .addResultColumn(column(sigma2, "beta", "beta2"))
                .addResultColumn(column(sigma3, "beta", "beta3"))
                .addResultColumn(column(sigma1, "gamma", "gamma1"))
                .addResultColumn(column(sigma2, "gamma", "gamma2"))
                .addResultColumn(column(sigma3, "gamma", "gamma3"))
                .addResultColumn(column(sigma1, "delta", "delta1"))
                .addResultColumn(column(sigma2, "delta", "delta2"))
                .addResultColumn(column(sigma3, "delta", "delta3"))
                .build().buildSelect());

        final Quantifier t4 = baseT();
        final Quantifier tau4 = baseTau();

        final Quantifier s4 = forEach(selectWithPredicates(t4,
                fieldPredicate(t4, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, cov1)),
                fieldPredicate(t4, "c", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(l2, "c123")))));
        final Quantifier sigma4 = forEach(selectWithPredicates(tau4,
                fieldPredicate(tau4, "beta", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, cov2)),
                fieldPredicate(tau4, "gamma", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, fieldValue(s4, "c"))),
                fieldPredicate(tau4, "alpha", new Comparisons.ValueComparison(Comparisons.Type.NOT_EQUALS, fieldValue(lambda1, "alpha1"))),
                fieldPredicate(tau4, "alpha", new Comparisons.ValueComparison(Comparisons.Type.NOT_EQUALS, fieldValue(lambda1, "alpha2"))),
                fieldPredicate(tau4, "alpha", new Comparisons.ValueComparison(Comparisons.Type.NOT_EQUALS, fieldValue(lambda1, "alpha3")))));

        final Quantifier mixed4 = forEach(join(s4, sigma4)
                .addResultColumn(column(s4, "a", "a4"))
                .addResultColumn(column(sigma4, "alpha", "alpha4"))
                .addResultColumn(column(s4, "b", "b4"))
                .addResultColumn(column(sigma4, "beta", "beta4"))
                .addResultColumn(column(s4, "c", "c4"))
                .addResultColumn(column(sigma4, "gamma", "gamma4"))
                .addPredicate(fieldPredicate(s4, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(sigma4, "alpha"))))
                .build().buildSelect());

        //
        // Starting top-level SELECT.
        // It is ultimately built on 8 base quantifiers, but there are correlations
        // between the different legs, so not all of those can be merged in at first.
        //
        final SelectExpression topJoin = join(l2, lambda1, mixed4)
                .addResultColumn(projectColumn(l2, "a1"))
                .addResultColumn(projectColumn(l2, "a2"))
                .addResultColumn(projectColumn(l2, "a3"))
                .addResultColumn(projectColumn(mixed4, "a4"))
                .addResultColumn(projectColumn(lambda1, "alpha1"))
                .addResultColumn(projectColumn(lambda1, "alpha2"))
                .addResultColumn(projectColumn(lambda1, "alpha3"))
                .addResultColumn(projectColumn(mixed4, "alpha4"))
                .build().buildSelect();

        // First step: All the 3 direct child select expressions are merged. They do have dependencies among themselves
        // that are resolved and sub graphs are accordingly rebased. The resultant top level select now sources from
        // the 7 quantifiers of the child expressions.

        final Quantifier rebasedS4 = forEach(selectWithPredicates(t4,
                fieldPredicate(t4, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, cov1)),
                fieldPredicate(t4, "c", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(s3, "c")))));

        final Quantifier rebasedSigma4 = forEach(selectWithPredicates(tau4,
                fieldPredicate(tau4, "beta", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, cov2)),
                fieldPredicate(tau4, "gamma", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, fieldValue(rebasedS4, "c"))),
                fieldPredicate(tau4, "alpha", new Comparisons.ValueComparison(Comparisons.Type.NOT_EQUALS, fieldValue(sigma1, "alpha"))),
                fieldPredicate(tau4, "alpha", new Comparisons.ValueComparison(Comparisons.Type.NOT_EQUALS, fieldValue(sigma2, "alpha"))),
                fieldPredicate(tau4, "alpha", new Comparisons.ValueComparison(Comparisons.Type.NOT_EQUALS, fieldValue(sigma3, "alpha")))));

        final SelectExpression firstMerge  = join(l1, s3, sigma1, sigma2, sigma3, rebasedS4, rebasedSigma4)
                .addResultColumn(column(l1, "a1", "a1"))
                .addResultColumn(column(l1, "a2", "a2"))
                .addResultColumn(column(s3, "a", "a3"))
                .addResultColumn(column(rebasedS4, "a", "a4"))
                .addResultColumn(column(sigma1, "alpha", "alpha1"))
                .addResultColumn(column(sigma2, "alpha", "alpha2"))
                .addResultColumn(column(sigma3, "alpha", "alpha3"))
                .addResultColumn(column(rebasedSigma4, "alpha", "alpha4"))
                .addPredicate(fieldPredicate(l1, "d1", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(l1, "d2"))))
                .addPredicate(fieldPredicate(rebasedS4, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(rebasedSigma4, "alpha"))))
                .build().buildSelect();
        testHelper.assertYields(topJoin, firstMerge);

        // Final step: In this step, all now 8 direct child select expressions are merged. These, too, have dependencies
        // among themselves that are resolved and the enumeration order of quantifiers follow the dependency map.

        final SelectExpression finalMerge  = join(t3, t1, t2, t4, tau2, tau1, tau3, tau4)
                .addResultColumn(column(t1, "a", "a1"))
                .addResultColumn(column(t2, "a", "a2"))
                .addResultColumn(column(t3, "a", "a3"))
                .addResultColumn(column(t4, "a", "a4"))
                .addResultColumn(column(tau1, "alpha", "alpha1"))
                .addResultColumn(column(tau2, "alpha", "alpha2"))
                .addResultColumn(column(tau3, "alpha", "alpha3"))
                .addResultColumn(column(tau4, "alpha", "alpha4"))
                .addPredicate(fieldPredicate(t3, "a", EQUALS_42))
                .addPredicate(fieldPredicate(t1, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(t2, "a"))))
                .addPredicate(fieldPredicate(t1, "b", EQUALS_PARAM))
                .addPredicate(fieldPredicate(t2, "b", EQUALS_PARAM))
                .addPredicate(fieldPredicate(t1, "c", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(t3, "c"))))
                .addPredicate(fieldPredicate(t2, "c", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(t3, "c"))))
                .addPredicate(fieldPredicate(t4, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, cov1)))
                .addPredicate(fieldPredicate(t4, "c", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(t3, "c"))))
                .addPredicate(fieldPredicate(tau2, "beta", EQUALS_PARAM))
                .addPredicate(fieldPredicate(tau1, "alpha", EQUALS_42))
                .addPredicate(fieldPredicate(tau1, "beta", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tau2, "delta"))))
                .addPredicate(fieldPredicate(tau3, "alpha", new Comparisons.NullComparison(Comparisons.Type.IS_NULL)))
                .addPredicate(fieldPredicate(tau2, "gamma", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, fieldValue(tau3, "gamma"))))
                .addPredicate(fieldPredicate(tau4, "beta", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, cov2)))
                .addPredicate(fieldPredicate(tau4, "gamma", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, fieldValue(t4, "c"))))
                .addPredicate(fieldPredicate(tau4, "alpha", new Comparisons.ValueComparison(Comparisons.Type.NOT_EQUALS, fieldValue(tau1, "alpha"))))
                .addPredicate(fieldPredicate(tau4, "alpha", new Comparisons.ValueComparison(Comparisons.Type.NOT_EQUALS, fieldValue(tau2, "alpha"))))
                .addPredicate(fieldPredicate(tau4, "alpha", new Comparisons.ValueComparison(Comparisons.Type.NOT_EQUALS, fieldValue(tau3, "alpha"))))
                .addPredicate(fieldPredicate(t1, "d", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(t2, "d"))))
                .addPredicate(fieldPredicate(t4, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tau4, "alpha"))))
                .build().buildSelect();
        testHelper.assertYields(firstMerge, finalMerge);

        // No more merges!
        testHelper.assertYieldsNothing(finalMerge, true);
    }

    /**
     * Test the case where we have multiple variants underneath a reference. We merge the variant with the fewest
     * {@link SelectExpression}s.
     */
    @Test
    void combineWithVariants() {
        final Quantifier baseQun = baseT();

        // Two equivalent expressions which all have the same predicates
        final SelectExpression expr1 = selectWithPredicates(baseQun,
                fieldPredicate(baseQun, "a", EQUALS_42),
                fieldPredicate(baseQun, "b", EQUALS_PARAM));
        final LogicalFilterExpression expr2 = new LogicalFilterExpression(
                ImmutableList.of(fieldPredicate(baseQun, "a", EQUALS_42), fieldPredicate(baseQun, "b", EQUALS_PARAM)),
                baseQun);

        // Another one with the same semantics, but the predicates are still at different layers in the DAG
        final Quantifier paramPredQun = forEach(selectWithPredicates(baseQun, fieldPredicate(baseQun, "b", EQUALS_PARAM)));
        final SelectExpression expr3 = selectWithPredicates(paramPredQun,
                fieldPredicate(paramPredQun, "a", EQUALS_42));

        // Variant with a "distinct" in the middle. This is not pushable through
        final Quantifier qunForDistinct = forEach(selectWithPredicates(baseQun,
                fieldPredicate(baseQun, "a", EQUALS_42),
                fieldPredicate(baseQun, "b", EQUALS_PARAM)));
        final LogicalDistinctExpression expr4 = new LogicalDistinctExpression(qunForDistinct);

        final Reference lowerRef = Reference.ofFinalExpressions(PlannerStage.INITIAL, ImmutableSet.of(expr1, expr2, expr3, expr4));
        final Quantifier lowerQun = Quantifier.forEach(lowerRef);

        // Select on top of the lower qun
        final SelectExpression upper = selectWithPredicates(lowerQun, ImmutableList.of("c", "d"),
                fieldPredicate(lowerQun, "d", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, fieldValue(lowerQun, "b"))));

        final SelectExpression merged1 = selectWithPredicates(baseQun, ImmutableList.of("c", "d"),
                fieldPredicate(baseQun, "a", EQUALS_42),
                fieldPredicate(baseQun, "b", EQUALS_PARAM),
                fieldPredicate(baseQun, "d", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, fieldValue(baseQun, "b"))));

        testHelper.assertYields(upper, merged1);
    }

    /**
     * Test of what happens if merging the selects results in the same quantifier being added multiple
     * times to a combined {@link SelectExpression}. This is done by creating a base quantifier, and then
     * creating a join between that base quantifier and a select on top of it. Merging attempts to
     * add the same quantifier multiple times to the upper select.
     */
    @Test
    void mergeWithDiamond() {
        Quantifier baseQun = baseT();

        final Quantifier lowerQun1 = forEach(selectWithPredicates(
                baseQun, ImmutableList.of("b", "c", "d"),
                fieldPredicate(baseQun, "a", EQUALS_42)));

        baseQun = Quantifier.forEach(baseQun.getRangesOver());
        final SelectExpression upper = join(lowerQun1, baseQun)
                .addResultColumn(projectColumn(baseQun, "a"))
                .addResultColumn(projectColumn(lowerQun1, "b"))
                .addResultColumn(projectColumn(lowerQun1, "c"))
                .addPredicate(fieldPredicate(lowerQun1, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(baseQun, "d"))))
                .build().buildSelect();

        final Quantifier baseQun1 = baseT(); // 14
        final Quantifier baseQun2 = baseT(); // 10

        final SelectExpression merged = join(baseQun1, baseQun2)
                .addResultColumn(projectColumn(baseQun1, "a"))
                .addResultColumn(projectColumn(baseQun2, "b"))
                .addResultColumn(projectColumn(baseQun2, "c"))
                .addPredicate(fieldPredicate(baseQun2, "a", EQUALS_42))
                .addPredicate(fieldPredicate(baseQun2, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(baseQun1, "d"))))
                .build().buildSelect();

        testHelper.assertYields(upper, merged);
    }

    /**
     * Test merging up two selects with duplicate quantifiers. Here, the same quantifier (in this case standing in for the base
     * records) is the basis of both the left hand and right hand side of a join. Each expression has predicates on it,
     * and the top level select projects up from it. So, something like:
     * <pre>{@code
     * SELECT L.c AS c1, R.c AS c2, L.d AS d1, L.d AS d2
     *   FROM (SELECT a, b, c, d, FROM T WHERE T.a = 42) AS L,
     *        (SELECT a, b, c, d, FROM T WHERE T.b = ?param) AS R
     *   WHERE L.a = R.a AND L.b = R.b
     * }</pre>
     * <p>
     * And in the query graph, the {@code T} value is a single (re-used) quantifier. Note that though this
     * quantifier is re-used, it's not ambiguous at any point which one is being referred to. The rewritten graph
     * may look something like:
     * </p>
     * <pre>{@code
     * SELECT L.c AS c1, R.c AS c2, L.d AS d1, L.d AS d2
     *   FROM T AS L,
     *        T AS R
     *   WHERE L.a = 42 AND R.b = ?param AND L.a = R.a AND L.b = R.b
     * }</pre>
     * <p>
     * Note that the quantifiers for {@code T AS L} and {@code T AS R} need to be disambiguated. That is,
     * (at least) one of them should be copied into a new quantifier so that the merged select can consume
     * both of them.
     * </p>
     */
    @Test
    void mergeUpAvoidingDuplicates() {
        final Quantifier baseQun = baseT();

        final Quantifier leftQun = forEach(selectWithPredicates(baseQun,
                ImmutableList.of("a", "b", "c", "d"),
                fieldPredicate(baseQun, "a", EQUALS_42)));
        final Quantifier rightQun = forEach(selectWithPredicates(baseQun,
                ImmutableList.of("a", "b", "c", "d"),
                fieldPredicate(baseQun, "b", EQUALS_PARAM)));

        final SelectExpression upper = join(leftQun, rightQun)
                .addResultColumn(column(leftQun, "c", "c1"))
                .addResultColumn(column(rightQun, "c", "c2"))
                .addResultColumn(column(leftQun, "d", "d1"))
                .addResultColumn(column(rightQun, "d", "d2"))
                .addPredicate(fieldPredicate(leftQun, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(rightQun, "a"))))
                .addPredicate(fieldPredicate(rightQun, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(leftQun, "b"))))
                .build().buildSelect();

        final Quantifier base1 = baseT();
        final Quantifier base2 = Quantifier.forEach(base1.getRangesOver());

        final SelectExpression expected = join(base1, base2)
                .addResultColumn(column(base1, "c", "c1"))
                .addResultColumn(column(base2, "c", "c2"))
                .addResultColumn(column(base1, "d", "d1"))
                .addResultColumn(column(base2, "d", "d2"))
                .addPredicate(fieldPredicate(base1, "a", EQUALS_42))
                .addPredicate(fieldPredicate(base2, "b", EQUALS_PARAM))
                .addPredicate(fieldPredicate(base1, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(base2, "a"))))
                .addPredicate(fieldPredicate(base2, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(base1, "b"))))
                .build().buildSelect();

        testHelper.assertYields(upper, expected);
    }

    /**
     * Similar case to {@link #mergeUpAvoidingDuplicates()}, but in this case, there are correlations between some of the merged-up
     * children that also require fixing. So, we start with something like:
     * <pre>{@code
     * SELECT L.a AS a1, R.a AS a2, L.b AS b1, R.b AS b2, L.c AS c1, R.c AS c2, L.d AS d1, L.d AS d2
     *   FROM (SELECT v.x AS a, U.b AS b, U.c AS c, U.d AS d FROM values(x = @0, y = @1) AS v, (SELECT b, c, d FROM T WHERE T.a = v.x) AS U) AS L,
     *        (SELECT U.a AS a, v.y AS b, U.c AS c, U.d AS d FROM values(x = @0, y = @1) AS v, (SELECT a, c, d FROM T WHERE T.b = v.y) AS U) AS R
     *   WHERE L.a = R.a AND L.b = R.b
     * }</pre>
     * <p>
     * In this case, the two values boxes (both denoted {@code v}) are the same quantifier. When we do the
     * merge up, we get:
     * </p>
     * <pre>{@code
     * SELECT v1.x AS a1, R.a AS a2, L.b AS b1, v2.y AS b2, L.c AS c1, R.c AS c2, L.d AS d1, L.d AS d2
     *   FROM values(x = @0, y = @1) AS v1,
     *        (SELECT b, c, d FROM T WHERE T.a = v1.x) AS L,
     *        values(x = @0, y = @1) AS v2
     *        (SELECT a, c, d FROM T WHERE T.b = v2.x) AS R
     *   WHERE v1.x = R.a AND L.b = v2.y
     * }</pre>
     * <p>
     * Now we have two copies of the values box pulled into the top-level box. It needs to have a new quantifier
     * name, <em>and</em> the correlation to it within each of {@code L} and {@code R} need to point to
     * the appropriate one.
     * </p>
     */
    @Test
    void mergeUpWithRenamedCorrelations() {
        final ConstantObjectValue cov0 = ConstantObjectValue.of(Quantifier.constant(), "0", Type.primitiveType(Type.TypeCode.LONG, false));
        final ConstantObjectValue cov1 = ConstantObjectValue.of(Quantifier.constant(), "1", Type.primitiveType(Type.TypeCode.STRING, false));
        final Quantifier valuesBox = valuesQun(ImmutableMap.of("x", cov0, "y", cov1));

        final Quantifier leftBase = baseT();
        final Quantifier lowerLeft = forEach(selectWithPredicates(leftBase,
                ImmutableList.of("b", "c", "d"),
                fieldPredicate(leftBase, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(valuesBox, "x")))));
        final Quantifier leftQun = forEach(join(valuesBox, lowerLeft)
                .addResultColumn(column(valuesBox, "x", "a"))
                .addResultColumn(projectColumn(lowerLeft, "b"))
                .addResultColumn(projectColumn(lowerLeft, "c"))
                .addResultColumn(projectColumn(lowerLeft, "d"))
                .build().buildSelect());

        final Quantifier rightBase = baseT();
        final Quantifier lowerRight = forEach(selectWithPredicates(rightBase,
                ImmutableList.of("a", "c", "d"),
                fieldPredicate(rightBase, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(valuesBox, "y")))));
        final Quantifier rightQun = forEach(join(valuesBox, lowerRight)
                .addResultColumn(projectColumn(lowerRight, "a"))
                .addResultColumn(column(valuesBox, "y", "b"))
                .addResultColumn(projectColumn(lowerRight, "c"))
                .addResultColumn(projectColumn(lowerRight, "d"))
                .build().buildSelect());

        final SelectExpression upper = join(leftQun, rightQun)
                .addResultColumn(column(leftQun, "a", "a1"))
                .addResultColumn(column(rightQun, "a", "a2"))
                .addResultColumn(column(leftQun, "b", "b1"))
                .addResultColumn(column(rightQun, "b", "b2"))
                .addResultColumn(column(leftQun, "c", "c1"))
                .addResultColumn(column(rightQun, "c", "c2"))
                .addResultColumn(column(leftQun, "d", "d1"))
                .addResultColumn(column(rightQun, "d", "d2"))
                .addPredicate(fieldPredicate(leftQun, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(rightQun, "a"))))
                .addPredicate(fieldPredicate(leftQun, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(rightQun, "b"))))
                .build().buildSelect();

        final Quantifier valuesBox1 = Quantifier.forEach(valuesBox.getRangesOver());
        final Quantifier newLowerLeft = forEach(selectWithPredicates(leftBase,
                ImmutableList.of("b", "c", "d"),
                fieldPredicate(leftBase, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(valuesBox1, "x")))));
        final Quantifier valuesBox2 = Quantifier.forEach(valuesBox.getRangesOver());
        final Quantifier newLowerRight = forEach(selectWithPredicates(rightBase,
                ImmutableList.of("a", "c", "d"),
                fieldPredicate(rightBase, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(valuesBox2, "y")))));
        final SelectExpression expected = join(valuesBox1, newLowerLeft, valuesBox2, newLowerRight)
                .addResultColumn(column(valuesBox1, "x", "a1"))
                .addResultColumn(column(newLowerRight, "a", "a2"))
                .addResultColumn(column(newLowerLeft, "b", "b1"))
                .addResultColumn(column(valuesBox2, "y", "b2"))
                .addResultColumn(column(newLowerLeft, "c", "c1"))
                .addResultColumn(column(newLowerRight, "c", "c2"))
                .addResultColumn(column(newLowerLeft, "d", "d1"))
                .addResultColumn(column(newLowerRight, "d", "d2"))
                .addPredicate(fieldPredicate(valuesBox1, "x", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(newLowerRight, "a"))))
                .addPredicate(fieldPredicate(newLowerLeft, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(valuesBox2, "y"))))
                .build().buildSelect();

        testHelper.assertYields(upper, expected);
    }
}
