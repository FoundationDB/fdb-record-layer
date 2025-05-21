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
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalDistinctExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ExistsPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.debug.DebuggerWithSymbolTables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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
import static com.apple.foundationdb.record.query.plan.cascades.rules.RuleTestHelper.EQUALS_42;
import static com.apple.foundationdb.record.query.plan.cascades.rules.RuleTestHelper.EQUALS_PARAM;
import static com.apple.foundationdb.record.query.plan.cascades.rules.RuleTestHelper.GREATER_THAN_HELLO;
import static com.apple.foundationdb.record.query.plan.cascades.rules.RuleTestHelper.baseT;
import static com.apple.foundationdb.record.query.plan.cascades.rules.RuleTestHelper.baseTau;
import static com.apple.foundationdb.record.query.plan.cascades.rules.RuleTestHelper.join;
import static org.assertj.core.api.Assertions.assertThat;

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

        Quantifier explodeGQun = forEach(new ExplodeExpression(fieldValue(baseQun, "g")));
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

        Quantifier explodeGQun = forEach(new ExplodeExpression(fieldValue(baseQun, "g")));
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
        testHelper.assertYieldsNothing(upper, false);
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
     * Because of the way expressions get merged, this does happen by applying the rule multiple times.
     * The first time, we create intermediate forms that pull up one join or the other, and then we finally
     * add in the combined form later.
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
        // Invoking the rule, we should get two rewrites: one of which pulls up the first join, and the second of
        // which pulls up the second join
        //
        final SelectExpression pullUpLeftJoin = join(tQun, tauQun, join2Qun)
                .addResultColumn(column(tQun, "a", "a0"))
                .addResultColumn(column(tauQun, "alpha", "alpha0"))
                .addResultColumn(projectColumn(join2Qun, "a1"))
                .addResultColumn(projectColumn(join2Qun, "a2"))
                .addResultColumn(column(join2Qun, "alpha", "alpha1"))
                .addPredicate(fieldPredicate(tQun, "c", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, fieldValue(tauQun, "gamma"))))
                .addPredicate(fieldPredicate(tQun, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(join2Qun, "b1"))))
                .addPredicate(fieldPredicate(tQun, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(join2Qun, "b2"))))
                .addPredicate(fieldPredicate(tauQun, "beta", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(join2Qun, "beta"))))
                .build().buildSelect();
        final SelectExpression pullUpRightJoin = join(join1Qun, tQun2, tQun3, tauQun2)
                .addResultColumn(column(join1Qun, "a", "a0"))
                .addResultColumn(column(join1Qun, "alpha", "alpha0"))
                .addResultColumn(column(tQun2, "a", "a1"))
                .addResultColumn(column(tQun3, "a", "a2"))
                .addResultColumn(column(tauQun2, "alpha", "alpha1"))
                .addPredicate(fieldPredicate(tQun2, "d", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tQun3, "d"))))
                .addPredicate(fieldPredicate(tQun2, "d", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tauQun2, "delta"))))
                .addPredicate(fieldPredicate(join1Qun, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tQun2, "b"))))
                .addPredicate(fieldPredicate(join1Qun, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tQun3, "b"))))
                .addPredicate(fieldPredicate(join1Qun, "beta", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tauQun2, "beta"))))
                .build().buildSelect();

        testHelper.assertYields(upperJoin, pullUpLeftJoin, pullUpRightJoin);

        //
        // Invoking a rule a second time, we should get the combined five-way join.
        // Note that the order of predicates depends on which comes first
        //
        final SelectExpression combinedLFirst = join(tQun, tauQun, tQun2, tQun3, tauQun2)
                .addResultColumn(column(tQun, "a", "a0"))
                .addResultColumn(column(tauQun, "alpha", "alpha0"))
                .addResultColumn(column(tQun2, "a", "a1"))
                .addResultColumn(column(tQun3, "a", "a2"))
                .addResultColumn(column(tauQun2, "alpha", "alpha1"))
                .addPredicate(fieldPredicate(tQun2, "d", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tQun3, "d"))))
                .addPredicate(fieldPredicate(tQun2, "d", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tauQun2, "delta"))))
                .addPredicate(fieldPredicate(tQun, "c", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, fieldValue(tauQun, "gamma"))))
                .addPredicate(fieldPredicate(tQun, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tQun2, "b"))))
                .addPredicate(fieldPredicate(tQun, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tQun3, "b"))))
                .addPredicate(fieldPredicate(tauQun, "beta", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tauQun2, "beta"))))
                .build().buildSelect();
        final SelectExpression combinedRFirst = join(tQun, tauQun, tQun2, tQun3, tauQun2)
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

        // For better or worse, these two expressions are considered different because their predicates are in a
        // different order. We'll have to rely on the cost model to reliably collapse these to the same single variant
        assertThat(pullUpLeftJoin)
                .as("selects with different predicate order should be considered unequal")
                .isNotEqualTo(pullUpRightJoin);

        // These two expressions are
        testHelper.assertYields(pullUpLeftJoin, combinedLFirst);
        testHelper.assertYields(pullUpRightJoin, combinedRFirst);
    }

    /**
     * Test the case where we have multiple variants underneath a reference. We create additional variants
     * for each such element at a higher level.
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

        final Reference lowerRef = Reference.ofExploratoryExpressions(PlannerStage.CANONICAL, ImmutableSet.of(expr1, expr2, expr3, expr4));
        final Quantifier lowerQun = Quantifier.forEach(lowerRef);

        // Select on top of the lower qun
        final SelectExpression upper = selectWithPredicates(lowerQun, ImmutableList.of("c", "d"),
                fieldPredicate(lowerQun, "d", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, fieldValue(lowerQun, "b"))));

        // From variants expr1 and expr2. The two expressions produce the same expression when merged which should then be collapsed by the memo structure
        final SelectExpression merged1 = selectWithPredicates(baseQun, ImmutableList.of("c", "d"),
                fieldPredicate(baseQun, "a", EQUALS_42),
                fieldPredicate(baseQun, "b", EQUALS_PARAM),
                fieldPredicate(baseQun, "d", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, fieldValue(baseQun, "b"))));
        // From variant expr3. It is distinct from the other one, so it _should_ appear in the final memo
        final SelectExpression merged2 = selectWithPredicates(paramPredQun, ImmutableList.of("c", "d"),
                fieldPredicate(paramPredQun, "a", EQUALS_42),
                fieldPredicate(paramPredQun, "d", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, fieldValue(paramPredQun, "b"))));
        // Variant expr4 should not yield anything

        testHelper.assertYields(upper, merged1, merged2);
    }

    /**
     * Test of what happens if merging the selects results in the same quantifier being added multiple
     * times to a combined {@link SelectExpression}. This is done by creating a base quantifier, and then
     * creating a join between that base quantifier and a select on top of it. Merging attempts to
     * add the same quantifier multiple times to the upper select.
     */
    @Disabled
    @Test
    void mergeWithSameDownstreamQuantifier() {
        final Quantifier baseQun = baseT();

        final Quantifier lowerQun1 = forEach(selectWithPredicates(
                baseQun, ImmutableList.of("b", "c", "d"),
                fieldPredicate(baseQun, "a", EQUALS_42)));

        final SelectExpression upper = join(lowerQun1, baseQun)
                .addResultColumn(projectColumn(baseQun, "a"))
                .addResultColumn(projectColumn(lowerQun1, "b"))
                .addResultColumn(projectColumn(lowerQun1, "c"))
                .addPredicate(fieldPredicate(lowerQun1, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(baseQun, "d"))))
                .build().buildSelect();

        // This currently fails with an IllegalArgumentException as the base quantifier is added to
        // a new merged select box. It's unclear what the semantics of this even should be
        testHelper.assertYieldsNothing(upper, true);
    }
}
