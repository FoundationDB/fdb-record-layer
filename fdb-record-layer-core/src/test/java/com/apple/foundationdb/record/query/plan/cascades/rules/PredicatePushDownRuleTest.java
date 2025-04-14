/*
 * PredicatePushDownRuleTest.java
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

import com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.debug.DebuggerWithSymbolTables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.apple.foundationdb.record.query.plan.cascades.rules.RewriteRuleTestHelper.EQUALS_42;
import static com.apple.foundationdb.record.query.plan.cascades.rules.RewriteRuleTestHelper.EQUALS_PARAM;
import static com.apple.foundationdb.record.query.plan.cascades.rules.RewriteRuleTestHelper.GREATER_THAN_HELLO;
import static com.apple.foundationdb.record.query.plan.cascades.rules.RewriteRuleTestHelper.baseT;
import static com.apple.foundationdb.record.query.plan.cascades.rules.RewriteRuleTestHelper.baseTau;
import static com.apple.foundationdb.record.query.plan.cascades.rules.RewriteRuleTestHelper.fieldPredicate;
import static com.apple.foundationdb.record.query.plan.cascades.rules.RewriteRuleTestHelper.fieldValue;
import static com.apple.foundationdb.record.query.plan.cascades.rules.RewriteRuleTestHelper.forEach;
import static com.apple.foundationdb.record.query.plan.cascades.rules.RewriteRuleTestHelper.join;
import static com.apple.foundationdb.record.query.plan.cascades.rules.RewriteRuleTestHelper.selectWithPredicates;

/**
 * Tests of the {@link PredicatePushDownRule}. These operate by constructing expressions that
 * we should be able to push down, and then validating that the rule produces a transformed
 * expression that makes sense.
 */
public class PredicatePushDownRuleTest {
    @Nonnull
    private static final PredicatePushDownRule rule = new PredicatePushDownRule();
    @Nonnull
    private static final RewriteRuleTestHelper testHelper = new RewriteRuleTestHelper(rule);

    @BeforeEach
    void setUp() {
        Debugger.setDebugger(DebuggerWithSymbolTables.withSanityChecks());
        Debugger.setup();
    }

    /**
     * Test a simple rewrite of a single predicate. It should go from:
     * <pre>{@code
     * SELECT b FROM (SELECT a, b FROM T) WHERE a = 42
     * }</pre>
     * <p>
     * And become:
     * </p>
     * <pre>{@code
     * SELECT b FROM (SELECT a, b FROM T WHERE a = 42)
     * }</pre>
     */
    @Test
    void pushDownSimplePredicate() {
        Quantifier baseQun = baseT();

        Quantifier lowerQun = forEach(selectWithPredicates(
                baseQun, List.of("a", "b")
        ));
        SelectExpression higher = selectWithPredicates(
                lowerQun, List.of("b"),
                fieldPredicate(lowerQun, "a", EQUALS_42)
        );

        Quantifier newLowerQun = forEach(selectWithPredicates(
                baseQun, List.of("a", "b"),
                fieldPredicate(baseQun, "a", EQUALS_42)
        ));
        SelectExpression newHigher = selectWithPredicates(
                newLowerQun, List.of("b")
        );

        testHelper.assertYields(higher, newHigher);
    }

    @Test
    void pushSimplePredicateIntoLogicalFilter() {
        Quantifier baseQun = baseT();

        Quantifier filtered = forEach(new LogicalFilterExpression(List.of(), baseQun));
        SelectExpression select = selectWithPredicates(
                filtered, List.of("c"),
                fieldPredicate(filtered, "b", GREATER_THAN_HELLO)
        );

        Quantifier newFiltered = forEach(selectWithPredicates(
                baseQun,
                fieldPredicate(baseQun, "b", GREATER_THAN_HELLO)
        ));
        SelectExpression newSelect = selectWithPredicates(
                newFiltered, List.of("c")
        );

        testHelper.assertYields(select, newSelect);
    }

    /**
     * Test a simple rewrite of a single parameter predicate. It should go from:
     * <pre>{@code
     * SELECT a FROM (SELECT a, b, d FROM T) WHERE b = d
     * }</pre>
     * <p>
     * And become:
     * </p>
     * <pre>{@code
     * SELECT a FROM (SELECT a, b FROM T WHERE b = d)
     * }</pre>
     */
    @Test
    void pushDownFieldValuePredicate() {
        Quantifier baseQun = baseT();

        Quantifier lowerQun = forEach(selectWithPredicates(
                baseQun, List.of("a", "b", "d")
        ));
        SelectExpression higher = selectWithPredicates(
                lowerQun, List.of("a"),
                fieldPredicate(lowerQun, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(lowerQun, "d")))
        );

        // Note the pushed down predicate now references the baseQun instead of the lowerQun
        Quantifier newLowerQun = forEach(selectWithPredicates(
                baseQun, List.of("a", "b", "d"),
                fieldPredicate(baseQun, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(baseQun, "d")))
        ));
        SelectExpression newHigher = selectWithPredicates(
                newLowerQun, List.of("a")
        );

        testHelper.assertYields(higher, newHigher);
    }

    /**
     * Test a simple comparison push down of a constant object comparison. It should go from:
     * <pre>{@code
     * SELECT a, b FROM (SELECT a, b, c FROM T) WHERE c = @1
     * }</pre>
     * <p>
     * And become:
     * </p>
     * <pre>{@code
     * SELECT a, b FROM (SELECT a, b, c FROM T WHERE c = @1)
     * }</pre>
     */
    @Test
    void pushDownConstantValuePredicate() {
        Quantifier baseQun = baseT();
        Comparisons.Comparison constantComparison = new Comparisons.ValueComparison(Comparisons.Type.EQUALS, ConstantObjectValue.of(Quantifier.constant(), "1", Type.primitiveType(Type.TypeCode.BYTES, false)));

        Quantifier lowerQun = forEach(selectWithPredicates(
                baseQun, List.of("a", "b", "c")
        ));
        SelectExpression higher = selectWithPredicates(
                lowerQun, List.of("a", "b"),
                fieldPredicate(lowerQun, "c", constantComparison)
        );

        Quantifier newLowerQun = forEach(selectWithPredicates(
                baseQun, List.of("a", "b", "c"),
                fieldPredicate(baseQun, "c", constantComparison)
        ));
        SelectExpression newHigher = selectWithPredicates(
                newLowerQun, List.of("a", "b")
        );

        testHelper.assertYields(higher, newHigher);
    }

    /**
     * Test that when pushing down a predicate to a place that already has a predicate, we retain the existing predicate.
     * <pre>{@code
     * SELECT a FROM (SELECT a, b, c FROM T WHERE b = @1) WHERE c = @2
     * }</pre>
     * <p>
     * And become:
     * </p>
     * <pre>{@code
     * SELECT a FROM (SELECT a, b, c FROM T WHERE b = @1 AND c = @2)
     * }</pre>
     */
    @Test
    void pushDownToPlaceWithExistingPredicates() {
        Quantifier baseQun = baseT();
        Comparisons.Comparison bComparison = new Comparisons.ValueComparison(Comparisons.Type.EQUALS, ConstantObjectValue.of(Quantifier.constant(), "1", Type.primitiveType(Type.TypeCode.BYTES, false)));
        Comparisons.Comparison cComparison = new Comparisons.ValueComparison(Comparisons.Type.EQUALS, ConstantObjectValue.of(Quantifier.constant(), "2", Type.primitiveType(Type.TypeCode.STRING, false)));

        Quantifier lowerQun = forEach(selectWithPredicates(
                baseQun, List.of("a", "b", "c"),
                fieldPredicate(baseQun, "b", bComparison)
        ));
        SelectExpression higher = selectWithPredicates(
                lowerQun, List.of("a"),
                fieldPredicate(lowerQun, "c", cComparison)
        );

        Quantifier newLowerQun = forEach(selectWithPredicates(
                baseQun, List.of("a", "b", "c"),
                fieldPredicate(baseQun, "b", bComparison),
                fieldPredicate(baseQun, "c", cComparison)
        ));
        SelectExpression newHigher = selectWithPredicates(
                newLowerQun, List.of("a")
        );

        testHelper.assertYields(higher, newHigher);
    }

    /**
     * Test a rewrite of a predicate between two fields in the same quantifier. It should go from:
     * <pre>{@code
     * SELECT b FROM (SELECT a, b FROM T) WHERE a = $p
     * }</pre>
     * <p>
     * And become:
     * </p>
     * <pre>{@code
     * SELECT b FROM (SELECT a, b FROM T WHERE a = $p)
     * }</pre>
     */
    @Test
    void pushDownParameterPredicate() {
        Quantifier baseQun = baseT();

        Quantifier lowerQun = forEach(selectWithPredicates(
                baseQun, List.of("a", "b")
        ));
        SelectExpression higher = selectWithPredicates(
                lowerQun, List.of("b"),
                fieldPredicate(lowerQun, "a", EQUALS_PARAM)
        );

        Quantifier newLowerQun = forEach(selectWithPredicates(
                baseQun, List.of("a", "b"),
                fieldPredicate(baseQun, "a", EQUALS_PARAM)
        ));
        SelectExpression newHigher = selectWithPredicates(
                newLowerQun, List.of("b")
        );

        testHelper.assertYields(higher, newHigher);
    }

    /**
     * Push down an OR predicate. This takes a query like:
     * <pre>{@code
     * SELECT a, b FROM (SELECT a, b, c FROM t) WHERE a = $p OR b > 'hello' OR @1 = d
     * }</pre>
     * <p>
     * And modifies it to:
     * </p>
     * <pre>{@code
     * SELECT a, b FROM (SELECT a, b, c FROM t WHERE a = $p OR b > 'hello' OR @1 = d)
     * }</pre>
     */
    @Test
    void pushDownOrPredicate() {
        final ConstantObjectValue constantStr = ConstantObjectValue.of(Quantifier.constant(), "1", Type.primitiveType(Type.TypeCode.STRING, false));
        Quantifier baseQun = baseT();

        Quantifier lowerQun = forEach(selectWithPredicates(
                baseQun, List.of("a", "b", "d")
        ));
        SelectExpression higher = selectWithPredicates(
                lowerQun, List.of("a", "b"),
                OrPredicate.or(List.of(
                        fieldPredicate(lowerQun, "a", EQUALS_PARAM),
                        fieldPredicate(lowerQun, "b", GREATER_THAN_HELLO),
                        constantStr.withComparison(new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(lowerQun, "d")))
                ))
        );

        Quantifier newLowerQun = forEach(selectWithPredicates(
                baseQun, List.of("a", "b", "d"),
                OrPredicate.or(List.of(
                        fieldPredicate(baseQun, "a", EQUALS_PARAM),
                        fieldPredicate(baseQun, "b", GREATER_THAN_HELLO),
                        constantStr.withComparison(new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(baseQun, "d")))
                ))
        ));
        SelectExpression newHigher = selectWithPredicates(
                newLowerQun, List.of("a", "b")
        );

        testHelper.assertYields(higher, newHigher);
    }

    /**
     * Test a simple rewrite of multiple predicates. It should go from:
     * <pre>{@code
     * SELECT b FROM (SELECT a, b FROM T) WHERE a = 42 AND b > 'hello'
     * }</pre>
     * <p>
     * And become:
     * </p>
     * <pre>{@code
     * SELECT b FROM (SELECT a, b FROM T WHERE a = 42 AND b > 'hello')
     * }</pre>
     */
    @Test
    void pushDownMultiplePredicates() {
        Quantifier baseQun = baseT();

        Quantifier lowerQun = forEach(selectWithPredicates(
                baseQun, List.of("a", "b")
        ));
        SelectExpression higher = selectWithPredicates(
                lowerQun, List.of("b"),
                fieldPredicate(lowerQun, "a", EQUALS_42),
                fieldPredicate(lowerQun, "b", GREATER_THAN_HELLO)
        );

        Quantifier newLowerQun = forEach(selectWithPredicates(
                baseQun, List.of("a", "b"),
                fieldPredicate(baseQun, "a", EQUALS_42),
                fieldPredicate(baseQun, "b", GREATER_THAN_HELLO)
        ));
        SelectExpression newHigher = selectWithPredicates(
                newLowerQun, List.of("b")
        );

        testHelper.assertYields(higher, newHigher);
    }

    /**
     * Test pushing down multiple predicates into a logical filter. This takes something like:
     * <pre>{@code
     * SELECT b, c FROM (FILTER t WHERE a = 42) WHERE b > d AND c != @1
     * }</pre>
     * <p>
     * And rewrites it as:
     * </p>
     * <pre>{@code
     * SELECT b, c FROM (SELECT * FROM t WHERE a = 42 AND b > d AND c != @1)
     * }</pre>
     */
    @Test
    void pushDownMultiplePredicatesToLogicalFilter() {
        final ConstantObjectValue constantBytes = ConstantObjectValue.of(Quantifier.constant(), "1", Type.primitiveType(Type.TypeCode.BYTES, false));
        Quantifier baseQun = baseT();

        Quantifier filtered = forEach(new LogicalFilterExpression(List.of(fieldPredicate(baseQun, "a", EQUALS_42)), baseQun));
        SelectExpression select = selectWithPredicates(
                filtered, List.of("b", "c"),
                fieldPredicate(filtered, "b", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, fieldValue(filtered, "d"))),
                fieldPredicate(filtered, "c", new Comparisons.ValueComparison(Comparisons.Type.NOT_EQUALS, constantBytes))
        );

        Quantifier newFiltered = forEach(selectWithPredicates(baseQun,
                fieldPredicate(baseQun, "a", EQUALS_42),
                fieldPredicate(baseQun, "b", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, fieldValue(baseQun, "d"))),
                fieldPredicate(baseQun, "c", new Comparisons.ValueComparison(Comparisons.Type.NOT_EQUALS, constantBytes))
        ));
        SelectExpression newSelect = selectWithPredicates(
                newFiltered, List.of("b", "c")
        );

        testHelper.assertYields(select, newSelect);
    }

    /**
     * Test a rewrite where field names have been moved around by the lower select. So something like:
     * <pre>{@code
     * SELECT y FROM (SELECT a AS x, b AS y FROM T) WHERE x = 42 AND y > 'hello'
     * }</pre>
     * <p>
     * Should become:
     * </p>
     * <pre>{@code
     * SELECT y FROM (SELECT a AS x, b AS y FROM T WHERE a = 42 AND b > 'hello')
     * }</pre>
     */
    @Test
    void testPushDownWithFieldRenames() {
        Quantifier baseQun = baseT();

        Quantifier lowerQun = forEach(selectWithPredicates(
                baseQun, Map.of("a", "x", "b", "y")
        ));
        SelectExpression higher = selectWithPredicates(
                lowerQun, List.of("y"),
                fieldPredicate(lowerQun, "x", EQUALS_42),
                fieldPredicate(lowerQun, "y", GREATER_THAN_HELLO)
        );

        Quantifier newLowerQun = forEach(selectWithPredicates(
                baseQun, Map.of("a", "x", "b", "y"),
                fieldPredicate(baseQun, "a", EQUALS_42),
                fieldPredicate(baseQun, "b", GREATER_THAN_HELLO)
        ));
        SelectExpression newHigher = selectWithPredicates(
                newLowerQun, List.of("y")
        );

        testHelper.assertYields(higher, newHigher);
    }

    /**
     * Test to make sure proper field renaming happens during the push down of predicates with values.
     * So, for an expression like:
     * <pre>{@code
     * SELECT x, y, z FROM (SELECT a AS x, b AS y, d AS z FROM t) WHERE y > z
     * }</pre>
     * <p>
     * Then we expect both {@code y} and {@code z} to be renamed to their original names when pushed down:
     * </p>
     * <pre>{@code
     * SELECT x, y, z FROM (SELECT a AS x, b AS y, d AS z FROM t WHERE b > d)
     * }</pre>
     */
    @Test
    void testRenameFieldComparisonWithValues() {
        Quantifier baseQun = baseT();

        Quantifier lowerQun = forEach(selectWithPredicates(
                baseQun, Map.of("a", "x", "b", "y", "d", "z")
        ));
        SelectExpression higher = selectWithPredicates(
                lowerQun, List.of("x", "y", "z"),
                fieldPredicate(lowerQun, "y", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, fieldValue(lowerQun, "z")))
        );

        Quantifier newLowerQun = forEach(selectWithPredicates(
                baseQun, Map.of("a", "x", "b", "y", "d", "z"),
                fieldPredicate(baseQun, "b", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, fieldValue(baseQun, "d")))
        ));
        SelectExpression newHigher = selectWithPredicates(
                newLowerQun, List.of("x", "y", "z")
        );

        testHelper.assertYields(higher, newHigher);
    }

    /**
     * Test that a predicate used as part of a join (that is, involving more than one quantifier from different legs of a join)
     * is not pushed down. In this case, the query looks something like:
     * <pre>{@code
     * SELECT x.a, y.alpha FROM (SELECT a, b FROM t) AS x, (SELECT alpha, beta FROM tau) AS y WHERE x.b = y.beta
     * }</pre>
     * <p>
     * This does not yield any rewrites as the only predicate is used for joining.
     * </p>
     */
    @Test
    void testDoesNotPushJoinCriteria() {
        Quantifier t = baseT();
        Quantifier tau = baseTau();

        Quantifier tLowQun = forEach(selectWithPredicates(
                t, List.of("a", "b")
        ));
        Quantifier tauLowQun = forEach(selectWithPredicates(
                tau, List.of("alpha", "beta")
        ));
        SelectExpression higher = join(tLowQun, tauLowQun)
                .addResultColumn(FDBQueryGraphTestHelpers.projectColumn(tLowQun, "a"))
                .addResultColumn(FDBQueryGraphTestHelpers.projectColumn(tauLowQun, "alpha"))
                .addPredicate(fieldPredicate(tLowQun, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tauLowQun, "beta"))))
                .build()
                .buildSelect();

        testHelper.assertYieldsNothing(higher);
    }

    /**
     * Test that if we have a join query and an OR predicate that spans multiple legs of the join, it doesn't get pushed down.
     * For example, this query:
     * <pre>{@code
     * SELECT t.b, tau.beta
     *   FROM (SELECT a, b FROM t) t,
     *        (SELECT alpha, beta FROM tau) tau
     *   WHERE t.a = tau.alpha
     *     AND (t.b > 'hello' OR tau.beta > 'hello')
     * }</pre>
     * <p>
     * In this case, there's nothing to do be done, as we can't push the OR to either side, and we can't break
     * up the predicate in some way.
     * </p>
     */
    @Test
    void doesNotPushOrWithMixedJoinElements() {
        Quantifier t = baseT();
        Quantifier tau = baseTau();

        Quantifier tLowQun = forEach(selectWithPredicates(
                t, List.of("a", "b")
        ));
        Quantifier tauLowQun = forEach(selectWithPredicates(
                tau, List.of("alpha", "beta")
        ));
        SelectExpression higher = join(tLowQun, tauLowQun)
                .addResultColumn(FDBQueryGraphTestHelpers.projectColumn(tLowQun, "b"))
                .addResultColumn(FDBQueryGraphTestHelpers.projectColumn(tauLowQun, "beta"))
                .addPredicate(fieldPredicate(tLowQun, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tauLowQun, "alpha"))))
                .addPredicate(OrPredicate.or(List.of(fieldPredicate(tLowQun, "b", GREATER_THAN_HELLO), fieldPredicate(tauLowQun, "beta", GREATER_THAN_HELLO))))
                .build()
                .buildSelect();

        testHelper.assertYieldsNothing(higher);
    }

    /**
     * Test that predicates are pushed down the appropriate leg of a JOIN. For example, given the query:
     * <pre>{@code
     * SELECT t.a, tau.alpha
     *   FROM (SELECT a, b, c FROM t) t,
     *        (SELECT alpha, beta, gamma FROM tau) tau
     *   WHERE t.b = tau.beta AND t.a = @1 AND tau.alpha = @2
     * }</pre>
     * <p>
     * We can push down the two predicates (that aren't the join criteria) to the two legs:
     * </p>
     * <pre>{@code
     * SELECT t.a, tau.alpha
     *   FROM (SELECT a, b, c FROM t WHERE t.a = @1) t,
     *        (SELECT alpha, beta, gamma FROM tau WHERE tau.alpha = @2) tau
     *   WHERE t.b = tau.beta
     * }</pre>
     * <p>
     * Note that each rule invocation only pushes down a single predicate down a single leg, but they wind up
     * with the same final statement at the end.
     * </p>
     */
    @Test
    void testPartitionPredicatesByJoinSource() {
        final Quantifier t = baseT();
        final Quantifier tau = baseTau();

        final Comparisons.Comparison cComparison = new Comparisons.ValueComparison(Comparisons.Type.EQUALS, ConstantObjectValue.of(Quantifier.constant(), "1", Type.primitiveType(Type.TypeCode.BYTES, false)));
        final Comparisons.Comparison gammaComparison = new Comparisons.ValueComparison(Comparisons.Type.EQUALS, ConstantObjectValue.of(Quantifier.constant(), "2", Type.primitiveType(Type.TypeCode.BYTES, false)));

        final Quantifier tLowQun = forEach(selectWithPredicates(
                t, List.of("a", "b", "c")
        ));
        final Quantifier tauLowQun = forEach(selectWithPredicates(
                tau, List.of("alpha", "beta", "gamma")
        ));
        SelectExpression higher = join(tLowQun, tauLowQun)
                .addResultColumn(FDBQueryGraphTestHelpers.projectColumn(tLowQun, "a"))
                .addResultColumn(FDBQueryGraphTestHelpers.projectColumn(tauLowQun, "alpha"))
                .addPredicate(fieldPredicate(tLowQun, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tauLowQun, "beta"))))
                .addPredicate(fieldPredicate(tLowQun, "c", cComparison))
                .addPredicate(fieldPredicate(tauLowQun, "gamma", gammaComparison))
                .build()
                .buildSelect();

        // As we only invoke the rule once, we get two transformations of the join, one which pushes down the predicate
        // on table t and the other that pushes down the predicate on table tau
        final Quantifier newTLowQun = forEach(selectWithPredicates(
                t, List.of("a", "b", "c"),
                fieldPredicate(t, "c", cComparison)
        ));
        final Quantifier newTauLowQun = forEach(selectWithPredicates(
                tau, List.of("alpha", "beta", "gamma"),
                fieldPredicate(tau, "gamma", gammaComparison)
        ));
        final SelectExpression newHigherWithNewT = join(newTLowQun, tauLowQun)
                .addResultColumn(FDBQueryGraphTestHelpers.projectColumn(newTLowQun, "a"))
                .addResultColumn(FDBQueryGraphTestHelpers.projectColumn(tauLowQun, "alpha"))
                .addPredicate(fieldPredicate(newTLowQun, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tauLowQun, "beta"))))
                .addPredicate(fieldPredicate(tauLowQun, "gamma", gammaComparison))
                .build()
                .buildSelect();
        final SelectExpression newHigherWithNewTau = join(tLowQun, newTauLowQun)
                .addResultColumn(FDBQueryGraphTestHelpers.projectColumn(tLowQun, "a"))
                .addResultColumn(FDBQueryGraphTestHelpers.projectColumn(newTauLowQun, "alpha"))
                .addPredicate(fieldPredicate(tLowQun, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(newTauLowQun, "beta"))))
                .addPredicate(fieldPredicate(tLowQun, "c", cComparison))
                .build()
                .buildSelect();

        testHelper.assertYields(higher, newHigherWithNewT, newHigherWithNewTau);

        // If the rule is pushed to either of the new expressions, we should get a final version that pushes all
        // predicates down to both sides
        final SelectExpression newestHigher = join(newTLowQun, newTauLowQun)
                .addResultColumn(FDBQueryGraphTestHelpers.projectColumn(newTLowQun, "a"))
                .addResultColumn(FDBQueryGraphTestHelpers.projectColumn(newTauLowQun, "alpha"))
                .addPredicate(fieldPredicate(newTLowQun, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(newTauLowQun, "beta"))))
                .build()
                .buildSelect();

        testHelper.assertYields(newHigherWithNewT, newestHigher);
        testHelper.assertYields(newHigherWithNewTau, newestHigher);
    }

    /**
     * Test rewriting predicates on a select on top of a join. This validates that the predicates from the original
     * are applied to the correct join source. This takes this expression:
     * <pre>{@code
     * SELECT b, c1, c2 FROM (
     *   SELECT t.a AS a1, tau.alpha AS a2, t.b AS b, t.c AS c1, tau.gamma AS c2
     *   FROM t, tau
     *   WHERE t.b = tau.beta
     * ) WHERE a1 = 42 AND a2 = $param
     * }</pre>
     * <p>
     * And rewrites it as:
     * </p>
     * <pre>{@code
     * SELECT b, c1, c2 FROM (
     *   SELECT t.a AS a1, tau.alpha AS a2, t.b AS b, t.c AS c1, tau.gamma AS c2
     *   FROM t, tau
     *   WHERE t.b = tau.beta AND t.a = 42 AND tau.alpha = $param
     * )
     * }</pre>
     * <p>
     * Note the translation of {@code a1}  and {@code a2} to their original names and sources.
     * </p>
     */
    @Test
    void rewritePushedPredicatesOntoAppropriateJoinSource() {
        final Quantifier t = baseT();
        final Quantifier tau = baseTau();

        final Quantifier originalJoinQun = forEach(join(t, tau)
                .addResultColumn(Column.of(Optional.of("a1"), fieldValue(t, "a")))
                .addResultColumn(Column.of(Optional.of("a2"), fieldValue(tau, "alpha")))
                .addResultColumn(Column.of(Optional.of("b"), fieldValue(t, "b")))
                .addResultColumn(Column.of(Optional.of("c1"), fieldValue(t, "c")))
                .addResultColumn(Column.of(Optional.of("c2"), fieldValue(tau, "gamma")))
                .addPredicate(fieldPredicate(t, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tau, "beta"))))
                .build()
                .buildSelect());
        final SelectExpression higher = selectWithPredicates(originalJoinQun,
                List.of("b", "c1", "c2"),
                fieldPredicate(originalJoinQun, "a1", EQUALS_42),
                fieldPredicate(originalJoinQun, "a2", EQUALS_PARAM));

        final Quantifier newJoinQun = forEach(join(t, tau)
                .addResultColumn(Column.of(Optional.of("a1"), fieldValue(t, "a")))
                .addResultColumn(Column.of(Optional.of("a2"), fieldValue(tau, "alpha")))
                .addResultColumn(Column.of(Optional.of("b"), fieldValue(t, "b")))
                .addResultColumn(Column.of(Optional.of("c1"), fieldValue(t, "c")))
                .addResultColumn(Column.of(Optional.of("c2"), fieldValue(tau, "gamma")))
                .addPredicate(fieldPredicate(t, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tau, "beta"))))
                .addPredicate(fieldPredicate(t, "a", EQUALS_42))
                .addPredicate(fieldPredicate(tau, "alpha", EQUALS_PARAM))
                .build()
                .buildSelect());
        final SelectExpression newHigher = selectWithPredicates(newJoinQun,
                List.of("b", "c1", "c2"));

        testHelper.assertYields(higher, newHigher);
    }

    /**
     * Push a single predicate through a union. This transforms the statement:
     * <pre>{@code
     * SELECT y, z
     *   FROM (
     *      SELECT a AS x, b AS y, c AS z FROM t
     *      UNION
     *      SELECT alpha AS x, beta AS y, gamma AS z FROM tau
     *   )
     *   WHERE x = 42
     * }</pre>
     * <p>
     * And rewrites it as:
     * </p>
     * <pre>{@code
     * SELECT y, z
     *   FROM (
     *      SELECT * FROM (SELECT a AS x, b AS y, c AS z FROM t) WHERE x = 42
     *      UNION
     *      SELECT * FROM (SELECT alpha AS x, beta AS y, gamma AS z FROM tau) WHERE x = 42
     *   )
     * }</pre>
     * <p>
     * Note the introduction of additional {@code SELECT}s in the union legs. These come from the fact
     * that the initial rewrite only pushes the predicate down one level. When run in
     * the full planner, the predicate will be pushed down all the way, and additional simplification
     * rules will ensure that the intermediate {@code SELECT}s will be merged.
     * </p>
     */
    @Test
    void pushSimplePredicateThroughUnion() {
        final Quantifier t = baseT();
        final Quantifier tau = baseTau();

        final Quantifier selectFromTQun = forEach(selectWithPredicates(
                t, ImmutableMap.of("a", "x", "b", "y", "c", "z")
        ));
        final Quantifier selectFromTauQun = forEach(selectWithPredicates(
                tau, ImmutableMap.of("alpha", "x", "beta", "y", "gamma", "z")
        ));
        final Quantifier unionQun = forEach(new LogicalUnionExpression(ImmutableList.of(selectFromTQun, selectFromTauQun)));
        final SelectExpression topSelect = selectWithPredicates(
                unionQun, List.of("y", "z"),
                fieldPredicate(unionQun, "x", EQUALS_42)
        );

        final Quantifier newUnionQun = forEach(new LogicalUnionExpression(ImmutableList.of(
                forEach(selectWithPredicates(selectFromTQun, fieldPredicate(selectFromTQun, "x", EQUALS_42))),
                forEach(selectWithPredicates(selectFromTauQun, fieldPredicate(selectFromTauQun, "x", EQUALS_42)))
        )));
        final SelectExpression newTopSelect = selectWithPredicates(
                newUnionQun, List.of("y", "z")
        );

        testHelper.assertYields(topSelect, newTopSelect);
    }
}
