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
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.PlannerStage;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalDistinctExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUniqueExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ExistsPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.debug.DebuggerWithSymbolTables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

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
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.join;

/**
 * Tests of the {@link PredicatePushDownRule}. These operate by constructing expressions that
 * we should be able to push down, and then validating that the rule produces a transformed
 * expression that makes sense.
 */
public class PredicatePushDownRuleTest {
    @Nonnull
    private static final PredicatePushDownRule rule = new PredicatePushDownRule();
    @Nonnull
    private static final RuleTestHelper testHelper = new RuleTestHelper(rule);

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

    /**
     * Test a simple rewrite of a single parameter predicate with the existence of other predicates. It should go from:
     * <pre>{@code
     * SELECT a FROM (SELECT a, b, d FROM T) WHERE a = 42 AND EXISTS (SELECT alpha FROM TAU)
     * }</pre>
     * <p>
     * And become:
     * </p>
     * <pre>{@code
     * SELECT a FROM (SELECT a, b FROM T WHERE a = 42) WHERE EXISTS (SELECT alpha FROM TAU)
     * }</pre>
     */
    @Test
    void pushDownOnePredicateOfMultiple() {
        Quantifier baseQun = baseT();

        Quantifier lowerQun = forEach(selectWithPredicates(
                baseQun, ImmutableList.of("a", "b")
        ));
        Quantifier existentialQun = exists(selectWithPredicates(
                baseTau(), ImmutableList.of("alpha")
        ));

        GraphExpansion.Builder builder = GraphExpansion.builder().addQuantifier(lowerQun).addQuantifier(existentialQun);
        builder.addResultColumn(column(lowerQun, "b", "b"));
        builder.addAllPredicates(ImmutableList.of(
                fieldPredicate(lowerQun, "a", EQUALS_42),
                new ExistsPredicate(existentialQun.getAlias())
        ));
        SelectExpression higher = builder.build().buildSelect();


        Quantifier newLowerQun = forEach(selectWithPredicates(
                baseQun, ImmutableList.of("a", "b"),
                fieldPredicate(baseQun, "a", EQUALS_42)
        ));

        GraphExpansion.Builder newBuilder = GraphExpansion.builder().addQuantifier(newLowerQun).addQuantifier(existentialQun);
        newBuilder.addResultColumn(column(newLowerQun, "b", "b"));
        newBuilder.addAllPredicates(ImmutableList.of(
                new ExistsPredicate(existentialQun.getAlias())
        ));
        SelectExpression newHigher = newBuilder.build().buildSelect();

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
     * Test when we try to push a predicate that cannot be pushed down. In this case, we have
     * a query like:
     * <pre>{@code
     * SELECT a, b, c FROM t WHERE b > 'hello'
     * }</pre>
     * <p>
     * In this case, the QGM contains a {@link SelectExpression} on top of a
     * {@link com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression}
     * on top of a {@link com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression}.
     * The predicate cannot be pushed through the type filter, and so we expect nothing to be returned.
     * </p>
     */
    @Test
    void cannotPushDownPredicate() {
        Quantifier baseQun = baseT();
        SelectExpression singleExpression = selectWithPredicates(
                baseQun, List.of("a", "b", "c"),
                fieldPredicate(baseQun, "b", GREATER_THAN_HELLO));

        testHelper.assertYieldsNothing(singleExpression, true);
    }

    /**
     * Test what happens when there is a reference with multiple expressions that can accept a pushed
     * predicate. If that is the case, during the rewrite, the predicate should be pushed down to
     * all children of the original expression.
     */
    @Test
    void canPushDownToMultipleChildren() {
        Quantifier baseQun = baseT();

        SelectExpression lower1 = selectWithPredicates(
                baseQun, List.of("a", "b", "c")
        );
        SelectExpression lower2 = selectWithPredicates(
                baseQun, List.of("a", "b", "c"),
                new ConstantPredicate(true)
        );
        Reference lowerRef = Reference.ofFinalExpressions(PlannerStage.INITIAL, ImmutableSet.of(lower1, lower2));
        Quantifier lowerQun = Quantifier.forEach(lowerRef);

        SelectExpression higher = selectWithPredicates(
                lowerQun, List.of("b", "c"),
                fieldPredicate(lowerQun, "a", EQUALS_42)
        );

        SelectExpression newLower1 = selectWithPredicates(
                baseQun, List.of("a", "b", "c"),
                fieldPredicate(baseQun, "a", EQUALS_42)
        );
        SelectExpression newLower2 = selectWithPredicates(
                baseQun, List.of("a", "b", "c"),
                new ConstantPredicate(true),
                fieldPredicate(baseQun, "a", EQUALS_42)
        );
        Reference newLowerRef = Reference.ofFinalExpressions(PlannerStage.CANONICAL, ImmutableSet.of(newLower1, newLower2));
        Quantifier newLowerQun = Quantifier.forEach(newLowerRef);

        SelectExpression newHigher = selectWithPredicates(
                newLowerQun, List.of("b", "c")
        );

        testHelper.assertYields(higher, newHigher);
    }

    /**
     * Test what happens if we have multiple children, some of whom can push down the predicate
     * and some of whom can't. In this case, we expect it to create a new reference that contains
     * only the children that accept the predicate.
     */
    @Test
    void canPushDownToSomeChildren() {
        Quantifier baseQun = baseT();

        // Add a second expression to the baseQun quantifier. It is identical
        // to the original quantifier contents, except it inserts an additional
        // select (which does no filtering or projection).
        Quantifier baseQun2 = baseT();
        SelectExpression selectAllT = selectWithPredicates(baseQun2);
        baseQun.getRangesOver().insertFinalExpression(selectAllT);

        SelectExpression higher = selectWithPredicates(
                baseQun, List.of("a", "c"),
                fieldPredicate(baseQun, "b", EQUALS_PARAM));

        Quantifier newLowerQun = forEach(selectWithPredicates(baseQun2,
                fieldPredicate(baseQun2, "b", EQUALS_PARAM)
        ));
        SelectExpression newHigher = selectWithPredicates(
                newLowerQun, List.of("a", "c")
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
     * Test a rewrite of a predicate that has been unnested by projections. So:
     * <pre>{@code
     * SELECT a, b, one FROM (SELECT a, b, p.one, p.two FROM T) WHERE two = ?
     * }</pre>
     * <p>
     * Becomes:
     * </p>
     * <pre>{@code
     * SELECT a, b, one FROM (SELECT a, b p.one, p.two FROM T WHERE p.two = ?)
     * }</pre>
     */
    @Test
    void pushDownPredicateOnScalarNested() {
        Quantifier baseQun = baseT();

        Quantifier lowerQun = forEach(selectWithPredicates(
                baseQun, ImmutableMap.of("a", "a", "b", "b", "e.one", "one", "e.two", "two")
        ));
        SelectExpression higher = selectWithPredicates(
                lowerQun, List.of("a", "b", "one"),
                fieldPredicate(lowerQun, "two", EQUALS_PARAM)
        );

        Quantifier newLowerQun = forEach(selectWithPredicates(
                baseQun, ImmutableMap.of("a", "a", "b", "b", "e.one", "one", "e.two", "two"),
                fieldPredicate(baseQun, "e.two", EQUALS_PARAM)
        ));
        SelectExpression newHigher = selectWithPredicates(
                newLowerQun, List.of("a", "b", "one")
        );

        testHelper.assertYields(higher, newHigher);
    }

    @Test
    void pushDownPredicateOnRepeatedNested() {
        Quantifier baseQun = baseT();
        Quantifier explodeQun = forEach(new ExplodeExpression(fieldValue(baseQun, "g")));

        Quantifier nestedSelectQun = forEach(selectWithPredicates(
                explodeQun, List.of("two", "three")
        ));
        SelectExpression topSelect = join(baseQun, nestedSelectQun)
                .addResultColumn(projectColumn(baseQun, "a"))
                .addResultColumn(projectColumn(nestedSelectQun, "three"))
                .addPredicate(fieldPredicate(baseQun, "b", GREATER_THAN_HELLO))
                .addPredicate(fieldPredicate(nestedSelectQun, "two", EQUALS_PARAM))
                .build()
                .buildSelect();

        Quantifier newNestedSelectQun = forEach(selectWithPredicates(
                explodeQun, List.of("two", "three"),
                fieldPredicate(explodeQun, "two", EQUALS_PARAM)
        ));
        SelectExpression newTopSelect = join(baseQun, newNestedSelectQun)
                .addResultColumn(projectColumn(baseQun, "a"))
                .addResultColumn(projectColumn(newNestedSelectQun, "three"))
                .addPredicate(fieldPredicate(baseQun, "b", GREATER_THAN_HELLO))
                .build()
                .buildSelect();

        testHelper.assertYields(topSelect, newTopSelect);
    }

    @Test
    void doNotPushDownToExistential() {
        Quantifier baseQun = baseT();
        Quantifier explodeQun = forEach(new ExplodeExpression(fieldValue(baseQun, "g")));

        Quantifier nestedExistsQun = exists(selectWithPredicates(
                explodeQun,
                fieldPredicate(explodeQun, "two", GREATER_THAN_HELLO)
        ));
        SelectExpression topSelect = join(baseQun, nestedExistsQun)
                .addResultColumn(projectColumn(baseQun, "a"))
                .addPredicate(new ExistsPredicate(nestedExistsQun.getAlias()))
                .addPredicate(fieldPredicate(baseQun, "b", GREATER_THAN_HELLO))
                .build()
                .buildSelect();

        testHelper.assertYieldsNothing(topSelect, true);
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
     * Do not push through a for each quantifier with null-on-empty semantics. The reason for this is that
     * pushing down the predicate can change whether the quantifier returns empty or not. For example,
     * the query in the test is:
     * <pre>{@code
     * SELECT a, c
     *   FROM (SELECT a, b, c FROM t WHERE starts_with(d, 'blah')) OR ELSE NULL
     *   WHERE b = 'bar'
     * }</pre>
     * <p>
     * Suppose there is only one element in {@code t} that satisfies the predicate on {@code d}, and
     * suppose further that {@code b} does not equal {@code "bar"} on that row. Then the initial query
     * would return no results (as the single row would be returned by the lower select and then filtered
     * out by the {@code b = 'bar'} filter). If it were to be rewritten the same way as other select
     * queries are, say:
     * </p>
     * <pre>{@code
     * SELECT a, c
     *   FROM (SELECT a, b, c FROM t WHERE starts_with(d, 'blah') AND b = 'bar') OR ELSE NULL
     * }</pre>
     * <p>
     * Then the underlying query would return zero results, so we'd get back a single {@code null}
     * value back. We could theoretically re-apply the predicate, something like:
     * </p>
     * <pre>{@code
     * SELECT a, c
     *   FROM (SELECT a, b, c FROM t WHERE starts_with(d, 'blah') AND b = 'bar') OR ELSE NULL
     *   WHERE b = 'bar'
     * }</pre>
     * <p>
     * And that works here because the {@code b} predicate can never be true if the underlying quantifier
     * returned an injected {@code null}. However, if the predicate were {@code b IS NULL}, we'd have a harder
     * time, as we'd be unable to validate whether the result from the lower select was {@code null} because
     * there was an all-null row or if the null came from
     * </p>
     */
    @Test
    void doNotPushIntoSelectWithNullOnEmpty() {
        Quantifier baseQun = baseT();

        SelectExpression lowerSelect = selectWithPredicates(baseQun,
                ImmutableList.of("a", "b", "c"),
                fieldPredicate(baseQun, "d", new Comparisons.SimpleComparison(Comparisons.Type.STARTS_WITH, "blah"))
        );
        Quantifier lowerQun = Quantifier.forEachWithNullOnEmpty(Reference.initialOf(lowerSelect));

        SelectExpression higher = selectWithPredicates(lowerQun,
                ImmutableList.of("a", "c"),
                fieldPredicate(lowerQun, "b", new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "bar"))
        );

        // The rule shouldn't even match if there are only null-on-empty quantifiers.
        // If this assert fails because we start inspecting this kind of expression more,
        // that's fine, but we may need additional test coverage
        testHelper.assertYieldsNothing(higher, false);
    }

    /**
     * Similar to {@link #doNotPushIntoSelectWithNullOnEmpty()}, but the predicate is {@code IS NULL}. This has
     * some extra complexity, as if it gets pushed down, that it can end up returning a {@code null} value back,
     * and so we have to guard against the possibility of us evaluating the {@code IS NULL} as {@code true} and
     * returning a result. The query in question here is similar to the one in {@link #doNotPushIntoSelectWithNullOnEmpty()},
     * but the predicate is a null check which means that unlike in the other case, it is definitely not
     * sufficient to re-apply the predicate in the top-level select:
     * <pre>{@code
     * SELECT a, c
     *   FROM (SELECT a, b, c, d, WHERE starts_with(d, 'blah') OR ELSE NULL
     *   WHERE b IS NULL
     * }</pre>
     */
    @Test
    void doNotPushNullCheckIntoSelectWithNullOnEmpty() {
        Quantifier baseQun = baseT();

        SelectExpression lowerSelect = selectWithPredicates(baseQun,
                ImmutableList.of("a", "b", "c", "d"),
                fieldPredicate(baseQun, "d", new Comparisons.SimpleComparison(Comparisons.Type.STARTS_WITH, "blah"))
        );
        Quantifier lowerQun = Quantifier.forEachWithNullOnEmpty(Reference.initialOf(lowerSelect));

        SelectExpression higher = selectWithPredicates(lowerQun,
                ImmutableList.of("a", "c"),
                fieldPredicate(lowerQun, "b", new Comparisons.NullComparison(Comparisons.Type.IS_NULL))
        );

        testHelper.assertYieldsNothing(higher, false);
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

        testHelper.assertYieldsNothing(higher, true);
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

        testHelper.assertYieldsNothing(higher, true);
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
                .addResultColumn(column(t, "a", "a1"))
                .addResultColumn(column(tau, "alpha", "a2"))
                .addResultColumn(column(t, "b", "b"))
                .addResultColumn(column(t, "c", "c1"))
                .addResultColumn(column(tau, "gamma", "c2"))
                .addPredicate(fieldPredicate(t, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(tau, "beta"))))
                .build()
                .buildSelect());
        final SelectExpression higher = selectWithPredicates(originalJoinQun,
                List.of("b", "c1", "c2"),
                fieldPredicate(originalJoinQun, "a1", EQUALS_42),
                fieldPredicate(originalJoinQun, "a2", EQUALS_PARAM));

        final Quantifier newJoinQun = forEach(join(t, tau)
                .addResultColumn(column(t, "a", "a1"))
                .addResultColumn(column(tau, "alpha", "a2"))
                .addResultColumn(column(t, "b", "b"))
                .addResultColumn(column(t, "c", "c1"))
                .addResultColumn(column(tau, "gamma", "c2"))
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

    private <R extends RelationalExpression> void pushThroughSingleton(@Nonnull Function<Quantifier, R> wrap) {
        // Simple case: select on top of wrapped base. Push predicate through expression
        final Quantifier base = baseT();

        Quantifier wrapped = forEach(wrap.apply(base));
        SelectExpression select = selectWithPredicates(
                wrapped, List.of("a", "b"),
                fieldPredicate(wrapped, "d", EQUALS_PARAM)
        );

        Quantifier newInner = forEach(selectWithPredicates(base,
                fieldPredicate(base, "d", EQUALS_PARAM)));
        Quantifier newWrapped = forEach(wrap.apply(newInner));
        SelectExpression newSelect = selectWithPredicates(newWrapped, List.of("a", "b"));

        testHelper.assertYields(select, newSelect);

        // Slightly more involved: the inner select is a quantifier with a null-on-empty quantifier
        Quantifier baseOrEmpty = forEachWithNullOnEmpty(selectWithPredicates(base));
        Quantifier wrappedWithEmpty = forEach(wrap.apply(baseOrEmpty));
        SelectExpression selectWithEmpty = selectWithPredicates(
                wrappedWithEmpty, List.of("c", "d"),
                fieldPredicate(wrappedWithEmpty, "a", EQUALS_42));

        Quantifier newInnerWithEmpty = forEach(selectWithPredicates(baseOrEmpty,
                fieldPredicate(baseOrEmpty, "a", EQUALS_42)));
        Quantifier newWrappedWithEmpty = forEach(wrap.apply(newInnerWithEmpty));
        SelectExpression newSelectWithEmpty = selectWithPredicates(newWrappedWithEmpty, List.of("c", "d"));

        testHelper.assertYields(selectWithEmpty, newSelectWithEmpty);

        // Finally, do not push through if the quantifier itself has a null-on-empty
        Quantifier wrappedOrEmpty = forEachWithNullOnEmpty(wrap.apply(base));
        SelectExpression selectOnEmptyDirectly = selectWithPredicates(
                wrappedOrEmpty, List.of("a", "c"),
                fieldPredicate(wrappedOrEmpty, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(wrappedOrEmpty, "d"))));
        testHelper.assertYieldsNothing(selectOnEmptyDirectly, false);
    }

    /**
     * Validate that predicates can be pushed through a {@link LogicalSortExpression}.
     */
    @Test
    void pushThroughSort() {
        Quantifier tempBase = baseT();

        final TranslationMap toCurrentTranslationMap = TranslationMap
                .rebaseWithAliasMap(AliasMap.ofAliases(tempBase.getAlias(), Quantifier.current()));
        final RequestedOrdering ordering = RequestedOrdering.ofParts(
                ImmutableList.of(
                        new OrderingPart.RequestedOrderingPart(fieldValue(tempBase, "a").translateCorrelations(toCurrentTranslationMap), OrderingPart.RequestedSortOrder.ASCENDING),
                        new OrderingPart.RequestedOrderingPart(fieldValue(tempBase, "c").translateCorrelations(toCurrentTranslationMap), OrderingPart.RequestedSortOrder.DESCENDING)
                ),
                RequestedOrdering.Distinctness.PRESERVE_DISTINCTNESS,
                true,
                ImmutableSet.of()
        );
        pushThroughSingleton(qun -> new LogicalSortExpression(ordering, qun));
    }

    /**
     * Validate that predicates can be pushed through a {@link LogicalDistinctExpression}.
     */
    @Test
    void pushThroughDistinct() {
        pushThroughSingleton(LogicalDistinctExpression::new);
    }

    /**
     * Validate that predicates can be pushed through a {@link LogicalUniqueExpression}.
     */
    @Test
    void pushThroughUnique() {
        pushThroughSingleton(LogicalUniqueExpression::new);
    }

    /**
     * Push predicate on a grouping column through a group by. This is something like:
     * <pre>{@code
     * SELECT b, d, sum(a)
     *  FROM T
     *  GROUP BY b, d
     *  HAVING b = ?param
     * }</pre>
     * <p>
     * And rewrites it as:
     * </p>
     * <pre>{@code
     * SELECT b, d, sum(a)
     *  FROM T
     *  WHERE b = ?param
     *  GROUP BY b, d
     * }</pre>
     * <p>
     * In other words, it moves predicates from the {@code SELECT HAVING} to the {@code SELECT WHERE}.
     * Note that there is a bunch of field re-naming stuff that is elided in the SQL description
     * above but that the rewriting has to handle.
     * </p>
     */
    @Disabled // should work once we add support for pushing through predicates on grouping columns
    @Test
    void pushDownGroupingValuePredicateWithNestedResult() {
        Quantifier base = baseT();

        Quantifier groupByQun = forEach(new GroupByExpression(
                RecordConstructorValue.ofColumns(ImmutableList.of(
                        projectColumn(base, "b"),
                        projectColumn(base, "d")
                )),
                (AggregateValue)new NumericAggregationValue.SumFn().encapsulate(ImmutableList.of(fieldValue(base, "a"))),
                GroupByExpression::nestedResults,
                base
        ));
        SelectExpression selectHaving = selectWithPredicates(groupByQun,
                ImmutableMap.of("_0.b", "b", "_0.d", "d", "_1", "sum"),
                fieldPredicate(groupByQun, "_0.b", EQUALS_PARAM));

        Quantifier newInner = forEach(selectWithPredicates(
                base, fieldPredicate(base, "b", EQUALS_PARAM)
        ));
        Quantifier newGroupByQun = forEach(new GroupByExpression(
                RecordConstructorValue.ofColumns(ImmutableList.of(
                        projectColumn(newInner, "b"),
                        projectColumn(newInner, "d")
                )),
                (AggregateValue)new NumericAggregationValue.SumFn().encapsulate(ImmutableList.of(fieldValue(newInner, "a"))),
                GroupByExpression::nestedResults,
                newInner
        ));
        SelectExpression newSelectHaving = selectWithPredicates(newGroupByQun,
                ImmutableMap.of("_0.b", "b", "_0.d", "d", "_1", "sum"));

        testHelper.assertYields(selectHaving, newSelectHaving);
    }

    /**
     * Push a predicate through a group by expression. This is like {@link #pushDownGroupingValuePredicateWithNestedResult()},
     * but it uses an alternative format for expressing the result value of the group by expression.
     *
     * @see #pushDownGroupingValuePredicateWithNestedResult()
     */
    @Disabled // should work once we add support for pushing through predicates on grouping columns
    @Test
    void pushDownGroupingValuePredicateWithFlattenedResults() {
        final Comparisons.ValueComparison constantComparison = new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, ConstantObjectValue.of(Quantifier.constant(), "1", Type.primitiveType(Type.TypeCode.BYTES)));
        Quantifier base = baseT();

        Quantifier groupByQun = forEach(new GroupByExpression(
                RecordConstructorValue.ofColumns(ImmutableList.of(
                        projectColumn(base, "d"),
                        projectColumn(base, "c")
                )),
                (AggregateValue)new NumericAggregationValue.MaxFn().encapsulate(ImmutableList.of(fieldValue(base, "e.one"))),
                GroupByExpression::flattenedResults,
                base
        ));
        SelectExpression selectHaving = selectWithPredicates(groupByQun,
                ImmutableMap.of("_0", "d", "_1", "c", "_2", "max"),
                fieldPredicate(groupByQun, "_1", constantComparison));

        Quantifier newInner = forEach(selectWithPredicates(
                base, fieldPredicate(base, "c", constantComparison)
        ));
        Quantifier newGroupByQun = forEach(new GroupByExpression(
                RecordConstructorValue.ofColumns(ImmutableList.of(
                        projectColumn(newInner, "d"),
                        projectColumn(newInner, "c")
                )),
                (AggregateValue)new NumericAggregationValue.MaxFn().encapsulate(ImmutableList.of(fieldValue(newInner, "e.one"))),
                GroupByExpression::flattenedResults,
                newInner
        ));
        SelectExpression newSelectHaving = selectWithPredicates(newGroupByQun,
                ImmutableMap.of("_0", "d", "_1", "c", "_2", "max"));

        testHelper.assertYields(selectHaving, newSelectHaving);
    }

    /**
     * Do not push predicate on an aggregate value through a group by. This is something like:
     * <pre>{@code
     * SELECT b, d, sum(a)
     *  FROM T
     *  GROUP BY b, d
     *  HAVING sum(a) < @1
     * }</pre>
     * <p>
     * In this case, because the sum is dependent on the grouping, it would be invalid to do something
     * like:
     * </p>
     * <pre>{@code
     * SELECT b, d, sum(a)
     *  FROM T
     *  WHERE sum(a) < @1
     *  GROUP BY b, d
     * }</pre>
     * <p>
     * Which probably doesn't mean anything at all, but if it does, it isn't semantically equivalent.
     * Nevertheless, it is (approximately) what some buggy versions of predicate push down through a
     * group-by expression would do. Validate that this instead yields nothing.
     * </p>
     */
    @Test
    void doNotPushDownPredicateOnAggregateValueNestedResult() {
        final Comparisons.ValueComparison constantComparison = new Comparisons.ValueComparison(Comparisons.Type.LESS_THAN, ConstantObjectValue.of(Quantifier.constant(), "1", Type.primitiveType(Type.TypeCode.LONG)));
        Quantifier base = baseT();

        Quantifier groupByQun = forEach(new GroupByExpression(
                RecordConstructorValue.ofColumns(ImmutableList.of(
                        projectColumn(base, "b"),
                        projectColumn(base, "c")
                )),
                (AggregateValue)new NumericAggregationValue.SumFn().encapsulate(ImmutableList.of(fieldValue(base, "a"))),
                GroupByExpression::nestedResults,
                base
        ));
        SelectExpression selectHaving = selectWithPredicates(
                groupByQun,
                ImmutableMap.of("_0.b", "b", "_0.c", "c", "_1", "sum"),
                fieldPredicate(groupByQun, "_1", constantComparison)
        );

        testHelper.assertYieldsNothing(selectHaving, true);
    }

    /**
     * Do not push predicate on an aggregate value through a group by. This is like
     * {@link #doNotPushDownPredicateOnAggregateValueNestedResult()}, but it uses an
     * alternative form for the group by expression's result value.
     *
     * @see #doNotPushDownPredicateOnAggregateValueNestedResult()
     */
    @Test
    void doNotPushDownPredicateOnAggregateValueFlattenedResult() {
        final Comparisons.ValueComparison constantComparison = new Comparisons.ValueComparison(Comparisons.Type.LESS_THAN, ConstantObjectValue.of(Quantifier.constant(), "1", Type.primitiveType(Type.TypeCode.LONG)));
        Quantifier base = baseT();

        Quantifier groupByQun = forEach(new GroupByExpression(
                RecordConstructorValue.ofColumns(ImmutableList.of(
                        projectColumn(base, "b"),
                        projectColumn(base, "c")
                )),
                (AggregateValue)new NumericAggregationValue.SumFn().encapsulate(ImmutableList.of(fieldValue(base, "a"))),
                GroupByExpression::flattenedResults,
                base
        ));
        SelectExpression selectHaving = selectWithPredicates(
                groupByQun,
                ImmutableMap.of("_0", "b", "_1", "c", "_2", "sum"),
                fieldPredicate(groupByQun, "_2", constantComparison)
        );

        testHelper.assertYieldsNothing(selectHaving, true);
    }
}
