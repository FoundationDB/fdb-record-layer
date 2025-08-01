/*
 * DecorrelateValuesRuleTest.java
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
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.PlannerStage;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalDistinctExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.TableFunctionExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ExistsPredicate;
import com.apple.foundationdb.record.query.plan.cascades.properties.CardinalitiesProperty;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.AndOrValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.CountValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RangeValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.StreamingValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.debug.DebuggerWithSymbolTables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Map;
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
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.baseT;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.baseTau;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.join;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.rangeOneQun;
import static com.apple.foundationdb.record.query.plan.cascades.RuleTestHelper.valuesQun;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Tests of the {@link DecorrelateValuesRule}. These manipulate the values box in order to ensure that
 * the values are pushed down into the tree.
 */
class DecorrelateValuesRuleTest {
    @Nonnull
    private static final DecorrelateValuesRule rule = new DecorrelateValuesRule();
    @Nonnull
    private static final RuleTestHelper testHelper = new RuleTestHelper(rule);

    @BeforeEach
    void setUpDebugger() {
        Debugger.setDebugger(DebuggerWithSymbolTables.withSanityChecks());
        Debugger.setup();
    }

    /**
     * Merge a values box into a select. Here, we have something like:
     * <pre>{@code
     * SELECT T.b, V.y FROM T, values(x = 3, y = @0) as V WHERE T.a = V.x
     * }</pre>
     * <p>
     * And it rewrites it as:
     * </p>
     * <pre>{@code
     * SELECT b, @0 as y FROM T WHERE T.a = 3
     * }</pre>
     * <p>
     * That is, it rewrites the predicate and the result value to inline the values, and then
     * it removes the values box as that box is no longer necessary.
     * </p>
     */
    @Test
    void simpleDecorrelation() {
        final ConstantObjectValue cov = ConstantObjectValue.of(Quantifier.constant(), "0", Type.primitiveType(Type.TypeCode.BOOLEAN, false));
        final Quantifier baseQun = baseT();
        final Quantifier valuesQun = valuesQun(ImmutableMap.of("x", LiteralValue.ofScalar(3L), "y", cov));

        final SelectExpression selectExpression = join(baseQun, valuesQun)
                .addResultColumn(projectColumn(baseQun, "b"))
                .addResultColumn(projectColumn(valuesQun, "y"))
                .addPredicate(fieldPredicate(baseQun, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(valuesQun, "x"))))
                .build().buildSelect();

        final SelectExpression expected = GraphExpansion.builder()
                .addQuantifier(baseQun)
                .addResultColumn(projectColumn(baseQun, "b"))
                .addResultColumn(Column.of(Optional.of("y"), cov))
                .addPredicate(fieldPredicate(baseQun, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, LiteralValue.ofScalar(3L))))
                .build().buildSelect();

        testHelper.assertYields(selectExpression, expected);
    }

    /**
     * Decorrelate multiple values boxes at once. This is a similar setup to {@link #simpleDecorrelation()}, but
     * we start with multiple values boxes. The rule should take all of those and operate on them at the same time.
     * In this case, that means in-lining the values in the result and predicates, after which the values can be
     * removed.
     */
    @Test
    void multipleValuesDecorrelation() {
        final Quantifier baseQun = baseT();
        final Quantifier otherQun = baseTau();
        final Value alphaPlus10 = (Value) new ArithmeticValue.AddFn().encapsulate(ImmutableList.of(fieldValue(otherQun, "alpha"), LiteralValue.ofScalar(10L)));

        final Quantifier values1 = valuesQun(LiteralValue.ofScalar(42L));
        final Quantifier values2 = valuesQun(ImmutableMap.of("x", alphaPlus10, "y", LiteralValue.ofScalar("hello")));
        final Quantifier values3 = valuesQun(ImmutableMap.of("z", fieldValue(otherQun, "gamma")));

        final SelectExpression selectExpression = join(baseQun, values1, values2, values3)
                .addResultColumn(projectColumn(baseQun, "a"))
                .addResultColumn(projectColumn(baseQun, "c"))
                .addResultColumn(projectColumn(values3, "z"))
                .addPredicate(fieldPredicate(baseQun, "a", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, fieldValue(values2, "x"))))
                .addPredicate(fieldPredicate(baseQun, "a", new Comparisons.ValueComparison(Comparisons.Type.LESS_THAN, values1.getFlowedObjectValue())))
                .addPredicate(fieldPredicate(baseQun, "b", new Comparisons.ValueComparison(Comparisons.Type.STARTS_WITH, fieldValue(values2, "y"))))
                .addPredicate(fieldPredicate(baseQun, "c", new Comparisons.ValueComparison(Comparisons.Type.NOT_EQUALS, fieldValue(values3, "z"))))
                .build().buildSelect();

        final SelectExpression expected = GraphExpansion.builder()
                .addQuantifier(baseQun)
                .addResultColumn(projectColumn(baseQun, "a"))
                .addResultColumn(projectColumn(baseQun, "c"))
                .addResultColumn(column(otherQun, "gamma", "z"))
                .addPredicate(fieldPredicate(baseQun, "a", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, alphaPlus10)))
                .addPredicate(fieldPredicate(baseQun, "a", new Comparisons.ValueComparison(Comparisons.Type.LESS_THAN, LiteralValue.ofScalar(42L))))
                .addPredicate(fieldPredicate(baseQun, "b", new Comparisons.ValueComparison(Comparisons.Type.STARTS_WITH, LiteralValue.ofScalar("hello"))))
                .addPredicate(fieldPredicate(baseQun, "c", new Comparisons.ValueComparison(Comparisons.Type.NOT_EQUALS, fieldValue(otherQun, "gamma"))))
                .build().buildSelect();

        testHelper.assertYields(selectExpression, expected);
    }

    /**
     * Push a value into a child select expression. In particular, this actually looks like the kind of expression
     * one would get if one defined a table valued function:
     * <pre>{@code
     * CREATE FUNCTION foo(x string, y bytes)
     *   AS SELECT a, c, d FROM T where b = @x AND @y >= T.c
     * }</pre>
     * <p>
     * And then invoked the function with arguments like {@code foo('hello', @0)}. In that case, function execution
     * would get rewritten into a join between a values box and the original statement in a manner like:
     * </p>
     * <pre>{@code
     *    SELECT t.* FROM
     *      values(x = 'hello', y = @0) AS v,
     *      (SELECT a, c, d FROM T WHERE b = v.x AND v.y >= c) AS t
     * }</pre>
     * <p>
     * Which is the kind of expression we test out here. The result of this is to push the values into the rhs of the
     * join:
     * </p>
     * <pre>{@code
     *    SELECT t.* FROM
     *      (SELECT a, c, d FROM values(x = 'hello', y = @0) AS v, T WHERE b = v.x AND v.y >= c) AS t
     * }</pre>
     * <p>
     * Note that this is now in the form of expressions contemplated by the test {@link #simpleDecorrelation()}, and
     * that a further application of the rule would remove the values box in its entirety.
     * </p>
     */
    @Test
    void pushIntoChildSelect() {
        final Value cov = ConstantObjectValue.of(Quantifier.constant(), "0", Type.primitiveType(Type.TypeCode.BYTES, false));
        final Quantifier valueBox = valuesQun(ImmutableMap.of("x", LiteralValue.ofScalar("hello"), "y", cov));

        final Quantifier baseQun = baseT();
        final Quantifier childSelectQun = forEach(selectWithPredicates(baseQun,
                ImmutableList.of("a", "c", "d"),
                fieldPredicate(baseQun, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(valueBox, "x"))),
                fieldPredicate(valueBox, "y", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, fieldValue(baseQun, "c")))));

        final SelectExpression topSelect = join(valueBox, childSelectQun)
                .addResultColumn(projectColumn(childSelectQun, "a"))
                .addResultColumn(projectColumn(childSelectQun, "c"))
                .addResultColumn(projectColumn(childSelectQun, "d"))
                .build().buildSelect();

        final Quantifier updatedChildSelectQun = forEach(join(valueBox, baseQun)
                .addResultColumn(projectColumn(baseQun, "a"))
                .addResultColumn(projectColumn(baseQun, "c"))
                .addResultColumn(projectColumn(baseQun, "d"))
                .addPredicate(fieldPredicate(baseQun, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(valueBox, "x"))))
                .addPredicate(fieldPredicate(valueBox, "y", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN_OR_EQUALS, fieldValue(baseQun, "c"))))
                .build().buildSelect());
        final SelectExpression expected = new SelectExpression(updatedChildSelectQun.getFlowedObjectValue(), ImmutableList.of(updatedChildSelectQun), ImmutableList.of());

        testHelper.assertYields(topSelect, expected);
    }

    /**
     * Similar test to {@link #pushIntoChildSelect()}, but the child expression is a {@link LogicalFilterExpression} instead of a {@link SelectExpression}.
     * This rule will turn the logical filter into a select, which it will then add join parameters to.
     */
    @Test
    void pushIntoChildFilter() {
        final Value cov = ConstantObjectValue.of(Quantifier.constant(), "0", Type.primitiveType(Type.TypeCode.LONG, false));
        final Quantifier otherQun = baseTau();
        final Quantifier valueBox = valuesQun(ImmutableMap.of("x", cov, "y", LiteralValue.ofScalar("hello"), "z", fieldValue(otherQun, "gamma")));

        final Quantifier baseQun = baseT();
        final Quantifier childFilter = forEach(new LogicalFilterExpression(ImmutableList.of(
                fieldPredicate(baseQun, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(valueBox, "x"))),
                fieldPredicate(baseQun, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(valueBox, "y"))),
                fieldPredicate(baseQun, "c", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(valueBox, "z")))
        ), baseQun));

        final SelectExpression topSelect = join(valueBox, childFilter)
                .addResultColumn(projectColumn(childFilter, "d"))
                .build().buildSelect();

        final Quantifier updatedChildSelect = forEach(new SelectExpression(baseQun.getFlowedObjectValue(), ImmutableList.of(valueBox, baseQun), ImmutableList.of(
                fieldPredicate(baseQun, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(valueBox, "x"))),
                fieldPredicate(baseQun, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(valueBox, "y"))),
                fieldPredicate(baseQun, "c", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(valueBox, "z")))
        )));
        final SelectExpression expected = selectWithPredicates(updatedChildSelect, ImmutableList.of("d"));

        testHelper.assertYields(topSelect, expected);
    }

    /**
     * Here, we start with an expression with multiple values boxes. It has multiple other children, and each one
     * is associated with only one values box. When we push values down, only push the relevant values into each child.
     */
    @Test
    void partitionValuesByChild() {
        final Value cov0 = ConstantObjectValue.of(Quantifier.constant(), "0", Type.primitiveType(Type.TypeCode.LONG, false));
        final Quantifier values0 = valuesQun(cov0);
        final Value cov1 = ConstantObjectValue.of(Quantifier.constant(), "1", Type.primitiveType(Type.TypeCode.STRING, false));
        final Quantifier values1 = valuesQun(cov1);
        final Value cov2 = ConstantObjectValue.of(Quantifier.constant(), "2", Type.primitiveType(Type.TypeCode.BYTES, false));
        final Quantifier values2 = valuesQun(cov2);
        final Value cov3 = ConstantObjectValue.of(Quantifier.constant(), "3", Type.primitiveType(Type.TypeCode.BOOLEAN, false));
        final Quantifier values3 = valuesQun(cov3);

        final Quantifier base0 = baseT();
        final Quantifier select0 = forEach(selectWithPredicates(base0, ImmutableList.of("b", "c", "d"),
                fieldPredicate(base0, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, values0.getFlowedObjectValue()))));

        final Quantifier base1 = baseT();
        final Quantifier select1 = forEach(selectWithPredicates(base1, ImmutableList.of("a", "c", "d"),
                values1.getFlowedObjectValue().withComparison(new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(base1, "b")))));

        final Quantifier base2 = baseT();
        final Quantifier select2 = forEach(selectWithPredicates(base2, ImmutableList.of("a", "b", "d"),
                fieldPredicate(base2, "c", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, new PromoteValue(values2.getFlowedObjectValue(), Type.primitiveType(Type.TypeCode.BYTES, false), null)))));

        // Note: this does not actually reference values3. It will be removed entirely during the rewrite
        final Quantifier base3 = baseT();
        final Quantifier select3 = forEach(selectWithPredicates(base3, ImmutableList.of("a", "b", "c"),
                fieldPredicate(base3, "d", new Comparisons.NullComparison(Comparisons.Type.IS_NULL))));

        final SelectExpression selectExpression = join(values0, select0, values1, values2, select1, select2, values3, select3)
                .addResultColumn(Column.of(Optional.of("a0"), values0.getFlowedObjectValue()))
                .addResultColumn(column(select0, "b", "b0"))
                .addResultColumn(column(select0, "c", "c0"))
                .addResultColumn(column(select0, "d", "d0"))
                .addResultColumn(column(select1, "a", "a1"))
                .addResultColumn(Column.of(Optional.of("b1"), values1.getFlowedObjectValue()))
                .addResultColumn(column(select1, "c", "c1"))
                .addResultColumn(column(select1, "d", "d1"))
                .addResultColumn(column(select2, "a", "a2"))
                .addResultColumn(column(select2, "b", "b2"))
                .addResultColumn(Column.of(Optional.of("c2"), values2.getFlowedObjectValue()))
                .addResultColumn(column(select2, "d", "d2"))
                .addResultColumn(column(select3, "a", "a3"))
                .addResultColumn(column(select3, "b", "b3"))
                .addResultColumn(column(select3, "c", "c3"))
                .addResultColumn(Column.of(Optional.of("d3"), new NullValue(Type.primitiveType(Type.TypeCode.STRING))))
                .build().buildSelect();

        final Quantifier newSelect0 = forEach(join(values0, base0)
                .addResultColumn(projectColumn(base0, "b"))
                .addResultColumn(projectColumn(base0, "c"))
                .addResultColumn(projectColumn(base0, "d"))
                .addPredicate(fieldPredicate(base0, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, values0.getFlowedObjectValue())))
                .build().buildSelect());

        final Quantifier newSelect1 = forEach(join(values1, base1)
                .addResultColumn(projectColumn(base1, "a"))
                .addResultColumn(projectColumn(base1, "c"))
                .addResultColumn(projectColumn(base1, "d"))
                .addPredicate(values1.getFlowedObjectValue().withComparison(new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(base1, "b"))))
                .build().buildSelect());

        final Quantifier newSelect2 = forEach(join(values2, base2)
                .addResultColumn(projectColumn(base2, "a"))
                .addResultColumn(projectColumn(base2, "b"))
                .addResultColumn(projectColumn(base2, "d"))
                .addPredicate(fieldPredicate(base2, "c", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, new PromoteValue(values2.getFlowedObjectValue(), Type.primitiveType(Type.TypeCode.BYTES, false), null))))
                .build().buildSelect());

        final SelectExpression expected = join(newSelect0, newSelect1, newSelect2, select3)
                .addResultColumn(Column.of(Optional.of("a0"), cov0))
                .addResultColumn(column(newSelect0, "b", "b0"))
                .addResultColumn(column(newSelect0, "c", "c0"))
                .addResultColumn(column(newSelect0, "d", "d0"))
                .addResultColumn(column(newSelect1, "a", "a1"))
                .addResultColumn(Column.of(Optional.of("b1"), cov1))
                .addResultColumn(column(newSelect1, "c", "c1"))
                .addResultColumn(column(newSelect1, "d", "d1"))
                .addResultColumn(column(newSelect2, "a", "a2"))
                .addResultColumn(column(newSelect2, "b", "b2"))
                .addResultColumn(Column.of(Optional.of("c2"), cov2))
                .addResultColumn(column(newSelect2, "d", "d2"))
                .addResultColumn(column(select3, "a", "a3"))
                .addResultColumn(column(select3, "b", "b3"))
                .addResultColumn(column(select3, "c", "c3"))
                .addResultColumn(Column.of(Optional.of("d3"), new NullValue(Type.primitiveType(Type.TypeCode.STRING))))
                .build().buildSelect();

        testHelper.assertYields(selectExpression, expected);
    }

    /**
     * Unlike many other rewrite rules, this rule can support pushing into an existential quantifier.
     * It will produce a new existential quantifier that incorporates the values box. This is legal
     * because the values box does not adjust the cardinality of the data underlying the existential.
     */
    @Test
    void pushValuesTohExistentialChild() {
        final ConstantObjectValue cov = ConstantObjectValue.of(Quantifier.constant(), "0", Type.primitiveType(Type.TypeCode.BYTES, true));
        final Quantifier valuesBox = valuesQun(cov);

        final Quantifier base = baseT();

        final Quantifier explodeQun = forEach(new ExplodeExpression(fieldValue(base, "g")));
        final Quantifier existsQun = exists(selectWithPredicates(explodeQun,
                fieldPredicate(explodeQun, "three", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, valuesBox.getFlowedObjectValue()))));

        final SelectExpression selectExpression = join(valuesBox, base, existsQun)
                .addResultColumn(projectColumn(base, "a"))
                .addPredicate(new ExistsPredicate(existsQun.getAlias()))
                .build().buildSelect();

        final Quantifier newExistsQun = exists(new SelectExpression(explodeQun.getFlowedObjectValue(),
                ImmutableList.of(valuesBox, explodeQun),
                ImmutableList.of(fieldPredicate(explodeQun, "three", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, valuesBox.getFlowedObjectValue())))));
        final SelectExpression expected = join(base, newExistsQun)
                .addResultColumn(projectColumn(base, "a"))
                .addPredicate(new ExistsPredicate(newExistsQun.getAlias()))
                .build().buildSelect();

        testHelper.assertYields(selectExpression, expected);
    }

    /**
     * Test that if one of the children is a {@link com.apple.foundationdb.record.query.plan.cascades.Quantifier.ForEach}
     * quantifier with null on empty that we can still push through that as well.
     */
    @Test
    void pushValuesToNullOnEmpty() {
        final ConstantObjectValue cov = ConstantObjectValue.of(Quantifier.constant(), "0", Type.primitiveType(Type.TypeCode.LONG, false));
        final Quantifier valuesBox = valuesQun(ImmutableMap.of("x", cov));

        final Quantifier base = baseT();

        final Quantifier explodeQun = forEach(new ExplodeExpression(fieldValue(base, "f")));
        final Quantifier equalsOrNull = forEachWithNullOnEmpty(selectWithPredicates(explodeQun,
                explodeQun.getFlowedObjectValue().withComparison(new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(valuesBox, "x")))));

        final SelectExpression selectExpression = join(valuesBox, base, equalsOrNull)
                .addResultColumn(projectColumn(base, "b"))
                .addPredicate(equalsOrNull.getFlowedObjectValue().withComparison(new Comparisons.NullComparison(Comparisons.Type.IS_NULL)))
                .build().buildSelect();

        final Quantifier newEqualsOrNull = forEachWithNullOnEmpty(new SelectExpression(explodeQun.getFlowedObjectValue(),
                ImmutableList.of(valuesBox, explodeQun),
                ImmutableList.of(explodeQun.getFlowedObjectValue().withComparison(new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(valuesBox, "x"))))));
        final SelectExpression expected = join(base, newEqualsOrNull)
                .addResultColumn(projectColumn(base, "b"))
                .addPredicate(newEqualsOrNull.getFlowedObjectValue().withComparison(new Comparisons.NullComparison(Comparisons.Type.IS_NULL)))
                .build().buildSelect();

        testHelper.assertYields(selectExpression, expected);
    }

    @Test
    void pushIntoExpressionsWithVariations() {
        final Quantifier valuesBox = valuesQun(Map.of("x", LiteralValue.ofScalar(42L), "y", LiteralValue.ofScalar("hello")));

        //
        // Create three expressions with the same result value. One of them is:
        //    SELECT c, d FROM T WHERE a = v.x AND b = v.y
        // Another is:
        //    SELECT DISTINCT c, d FROM T WHERE v.x = a AND v.y = b
        // And the final is:
        //    SELECT c, d FROM T
        // These are not actually equivalent, but they can nevertheless be placed in a reference
        //
        final Quantifier base = baseT();
        final SelectExpression selectBase = selectWithPredicates(base,
                ImmutableList.of("c", "d"),
                fieldPredicate(base, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(valuesBox, "x"))),
                fieldPredicate(base, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(valuesBox, "y"))));
        final Quantifier selectReversePredicatesQun = forEach(selectWithPredicates(base,
                ImmutableList.of("c", "d"),
                fieldPredicate(valuesBox, "x", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(base, "c"))),
                fieldPredicate(valuesBox, "y", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(base, "d")))));
        final LogicalDistinctExpression distinct = new LogicalDistinctExpression(selectReversePredicatesQun);
        final SelectExpression selectUncorrelated = selectWithPredicates(base,
                ImmutableList.of("c", "d"));

        //
        // Take the three expressions and treat them as variations of a single expression. Join this with the values
        // box
        //
        final Quantifier lower = Quantifier.forEach(Reference.ofFinalExpressions(PlannerStage.INITIAL, ImmutableList.of(selectBase, distinct, selectUncorrelated)));
        final SelectExpression selectExpression = join(valuesBox, lower)
                .addResultColumn(projectColumn(valuesBox, "x"))
                .addResultColumn(projectColumn(valuesBox, "y"))
                .addResultColumn(projectColumn(lower, "c"))
                .addResultColumn(projectColumn(lower, "d"))
                .build().buildSelect();

        //
        // Rewrite the original expressions. The first one creates a new select:
        //   SELECT c, d FROM values(x = 42, y = 'hello') AS V, T WHERE T.a = V.x AND T.b = V.y
        // That is, the select absorbs the values box. Running this rule a subsequent time would
        // remove the values box entirely
        //
        // The second expression needs to introduce a new select as it cannot be pushed into the
        // logical distinct expression directly:
        //   SELECT DISTINCT T.* FROM values(x = 42, y = 'hello') AS v, (SELECT c, d FROM T WHERE v.x = T.a AND v.y = T.b)
        // Running this rule again on the new inner select will push the values further, but note that it will leave
        // an empty select box (that is, one that neither projects nor filters but simply passes through) that will need
        // to be simplified by select merge.
        //
        // The third expression was not correlated to the values box, and so it is unchanged.
        //
        final SelectExpression newSelectBase = join(valuesBox, base)
                .addResultColumn(projectColumn(base, "c"))
                .addResultColumn(projectColumn(base, "d"))
                .addPredicate(fieldPredicate(base, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(valuesBox, "x"))))
                .addPredicate(fieldPredicate(base, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(valuesBox, "y"))))
                .build().buildSelect();
        final Quantifier newDistinctChildQun = forEach(new SelectExpression(
                selectReversePredicatesQun.getFlowedObjectValue(),
                ImmutableList.of(valuesBox, selectReversePredicatesQun),
                ImmutableList.of()
        ));
        final LogicalDistinctExpression newDistinct = new LogicalDistinctExpression(newDistinctChildQun);

        //
        // Validate that we get a re-written child with the expected variations. Note that the values box
        // has also been applied to the result columns
        //
        final Quantifier newLower = Quantifier.forEach(Reference.ofFinalExpressions(PlannerStage.INITIAL, ImmutableList.of(newSelectBase, newDistinct, selectUncorrelated)));
        final SelectExpression expected = GraphExpansion.builder()
                .addQuantifier(newLower)
                .addResultColumn(Column.of(Optional.of("x"), LiteralValue.ofScalar(42L)))
                .addResultColumn(Column.of(Optional.of("y"), LiteralValue.ofScalar("hello")))
                .addResultColumn(projectColumn(newLower, "c"))
                .addResultColumn(projectColumn(newLower, "d"))
                .build().buildSelect();

        testHelper.assertYields(selectExpression, expected);
    }

    /**
     * Push a values box into an explode expression. The explode expression itself has no children, but
     * it has a return value that would need to be translated to the one in the value.
     */
    @Test
    void pushToExplodeChild() {
        final ConstantObjectValue cov = ConstantObjectValue.of(Quantifier.current(), "0", new Type.Array(false, Type.primitiveType(Type.TypeCode.LONG, false)));
        final Quantifier valuesBox = valuesQun(ImmutableMap.of("x", cov));

        final Quantifier base = baseT();

        final Quantifier explodeQun = forEach(new ExplodeExpression(fieldValue(valuesBox, "x")));
        final SelectExpression selectExpression = join(valuesBox, base, explodeQun)
                .addResultColumn(projectColumn(base, "a"))
                .addResultColumn(projectColumn(base, "b"))
                .addPredicate(fieldPredicate(base, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, explodeQun.getFlowedObjectValue())))
                .build().buildSelect();

        final Quantifier newExplodeQun = forEach(new ExplodeExpression(cov));
        final SelectExpression expected = join(base, newExplodeQun)
                .addResultColumn(projectColumn(base, "a"))
                .addResultColumn(projectColumn(base, "b"))
                .addPredicate(fieldPredicate(base, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, newExplodeQun.getFlowedObjectValue())))
                .build().buildSelect();

        testHelper.assertYields(selectExpression, expected);
    }

    /**
     * Push a set of values boxes into a {@link LogicalUnionExpression}. Each values box can be referenced by one
     * or more child quantifier, so this ensures that each child gets references to only the values it references.
     */
    @Test
    void pushToUnionChild() {
        final ConstantObjectValue cov0 = ConstantObjectValue.of(Quantifier.current(), "0", Type.primitiveType(Type.TypeCode.LONG, true));
        final ConstantObjectValue cov1 = ConstantObjectValue.of(Quantifier.current(), "1", Type.primitiveType(Type.TypeCode.STRING, false));
        final Quantifier valuesBox1 = valuesQun(ImmutableMap.of("x", cov0, "y", cov1));

        final Quantifier valuesBox2 = valuesQun(LiteralValue.ofScalar(42L));
        final Quantifier valuesBox3 = valuesQun(LiteralValue.ofScalar("hello"));

        final Quantifier tQun = baseT();
        final SelectExpression tLeg = selectWithPredicates(tQun,
                ImmutableMap.of("a", "one", "b", "two", "c", "three"),
                fieldPredicate(tQun, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(valuesBox1, "x"))),
                fieldPredicate(tQun, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(valuesBox1, "y")))
        );
        final Quantifier tLegQun = forEach(tLeg);

        final Quantifier tauQun = baseTau();
        final SelectExpression tauLeg = selectWithPredicates(tauQun,
                ImmutableMap.of("alpha", "one", "beta", "two", "gamma", "three"),
                fieldPredicate(tauQun, "alpha", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, valuesBox2.getFlowedObjectValue())),
                fieldPredicate(tauQun, "beta", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, valuesBox3.getFlowedObjectValue()))
        );
        final Quantifier tauLegQun = forEachWithNullOnEmpty(tauLeg);

        final Quantifier unionQun = forEach(new LogicalUnionExpression(ImmutableList.of(tLegQun, tauLegQun)));
        final SelectExpression selectExpression = new SelectExpression(unionQun.getFlowedObjectValue(),
                ImmutableList.of(valuesBox1, valuesBox2, valuesBox3, unionQun),
                ImmutableList.of());

        //
        // Create new legs of the union. Each one will include a select box over the original expression
        //

        final Quantifier newTLegExprQun = forEach(tLeg);
        final Quantifier newTLegQun = forEach(new SelectExpression(newTLegExprQun.getFlowedObjectValue(),
                ImmutableList.of(valuesBox1, newTLegExprQun),
                ImmutableList.of()));

        final Quantifier newTauLegExprQun = forEach(tauLeg);
        final Quantifier newTauLegQun = forEachWithNullOnEmpty(new SelectExpression(newTauLegExprQun.getFlowedObjectValue(),
                ImmutableList.of(valuesBox2, valuesBox3, newTauLegExprQun),
                ImmutableList.of()));

        final Quantifier newUnionQun = forEach(new LogicalUnionExpression(ImmutableList.of(newTLegQun, newTauLegQun)));
        final SelectExpression expected = new SelectExpression(newUnionQun.getFlowedObjectValue(),
                ImmutableList.of(newUnionQun),
                ImmutableList.of());

        testHelper.assertYields(selectExpression, expected);
    }

    /**
     * Trim uncorrelated (that is, extraneous) values boxes. Here, we start with a query like:
     * <pre>{@code
     * SELECT s.d
     *    FROM values(x = @0, y = 42) AS v,
     *       (SELECT b, c, d FROM T WHERE a = 42) AS s
     *    WHERE s.b = @0
     * }</pre>
     * <p>
     * Note that nothing is actually correlated to the values box. We can therefore just remove it
     * and get the semantically equivalent:
     * </p>
     * <pre>{@code
     * SELECT s.d
     *    FROM (SELECT b, c, d FROM T WHERE a = 42) AS s
     *    WHERE s.b = @0
     * }</pre>
     */
    @Test
    void trimUncorrelatedValuesBoxes() {
        final ConstantObjectValue cov = ConstantObjectValue.of(Quantifier.constant(), "0", Type.primitiveType(Type.TypeCode.BYTES, false));
        final Quantifier valuesBox = valuesQun(ImmutableMap.of("x", cov, "y", LiteralValue.ofScalar(42L)));

        final Quantifier base = baseT();
        final Quantifier lowerSelect = forEach(selectWithPredicates(base,
                ImmutableList.of("b", "c", "d"),
                fieldPredicate(base, "a", EQUALS_42)));
        final SelectExpression selectExpression = join(valuesBox, lowerSelect)
                .addResultColumn(projectColumn(lowerSelect, "d"))
                .addPredicate(fieldPredicate(lowerSelect, "c", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, cov)))
                .build().buildSelect();

        final SelectExpression expected = selectWithPredicates(lowerSelect,
                ImmutableList.of("d"),
                fieldPredicate(lowerSelect, "c", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, cov)));

        testHelper.assertYields(selectExpression, expected);
    }

    /**
     * Similar to {@link #trimUncorrelatedValuesBoxes()}, this also operates on an uncorrelated values box.
     * In this case, however, the uncorrelated box is used only in predicates on the top value. So we still
     * want to leave the child boxes the same, but we re-write the predicate at the top level.
     * <p>
     * In particular we start with a query like:
     * </p>
     * <pre>{@code
     * SELECT v.y, s.d
     *    FROM values(x = @0, y = 42) AS v,
     *       (SELECT b, c, d FROM T WHERE a = 42) AS s
     *    WHERE s.b = v.x
     * }</pre>
     * <p>
     * The lower select should remain the same, as it is not correlated to the values box. However, the
     * result value and predicates should be rewritten in response to the now removed values box:
     * </p>
     * <pre>{@code
     * SELECT 42 AS y, s.d
     *    FROM (SELECT b, c, d FROM T WHERE a = 42) AS s
     *    WHERE s = @0
     * }</pre>
     */
    @Test
    void rewritePredicatesAndReturnValueOnUncorrelatedValuesBox() {
        final ConstantObjectValue cov = ConstantObjectValue.of(Quantifier.constant(), "0", Type.primitiveType(Type.TypeCode.BYTES, false));
        final Quantifier valuesBox = valuesQun(ImmutableMap.of("x", cov, "y", LiteralValue.ofScalar(42L)));

        final Quantifier base = baseT();
        final Quantifier lowerSelect = forEach(selectWithPredicates(base,
                ImmutableList.of("b", "c", "d"),
                fieldPredicate(base, "a", EQUALS_42)));
        final SelectExpression selectExpression = join(valuesBox, lowerSelect)
                .addResultColumn(projectColumn(valuesBox, "y"))
                .addResultColumn(projectColumn(lowerSelect, "d"))
                .addPredicate(fieldPredicate(lowerSelect, "c", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(valuesBox, "x"))))
                .build().buildSelect();

        final SelectExpression expected = GraphExpansion.builder()
                .addQuantifier(lowerSelect)
                .addResultColumn(Column.of(Optional.of("y"), LiteralValue.ofScalar(42L)))
                .addResultColumn(projectColumn(lowerSelect, "d"))
                .addPredicate(fieldPredicate(lowerSelect, "c", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, cov)))
                .build().buildSelect();

        testHelper.assertYields(selectExpression, expected);
    }

    /**
     * Create a values box with variants. In this case, we simulate a potential simplification of the values in the box by
     * running the rule with both {@code 1 + 2} and with {@code 3} as potential variants.
     */
    @Test
    void multipleValueChildrenVariants() {
        final Quantifier valuesBox = valuesQun((Value) new ArithmeticValue.AddFn().encapsulate(ImmutableList.of(LiteralValue.ofScalar(1L), LiteralValue.ofScalar(2L))));
        SelectExpression valueExpr = (SelectExpression) valuesBox.getRangesOver().get();
        valuesBox.getRangesOver().insertFinalExpression(new SelectExpression(new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG, true), 3L), valueExpr.getQuantifiers(), ImmutableList.of()));

        final Quantifier base = baseT();
        final Quantifier lowerSelect = forEach(selectWithPredicates(base, ImmutableList.of("a", "b", "c"),
                fieldPredicate(base, "a", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, valuesBox.getFlowedObjectValue()))));

        final SelectExpression select = join(valuesBox, lowerSelect)
                .addResultColumn(Column.of(Optional.of("minA"), valuesBox.getFlowedObjectValue()))
                .addResultColumn(projectColumn(lowerSelect, "a"))
                .addResultColumn(projectColumn(lowerSelect, "b"))
                .build().buildSelect();

        final Quantifier newLowerSelect = forEach(join(valuesBox, base)
                .addResultColumn(projectColumn(base, "a"))
                .addResultColumn(projectColumn(base, "b"))
                .addResultColumn(projectColumn(base, "c"))
                .addPredicate(fieldPredicate(base, "a", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, valuesBox.getFlowedObjectValue())))
                .build().buildSelect());
        final SelectExpression expected1 = join(newLowerSelect)
                .addResultColumn(Column.of(Optional.of("minA"), (Value) new ArithmeticValue.AddFn().encapsulate(ImmutableList.of(LiteralValue.ofScalar(1L), LiteralValue.ofScalar(2L)))))
                .addResultColumn(projectColumn(newLowerSelect, "a"))
                .addResultColumn(projectColumn(newLowerSelect, "b"))
                .build().buildSelect();
        final SelectExpression expected2 = join(newLowerSelect)
                .addResultColumn(Column.of(Optional.of("minA"), new LiteralValue<>(Type.primitiveType(Type.TypeCode.LONG, true), 3L)))
                .addResultColumn(projectColumn(newLowerSelect, "a"))
                .addResultColumn(projectColumn(newLowerSelect, "b"))
                .build().buildSelect();

        // Expect one final variant for every variant of the values box
        // In the future, we may want to restrict this to only creating one
        // variant (for the simplest value)
        testHelper.assertYields(select, expected1, expected2);
    }

    /**
     * Test what happens when there are multiple values boxes each with multiple variants. In this case, the variants represent
     * adding zero to a constant long or {@code AND}'ing a constant boolean with {@code true}, both of which could be simplified
     * to just the original constants.
     */
    @Test
    void multipleVariantsFromTwoValuesChildren() {
        final ConstantObjectValue cov0 = ConstantObjectValue.of(Quantifier.constant(), "0", Type.primitiveType(Type.TypeCode.LONG, true));
        final Value cov0PlusZero = (Value) new ArithmeticValue.AddFn().encapsulate(ImmutableList.of(cov0, LiteralValue.ofScalar(0L)));
        final Quantifier valuesBox0 = valuesQun(cov0PlusZero);
        valuesBox0.getRangesOver().insertFinalExpression(new SelectExpression(cov0, valuesBox0.getRangesOver().get().getQuantifiers(), ImmutableList.of()));

        final ConstantObjectValue cov1 = ConstantObjectValue.of(Quantifier.constant(), "1", Type.primitiveType(Type.TypeCode.BOOLEAN, true));
        final Value cov1AndTrue = (Value) new AndOrValue.AndFn().encapsulate(ImmutableList.of(cov1, LiteralValue.ofScalar(true)));
        final Quantifier valuesBox1 = valuesQun(cov1AndTrue);
        valuesBox1.getRangesOver().insertFinalExpression(new SelectExpression(cov1, valuesBox1.getRangesOver().get().getQuantifiers(), ImmutableList.of()));

        final Quantifier base = baseT();
        final SelectExpression selectExpression = join(valuesBox0, valuesBox1, base)
                .addResultColumn(projectColumn(base, "b"))
                .addPredicate(fieldPredicate(base, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, valuesBox0.getFlowedObjectValue())))
                .addPredicate(valuesBox1.getFlowedObjectValue().withComparison(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, true)))
                .build().buildSelect();

        final SelectExpression expected1 = selectWithPredicates(base, ImmutableList.of("b"),
                fieldPredicate(base, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, cov0)),
                cov1.withComparison(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, true)));
        final SelectExpression expected2 = selectWithPredicates(base, ImmutableList.of("b"),
                fieldPredicate(base, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, cov0)),
                cov1AndTrue.withComparison(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, true)));
        final SelectExpression expected3 = selectWithPredicates(base, ImmutableList.of("b"),
                fieldPredicate(base, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, cov0PlusZero)),
                cov1.withComparison(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, true)));
        final SelectExpression expected4 = selectWithPredicates(base, ImmutableList.of("b"),
                fieldPredicate(base, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, cov0PlusZero)),
                cov1AndTrue.withComparison(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, true)));

        // Expect one final variant for every variant in the cross-product of the
        // available values box variants.
        // In the future, we may want to restrict this to only creating one
        // variant (picking the simplest value from each box)
        testHelper.assertYields(selectExpression, expected1, expected2, expected3, expected4);
    }

    @Test
    void multipleRangeOneVariants() {
        final Quantifier rangeQun = rangeOneQun();
        final TableFunctionExpression tf2 = new TableFunctionExpression(new RangeValue(LiteralValue.ofScalar(2L), LiteralValue.ofScalar(0L), LiteralValue.ofScalar(2L)));
        assertEquals(CardinalitiesProperty.Cardinalities.exactlyOne(), tf2.getValue().getCardinalities());
        assertNotEquals(rangeQun.getRangesOver().get(), tf2);
        rangeQun.getRangesOver().insertFinalExpression(tf2);

        final Quantifier valuesBox = forEach(new SelectExpression(LiteralValue.ofScalar("hello"), ImmutableList.of(rangeQun), ImmutableList.of()));
        final Quantifier base = baseT();
        final Quantifier lowerSelect = forEach(selectWithPredicates(base, ImmutableList.of("a", "c"),
                fieldPredicate(base, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, valuesBox.getFlowedObjectValue()))));
        final SelectExpression select = join(valuesBox, lowerSelect)
                .addResultColumn(projectColumn(lowerSelect, "a"))
                .addResultColumn(Column.of(Optional.of("b"), valuesBox.getFlowedObjectValue()))
                .addResultColumn(projectColumn(lowerSelect, "c"))
                .build().buildSelect();

        final Quantifier newLowerSelect = forEach(join(valuesBox, base)
                .addResultColumn(projectColumn(base, "a"))
                .addResultColumn(projectColumn(base, "c"))
                .addPredicate(fieldPredicate(base, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, valuesBox.getFlowedObjectValue())))
                .build().buildSelect());
        final SelectExpression expected = join(newLowerSelect)
                .addResultColumn(projectColumn(newLowerSelect, "a"))
                .addResultColumn(Column.of(Optional.of("b"), LiteralValue.ofScalar("hello")))
                .addResultColumn(projectColumn(newLowerSelect, "c"))
                .build().buildSelect();

        // Executed twice, once for each variant of the underlying range. As they produce the same result,
        // we only get one expression yielded
        TestRuleExecution execution = testHelper.assertYields(select, expected);
        assertEquals(1, execution.getRuleMatchedCount());
    }

    /**
     * This pushes a values box into a {@link GroupByExpression}'s children. In this case the original query is
     * something like:
     * <pre>{@code
     * SELECT T.b, T.c, sum(T.a) as sum FROM
     *    values(@0) as v,
     *    T
     *    WHERE T.d = v
     *    GROUP BY T.b, T.c
     * }</pre>
     * <p>
     * This will be rewritten as:
     * </p>
     * <pre>{@code
     * SELECT T.b, T.c, sum(T.a) as sum FROM
     *    (SELECT T.* FROM values(@0) as v, T WHERE T.d = v) T
     *    GROUP BY T.b, T.c
     * }</pre>
     * <p>
     * A later step will simplify the inner select.
     * </p>
     */
    @Test
    void pushIntoGroupBySelectWhere() {
        final ConstantObjectValue cov = ConstantObjectValue.of(Quantifier.constant(), "0", Type.primitiveType(Type.TypeCode.STRING, false));
        final Quantifier valuesBox = valuesQun(cov);

        final Quantifier base = baseT();
        final Quantifier selectWhere = forEach(new SelectExpression(base.getFlowedObjectValue(),
                ImmutableList.of(base),
                ImmutableList.of(fieldPredicate(base, "d", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, valuesBox.getFlowedObjectValue())))));

        final Quantifier groupBy = forEach(new GroupByExpression(
                RecordConstructorValue.ofUnnamed(ImmutableList.of(fieldValue(selectWhere, "b"), fieldValue(selectWhere, "c"))),
                (AggregateValue) new NumericAggregationValue.SumFn().encapsulate(ImmutableList.of(fieldValue(selectWhere, "a"))),
                GroupByExpression::nestedResults,
                selectWhere
        ));

        final SelectExpression selectHaving = join(valuesBox, groupBy)
                .addResultColumn(column(groupBy, "_0._0", "b"))
                .addResultColumn(column(groupBy, "_0._1", "c"))
                .addResultColumn(column(groupBy, "_1", "sum"))
                .build().buildSelect();

        //
        // Here, we leave the original select where as it is, and we insert a new select expression (with the values box)
        // and join it with the existing select where quantifier. This allows us to push down the correlation, and future
        // rewrites 
        //
        final Quantifier newOverSelectWhere = forEach(new SelectExpression(selectWhere.getFlowedObjectValue(),
                ImmutableList.of(valuesBox, selectWhere),
                ImmutableList.of()));
        final Quantifier newGroupBy = forEach(new GroupByExpression(
                RecordConstructorValue.ofUnnamed(ImmutableList.of(fieldValue(newOverSelectWhere, "b"), fieldValue(newOverSelectWhere, "c"))),
                (AggregateValue) new NumericAggregationValue.SumFn().encapsulate(ImmutableList.of(fieldValue(newOverSelectWhere, "a"))),
                GroupByExpression::nestedResults,
                newOverSelectWhere
        ));
        final SelectExpression newSelectHaving = GraphExpansion.builder()
                .addQuantifier(newGroupBy)
                .addResultColumn(column(newGroupBy, "_0._0", "b"))
                .addResultColumn(column(newGroupBy, "_0._1", "c"))
                .addResultColumn(column(newGroupBy, "_1", "sum"))
                .build().buildSelect();

        testHelper.assertYields(selectHaving, newSelectHaving);
    }

    /**
     * This pushes a values box into a {@link GroupByExpression}'s grouping columns. In this case the original query is
     * something like:
     * <pre>{@code
     * SELECT v AS group, min(T.a) AS min FROM
     *    values(@0) AS v,
     *    T
     *    WHERE T.b = 'hello'
     *    GROUP BY v
     * }</pre>
     * <p>
     * Note that the values box is only referenced in the grouping columns. This will be rewritten as:
     * </p>
     * <pre>{@code
     * SELECT @0 AS group, min(T.a) AS min FROM
     *    T
     *    WHERE T.b = 'hello'
     *    GROUP BY @0
     * }</pre>
     * <p>
     * A later step will simplify the inner select. Note that the inner grouping logic is the same,
     * as the values quantifier does not need to be pushed into the children.
     * </p>
     */
    @Test
    void pushIntoGroupByGroupingColumns() {
        final ConstantObjectValue cov = ConstantObjectValue.of(Quantifier.constant(), "0", Type.primitiveType(Type.TypeCode.STRING, false));
        final Quantifier valuesBox = valuesQun(cov);

        final Quantifier base = baseT();
        final Quantifier selectWhere = forEach(new SelectExpression(base.getFlowedObjectValue(),
                ImmutableList.of(base),
                ImmutableList.of(fieldPredicate(base, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, LiteralValue.ofScalar("hello"))))));

        final Quantifier groupBy = forEach(new GroupByExpression(
                RecordConstructorValue.ofUnnamed(ImmutableList.of(valuesBox.getFlowedObjectValue())),
                (AggregateValue) new NumericAggregationValue.MinFn().encapsulate(ImmutableList.of(fieldValue(selectWhere, "a"))),
                GroupByExpression::nestedResults,
                selectWhere
        ));

        final SelectExpression selectHaving = join(valuesBox, groupBy)
                .addResultColumn(column(groupBy, "_0._0", "group"))
                .addResultColumn(column(groupBy, "_1", "min"))
                .build().buildSelect();

        final Quantifier newGroupBy = forEach(new GroupByExpression(
                RecordConstructorValue.ofUnnamed(ImmutableList.of(cov)),
                (AggregateValue) new NumericAggregationValue.MinFn().encapsulate(ImmutableList.of(fieldValue(selectWhere, "a"))),
                GroupByExpression::nestedResults,
                selectWhere
        ));
        final SelectExpression newSelectHaving = GraphExpansion.builder()
                .addQuantifier(newGroupBy)
                .addResultColumn(column(newGroupBy, "_0._0", "group"))
                .addResultColumn(column(newGroupBy, "_1", "min"))
                .build().buildSelect();

        testHelper.assertYields(selectHaving, newSelectHaving);
    }

    private void doNotTreatQuantifierAsValuesBox(@Nonnull Quantifier notQuiteValuesQun) {
        final Quantifier base = baseT();
        final Quantifier correlatedSelect = forEach(selectWithPredicates(base,
                ImmutableList.of("b", "c", "d"),
                fieldPredicate(base, "a", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, notQuiteValuesQun.getFlowedObjectValue()))));

        final SelectExpression selectExpression = join(notQuiteValuesQun, correlatedSelect)
                .addResultColumn(projectColumn(correlatedSelect, "b"))
                .build().buildSelect();

        testHelper.assertYieldsNothing(selectExpression, true);
    }

    /**
     * Make sure that we don't treat an ungrouped count object like a values box. Like a values box, the cardinality
     * of an ungrouped value is exactly one. But it has additional meaning, and so we don't want to move it around
     * like we do a values box. This is done here with a query like:
     * <pre>{@code
     * SELECT x.count, y.b, y.c, y.d
     *   FROM (SELECT count(*) AS count FROM T) x,
     *        (SELECT b, c, d FROM T WHERE a = x.count) y
     * }</pre>
     * <p>
     * We then assert that the count box does not get pushed into the {@code y} leg by this rule.
     * </p>
     */
    @Test
    void doNotTreatUngroupedCountAsValues() {
        final Quantifier base = baseT();
        final Quantifier selectWhere = forEach(new SelectExpression(base.getFlowedObjectValue(), ImmutableList.of(base), ImmutableList.of()));
        final Quantifier groupBy = forEach(new GroupByExpression(
                null,
                (AggregateValue) new CountValue.CountFn().encapsulate(ImmutableList.of(RecordConstructorValue.ofColumns(ImmutableList.of()))),
                GroupByExpression::nestedResults,
                selectWhere));

        //
        // Make sure that the cardinality of the ungrouped count value is equal to exactly one, which
        // is one of the criteria we look for when choosing a values box
        //
        CardinalitiesProperty.Cardinalities cardinalities = CardinalitiesProperty.cardinalities().evaluate(groupBy.getRangesOver());
        assertEquals(CardinalitiesProperty.Cardinalities.exactlyOne(), cardinalities);

        final Quantifier selectHaving = forEach(new SelectExpression(
                fieldValue(groupBy, "_0"),
                ImmutableList.of(groupBy),
                ImmutableList.of()
        ));

        doNotTreatQuantifierAsValuesBox(selectHaving);
    }

    /**
     * Make sure we don't allow values boxes with predicates. In principle, we could allow this as long as the
     * predicates are tautologies, but it doesn't buy us much to do so.
     */
    @Test
    void doNotUseValuesBoxWithPredicates() {
        final Quantifier rangeOne = rangeOneQun();

        final Quantifier notQuiteValuesQun = forEach(new SelectExpression(
                LiteralValue.ofScalar(42L),
                ImmutableList.of(rangeOne),
                ImmutableList.of(ConstantPredicate.TRUE)
        ));
        doNotTreatQuantifierAsValuesBox(notQuiteValuesQun);
    }

    /**
     * Do not allow a values box with multiple children, even if the cardinality
     * is one.
     */
    @Test
    void doNotAllowValuesBoxWithJoin() {
        final Quantifier rangeOneA = rangeOneQun();
        final Quantifier rangeOneB = rangeOneQun();
        final Quantifier valuesBox = forEach(new SelectExpression(
                LiteralValue.ofScalar(42L),
                ImmutableList.of(rangeOneA, rangeOneB),
                ImmutableList.of()
        ));

        //
        // Note that the cardinality for the join is still one
        //
        CardinalitiesProperty.Cardinalities cardinalities = CardinalitiesProperty.cardinalities().evaluate(valuesBox.getRangesOver());
        assertEquals(CardinalitiesProperty.Cardinalities.exactlyOne(), cardinalities);

        doNotTreatQuantifierAsValuesBox(valuesBox);
    }

    /**
     * Disallow a values box over {@code range(2)} instead of {@code range(1)}. The cardinality here is
     * different, and we'd expect
     */
    @Test
    void doNotTreatRangeTwoAsValues() {
        final Quantifier rangeTwoQun = forEach(new TableFunctionExpression(
                (StreamingValue) new RangeValue.RangeFn().encapsulate(ImmutableList.of(LiteralValue.ofScalar(2L)))));
        final Quantifier valuesBox = forEach(new SelectExpression(LiteralValue.ofScalar(42L), ImmutableList.of(rangeTwoQun), ImmutableList.of()));
        doNotTreatQuantifierAsValuesBox(valuesBox);
    }

    /**
     * Make sure we don't match a values box which references a constant value. In theory, if we
     * checked that the constant was set to 1, this may be acceptable, but we'd need to make sure
     * we generated an appropriate query plan constraint to ensure that if we cached the plan,
     * we double check that this constant box is still set to 1.
     */
    @Test
    void doNotTreatRangeWithConstantObjectValueAsValueBox() {
        final ConstantObjectValue endCov = ConstantObjectValue.of(Quantifier.constant(), "0", Type.primitiveType(Type.TypeCode.LONG, false));
        final Quantifier rangeQun = forEach(new TableFunctionExpression(
                (StreamingValue) new RangeValue.RangeFn().encapsulate(ImmutableList.of(endCov))));
        final Quantifier valuesBox = forEach(new SelectExpression(LiteralValue.ofScalar(42L), ImmutableList.of(rangeQun), ImmutableList.of()));
        doNotTreatQuantifierAsValuesBox(valuesBox);
    }

    /**
     * Reject a "values box" that includes correlations to other children at that same level.
     * We do allow for the values box to have correlations to other parts of the tree, but this
     * one is unsafe as we can't push the values box into anything that it correlates to. In theory,
     * we could push it to other children.
     */
    @Test
    void doNotUseValuesBoxWithCorrelationsInTheValue() {
        final Quantifier base = baseT();
        final Quantifier lowerSelect = forEach(selectWithPredicates(base, ImmutableList.of("a", "b", "c"),
                fieldPredicate(base, "d", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(base, "b")))));

        final Quantifier valuesBox = valuesQun(ImmutableMap.of("x", fieldValue(lowerSelect, "b")));

        final SelectExpression selectExpression = join(valuesBox, lowerSelect)
                .addResultColumn(projectColumn(lowerSelect, "a"))
                .addResultColumn(projectColumn(valuesBox, "x"))
                .addResultColumn(projectColumn(lowerSelect, "c"))
                .build().buildSelect();

        testHelper.assertYieldsNothing(selectExpression, true);
    }

    /**
     * If we have correlations between values boxes, ensure that they are de-correlated in stages. In the first pass,
     * values without correlations pointing in should be pushed down, including pushing into the referencing values
     * box(es). Then another application of the rule on the rewritten values box should yield an appropriate new
     * values box. Finally, applying the rule to the top expression should simplify this fully.
     *
     * <p>
     * The exact sequence here is something like:
     * </p>
     * <pre>{@code
     *    SELECT T.a, T.b, T.c
     *       FROM T, values(x = @0, y = @1) AS v1, values(z = v1.y + 1) AS v2
     *       WHERE T.d = v1.x AND T.a > v2.z
     * }</pre>
     * <p>
     * The first stage pushes the {@code v1} down:
     * </p>
     * <pre>{@code
     *    SELECT T.a, T.b, T.c
     *       FROM T, (SELECT v1.y + 1 AS z FROM values(x = @0, y = @1) AS v1, range(1)) AS v2
     *       WHERE T.d = @0 AND T.a > v2.z
     * }</pre>
     * <p>
     * Then we re-write {@code v2}:
     * </p>
     * <pre>{@code
     *    SELECT T.a, T.b, T.c
     *       FROM T, values(z = @1 + 1) AS v2
     *       WHERE T.d = @0 AND T.a > v2.z
     * }</pre>
     * <p>
     * Which lets us rewrite the final expression as:
     * </p>
     * <pre>{@code
     *    SELECT T.a, T.b, T.c
     *       FROM T
     *       WHERE T.d = @0 AND T.a > (@1 + 1)
     * }</pre>
     */
    @Test
    void translateInterCorrelatedValuesBoxesInSequence() {
        final ConstantObjectValue cov1 = ConstantObjectValue.of(Quantifier.constant(), "0", Type.primitiveType(Type.TypeCode.STRING, false));
        final ConstantObjectValue cov2 = ConstantObjectValue.of(Quantifier.constant(), "1", Type.primitiveType(Type.TypeCode.INT, false));
        final Quantifier base = baseT();

        final Quantifier range1 = rangeOneQun();
        final Quantifier valuesBox1 = forEach(GraphExpansion.builder()
                .addQuantifier(range1)
                .addResultColumn(Column.of(Optional.of("x"), cov1))
                .addResultColumn(Column.of(Optional.of("y"), cov2))
                .build().buildSelect());
        final Quantifier range2 = rangeOneQun();
        final Quantifier valuesBox2 = forEach(GraphExpansion.builder()
                .addQuantifier(range2)
                .addResultColumn(Column.of(Optional.of("z"), (Value) new ArithmeticValue.AddFn().encapsulate(ImmutableList.of(fieldValue(valuesBox1, "y"), LiteralValue.ofScalar(1L)))))
                .build().buildSelect());

        // Original select
        final SelectExpression selectExpression = join(valuesBox1, valuesBox2, base)
                .addResultColumn(projectColumn(base, "a"))
                .addResultColumn(projectColumn(base, "b"))
                .addResultColumn(projectColumn(base, "c"))
                .addPredicate(fieldPredicate(base, "d", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(valuesBox1, "x"))))
                .addPredicate(fieldPredicate(base, "a", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, fieldValue(valuesBox2, "z"))))
                .build().buildSelect();

        // Push down values box 1
        final SelectExpression newValuesBox2Expr = join(valuesBox1, range2)
                .addResultColumn(Column.of(Optional.of("z"), (Value) new ArithmeticValue.AddFn().encapsulate(ImmutableList.of(fieldValue(valuesBox1, "y"), LiteralValue.ofScalar(1L)))))
                .build().buildSelect();
        final Quantifier newValuesBox2 = forEach(newValuesBox2Expr);

        final SelectExpression expected1 = join(newValuesBox2, base)
                .addResultColumn(projectColumn(base, "a"))
                .addResultColumn(projectColumn(base, "b"))
                .addResultColumn(projectColumn(base, "c"))
                .addPredicate(fieldPredicate(base, "d", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, cov1)))
                .addPredicate(fieldPredicate(base, "a", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, fieldValue(newValuesBox2, "z"))))
                .build().buildSelect();
        testHelper.assertYields(selectExpression, expected1);

        // Push values box1 into values box 2
        final SelectExpression finalValuesBox2 = GraphExpansion.builder()
                .addQuantifier(range2)
                .addResultColumn(Column.of(Optional.of("z"), (Value) new ArithmeticValue.AddFn().encapsulate(ImmutableList.of(cov2, LiteralValue.ofScalar(1L)))))
                .build().buildSelect();
        testHelper.assertYields(newValuesBox2Expr, finalValuesBox2);

        // Simulate the rule having yielded the simplified values box 2 by inserting the expression back into expected1,
        // and then re-run the rule on the outer select
        newValuesBox2.getRangesOver().insertFinalExpression(finalValuesBox2);
        final SelectExpression expected2 = selectWithPredicates(base,
                ImmutableList.of("a", "b", "c"),
                fieldPredicate(base, "d", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, cov1)),
                fieldPredicate(base, "a", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, (Value) new ArithmeticValue.AddFn().encapsulate(ImmutableList.of(cov2, LiteralValue.ofScalar(1L)))))
        );
        testHelper.assertYields(expected1, expected2);
    }

    /**
     * Handle a case where there is a values box with multiple variants, one of which is correlated to a sibling
     * and another isn't. In theory, we could actually match the uncorrelated expression, but then we'd have to
     * be diligent about copying over <em>only</em> the uncorrelated expression when we push the quantifier
     * down. That's complicated by the memoizer, so long story shirt, we don't currently modify it.
     */
    @Test
    void valueBoxWithCorrelatedAndUncorrelatedVariants() {
        // Start with valuesBox2 correlated to valuesBox1, and then add a variant that is not correlated
        final Quantifier valuesBox1 = valuesQun(ImmutableMap.of("x", LiteralValue.ofScalar(42L), "y", LiteralValue.ofScalar("hello")));
        final Quantifier valuesBox2 = valuesQun(ImmutableMap.of("z", fieldValue(valuesBox1, "y")));
        final SelectExpression uncorrelatedValueBox2Expr = GraphExpansion.builder()
                .addAllQuantifiers(valuesBox2.getRangesOver().get().getQuantifiers())
                .addResultColumn(Column.of(Optional.of("z"), LiteralValue.ofScalar("hello")))
                .build().buildSelect();
        valuesBox2.getRangesOver().insertFinalExpression(uncorrelatedValueBox2Expr);

        final Quantifier base = baseT();
        final Quantifier lowerSelectQun = forEach(selectWithPredicates(base,
                ImmutableList.of("a", "b", "c", "d"),
                fieldPredicate(base, "b", new Comparisons.ValueComparison(Comparisons.Type.STARTS_WITH, fieldValue(valuesBox2, "z")))));

        final SelectExpression selectExpression = join(valuesBox1, valuesBox2, lowerSelectQun)
                .addResultColumn(projectColumn(valuesBox1, "x"))
                .addResultColumn(projectColumn(valuesBox2, "z"))
                .addResultColumn(projectColumn(lowerSelectQun, "a"))
                .addResultColumn(projectColumn(lowerSelectQun, "b"))
                .addResultColumn(projectColumn(lowerSelectQun, "c"))
                .addPredicate(fieldPredicate(lowerSelectQun, "a", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, fieldValue(valuesBox1, "x"))))
                .addPredicate(fieldPredicate(lowerSelectQun, "d", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(valuesBox2, "z"))))
                .build().buildSelect();

        //
        // Match both value boxes. In this case, we only extract out the uncorrelated values box variant
        //
        final Quantifier trimmedValuesBox2 = valuesQun(ImmutableMap.of("z", LiteralValue.ofScalar("hello")));
        final Quantifier newLowerSelectQun = forEach(join(trimmedValuesBox2, base)
                .addResultColumn(projectColumn(base, "a"))
                .addResultColumn(projectColumn(base, "b"))
                .addResultColumn(projectColumn(base, "c"))
                .addResultColumn(projectColumn(base, "d"))
                .addPredicate(fieldPredicate(base, "b", new Comparisons.ValueComparison(Comparisons.Type.STARTS_WITH, fieldValue(trimmedValuesBox2, "z"))))
                .build().buildSelect());

        final SelectExpression expected1 = join(newLowerSelectQun)
                .addResultColumn(Column.of(Optional.of("x"), LiteralValue.ofScalar(42L)))
                .addResultColumn(Column.of(Optional.of("z"), LiteralValue.ofScalar("hello")))
                .addResultColumn(projectColumn(newLowerSelectQun, "a"))
                .addResultColumn(projectColumn(newLowerSelectQun, "b"))
                .addResultColumn(projectColumn(newLowerSelectQun, "c"))
                .addPredicate(fieldPredicate(newLowerSelectQun, "a", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, LiteralValue.ofScalar(42L))))
                .addPredicate(fieldPredicate(newLowerSelectQun, "d", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, LiteralValue.ofScalar("hello"))))
                .build().buildSelect();

        //
        // Only match values box 1, as values box 2 has an expression with a correlation.
        // In theory, we could also match the non-correlated expression directly, but
        // we have to be more careful with memoization
        //
        final Quantifier valuesBox2WithExtraChild = Quantifier.forEach(
                Reference.initialOf(
                        join(valuesBox1, rangeOneQun())
                                .addResultColumn(Column.of(Optional.of("z"), fieldValue(valuesBox1, "y")))
                                .build().buildSelect()),
                // Copy the alias over to ensure correlations from lower select are preserved
                valuesBox2.getAlias());
        // The uncorrelated variant also gets copied over (un-modified)
        valuesBox2WithExtraChild.getRangesOver().insertFinalExpression(uncorrelatedValueBox2Expr);

        final SelectExpression expected2 = join(valuesBox2WithExtraChild, lowerSelectQun)
                .addResultColumn(Column.of(Optional.of("x"), LiteralValue.ofScalar(42L)))
                .addResultColumn(projectColumn(valuesBox2WithExtraChild, "z"))
                .addResultColumn(projectColumn(lowerSelectQun, "a"))
                .addResultColumn(projectColumn(lowerSelectQun, "b"))
                .addResultColumn(projectColumn(lowerSelectQun, "c"))
                .addPredicate(fieldPredicate(lowerSelectQun, "a", new Comparisons.ValueComparison(Comparisons.Type.GREATER_THAN, LiteralValue.ofScalar(42L))))
                .addPredicate(fieldPredicate(lowerSelectQun, "d", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(valuesBox2WithExtraChild, "z"))))
                .build().buildSelect();

        testHelper.assertYields(selectExpression, expected1, expected2);
    }

    /**
     * If we have an existential quantifier over a values box, we should not
     * attempt to push it down. This is because the existential quantifier does
     * not forward along its values in a way that is useful.
     */
    @Test
    void doNotPushDownExistentialValuesQuantifier() {
        final Quantifier valuesBox = valuesQun(LiteralValue.ofScalar(42L));
        final Quantifier existsValues = Quantifier.existential(valuesBox.getRangesOver());

        final Quantifier base = baseT();
        final SelectExpression selectExpression = join(base, existsValues)
                .addResultColumn(projectColumn(base, "a"))
                .addPredicate(new ExistsPredicate(existsValues.getAlias()))
                .build().buildSelect();

        testHelper.assertYieldsNothing(selectExpression, true);
    }

    /**
     * Validate that the rule is not applied if all the children are existential quantifiers.
     * The rule only needs to fire if there is at least one for each quantifier.
     */
    @Test
    void doNotMatchIfAllExistentialQuantifiers() {
        final Quantifier valuesBox = valuesQun(LiteralValue.ofScalar(42L));
        final Quantifier existsValues = Quantifier.existential(valuesBox.getRangesOver());

        final Quantifier base = baseT();
        final Quantifier existsT = exists(selectWithPredicates(base,
                fieldPredicate(base, "a", EQUALS_42)));
        final SelectExpression selectExpression = join(existsValues, existsT)
                .addResultColumn(Column.of(Optional.of("x"), LiteralValue.ofScalar("y")))
                .addPredicate(new ExistsPredicate(existsValues.getAlias()))
                .addPredicate(new ExistsPredicate(existsT.getAlias()))
                .build().buildSelect();

        // Note: it seems like we should _not_ match the rule here (rather than matching but
        // not yielding anything). It could be that the multi-matcher some(matcher) is vacuously
        // matching, which is leading this rule to be matched
        testHelper.assertYieldsNothing(selectExpression, true);
    }

    /**
     * Validate that if we had a select over just a values box, we replace it with a
     * single {@code range(1)} child.
     */
    @Test
    void removeValuesIfOnlyChild() {
        final Quantifier valuesBox = valuesQun(LiteralValue.ofScalar(true));
        final SelectExpression selectOnlyValue = new SelectExpression(
                LiteralValue.ofScalar("hello"),
                ImmutableList.of(valuesBox),
                ImmutableList.of(valuesBox.getFlowedObjectValue().withComparison(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, true)))
        );

        final SelectExpression expected = new SelectExpression(
                LiteralValue.ofScalar("hello"),
                ImmutableList.of(rangeOneQun()),
                ImmutableList.of(LiteralValue.ofScalar(true).withComparison(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, true)))
        );

        testHelper.assertYields(selectOnlyValue, expected);
    }

    /**
     * Validate that if we had a select over multiple values boxes, we push it down by introducing
     * a single {@code range(1)} child. This is similar to {@link #removeValuesIfOnlyChild()}, but it
     * validates the behavior if there are multiple such children. In particular, note that both children
     * are replaced with a single {@code range(1)} quantifier&mdash;not one for each child.
     */
    @Test
    void removeValuesIfAllChildren() {
        final Quantifier valuesBox1 = valuesQun(LiteralValue.ofScalar("hello"));
        final Quantifier valuesBox2 = valuesQun(LiteralValue.ofScalar("world"));
        final SelectExpression selectOnlyValue = new SelectExpression(
                LiteralValue.ofScalar(42L),
                ImmutableList.of(valuesBox1, valuesBox2),
                ImmutableList.of(valuesBox1.getFlowedObjectValue().withComparison(new Comparisons.ValueComparison(Comparisons.Type.LESS_THAN, valuesBox2.getFlowedObjectValue())))
        );

        final SelectExpression expected = new SelectExpression(
                LiteralValue.ofScalar(42L),
                ImmutableList.of(rangeOneQun()),
                ImmutableList.of(LiteralValue.ofScalar("hello").withComparison(new Comparisons.ValueComparison(Comparisons.Type.LESS_THAN, LiteralValue.ofScalar("world"))))
        );

        testHelper.assertYields(selectOnlyValue, expected);
    }
}
