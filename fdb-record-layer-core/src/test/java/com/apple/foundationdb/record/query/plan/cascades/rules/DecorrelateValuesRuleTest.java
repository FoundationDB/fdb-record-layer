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
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalUnionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ExistsPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.debug.DebuggerWithSymbolTables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import static com.apple.foundationdb.record.query.plan.cascades.rules.RuleTestHelper.baseT;
import static com.apple.foundationdb.record.query.plan.cascades.rules.RuleTestHelper.baseTau;
import static com.apple.foundationdb.record.query.plan.cascades.rules.RuleTestHelper.join;
import static com.apple.foundationdb.record.query.plan.cascades.rules.RuleTestHelper.valuesQun;

/**
 * Tests of the {@link DecorrelateValuesRule}.
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
     * Decorrelate multiple values boxes at once. This is a similar set up to {@link #simpleDecorrelation()}, but
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

    @Test
    void pushIntoGroupBySelectHaving() {
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
}
