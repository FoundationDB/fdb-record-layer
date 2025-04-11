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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.PlanContext;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.Traversal;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.debug.DebuggerWithSymbolTables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.AutoCloseableSoftAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Tests of the {@link PredicatePushDownRule}. These operate by constructing expressions that
 * we should be able to push down, and then validating that the rule produces a transformed
 * expression that makes sense.
 */
public class PredicatePushDownRuleTest {
    private static final PredicatePushDownRule rule = new PredicatePushDownRule();
    private static final Comparisons.Comparison EQUALS_42 = new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 42L);
    private static final Comparisons.Comparison GREATER_THAN_HELLO = new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, "hello");
    private static final Comparisons.Comparison EQUALS_PARAM = new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, "p");

    private static final Type.Record TYPE_T = Type.Record.fromFields(ImmutableList.of(
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, true), Optional.of("a")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("b")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.BYTES, true), Optional.of("c")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("d"))
    ));

    private static final Type.Record TYPE_TAU = Type.Record.fromFields(ImmutableList.of(
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, true), Optional.of("alpha")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("beta")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.BYTES, true), Optional.of("gamma")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("delta"))
    ));

    @Nonnull
    private static Reference runRewrite(RelationalExpression original) {
        Reference ref = Reference.of(original);
        PlanContext planContext = new FakePlanContext();
        rule.getMatcher().bindMatches(planContext.getPlannerConfiguration(), PlannerBindings.empty(), original).forEach(match -> {
            CascadesRuleCall ruleCall = new CascadesRuleCall(planContext, rule, ref, Traversal.withRoot(ref), new ArrayDeque<>(), match, EvaluationContext.EMPTY);
            ruleCall.run();
        });
        return ref;
    }

    private static void assertYields(RelationalExpression original, RelationalExpression... expected) {
        Reference ref = runRewrite(original);
        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            softly.assertThat(ref.getMembers())
                    .hasSize(1 + expected.length)
                    .containsAll(List.of(expected));
        }
    }

    private static void assertMakesNoChanges(RelationalExpression original) {
        Reference ref = runRewrite(original);
        Assertions.assertThat(ref.getMembers())
                .containsExactly(original);
    }

    @Nonnull
    private static Quantifier forEach(RelationalExpression relationalExpression) {
        return Quantifier.forEach(Reference.of(relationalExpression));
    }

    @Nonnull
    private static Quantifier baseT() {
        return forEach(new FullUnorderedScanExpression(Set.of("T"), TYPE_T, new AccessHints()));
    }

    @Nonnull
    private static Quantifier baseTau() {
        return forEach(new FullUnorderedScanExpression(Set.of("TAU"), TYPE_TAU, new AccessHints()));
    }

    private static FieldValue fieldValue(Quantifier qun, String fieldName) {
        return FieldValue.ofFieldNameAndFuseIfPossible(qun.getFlowedObjectValue(), fieldName);
    }

    @Nonnull
    private static QueryPredicate fieldPredicate(Quantifier qun, String fieldName, Comparisons.Comparison comparison) {
        return fieldValue(qun, fieldName).withComparison(comparison);
    }

    private static GraphExpansion.Builder join(Quantifier... quns) {
        return GraphExpansion.builder().addAllQuantifiers(List.of(quns));
    }

    @Nonnull
    private static SelectExpression selectWithPredicates(Quantifier qun, Map<String, String> projection, QueryPredicate... predicates) {
        GraphExpansion.Builder builder = GraphExpansion.builder().addQuantifier(qun);
        for (Map.Entry<String, String> p : projection.entrySet()) {
            builder.addResultColumn(Column.of(Optional.of(p.getValue()), FieldValue.ofFieldName(qun.getFlowedObjectValue(), p.getKey())));
        }
        builder.addAllPredicates(List.of(predicates));
        return builder.build().buildSelect();
    }

    @Nonnull
    private static SelectExpression selectWithPredicates(Quantifier qun, List<String> projection, QueryPredicate... predicates) {
        Map<String, String> identityProjectionMap = projection.stream().collect(ImmutableMap.toImmutableMap(Function.identity(), Function.identity()));
        return selectWithPredicates(qun, identityProjectionMap, predicates);
    }

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

        assertYields(higher, newHigher);
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

        assertYields(higher, newHigher);
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

        assertYields(higher, newHigher);
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

        assertYields(higher, newHigher);
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

        assertYields(higher, newHigher);
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

        assertYields(higher, newHigher);
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

        assertYields(higher, newHigher);
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

        assertYields(higher, newHigher);
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

        assertMakesNoChanges(higher);
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

        assertYields(higher, newHigherWithNewT, newHigherWithNewTau);

        // If the rule is pushed to either of the new expressions, we should get a final version that pushes all
        // predicates down to both sides
        final SelectExpression newestHigher = join(newTLowQun, newTauLowQun)
                .addResultColumn(FDBQueryGraphTestHelpers.projectColumn(newTLowQun, "a"))
                .addResultColumn(FDBQueryGraphTestHelpers.projectColumn(newTauLowQun, "alpha"))
                .addPredicate(fieldPredicate(newTLowQun, "b", new Comparisons.ValueComparison(Comparisons.Type.EQUALS, fieldValue(newTauLowQun, "beta"))))
                .build()
                .buildSelect();

        assertYields(newHigherWithNewT, newestHigher);
        assertYields(newHigherWithNewTau, newestHigher);
    }
}
