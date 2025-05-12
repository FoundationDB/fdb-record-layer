/*
 * RuleTestHelpers.java
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
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.PlanContext;
import com.apple.foundationdb.record.query.plan.cascades.PlannerStage;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.assertj.core.api.AutoCloseableSoftAssertions;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

import static com.apple.foundationdb.record.provider.foundationdb.query.FDBQueryGraphTestHelpers.forEach;

public class RuleTestHelper {
    @Nonnull
    public static final Comparisons.Comparison EQUALS_42 = new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 42L);
    @Nonnull
    public static final Comparisons.Comparison GREATER_THAN_HELLO = new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, "hello");
    @Nonnull
    public static final Comparisons.Comparison EQUALS_PARAM = new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, "p");

    @Nonnull
    public static final Type.Record TYPE_S = Type.Record.fromFields(ImmutableList.of(
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, true), Optional.of("one")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("two")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.BYTES, true), Optional.of("three"))
    ));

    @Nonnull
    public static final Type.Record TYPE_T = Type.Record.fromFields(ImmutableList.of(
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, true), Optional.of("a")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("b")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.BYTES, true), Optional.of("c")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("d")),
            Type.Record.Field.of(TYPE_S, Optional.of("e")),
            Type.Record.Field.of(new Type.Array(true, Type.primitiveType(Type.TypeCode.LONG, true)), Optional.of("f")),
            Type.Record.Field.of(new Type.Array(true, TYPE_S), Optional.of("g"))
    ));

    @Nonnull
    public static final Type.Record TYPE_TAU = Type.Record.fromFields(ImmutableList.of(
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, true), Optional.of("alpha")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("beta")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.BYTES, true), Optional.of("gamma")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("delta")),
            Type.Record.Field.of(TYPE_S, Optional.of("epsilon")),
            Type.Record.Field.of(new Type.Array(true, Type.primitiveType(Type.TypeCode.LONG, true)), Optional.of("zeta")),
            Type.Record.Field.of(new Type.Array(true, TYPE_S), Optional.of("eta"))
    ));

    @Nonnull
    public static Quantifier fuseQun() {
        return forEach(new FullUnorderedScanExpression(ImmutableSet.of("T", "TAU"), Type.Record.fromFields(ImmutableList.of()), new AccessHints()));
    }

    @Nonnull
    public static Quantifier baseT() {
        return forEach(new LogicalTypeFilterExpression(ImmutableSet.of("T"), fuseQun(), TYPE_T));
    }

    @Nonnull
    public static Quantifier baseTau() {
        return forEach(new LogicalTypeFilterExpression(ImmutableSet.of("TAU"), fuseQun(), TYPE_TAU));
    }

    @Nonnull
    public static GraphExpansion.Builder join(Quantifier... quns) {
        return GraphExpansion.builder().addAllQuantifiers(List.of(quns));
    }

    @Nonnull
    private final CascadesRule<? extends RelationalExpression> rule;

    public RuleTestHelper(@Nonnull CascadesRule<? extends RelationalExpression> rule) {
        this.rule = rule;
    }

    @Nonnull
    private TestRuleExecution run(RelationalExpression original) {
        Reference ref = Reference.ofExploratoryExpression(PlannerStage.CANONICAL, original);
        PlanContext planContext = new FakePlanContext();
        return TestRuleExecution.applyRule(planContext, rule, ref, EvaluationContext.EMPTY);
    }

    @Nonnull
    public TestRuleExecution assertYields(RelationalExpression original, RelationalExpression... expected) {
        TestRuleExecution execution = run(original);
        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            softly.assertThat(execution.getResult().getAllMemberExpressions())
                    .hasSize(1 + expected.length)
                    .containsAll(List.of(expected));
        }
        return execution;
    }

    @Nonnull
    public TestRuleExecution assertYieldsNothing(RelationalExpression original, boolean matched) {
        TestRuleExecution execution = run(original);
        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            softly.assertThat(execution.getResult().getAllMemberExpressions())
                    .containsExactly(original);
            softly.assertThat(execution.isRuleMatched())
                    .as("rule should %shave been matched", matched ? "" : "not ")
                    .isEqualTo(matched);
        }
        return execution;
    }
}
