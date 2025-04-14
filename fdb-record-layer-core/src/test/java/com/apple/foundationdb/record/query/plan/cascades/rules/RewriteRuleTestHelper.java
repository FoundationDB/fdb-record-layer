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
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.GraphExpansion;
import com.apple.foundationdb.record.query.plan.cascades.PlanContext;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.AutoCloseableSoftAssertions;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public class RewriteRuleTestHelper {
    @Nonnull
    public static final Comparisons.Comparison EQUALS_42 = new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 42L);
    @Nonnull
    public static final Comparisons.Comparison GREATER_THAN_HELLO = new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, "hello");
    @Nonnull
    public static final Comparisons.Comparison EQUALS_PARAM = new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, "p");

    @Nonnull
    public static final Type.Record TYPE_T = Type.Record.fromFields(ImmutableList.of(
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, true), Optional.of("a")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("b")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.BYTES, true), Optional.of("c")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("d"))
    ));

    @Nonnull
    public static final Type.Record TYPE_TAU = Type.Record.fromFields(ImmutableList.of(
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, true), Optional.of("alpha")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("beta")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.BYTES, true), Optional.of("gamma")),
            Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING, true), Optional.of("delta"))
    ));

    @Nonnull
    public static Quantifier forEach(RelationalExpression relationalExpression) {
        return Quantifier.forEach(Reference.of(relationalExpression));
    }

    @Nonnull
    public static Quantifier baseT() {
        return forEach(new FullUnorderedScanExpression(Set.of("T"), TYPE_T, new AccessHints()));
    }

    @Nonnull
    public static Quantifier baseTau() {
        return forEach(new FullUnorderedScanExpression(Set.of("TAU"), TYPE_TAU, new AccessHints()));
    }

    @Nonnull
    public static FieldValue fieldValue(Quantifier qun, String fieldName) {
        return FieldValue.ofFieldNameAndFuseIfPossible(qun.getFlowedObjectValue(), fieldName);
    }

    @Nonnull
    public static QueryPredicate fieldPredicate(Quantifier qun, String fieldName, Comparisons.Comparison comparison) {
        return fieldValue(qun, fieldName).withComparison(comparison);
    }

    @Nonnull
    public static GraphExpansion.Builder join(Quantifier... quns) {
        return GraphExpansion.builder().addAllQuantifiers(List.of(quns));
    }

    @Nonnull
    public static SelectExpression selectWithPredicates(Quantifier qun, Map<String, String> projection, QueryPredicate... predicates) {
        GraphExpansion.Builder builder = GraphExpansion.builder().addQuantifier(qun);
        for (Map.Entry<String, String> p : projection.entrySet()) {
            builder.addResultColumn(Column.of(Optional.of(p.getValue()), FieldValue.ofFieldName(qun.getFlowedObjectValue(), p.getKey())));
        }
        builder.addAllPredicates(List.of(predicates));
        return builder.build().buildSelect();
    }

    @Nonnull
    public static SelectExpression selectWithPredicates(Quantifier qun, List<String> projection, QueryPredicate... predicates) {
        Map<String, String> identityProjectionMap = projection.stream().collect(ImmutableMap.toImmutableMap(Function.identity(), Function.identity()));
        return selectWithPredicates(qun, identityProjectionMap, predicates);
    }

    @Nonnull
    public static SelectExpression selectWithPredicates(Quantifier qun, QueryPredicate... predicates) {
        return new SelectExpression(qun.getFlowedObjectValue(), List.of(qun), List.of(predicates));
    }

    @Nonnull
    private final CascadesRule<? extends RelationalExpression> rule;

    public RewriteRuleTestHelper(@Nonnull CascadesRule<? extends RelationalExpression> rule) {
        this.rule = rule;
    }

    @Nonnull
    private Reference runRewrite(RelationalExpression original) {
        Reference ref = Reference.of(original);
        PlanContext planContext = new FakePlanContext();
        TestRuleExecution ruleExecution = TestRuleExecution.applyRule(planContext, rule, ref, EvaluationContext.EMPTY);
        return ref;
    }

    public void assertYields(RelationalExpression original, RelationalExpression... expected) {
        Reference ref = runRewrite(original);
        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            softly.assertThat(ref.getMembers())
                    .hasSize(1 + expected.length)
                    .containsAll(List.of(expected));
        }
    }

    public void assertYieldsNothing(RelationalExpression original) {
        Reference ref = runRewrite(original);
        Assertions.assertThat(ref.getMembers())
                .containsExactly(original);
    }
}
