/*
 * ValueComparisonTest.java
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

package com.apple.foundationdb.record.query.expressions;

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.ParameterRelationshipGraph;
import com.apple.foundationdb.record.query.expressions.Comparisons.Comparison;
import com.apple.foundationdb.record.query.expressions.Comparisons.ValueComparison;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class ValueComparisonTest extends ComparisonsTestBase {
    @Test
    void withValueTest() {
        final ValueComparison original = comparison();
        final Value originalVectorValue = original.getComparandValue();
        final ValueComparison withNewValue = original.withValue(new LiteralValue<>(20));
        assertThat(withNewValue).isNotEqualTo(original);
        final ValueComparison withOldValue = original.withValue(originalVectorValue);
        assertThat(withOldValue.planHash(PlanHashable.CURRENT_FOR_CONTINUATION))
                .isEqualTo(original.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        assertThat(withOldValue).hasSameHashCodeAs(original);
        assertThat(withOldValue).isEqualTo(original);
    }

    @Test
    void withTypeTest() {
        final ValueComparison original = comparison();
        final Comparisons.Type originalVectorType = original.getType();
        final ValueComparison withNewType = original.withType(Comparisons.Type.DISTANCE_RANK_LESS_THAN);
        assertThat(withNewType).isNotEqualTo(original);
        final ValueComparison withOldType = original.withType(originalVectorType);
        assertThat(withOldType.planHash(PlanHashable.CURRENT_FOR_CONTINUATION))
                .isEqualTo(original.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        assertThat(withOldType).hasSameHashCodeAs(original);
        assertThat(withOldType).isEqualTo(original);
    }

    @Test
    void withParameterRelationshipMapTest() {
        final ValueComparison original = comparison();
        final ValueComparison withNewGraph =
                original.withParameterRelationshipMap(ParameterRelationshipGraph.empty());
        assertThat(withNewGraph).hasSameHashCodeAs(original);
        assertThat(withNewGraph).isEqualTo(original);
    }

    @Test
    void correlatedToTest() {
        final ValueComparison comparison = comparison();
        assertThat(comparison.getCorrelatedTo()).isEmpty();
        final ValueComparison correlatedComparison = correlatedComparison();
        assertThat(correlatedComparison.getCorrelatedTo()).containsExactly(q1());
        assertThat(correlatedComparison.isCorrelatedTo(q1())).isTrue();
        assertThat(correlatedComparison.isCorrelatedTo(q2())).isFalse();
    }

    @Test
    void replaceValuesTest() {
        final ValueComparison original = comparison();
        final TranslationMap translationMap =
                TranslationMap.regularBuilder()
                        .when(q1()).then(((sourceAlias, leafValue) -> original.getComparandValue()))
                        .build();

        final ValueComparison correlatedComparison = correlatedComparison();

        final Optional<Comparison> translatedOptional =
                correlatedComparison.replaceValuesMaybe(replacementFunctionFromTranslationMap(translationMap));
        assertThat(translatedOptional).contains(original);

        final TranslationMap badTranslationMap =
                TranslationMap.regularBuilder()
                        .when(q2()).then(((sourceAlias, leafValue) -> original.getComparandValue()))
                        .build();
        final Optional<Comparison> badlyTranslatedOptional =
                correlatedComparison.replaceValuesMaybe(replacementFunctionFromTranslationMap(badTranslationMap));
        assertThat(badlyTranslatedOptional).isEmpty();
    }

    @Test
    void translateCorrelationsTest() {
        final ValueComparison original = comparison();
        final TranslationMap translationMap =
                TranslationMap.regularBuilder()
                        .when(q1()).then(((sourceAlias, leafValue) -> original.getComparandValue()))
                        .build();

        final ValueComparison correlatedComparison = correlatedComparison();
        final ValueComparison translated =
                correlatedComparison.translateCorrelations(translationMap, false);
        assertThat(translated).isEqualTo(original);
    }

    @Test
    void protoRoundTripTest1() {
        protoRoundTripComparison(comparison());
    }

    @Test
    void protoRoundTripTest2() {
        protoRoundTripComparison(correlatedComparison());
    }

    @Test
    void explainTest() {
        final ValueComparison randomComparison = comparison();
        final ValueComparison randomComparison2 = comparison();
        assertThat(renderExplain(randomComparison)).isEqualTo(renderExplain(randomComparison2));
        assertThat(randomComparison.typelessString()).isEqualTo(randomComparison2.typelessString());
        assertThat(randomComparison).hasToString(randomComparison2.toString());

        final ValueComparison comparison = correlatedComparison();
        final ValueComparison comparison2 = correlatedComparison();
        assertThat(renderExplain(comparison)).isEqualTo(renderExplain(comparison2));
        assertThat(comparison.typelessString()).isEqualTo(comparison2.typelessString());
        assertThat(comparison).hasToString(comparison2.toString());
    }

    @Test
    void evalTest() {
        assertThat(comparison().eval(null, EvaluationContext.empty(), 10)).isTrue();
        assertThat(comparison().eval(null, EvaluationContext.empty(), 20)).isFalse();

        final EvaluationContext evaluationContext =
                EvaluationContext.empty().withBinding(Bindings.Internal.CORRELATION, q1(), 10);
        assertThat(comparison().eval(null, evaluationContext, 10)).isTrue();
        assertThat(comparison().eval(null, evaluationContext, 20)).isFalse();
    }

    @Nonnull
    private static ValueComparison correlatedComparison() {
        return new ValueComparison(Comparisons.Type.EQUALS,
                QuantifiedObjectValue.of(q1(), Type.primitiveType(Type.TypeCode.INT, false)));
    }

    @Nonnull
    private static ValueComparison comparison() {
        return new ValueComparison(Comparisons.Type.EQUALS, new LiteralValue<>(10));
    }
}
