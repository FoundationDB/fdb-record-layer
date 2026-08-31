/*
 * WithinDistanceComparisonTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.ParameterRelationshipGraph;
import com.apple.foundationdb.record.query.expressions.Comparisons.WithinDistanceComparison;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WithinDistanceComparisonTest extends ComparisonsTestBase {
    @Test
    void withValueTest() {
        final WithinDistanceComparison original = randomComparison();
        final Value originalCenterLat = original.getComparandValue();
        final WithinDistanceComparison withNewValue = original.withValue(randomDoubleValue());
        assertThat(withNewValue).isNotEqualTo(original);
        final WithinDistanceComparison withOldValue = original.withValue(originalCenterLat);
        assertThat(withOldValue.planHash(PlanHashable.CURRENT_FOR_CONTINUATION))
                .isEqualTo(original.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        assertThat(withOldValue).hasSameHashCodeAs(original);
        assertThat(withOldValue).isEqualTo(original);
    }

    @Test
    void withTypeTest() {
        final WithinDistanceComparison original = randomComparison();
        assertThat(original.getType()).isEqualTo(Comparisons.Type.WITHIN_DISTANCE);
        final WithinDistanceComparison sameType = original.withType(Comparisons.Type.WITHIN_DISTANCE);
        assertThat(sameType).isSameAs(original);
    }

    @Test
    void withParameterRelationshipMapTest() {
        final WithinDistanceComparison original = randomComparison();
        final WithinDistanceComparison withNewGraph =
                original.withParameterRelationshipMap(ParameterRelationshipGraph.empty());
        assertThat(withNewGraph).hasSameHashCodeAs(original);
        assertThat(withNewGraph).isEqualTo(original);
    }

    @Test
    void correlatedToTest() {
        final WithinDistanceComparison comparison = randomComparison();
        assertThat(comparison.getCorrelatedTo()).isEmpty();
        final WithinDistanceComparison correlated = correlatedComparison();
        assertThat(correlated.getCorrelatedTo()).containsExactlyInAnyOrder(q1(), q2(), q3());
        assertThat(correlated.isCorrelatedTo(q1())).isTrue();
        assertThat(correlated.isCorrelatedTo(q2())).isTrue();
        assertThat(correlated.isCorrelatedTo(q3())).isTrue();
    }

    @Test
    void replaceValuesTest() {
        final WithinDistanceComparison original = randomComparison();
        final TranslationMap translationMap =
                TranslationMap.regularBuilder()
                        .when(q1()).then(((sourceAlias, leafValue) -> original.getComparandValue()))
                        .when(q2()).then(((sourceAlias, leafValue) -> original.getCenterLongitudeValue()))
                        .when(q3()).then(((sourceAlias, leafValue) -> original.getRadiusMetersValue()))
                        .build();

        final WithinDistanceComparison correlated = correlatedComparison();
        final Optional<Comparisons.Comparison> translated =
                correlated.replaceValuesMaybe(replacementFunctionFromTranslationMap(translationMap));
        assertThat(translated).contains(original);

        final TranslationMap partialMap =
                TranslationMap.regularBuilder()
                        .when(q1()).then(((sourceAlias, leafValue) -> original.getComparandValue()))
                        .build();
        final Optional<Comparisons.Comparison> partial =
                correlated.replaceValuesMaybe(replacementFunctionFromTranslationMap(partialMap));
        assertThat(partial).isEmpty();
    }

    @Test
    void translateCorrelationsTest() {
        final WithinDistanceComparison original = randomComparison();
        final TranslationMap translationMap =
                TranslationMap.regularBuilder()
                        .when(q1()).then(((sourceAlias, leafValue) -> original.getComparandValue()))
                        .when(q2()).then(((sourceAlias, leafValue) -> original.getCenterLongitudeValue()))
                        .when(q3()).then(((sourceAlias, leafValue) -> original.getRadiusMetersValue()))
                        .build();

        final WithinDistanceComparison correlated = correlatedComparison();
        final WithinDistanceComparison translated =
                correlated.translateCorrelations(translationMap, false);
        assertThat(translated).isEqualTo(original);
    }

    @Test
    void protoRoundTripTest1() {
        protoRoundTripComparison(randomComparison());
    }

    @Test
    void protoRoundTripTest2() {
        protoRoundTripComparison(correlatedComparison());
    }

    @Test
    void explainTest() {
        final WithinDistanceComparison first = randomComparison();
        final WithinDistanceComparison second = randomComparison();
        assertThat(renderExplain(first)).isNotEqualTo(renderExplain(second));
        assertThat(first.typelessString()).isNotEqualTo(second.typelessString());
        assertThat(first).doesNotHaveToString(second.toString());

        final WithinDistanceComparison a = correlatedComparison();
        final WithinDistanceComparison b = correlatedComparison();
        assertThat(renderExplain(a)).isEqualTo(renderExplain(b));
        assertThat(a.typelessString()).isEqualTo(b.typelessString());
        assertThat(a).hasToString(b.toString());
    }

    @Test
    void evalTest() {
        assertThatThrownBy(() -> randomComparison().eval(null, EvaluationContext.empty(), 10.0))
                .isInstanceOf(IllegalStateException.class);
    }

    @Nonnull
    private static WithinDistanceComparison correlatedComparison() {
        return new WithinDistanceComparison(
                QuantifiedObjectValue.of(q1(), Type.primitiveType(Type.TypeCode.DOUBLE, false)),
                QuantifiedObjectValue.of(q2(), Type.primitiveType(Type.TypeCode.DOUBLE, false)),
                QuantifiedObjectValue.of(q3(), Type.primitiveType(Type.TypeCode.DOUBLE, false)));
    }

    @Nonnull
    private static WithinDistanceComparison randomComparison() {
        return new WithinDistanceComparison(randomDoubleValue(), randomDoubleValue(), randomDoubleValue());
    }

    @Nonnull
    private static LiteralValue<Double> randomDoubleValue() {
        return new LiteralValue<>(ThreadLocalRandom.current().nextDouble());
    }
}
