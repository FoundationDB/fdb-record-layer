/*
 * DistanceRankValueComparisonTest.java
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

import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.ParameterRelationshipGraph;
import com.apple.foundationdb.record.query.expressions.Comparisons.DistanceRankValueComparison;
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

class DistanceRankValueComparisonTest extends ComparisonsTestBase {
    @Test
    void withValueTest() {
        final DistanceRankValueComparison original = randomComparison();
        final Value originalVectorValue = original.getComparandValue();
        final DistanceRankValueComparison withNewValue = original.withValue(getRandomVectorValue());
        assertThat(withNewValue).isNotEqualTo(original);
        final DistanceRankValueComparison withOldValue = original.withValue(originalVectorValue);
        assertThat(withOldValue.planHash(PlanHashable.CURRENT_FOR_CONTINUATION))
                .isEqualTo(original.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        assertThat(withOldValue).hasSameHashCodeAs(original);
        assertThat(withOldValue).isEqualTo(original);
    }

    @Test
    void withTypeTest() {
        final DistanceRankValueComparison original = randomComparison();
        final Comparisons.Type originalVectorType = original.getType();
        final DistanceRankValueComparison withNewType = original.withType(Comparisons.Type.DISTANCE_RANK_LESS_THAN);
        assertThat(withNewType).isNotEqualTo(original);
        final DistanceRankValueComparison withOldType = original.withType(originalVectorType);
        assertThat(withOldType.planHash(PlanHashable.CURRENT_FOR_CONTINUATION))
                .isEqualTo(original.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        assertThat(withOldType).hasSameHashCodeAs(original);
        assertThat(withOldType).isEqualTo(original);
    }

    @Test
    void withParameterRelationshipMapTest() {
        final DistanceRankValueComparison original = randomComparison();
        final DistanceRankValueComparison withNewGraph =
                original.withParameterRelationshipMap(ParameterRelationshipGraph.empty());
        assertThat(withNewGraph).hasSameHashCodeAs(original);
        assertThat(withNewGraph).isEqualTo(original);
    }

    @Test
    void correlatedToTest() {
        final DistanceRankValueComparison comparison = randomComparison();
        assertThat(comparison.getCorrelatedTo()).isEmpty();
        final DistanceRankValueComparison correlatedComparison = correlatedComparison();
        assertThat(correlatedComparison.getCorrelatedTo()).containsExactly(q1(), q2());
        assertThat(correlatedComparison.isCorrelatedTo(q1())).isTrue();
        assertThat(correlatedComparison.isCorrelatedTo(q2())).isTrue();
        assertThat(correlatedComparison.isCorrelatedTo(q3())).isFalse();
    }

    @Test
    void replaceValuesTest() {
        final DistanceRankValueComparison original = randomComparison();
        final TranslationMap translationMap =
                TranslationMap.regularBuilder()
                        .when(q1()).then(((sourceAlias, leafValue) -> original.getComparandValue()))
                        .when(q2()).then(((sourceAlias, leafValue) -> original.getLimitValue()))
                        .build();

        final DistanceRankValueComparison correlatedComparison = correlatedComparison();

        final Optional<Comparisons.Comparison> translatedOptional =
                correlatedComparison.replaceValuesMaybe(replacementFunctionFromTranslationMap(translationMap));
        assertThat(translatedOptional).contains(original);

        final TranslationMap badTranslationMap =
                TranslationMap.regularBuilder()
                        .when(q1()).then(((sourceAlias, leafValue) -> original.getComparandValue()))
                        .build();
        final Optional<Comparisons.Comparison> badlyTranslatedOptional =
                correlatedComparison.replaceValuesMaybe(replacementFunctionFromTranslationMap(badTranslationMap));
        assertThat(badlyTranslatedOptional).isEmpty();
    }

    @Test
    void translateCorrelationsTest() {
        final DistanceRankValueComparison original = randomComparison();
        final TranslationMap translationMap =
                TranslationMap.regularBuilder()
                        .when(q1()).then(((sourceAlias, leafValue) -> original.getComparandValue()))
                        .when(q2()).then(((sourceAlias, leafValue) -> original.getLimitValue()))
                        .build();

        final DistanceRankValueComparison correlatedComparison = correlatedComparison();
        final DistanceRankValueComparison translated =
                correlatedComparison.translateCorrelations(translationMap, false);
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
        final DistanceRankValueComparison randomComparison = randomComparison();
        final DistanceRankValueComparison randomComparison2 = randomComparison();
        assertThat(renderExplain(randomComparison)).isNotEqualTo(renderExplain(randomComparison2));
        assertThat(randomComparison.typelessString()).isNotEqualTo(randomComparison2.typelessString());
        assertThat(randomComparison).doesNotHaveToString(randomComparison2.toString());

        final DistanceRankValueComparison comparison = correlatedComparison();
        final DistanceRankValueComparison comparison2 = correlatedComparison();
        assertThat(renderExplain(comparison)).isEqualTo(renderExplain(comparison2));
        assertThat(comparison.typelessString()).isEqualTo(comparison2.typelessString());
        assertThat(comparison).hasToString(comparison2.toString());
    }

    @Test
    void evalTest() {
        assertThatThrownBy(() -> randomComparison().eval(null, EvaluationContext.empty(), 10))
                .isInstanceOf(IllegalStateException.class);
    }

    @Nonnull
    private static DistanceRankValueComparison correlatedComparison() {
        return new DistanceRankValueComparison(Comparisons.Type.DISTANCE_RANK_LESS_THAN_OR_EQUAL,
                QuantifiedObjectValue.of(q1(), Type.Vector.of(false, 64, 128)),
                QuantifiedObjectValue.of(q2(), Type.primitiveType(Type.TypeCode.INT, false)),
                null, null);
    }

    @Nonnull
    private static DistanceRankValueComparison randomComparison() {
        return new DistanceRankValueComparison(Comparisons.Type.DISTANCE_RANK_LESS_THAN_OR_EQUAL,
                getRandomVectorValue(), new LiteralValue<>(10),
                null, null);
    }

    @Nonnull
    private static LiteralValue<DoubleRealVector> getRandomVectorValue() {
        final int numDimensions = 128;
        final double[] components = new double[128];
        for (int i = 0; i < numDimensions; i ++) {
            components[i] = ThreadLocalRandom.current().nextDouble();
        }
        return new LiteralValue<>(Type.Vector.of(false, 64, 128),
                new DoubleRealVector(components));
    }
}
