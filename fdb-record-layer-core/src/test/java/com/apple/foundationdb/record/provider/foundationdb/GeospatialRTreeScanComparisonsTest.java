/*
 * GeospatialRTreeScanComparisonsTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PDoubleValueOrParameter;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.DoubleValueOrParameter;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Correlation-aware behaviour of {@link GeospatialRTreeScanComparisons} when the center latitude/longitude and radius
 * are supplied by Cascades {@link com.apple.foundationdb.record.query.plan.cascades.values.Value} expressions rather
 * than literals or named-parameter bindings.
 */
class GeospatialRTreeScanComparisonsTest {

    @Nonnull
    private static final Type DOUBLE_TYPE = Type.primitiveType(Type.TypeCode.DOUBLE, false);

    @Test
    void valueBackedCenterAndRadiusReportCorrelations() {
        final DoubleValueOrParameter centerLatitude =
                DoubleValueOrParameter.valueExpression(QuantifiedObjectValue.of(q1(), DOUBLE_TYPE));
        final DoubleValueOrParameter centerLongitude =
                DoubleValueOrParameter.valueExpression(QuantifiedObjectValue.of(q2(), DOUBLE_TYPE));
        final DoubleValueOrParameter radius =
                DoubleValueOrParameter.valueExpression(QuantifiedObjectValue.of(q3(), DOUBLE_TYPE));

        final GeospatialRTreeScanComparisons comparisons = GeospatialRTreeScanComparisons.byCenterAndRadius(
                ScanComparisons.EMPTY, centerLatitude, centerLongitude, radius, ScanComparisons.EMPTY);

        assertThat(comparisons.getCorrelatedTo()).containsExactlyInAnyOrder(q1(), q2(), q3());
    }

    @Test
    void literalCenterAndRadiusHaveNoCorrelations() {
        final GeospatialRTreeScanComparisons comparisons = GeospatialRTreeScanComparisons.byCenterAndRadius(
                ScanComparisons.EMPTY,
                DoubleValueOrParameter.value(37.7749),
                DoubleValueOrParameter.value(-122.4194),
                DoubleValueOrParameter.value(1000.0),
                ScanComparisons.EMPTY);

        assertThat(comparisons.getCorrelatedTo()).isEmpty();
    }

    @Test
    void parameterCenterAndRadiusHaveNoCorrelations() {
        final GeospatialRTreeScanComparisons comparisons = GeospatialRTreeScanComparisons.byCenterAndRadius(
                ScanComparisons.EMPTY,
                DoubleValueOrParameter.parameter("lat"),
                DoubleValueOrParameter.parameter("lon"),
                DoubleValueOrParameter.parameter("r"),
                ScanComparisons.EMPTY);

        assertThat(comparisons.getCorrelatedTo()).isEmpty();
    }

    @Test
    void translateCorrelationsTranslatesValueBackedFields() {
        final ScanComparisons originalPrefix = correlatedPrefixScanComparisons();
        final GeospatialRTreeScanComparisons original = GeospatialRTreeScanComparisons.byCenterAndRadius(
                originalPrefix,
                DoubleValueOrParameter.valueExpression(QuantifiedObjectValue.of(q1(), DOUBLE_TYPE)),
                DoubleValueOrParameter.valueExpression(QuantifiedObjectValue.of(q2(), DOUBLE_TYPE)),
                DoubleValueOrParameter.valueExpression(QuantifiedObjectValue.of(q3(), DOUBLE_TYPE)),
                ScanComparisons.EMPTY);

        final TranslationMap translationMap =
                TranslationMap.regularBuilder()
                        .when(q1()).then(((sourceAlias, leafValue) -> QuantifiedObjectValue.of(q5(), DOUBLE_TYPE)))
                        .when(q2()).then(((sourceAlias, leafValue) -> QuantifiedObjectValue.of(q6(), DOUBLE_TYPE)))
                        .when(q3()).then(((sourceAlias, leafValue) -> QuantifiedObjectValue.of(q7(), DOUBLE_TYPE)))
                        .build();

        final IndexScanParameters translated = original.translateCorrelations(translationMap, false);
        assertThat(translated).isNotSameAs(original);
        assertThat(translated.getCorrelatedTo()).containsExactlyInAnyOrder(q5(), q6(), q7());
    }

    @Test
    void rebaseRewritesValueBackedFields() {
        final GeospatialRTreeScanComparisons original = GeospatialRTreeScanComparisons.byCenterAndRadius(
                ScanComparisons.EMPTY,
                DoubleValueOrParameter.valueExpression(QuantifiedObjectValue.of(q1(), DOUBLE_TYPE)),
                DoubleValueOrParameter.valueExpression(QuantifiedObjectValue.of(q2(), DOUBLE_TYPE)),
                DoubleValueOrParameter.valueExpression(QuantifiedObjectValue.of(q3(), DOUBLE_TYPE)),
                ScanComparisons.EMPTY);

        final AliasMap aliasMap =
                AliasMap.builder()
                        .put(q1(), q5())
                        .put(q2(), q6())
                        .put(q3(), q7())
                        .build();

        final IndexScanParameters rebased = original.rebase(aliasMap);
        assertThat(rebased.getCorrelatedTo()).containsExactlyInAnyOrder(q5(), q6(), q7());
        assertThat(rebased.rebase(aliasMap.inverse())).isEqualTo(original);
    }

    @Test
    void valueExpressionFactoryReturnsCorrelatedInstance() {
        final DoubleValueOrParameter latitude =
                DoubleValueOrParameter.valueExpression(QuantifiedObjectValue.of(q1(), DOUBLE_TYPE));
        assertThat(latitude.getCorrelatedTo()).containsExactly(q1());
    }

    @Test
    void literalDoubleValueOrParameterHasNoCorrelations() {
        assertThat(DoubleValueOrParameter.value(1.0).getCorrelatedTo()).isEmpty();
        assertThat(DoubleValueOrParameter.parameter("x").getCorrelatedTo()).isEmpty();
    }

    @Test
    void valueExpressionProtoRoundTrip() {
        final DoubleValueOrParameter original =
                DoubleValueOrParameter.valueExpression(new LiteralValue<>(42.0));
        final PDoubleValueOrParameter proto = original.toProto(PlanSerializationContext.newForCurrentMode());
        final DoubleValueOrParameter roundTripped =
                DoubleValueOrParameter.fromProto(PlanSerializationContext.newForCurrentMode(), proto);
        assertThat(roundTripped).isEqualTo(original);
    }

    @Test
    void literalDoubleValueProtoRoundTripUnchanged() {
        final DoubleValueOrParameter original = DoubleValueOrParameter.value(3.14);
        final PDoubleValueOrParameter proto = original.toProto(PlanSerializationContext.newForCurrentMode());
        final DoubleValueOrParameter roundTripped =
                DoubleValueOrParameter.fromProto(PlanSerializationContext.newForCurrentMode(), proto);
        assertThat(roundTripped).isEqualTo(original);
    }

    @Test
    void parameterDoubleValueProtoRoundTripUnchanged() {
        final DoubleValueOrParameter original = DoubleValueOrParameter.parameter("radius");
        final PDoubleValueOrParameter proto = original.toProto(PlanSerializationContext.newForCurrentMode());
        final DoubleValueOrParameter roundTripped =
                DoubleValueOrParameter.fromProto(PlanSerializationContext.newForCurrentMode(), proto);
        assertThat(roundTripped).isEqualTo(original);
    }

    @Nonnull
    private static ScanComparisons correlatedPrefixScanComparisons() {
        return new ScanComparisons.Builder()
                .addEqualityComparison(new Comparisons.ValueComparison(Comparisons.Type.EQUALS,
                        new LiteralValue<>(42)))
                .build();
    }

    @Nonnull
    private static CorrelationIdentifier q1() {
        return CorrelationIdentifier.of("q1");
    }

    @Nonnull
    private static CorrelationIdentifier q2() {
        return CorrelationIdentifier.of("q2");
    }

    @Nonnull
    private static CorrelationIdentifier q3() {
        return CorrelationIdentifier.of("q3");
    }

    @Nonnull
    private static CorrelationIdentifier q5() {
        return CorrelationIdentifier.of("q5");
    }

    @Nonnull
    private static CorrelationIdentifier q6() {
        return CorrelationIdentifier.of("q6");
    }

    @Nonnull
    private static CorrelationIdentifier q7() {
        return CorrelationIdentifier.of("q7");
    }
}
