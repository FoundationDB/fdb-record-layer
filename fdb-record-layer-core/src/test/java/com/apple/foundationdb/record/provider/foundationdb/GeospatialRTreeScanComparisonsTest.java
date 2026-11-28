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

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PGeospatialRTreeScanComparisons;
import com.apple.foundationdb.record.planprotos.PIndexScanParameters;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.DoubleValueOrParameter;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainFormatter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

class GeospatialRTreeScanComparisonsTest {
    @Test
    void translateCorrelationsTest() {
        final ScanComparisons originalPrefixScanComparisons = randomScanComparisons();
        final ScanComparisons originalSuffixScanComparisons = randomScanComparisons();
        final GeospatialRTreeScanComparisons original = byCenterAndRadius(originalPrefixScanComparisons, originalSuffixScanComparisons);
        final TranslationMap translationMap =
                TranslationMap.regularBuilder()
                        .when(q1()).then(((sourceAlias, leafValue) ->
                                                  Objects.requireNonNull(originalPrefixScanComparisons.getEqualityComparisons().get(0).getValue())))
                        .when(q2()).then(((sourceAlias, leafValue) ->
                                                  Objects.requireNonNull(Iterables.getOnlyElement(originalPrefixScanComparisons.getInequalityComparisons()).getValue())))
                        .when(q3()).then(((sourceAlias, leafValue) ->
                                                  Objects.requireNonNull(originalSuffixScanComparisons.getEqualityComparisons().get(0).getValue())))
                        .when(q4()).then(((sourceAlias, leafValue) ->
                                                  Objects.requireNonNull(Iterables.getOnlyElement(originalSuffixScanComparisons.getInequalityComparisons()).getValue())))
                        .build();

        final GeospatialRTreeScanComparisons correlated =
                byCenterAndRadius(correlatedScanComparisons(q1(), q2()), correlatedScanComparisons(q3(), q4()));

        final IndexScanParameters translated = correlated.translateCorrelations(translationMap, false);
        assertThat(translated).isEqualTo(original);

        // Nothing to translate, so the same instance comes back.
        assertThat(original.translateCorrelations(translationMap, false)).isSameAs(original);
    }

    @Test
    void rebaseTest() {
        final AliasMap aliasMap = aliasMap();
        final GeospatialRTreeScanComparisons original =
                byCenterAndRadius(correlatedScanComparisons(q1(), q2()), correlatedScanComparisons(q3(), q4()));

        final IndexScanParameters rebased = original.rebase(aliasMap);
        assertThat(rebased).isNotEqualTo(original);
        assertThat(rebased.getCorrelatedTo()).containsExactly(q5(), q6(), q7(), q8());

        final ImmutableList.Builder<String> originalDetailsBuilder = ImmutableList.builder();
        final ImmutableMap.Builder<String, Attribute> originalAttributeMapBuilder = ImmutableMap.builder();
        original.getPlannerGraphDetails(originalDetailsBuilder, originalAttributeMapBuilder);
        final ImmutableList<String> originalDetails = originalDetailsBuilder.build();
        final ImmutableMap<String, Attribute> originalAttributeMap = originalAttributeMapBuilder.build();

        final ImmutableList.Builder<String> rebasedDetailsBuilder = ImmutableList.builder();
        final ImmutableMap.Builder<String, Attribute> rebasedAttributeMapBuilder = ImmutableMap.builder();
        rebased.getPlannerGraphDetails(rebasedDetailsBuilder, rebasedAttributeMapBuilder);

        assertThat(rebasedDetailsBuilder.build()).isEqualTo(originalDetails);
        assertThat(rebasedAttributeMapBuilder.build()).doesNotHaveToString(originalAttributeMap.toString());
        assertThat(rebased).doesNotHaveToString(original.toString());
        assertThat(renderExplain(rebased)).isNotEqualTo(renderExplain(original));

        final IndexScanParameters inverseRebased = rebased.rebase(aliasMap.inverse());
        assertThat(inverseRebased).isEqualTo(original);

        final ImmutableList.Builder<String> inverseRebasedDetailsBuilder = ImmutableList.builder();
        final ImmutableMap.Builder<String, Attribute> inverseRebasedAttributeMapBuilder = ImmutableMap.builder();
        inverseRebased.getPlannerGraphDetails(inverseRebasedDetailsBuilder, inverseRebasedAttributeMapBuilder);
        assertThat(inverseRebasedDetailsBuilder.build()).isEqualTo(originalDetails);
        assertThat(inverseRebasedAttributeMapBuilder.build()).hasToString(originalAttributeMap.toString());
        assertThat(inverseRebased).hasToString(original.toString());
        assertThat(renderExplain(inverseRebased)).isEqualTo(renderExplain(original));
        assertThat(inverseRebased).hasSameHashCodeAs(original);
        assertThat(inverseRebased.semanticHashCode()).isEqualTo(original.semanticHashCode());
    }

    @Test
    void rebaseKeepsParametersTest() {
        final GeospatialRTreeScanComparisons original =
                GeospatialRTreeScanComparisons.byCenterAndRadius(correlatedScanComparisons(q1(), q2()),
                        DoubleValueOrParameter.parameter("lat"),
                        DoubleValueOrParameter.parameter("lon"),
                        DoubleValueOrParameter.parameter("radius"),
                        correlatedScanComparisons(q3(), q4()));

        final GeospatialRTreeScanComparisons rebased = (GeospatialRTreeScanComparisons)original.rebase(aliasMap());
        assertThat(rebased.getCorrelatedTo()).containsExactly(q5(), q6(), q7(), q8());
        assertThat(rebased).hasToString(original.toString()
                .replace("q1", "q5").replace("q2", "q6").replace("q3", "q7").replace("q4", "q8"));
    }

    @Test
    void protoRoundTripTest() {
        final ImmutableList<GeospatialRTreeScanComparisons> originals = ImmutableList.of(
                byCenterAndRadius(randomScanComparisons(), randomScanComparisons()),
                GeospatialRTreeScanComparisons.byCenterAndRadius(correlatedScanComparisons(q1(), q2()),
                        DoubleValueOrParameter.parameter("lat"),
                        DoubleValueOrParameter.parameter("lon"),
                        DoubleValueOrParameter.parameter("radius"),
                        null));
        final GeospatialRTreeScanComparisons.Deserializer deserializer = new GeospatialRTreeScanComparisons.Deserializer();
        for (final GeospatialRTreeScanComparisons original : originals) {
            final PGeospatialRTreeScanComparisons proto = original.toProto(PlanSerializationContext.newForCurrentMode());
            for (final GeospatialRTreeScanComparisons afterRoundTrip : ImmutableList.of(
                    GeospatialRTreeScanComparisons.fromProto(PlanSerializationContext.newForCurrentMode(), proto),
                    deserializer.fromProto(PlanSerializationContext.newForCurrentMode(), proto))) {
                assertThat(afterRoundTrip).isEqualTo(original);
                assertThat(afterRoundTrip).hasSameHashCodeAs(original);
                assertThat(afterRoundTrip).hasToString(original.toString());
                assertThat(afterRoundTrip.planHash(PlanHashable.CURRENT_FOR_CONTINUATION))
                        .isEqualTo(original.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
            }
        }
        assertThat(deserializer.getProtoMessageClass()).isEqualTo(PGeospatialRTreeScanComparisons.class);
    }

    @Test
    void indexScanParametersProtoTest() {
        final GeospatialRTreeScanComparisons original =
                byCenterAndRadius(randomScanComparisons(), randomScanComparisons());
        final PIndexScanParameters proto = original.toIndexScanParametersProto(PlanSerializationContext.newForCurrentMode());
        assertThat(proto.hasGeospatialRtreeScanComparisons()).isTrue();
        assertThat(proto.getGeospatialRtreeScanComparisons())
                .isEqualTo(original.toProto(PlanSerializationContext.newForCurrentMode()));

        final IndexScanParameters afterRoundTrip =
                IndexScanParameters.fromIndexScanParametersProto(PlanSerializationContext.newForCurrentMode(), proto);
        assertThat(afterRoundTrip).isEqualTo(original);
    }

    @Test
    void planHashTest() {
        final ScanComparisons prefix = randomScanComparisons();
        final ScanComparisons suffix = randomScanComparisons();
        final GeospatialRTreeScanComparisons original = byCenterAndRadius(prefix, suffix);
        final GeospatialRTreeScanComparisons same = byCenterAndRadius(prefix, suffix);
        final ImmutableList<GeospatialRTreeScanComparisons> different = ImmutableList.of(
                GeospatialRTreeScanComparisons.byCenterAndRadius(prefix, DoubleValueOrParameter.value(1.0),
                        DoubleValueOrParameter.value(-122.42), DoubleValueOrParameter.value(5000.0), suffix),
                GeospatialRTreeScanComparisons.byCenterAndRadius(prefix, DoubleValueOrParameter.value(37.77),
                        DoubleValueOrParameter.value(1.0), DoubleValueOrParameter.value(5000.0), suffix),
                GeospatialRTreeScanComparisons.byCenterAndRadius(prefix, DoubleValueOrParameter.value(37.77),
                        DoubleValueOrParameter.value(-122.42), DoubleValueOrParameter.value(1.0), suffix),
                GeospatialRTreeScanComparisons.byCenterAndRadius(null, DoubleValueOrParameter.value(37.77),
                        DoubleValueOrParameter.value(-122.42), DoubleValueOrParameter.value(5000.0), suffix),
                GeospatialRTreeScanComparisons.byCenterAndRadius(prefix, DoubleValueOrParameter.value(37.77),
                        DoubleValueOrParameter.value(-122.42), DoubleValueOrParameter.value(5000.0), null));

        for (final PlanHashable.PlanHashMode mode : ImmutableList.of(PlanHashable.CURRENT_LEGACY, PlanHashable.CURRENT_FOR_CONTINUATION)) {
            assertThat(same.planHash(mode)).isEqualTo(original.planHash(mode));
            for (final GeospatialRTreeScanComparisons other : different) {
                assertThat(other.planHash(mode)).isNotEqualTo(original.planHash(mode));
            }
        }
    }

    @Nonnull
    private static GeospatialRTreeScanComparisons byCenterAndRadius(@Nonnull final ScanComparisons prefixScanComparisons,
                                                                    @Nonnull final ScanComparisons suffixScanComparisons) {
        return GeospatialRTreeScanComparisons.byCenterAndRadius(prefixScanComparisons,
                DoubleValueOrParameter.value(37.77),
                DoubleValueOrParameter.value(-122.42),
                DoubleValueOrParameter.value(5000.0),
                suffixScanComparisons);
    }

    @Nonnull
    private static AliasMap aliasMap() {
        return AliasMap.builder()
                .put(q1(), q5())
                .put(q2(), q6())
                .put(q3(), q7())
                .put(q4(), q8())
                .build();
    }

    @Nonnull
    private static String renderExplain(@Nonnull final IndexScanParameters indexScanParameters) {
        return indexScanParameters.explain()
                .getExplainTokens()
                .render(DefaultExplainFormatter.forDebugging())
                .toString();
    }

    @Nonnull
    private static ScanComparisons randomScanComparisons() {
        return new ScanComparisons.Builder()
                .addEqualityComparison(
                        new Comparisons.ValueComparison(Comparisons.Type.EQUALS,
                                new LiteralValue<>(ThreadLocalRandom.current().nextInt(100))))
                .addInequalityComparison(
                        new Comparisons.ValueComparison(Comparisons.Type.LESS_THAN,
                                new LiteralValue<>(ThreadLocalRandom.current().nextInt(100))))
                .build();
    }

    @Nonnull
    private static ScanComparisons correlatedScanComparisons(@Nonnull final CorrelationIdentifier equalityAlias,
                                                             @Nonnull final CorrelationIdentifier inequalityAlias) {
        return new ScanComparisons.Builder()
                .addEqualityComparison(
                        new Comparisons.ValueComparison(Comparisons.Type.EQUALS,
                                QuantifiedObjectValue.of(equalityAlias, Type.primitiveType(Type.TypeCode.INT))))
                .addInequalityComparison(
                        new Comparisons.ValueComparison(Comparisons.Type.LESS_THAN,
                                QuantifiedObjectValue.of(inequalityAlias, Type.primitiveType(Type.TypeCode.INT))))
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
    private static CorrelationIdentifier q4() {
        return CorrelationIdentifier.of("q4");
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

    @Nonnull
    private static CorrelationIdentifier q8() {
        return CorrelationIdentifier.of("q8");
    }
}
