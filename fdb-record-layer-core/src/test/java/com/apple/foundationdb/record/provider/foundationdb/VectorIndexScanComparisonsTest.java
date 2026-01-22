/*
 * VectorIndexScanComparisonsTest.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.record.query.expressions.Comparisons;
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

class VectorIndexScanComparisonsTest {
    @Test
    void translateCorrelationsTest() {
        final ScanComparisons originalPrefixScanComparisons = randomPrefixScanComparisons();
        final Comparisons.DistanceRankValueComparison originalDistanceRankComparison = randomDistanceRankComparison();
        final VectorIndexScanOptions originalScanOptions =
                VectorIndexScanOptions.builder()
                        .putOption(VectorIndexScanOptions.HNSW_EF_SEARCH, 101)
                        .putOption(VectorIndexScanOptions.HNSW_RETURN_VECTORS, false)
                        .build();
        final VectorIndexScanComparisons original =
                VectorIndexScanComparisons.byDistance(originalPrefixScanComparisons,
                        originalDistanceRankComparison,
                        originalScanOptions);
        final TranslationMap translationMap =
                TranslationMap.regularBuilder()
                        .when(q1()).then(((sourceAlias, leafValue) ->
                                                  Objects.requireNonNull(originalPrefixScanComparisons.getEqualityComparisons().get(0).getValue())))
                        .when(q2()).then(((sourceAlias, leafValue) ->
                                                  Objects.requireNonNull(Iterables.getOnlyElement(originalPrefixScanComparisons.getInequalityComparisons()).getValue())))
                        .when(q3()).then(((sourceAlias, leafValue) ->
                                                  originalDistanceRankComparison.getComparandValue()))
                        .when(q4()).then(((sourceAlias, leafValue) ->
                                                  originalDistanceRankComparison.getLimitValue()))
                        .build();

        final Comparisons.DistanceRankValueComparison correlatedDistanceRankComparison = correlatedDistanceRankComparison();
        final ScanComparisons correlatedPrefixScanComparisons = correlatedPrefixScanComparisons();

        final VectorIndexScanComparisons correlated =
                VectorIndexScanComparisons.byDistance(correlatedPrefixScanComparisons,
                        correlatedDistanceRankComparison,
                        originalScanOptions);

        final IndexScanParameters translated =
                correlated.translateCorrelations(translationMap, false);
        assertThat(translated).isEqualTo(original);
    }

    @Test
    void rebaseTest() {
        final VectorIndexScanOptions originalScanOptions =
                VectorIndexScanOptions.builder()
                        .putOption(VectorIndexScanOptions.HNSW_EF_SEARCH, 101)
                        .putOption(VectorIndexScanOptions.HNSW_RETURN_VECTORS, false)
                        .build();
        final AliasMap aliasMap =
                AliasMap.builder()
                        .put(q1(), q5())
                        .put(q2(), q6())
                        .put(q3(), q7())
                        .put(q4(), q8())
                        .build();

        final ScanComparisons originalPrefixScanComparisons = correlatedPrefixScanComparisons();
        final Comparisons.DistanceRankValueComparison originalDistanceRankComparison = correlatedDistanceRankComparison();

        final VectorIndexScanComparisons original =
                VectorIndexScanComparisons.byDistance(originalPrefixScanComparisons,
                        originalDistanceRankComparison,
                        originalScanOptions);

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
    void withComparisonsAndOptions() {
        final VectorIndexScanOptions originalScanOptions =
                VectorIndexScanOptions.builder()
                        .putOption(VectorIndexScanOptions.HNSW_EF_SEARCH, 101)
                        .putOption(VectorIndexScanOptions.HNSW_RETURN_VECTORS, false)
                        .build();

        final ScanComparisons originalPrefixScanComparisons = correlatedPrefixScanComparisons();
        final Comparisons.DistanceRankValueComparison originalDistanceRankComparison = correlatedDistanceRankComparison();

        final VectorIndexScanComparisons original =
                VectorIndexScanComparisons.byDistance(originalPrefixScanComparisons,
                        originalDistanceRankComparison,
                        originalScanOptions);

        final AliasMap aliasMap =
                AliasMap.builder()
                        .put(q1(), q5())
                        .put(q2(), q6())
                        .put(q3(), q7())
                        .put(q4(), q8())
                        .build();

        final ScanComparisons rebasedPrefixScanComparisons = originalPrefixScanComparisons.rebase(aliasMap);
        final Comparisons.DistanceRankValueComparison rebasedDistanceRankComparison =
                (Comparisons.DistanceRankValueComparison)originalDistanceRankComparison.rebase(aliasMap);
        final VectorIndexScanOptions newScanOptions =
                VectorIndexScanOptions.builder()
                        .putOption(VectorIndexScanOptions.HNSW_EF_SEARCH, 100)
                        .putOption(VectorIndexScanOptions.HNSW_RETURN_VECTORS, true)
                        .build();

        final var newVectorIndexComparisons =
                original.withComparisonsAndOptions(rebasedPrefixScanComparisons, rebasedDistanceRankComparison,
                        newScanOptions);
        assertThat(newVectorIndexComparisons.getPrefixScanComparisons()).isEqualTo(rebasedPrefixScanComparisons);
        assertThat(newVectorIndexComparisons.getDistanceRankValueComparison()).isEqualTo(rebasedDistanceRankComparison);
        assertThat(newVectorIndexComparisons.getVectorIndexScanOptions()).isEqualTo(newScanOptions);
    }

    @Test
    void scanComparisonsTest1() {
        final ScanComparisons originalPrefixScanComparisons = correlatedPrefixScanComparisons();
        final Comparisons.DistanceRankValueComparison originalDistanceRankComparison = correlatedDistanceRankComparison();

        final VectorIndexScanComparisons original =
                VectorIndexScanComparisons.byDistance(originalPrefixScanComparisons,
                        originalDistanceRankComparison,
                        VectorIndexScanOptions.empty());
        assertThat(original.hasScanComparisons()).isTrue();
        final ScanComparisons scanComparisons = original.getScanComparisons();
        assertThat(scanComparisons).isEqualTo(originalPrefixScanComparisons);
    }

    @Test
    void scanComparisonsTest2() {
        final ScanComparisons originalPrefixScanComparisons = correlatedEqualsPrefixScanComparisons();
        final Comparisons.DistanceRankValueComparison originalDistanceRankComparison = correlatedDistanceRankComparison();

        final VectorIndexScanComparisons original =
                VectorIndexScanComparisons.byDistance(originalPrefixScanComparisons,
                        originalDistanceRankComparison,
                        VectorIndexScanOptions.empty());
        assertThat(original.hasScanComparisons()).isTrue();
        final ScanComparisons scanComparisons = original.getScanComparisons();
        assertThat(scanComparisons.getEqualityComparisons()).isEqualTo(originalPrefixScanComparisons.getEqualityComparisons());
        assertThat(scanComparisons.getInequalityComparisons()).hasSize(1)
                .allSatisfy(comparison -> assertThat(comparison).isEqualTo(originalDistanceRankComparison));
    }

    @Nonnull
    protected static String renderExplain(@Nonnull final IndexScanParameters vectorIndexScanComparisons) {
        return vectorIndexScanComparisons.explain()
                .getExplainTokens()
                .render(DefaultExplainFormatter.forDebugging())
                .toString();
    }

    @Nonnull
    private static ScanComparisons randomPrefixScanComparisons() {
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
    private static ScanComparisons correlatedPrefixScanComparisons() {
        return new ScanComparisons.Builder()
                .addEqualityComparison(
                        new Comparisons.ValueComparison(Comparisons.Type.EQUALS,
                                QuantifiedObjectValue.of(q1(), Type.primitiveType(Type.TypeCode.INT))))
                .addInequalityComparison(
                        new Comparisons.ValueComparison(Comparisons.Type.LESS_THAN,
                                QuantifiedObjectValue.of(q2(), Type.primitiveType(Type.TypeCode.INT))))
                .build();
    }

    @Nonnull
    private static ScanComparisons correlatedEqualsPrefixScanComparisons() {
        return new ScanComparisons.Builder()
                .addEqualityComparison(
                        new Comparisons.ValueComparison(Comparisons.Type.EQUALS,
                                QuantifiedObjectValue.of(q1(), Type.primitiveType(Type.TypeCode.INT))))
                .addEqualityComparison(
                        new Comparisons.ValueComparison(Comparisons.Type.EQUALS,
                                QuantifiedObjectValue.of(q2(), Type.primitiveType(Type.TypeCode.INT))))
                .build();
    }

    @Nonnull
    private static Comparisons.DistanceRankValueComparison randomDistanceRankComparison() {
        return new Comparisons.DistanceRankValueComparison(Comparisons.Type.DISTANCE_RANK_LESS_THAN_OR_EQUAL,
                getRandomVectorValue(), new LiteralValue<>(10), null, null);
    }

    @Nonnull
    private static Comparisons.DistanceRankValueComparison correlatedDistanceRankComparison() {
        return new Comparisons.DistanceRankValueComparison(Comparisons.Type.DISTANCE_RANK_LESS_THAN_OR_EQUAL,
                QuantifiedObjectValue.of(q3(), Type.Vector.of(false, 64, 128)),
                QuantifiedObjectValue.of(q4(), Type.primitiveType(Type.TypeCode.INT, false)), null, null);
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

    @Nonnull
    protected static CorrelationIdentifier q1() {
        return CorrelationIdentifier.of("q1");
    }

    @Nonnull
    protected static CorrelationIdentifier q2() {
        return CorrelationIdentifier.of("q2");
    }

    @Nonnull
    protected static CorrelationIdentifier q3() {
        return CorrelationIdentifier.of("q3");
    }

    @Nonnull
    protected static CorrelationIdentifier q4() {
        return CorrelationIdentifier.of("q4");
    }

    @Nonnull
    protected static CorrelationIdentifier q5() {
        return CorrelationIdentifier.of("q5");
    }

    @Nonnull
    protected static CorrelationIdentifier q6() {
        return CorrelationIdentifier.of("q6");
    }

    @Nonnull
    protected static CorrelationIdentifier q7() {
        return CorrelationIdentifier.of("q7");
    }

    @Nonnull
    protected static CorrelationIdentifier q8() {
        return CorrelationIdentifier.of("q8");
    }
}

