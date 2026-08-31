/*
 * MultidimensionalIndexScanMatchCandidateTest.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.DimensionsKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

/**
 * Tests for {@link MultidimensionalIndexScanMatchCandidate}.
 */
class MultidimensionalIndexScanMatchCandidateTest {

    private static final CorrelationIdentifier P0 = CorrelationIdentifier.of("p0");
    private static final CorrelationIdentifier P1 = CorrelationIdentifier.of("p1");
    private static final CorrelationIdentifier D0 = CorrelationIdentifier.of("d0");
    private static final CorrelationIdentifier D1 = CorrelationIdentifier.of("d1");
    private static final CorrelationIdentifier S0 = CorrelationIdentifier.of("s0");

    @Nonnull
    private static Index multidimIndex(@Nonnull final String name,
                                       @Nonnull final KeyExpression key) {
        return new Index(name, key, IndexTypes.MULTIDIMENSIONAL);
    }

    @Nonnull
    private static MultidimensionalIndexScanMatchCandidate createCandidate(@Nonnull final String indexName,
                                                                           @Nonnull final KeyExpression key,
                                                                           @Nonnull final List<CorrelationIdentifier> sargableAliases,
                                                                           @Nonnull final List<CorrelationIdentifier> orderingAliases,
                                                                           final int prefixSize,
                                                                           final int dimensionsSize) {
        final Index index = multidimIndex(indexName, key);
        final List<RecordType> queriedRecordTypes = Collections.emptyList();
        final Traversal traversal = Traversal.withRoot(Reference.empty());
        final Set<CorrelationIdentifier> parametersRequiredForBinding = ImmutableSet.of();
        final Type.Record baseType = Type.Record.fromFields(ImmutableList.of());
        final CorrelationIdentifier baseAlias = CorrelationIdentifier.of("base");
        return new MultidimensionalIndexScanMatchCandidate(
                index,
                queriedRecordTypes,
                traversal,
                sargableAliases,
                orderingAliases,
                parametersRequiredForBinding,
                baseType,
                baseAlias,
                key,
                null,
                prefixSize,
                dimensionsSize);
    }

    @Nonnull
    private static Comparisons.Comparison eq(final long v) {
        return new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, v);
    }

    @Nonnull
    private static Comparisons.Comparison lt(final long v) {
        return new Comparisons.SimpleComparison(Comparisons.Type.LESS_THAN, v);
    }

    @Test
    void testGetNameAndToString() {
        final KeyExpression key = DimensionsKeyExpression.of(field("prefix"), concat(field("d0"), field("d1")));
        final MultidimensionalIndexScanMatchCandidate candidate = createCandidate(
                "test_mdim_index", key, ImmutableList.of(), ImmutableList.of(), 1, 2);

        Assertions.assertEquals("test_mdim_index", candidate.getName());
        Assertions.assertEquals("multidimensional[test_mdim_index]", candidate.toString());
    }

    @Test
    void testGetPrefixAndDimensionsSizes() {
        final KeyExpression key = DimensionsKeyExpression.of(field("prefix"), concat(field("d0"), field("d1")));
        final MultidimensionalIndexScanMatchCandidate candidate = createCandidate(
                "test_mdim_index", key, ImmutableList.of(), ImmutableList.of(), 1, 2);

        Assertions.assertEquals(1, candidate.getPrefixSize());
        Assertions.assertEquals(2, candidate.getDimensionsSize());
    }

    @Test
    void testSargableAliasesReflectConstructor() {
        final KeyExpression key = DimensionsKeyExpression.of(field("prefix"), concat(field("d0"), field("d1")));
        final List<CorrelationIdentifier> aliases = ImmutableList.of(P0, D0, D1);
        final MultidimensionalIndexScanMatchCandidate candidate = createCandidate(
                "test_mdim_index", key, aliases, aliases.subList(0, 1), 1, 2);

        Assertions.assertEquals(aliases, candidate.getSargableAliases());
    }

    @Test
    void testOrderingAliasesReflectPrefixOnly() {
        final KeyExpression key = DimensionsKeyExpression.of(field("prefix"), concat(field("d0"), field("d1")));
        final List<CorrelationIdentifier> aliases = ImmutableList.of(P0, D0, D1);
        final List<CorrelationIdentifier> prefixOnly = ImmutableList.of(P0);
        final MultidimensionalIndexScanMatchCandidate candidate = createCandidate(
                "test_mdim_index", key, aliases, prefixOnly, 1, 2);

        Assertions.assertEquals(prefixOnly, candidate.getOrderingAliases());
    }

    @Test
    void testConstructorRejectsNegativePrefix() {
        final KeyExpression key = DimensionsKeyExpression.of(field("prefix"), concat(field("d0"), field("d1")));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                createCandidate("bad", key, ImmutableList.of(), ImmutableList.of(), -1, 2));
    }

    @Test
    void testConstructorRejectsNonPositiveDimensions() {
        final KeyExpression key = DimensionsKeyExpression.of(field("prefix"), concat(field("d0"), field("d1")));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                createCandidate("bad", key, ImmutableList.of(), ImmutableList.of(), 1, 0));
    }

    @Test
    void testComputeBoundPrefixMapAllPrefixEqualityAllDimensionsRanges() {
        final Map<CorrelationIdentifier, ComparisonRange> bindings = ImmutableMap.of(
                P0, ComparisonRange.from(eq(1L)),
                D0, ComparisonRange.from(lt(10L)),
                D1, ComparisonRange.from(lt(20L)));
        final Map<CorrelationIdentifier, ComparisonRange> prefixMap =
                MultidimensionalIndexScanMatchCandidate.computePrefixMap(
                        ImmutableList.of(P0, D0, D1), bindings, 1, 2);

        // All bindings retained: prefix equality plus both dimension inequalities.
        Assertions.assertEquals(3, prefixMap.size());
        Assertions.assertTrue(prefixMap.containsKey(P0));
        Assertions.assertTrue(prefixMap.containsKey(D0));
        Assertions.assertTrue(prefixMap.containsKey(D1));
    }

    @Test
    void testComputeBoundPrefixMapStopsAtUnboundPrefix() {
        // p0 has no binding; nothing else should carry forward.
        final Map<CorrelationIdentifier, ComparisonRange> bindings = ImmutableMap.of(
                D0, ComparisonRange.from(lt(10L)),
                D1, ComparisonRange.from(lt(20L)));
        final Map<CorrelationIdentifier, ComparisonRange> prefixMap =
                MultidimensionalIndexScanMatchCandidate.computePrefixMap(
                        ImmutableList.of(P0, D0, D1), bindings, 1, 2);

        Assertions.assertTrue(prefixMap.isEmpty());
    }

    @Test
    void testComputeBoundPrefixMapReturnsEmptyWhenAnyDimensionUnbound() {
        // Dimensions require every entry bound; missing D1 kills the whole scan candidate.
        final Map<CorrelationIdentifier, ComparisonRange> bindings = ImmutableMap.of(
                P0, ComparisonRange.from(eq(1L)),
                D0, ComparisonRange.from(lt(10L)));
        final Map<CorrelationIdentifier, ComparisonRange> prefixMap =
                MultidimensionalIndexScanMatchCandidate.computePrefixMap(
                        ImmutableList.of(P0, D0, D1), bindings, 1, 2);

        Assertions.assertTrue(prefixMap.isEmpty());
    }

    @Test
    void testComputeBoundPrefixMapReturnsEmptyOnPrefixInequality() {
        // Legacy planner rejects any non-equality prefix binding for a multidim scan; the override matches.
        final Map<CorrelationIdentifier, ComparisonRange> bindings = ImmutableMap.of(
                P0, ComparisonRange.from(eq(0L)),
                P1, ComparisonRange.from(lt(5L)),
                D0, ComparisonRange.from(lt(10L)),
                D1, ComparisonRange.from(lt(20L)));
        final Map<CorrelationIdentifier, ComparisonRange> prefixMap =
                MultidimensionalIndexScanMatchCandidate.computePrefixMap(
                        ImmutableList.of(P0, P1, D0, D1), bindings, 2, 2);

        Assertions.assertTrue(prefixMap.isEmpty());
    }

    @Test
    void testComputeBoundPrefixMapSuffixEqualityContinues() {
        // Suffix inherits default semantics: equality binds continue, inequality closes.
        final Map<CorrelationIdentifier, ComparisonRange> bindings = ImmutableMap.of(
                P0, ComparisonRange.from(eq(1L)),
                D0, ComparisonRange.from(lt(10L)),
                D1, ComparisonRange.from(lt(20L)),
                S0, ComparisonRange.from(eq(7L)));
        final Map<CorrelationIdentifier, ComparisonRange> prefixMap =
                MultidimensionalIndexScanMatchCandidate.computePrefixMap(
                        ImmutableList.of(P0, D0, D1, S0), bindings, 1, 2);

        Assertions.assertEquals(4, prefixMap.size());
        Assertions.assertTrue(prefixMap.containsKey(S0));
    }
}
