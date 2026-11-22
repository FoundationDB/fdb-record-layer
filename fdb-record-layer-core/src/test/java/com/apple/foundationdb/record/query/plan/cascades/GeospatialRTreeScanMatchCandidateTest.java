/*
 * GeospatialRTreeScanMatchCandidateTest.java
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
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

/**
 * Tests for {@link GeospatialRTreeScanMatchCandidate}. Coverage focuses on the invariants that distinguish this
 * candidate from a value-index candidate: a non-negative {@code prefixSize}, the "grouping + trailing coordinates"
 * sargable layout, and the ordering restriction to the grouping prefix (Hilbert-curve traversal is not a useful
 * external sort).
 */
class GeospatialRTreeScanMatchCandidateTest {

    private static final CorrelationIdentifier P0 = CorrelationIdentifier.of("p0");
    private static final CorrelationIdentifier COORDS = CorrelationIdentifier.of("coords");

    @Nonnull
    private static Index geoIndex(@Nonnull final String name, @Nonnull final KeyExpression key) {
        return new Index(name, key, IndexTypes.GEOSPATIAL_RTREE);
    }

    @Nonnull
    private static GeospatialRTreeScanMatchCandidate createCandidate(@Nonnull final String indexName,
                                                                     @Nonnull final KeyExpression key,
                                                                     @Nonnull final List<CorrelationIdentifier> sargableAliases,
                                                                     @Nonnull final List<CorrelationIdentifier> orderingAliases,
                                                                     final int prefixSize) {
        final Index index = geoIndex(indexName, key);
        final List<RecordType> queriedRecordTypes = Collections.emptyList();
        final Traversal traversal = Traversal.withRoot(Reference.empty());
        final Set<CorrelationIdentifier> parametersRequiredForBinding = ImmutableSet.of();
        final Type.Record baseType = Type.Record.fromFields(ImmutableList.of());
        final CorrelationIdentifier baseAlias = CorrelationIdentifier.of("base");
        return new GeospatialRTreeScanMatchCandidate(
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
                prefixSize);
    }

    @Test
    void testGetNameAndToString() {
        final KeyExpression key = concat(field("lat"), field("lon")).groupBy(field("country"));
        final GeospatialRTreeScanMatchCandidate candidate = createCandidate(
                "test_geo_index", key, ImmutableList.of(P0, COORDS), ImmutableList.of(P0), 1);

        Assertions.assertEquals("test_geo_index", candidate.getName());
        Assertions.assertEquals("geospatialRtree[test_geo_index]", candidate.toString());
    }

    @Test
    void testGetPrefixSize() {
        final KeyExpression key = concat(field("lat"), field("lon")).groupBy(field("country"));
        final GeospatialRTreeScanMatchCandidate candidate = createCandidate(
                "test_geo_index", key, ImmutableList.of(P0, COORDS), ImmutableList.of(P0), 1);

        Assertions.assertEquals(1, candidate.getPrefixSize());
    }

    @Test
    void testSargableAliasesReflectConstructor() {
        // Sargables are grouping placeholders followed by a single trailing coordinates placeholder.
        final KeyExpression key = concat(field("lat"), field("lon")).groupBy(field("country"));
        final List<CorrelationIdentifier> aliases = ImmutableList.of(P0, COORDS);
        final GeospatialRTreeScanMatchCandidate candidate = createCandidate(
                "test_geo_index", key, aliases, aliases.subList(0, 1), 1);

        Assertions.assertEquals(aliases, candidate.getSargableAliases());
    }

    @Test
    void testOrderingAliasesReflectPrefixOnly() {
        // Hilbert-curve traversal of the coordinates placeholder is not a meaningful external sort; only the grouping
        // prefix aliases participate in ordering.
        final KeyExpression key = concat(field("lat"), field("lon")).groupBy(field("country"));
        final List<CorrelationIdentifier> aliases = ImmutableList.of(P0, COORDS);
        final List<CorrelationIdentifier> prefixOnly = ImmutableList.of(P0);
        final GeospatialRTreeScanMatchCandidate candidate = createCandidate(
                "test_geo_index", key, aliases, prefixOnly, 1);

        Assertions.assertEquals(prefixOnly, candidate.getOrderingAliases());
    }

    @Test
    void testPrefixlessCandidateHasEmptyOrdering() {
        // Ungrouped geospatial index: no grouping placeholders, so no ordering aliases.
        final KeyExpression key = concat(field("lat"), field("lon"));
        final GeospatialRTreeScanMatchCandidate candidate = createCandidate(
                "test_geo_no_prefix", key, ImmutableList.of(COORDS), ImmutableList.of(), 0);

        Assertions.assertEquals(0, candidate.getPrefixSize());
        Assertions.assertEquals(1, candidate.getSargableAliases().size());
        Assertions.assertTrue(candidate.getOrderingAliases().isEmpty());
    }

    @Test
    void testConstructorRejectsNegativePrefix() {
        final KeyExpression key = concat(field("lat"), field("lon"));
        Assertions.assertThrows(IllegalArgumentException.class, () ->
                createCandidate("bad", key, ImmutableList.of(COORDS), ImmutableList.of(), -1));
    }
}
