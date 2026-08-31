/*
 * ExpansionVisitorTest.java
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
import com.apple.foundationdb.record.metadata.expressions.DimensionsKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.debug.DebuggerWithSymbolTables;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Optional;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

/**
 * Cross-cutting behavioral checks over the concrete {@link ExpansionVisitor} implementations.
 */
class ExpansionVisitorTest {

    @BeforeEach
    void setUpDebugger() {
        // The visitors register debug counters during expansion; unset debugger means NPE, so seed a real one.
        Debugger.setDebugger(DebuggerWithSymbolTables.withSanityChecks());
        Debugger.setup();
    }

    @Nonnull
    private static Type.Record threeLongFieldsRecord() {
        return Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, false), Optional.of("prefix")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, false), Optional.of("d0")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, false), Optional.of("d1"))));
    }

    @Nonnull
    private static Type.Record twoLongDimensionRecord() {
        return Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, false), Optional.of("d0")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, false), Optional.of("d1"))));
    }

    @Nonnull
    private static Type.Record geospatialCoordinatesRecord() {
        return Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.DOUBLE, false), Optional.of("lat")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.DOUBLE, false), Optional.of("lon"))));
    }

    @Nonnull
    private static Type.Record geospatialGroupedRecord() {
        return Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG, false), Optional.of("country")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.DOUBLE, false), Optional.of("lat")),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.DOUBLE, false), Optional.of("lon"))));
    }

    @Test
    void testDefaultExpandWithBaseQuantifierSupplierThrowsUnsupportedOperation() {
        final Index index = new Index("test_value_index", field("value"), IndexTypes.VALUE);
        final ValueIndexExpansionVisitor visitor = new ValueIndexExpansionVisitor(index, Collections.emptyList());

        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> visitor.expand(() -> null, null, false),
                "Default expand(Supplier, ...) should throw UnsupportedOperationException");
    }

    @Test
    void testWindowedIndexExpandWithRecordTypeNamesThrowsUnsupportedOperation() {
        final Index index = new Index("test_rank_index", field("score").ungrouped(), IndexTypes.RANK);
        final WindowedIndexExpansionVisitor visitor = new WindowedIndexExpansionVisitor(index, Collections.emptyList());

        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> visitor.expand(ImmutableSet.of("TestRecord"),
                        ImmutableSet.of("TestRecord"),
                        Type.Record.fromFields(ImmutableList.of()),
                        new IndexAccessHint("test_rank_index"),
                        null,
                        false),
                "WindowedIndexExpansionVisitor.expand(Set, Set, ...) should throw UnsupportedOperationException");
    }

    @Test
    void testMultidimensionalIndexExpansionVisitorRejectsNonMultidimensionalIndex() {
        final Index index = new Index("test_value_index", field("value"), IndexTypes.VALUE);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new MultidimensionalIndexExpansionVisitor(index, Collections.emptyList()),
                "MultidimensionalIndexExpansionVisitor should reject non-MULTIDIMENSIONAL index types");
    }

    @Test
    void testMultidimensionalIndexExpansionVisitorAcceptsMultidimensionalIndex() {
        final Index index = new Index("test_multidim_index",
                DimensionsKeyExpression.of(field("prefix"), concat(field("d0"), field("d1"))),
                IndexTypes.MULTIDIMENSIONAL);
        Assertions.assertDoesNotThrow(
                () -> new MultidimensionalIndexExpansionVisitor(index, Collections.emptyList()),
                "MultidimensionalIndexExpansionVisitor should accept MULTIDIMENSIONAL index types");
    }

    @Test
    void testMultidimensionalIndexExpansionVisitorProducesCandidateWithPrefix() {
        final KeyExpression key = DimensionsKeyExpression.of(field("prefix"), concat(field("d0"), field("d1")));
        final Index index = new Index("test_multidim_index", key, IndexTypes.MULTIDIMENSIONAL);
        final MultidimensionalIndexExpansionVisitor visitor =
                new MultidimensionalIndexExpansionVisitor(index, Collections.emptyList());

        final MatchCandidate candidate = visitor.expand(ImmutableSet.of("TestRecord"),
                ImmutableSet.of("TestRecord"),
                threeLongFieldsRecord(),
                new IndexAccessHint("test_multidim_index"),
                null,
                false);

        Assertions.assertTrue(candidate instanceof MultidimensionalIndexScanMatchCandidate,
                "visitor.expand should return a MultidimensionalIndexScanMatchCandidate");
        final MultidimensionalIndexScanMatchCandidate mdim = (MultidimensionalIndexScanMatchCandidate)candidate;
        Assertions.assertEquals(1, mdim.getPrefixSize());
        Assertions.assertEquals(2, mdim.getDimensionsSize());
        // Sargables cover every index column: prefix + dimensions.
        Assertions.assertEquals(3, mdim.getSargableAliases().size());
        // Ordering is restricted to the prefix segment.
        Assertions.assertEquals(1, mdim.getOrderingAliases().size());
        Assertions.assertEquals(mdim.getSargableAliases().get(0), mdim.getOrderingAliases().get(0));
    }

    @Test
    void testMultidimensionalIndexExpansionVisitorProducesCandidateWithoutPrefix() {
        // No prefix segment: DimensionsKeyExpression with an empty prefix and two dimensions.
        final KeyExpression key = DimensionsKeyExpression.of(null, concat(field("d0"), field("d1")));
        final Index index = new Index("test_multidim_no_prefix", key, IndexTypes.MULTIDIMENSIONAL);
        final MultidimensionalIndexExpansionVisitor visitor =
                new MultidimensionalIndexExpansionVisitor(index, Collections.emptyList());

        final MatchCandidate candidate = visitor.expand(ImmutableSet.of("TestRecord"),
                ImmutableSet.of("TestRecord"),
                twoLongDimensionRecord(),
                new IndexAccessHint("test_multidim_no_prefix"),
                null,
                false);

        Assertions.assertTrue(candidate instanceof MultidimensionalIndexScanMatchCandidate);
        final MultidimensionalIndexScanMatchCandidate mdim = (MultidimensionalIndexScanMatchCandidate)candidate;
        Assertions.assertEquals(0, mdim.getPrefixSize());
        Assertions.assertEquals(2, mdim.getDimensionsSize());
        Assertions.assertEquals(2, mdim.getSargableAliases().size());
        // Hilbert-curve traversal is not an external sort; a prefixless index has no ordering aliases.
        Assertions.assertTrue(mdim.getOrderingAliases().isEmpty());
    }

    @Test
    void testGeospatialRTreeIndexExpansionVisitorRejectsNonGeospatialIndex() {
        final Index index = new Index("test_value_index", field("value"), IndexTypes.VALUE);
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new GeospatialRTreeIndexExpansionVisitor(index, Collections.emptyList()),
                "GeospatialRTreeIndexExpansionVisitor should reject non-GEOSPATIAL_RTREE index types");
    }

    @Test
    void testGeospatialRTreeIndexExpansionVisitorAcceptsGeospatialIndex() {
        final Index index = new Index("test_geo_index",
                concat(field("lat"), field("lon")),
                IndexTypes.GEOSPATIAL_RTREE);
        Assertions.assertDoesNotThrow(
                () -> new GeospatialRTreeIndexExpansionVisitor(index, Collections.emptyList()),
                "GeospatialRTreeIndexExpansionVisitor should accept GEOSPATIAL_RTREE index types");
    }

    @Test
    void testGeospatialRTreeIndexExpansionVisitorProducesCandidateWithoutPrefix() {
        final Index index = new Index("test_geo_no_prefix",
                concat(field("lat"), field("lon")),
                IndexTypes.GEOSPATIAL_RTREE);
        final GeospatialRTreeIndexExpansionVisitor visitor =
                new GeospatialRTreeIndexExpansionVisitor(index, Collections.emptyList());

        final MatchCandidate candidate = visitor.expand(ImmutableSet.of("TestRecord"),
                ImmutableSet.of("TestRecord"),
                geospatialCoordinatesRecord(),
                new IndexAccessHint("test_geo_no_prefix"),
                null,
                false);

        Assertions.assertTrue(candidate instanceof GeospatialRTreeScanMatchCandidate,
                "visitor.expand should return a GeospatialRTreeScanMatchCandidate");
        final GeospatialRTreeScanMatchCandidate geo = (GeospatialRTreeScanMatchCandidate)candidate;
        Assertions.assertEquals(0, geo.getPrefixSize());
        // The single coordinates placeholder is the only sargable alias.
        Assertions.assertEquals(1, geo.getSargableAliases().size());
        // Hilbert-curve traversal is not an external sort; a prefixless index has no ordering aliases.
        Assertions.assertTrue(geo.getOrderingAliases().isEmpty());
    }

    @Test
    void testGeospatialRTreeIndexExpansionVisitorProducesCandidateWithGrouping() {
        // Grouped geospatial index: field("country") is the grouping prefix, the coordinates concat is the grouped
        // value. GroupingKeyExpression.of(groupedValue, groupByFirst) builds wholeKey = concat(groupByFirst, groupedValue).
        final Index index = new Index("test_geo_grouped",
                concat(field("lat"), field("lon")).groupBy(field("country")),
                IndexTypes.GEOSPATIAL_RTREE);
        final GeospatialRTreeIndexExpansionVisitor visitor =
                new GeospatialRTreeIndexExpansionVisitor(index, Collections.emptyList());

        final MatchCandidate candidate = visitor.expand(ImmutableSet.of("TestRecord"),
                ImmutableSet.of("TestRecord"),
                geospatialGroupedRecord(),
                new IndexAccessHint("test_geo_grouped"),
                null,
                false);

        Assertions.assertTrue(candidate instanceof GeospatialRTreeScanMatchCandidate,
                "visitor.expand should return a GeospatialRTreeScanMatchCandidate");
        final GeospatialRTreeScanMatchCandidate geo = (GeospatialRTreeScanMatchCandidate)candidate;
        Assertions.assertEquals(1, geo.getPrefixSize());
        // One grouping placeholder plus the trailing coordinates placeholder.
        Assertions.assertEquals(2, geo.getSargableAliases().size());
        // The grouping placeholder feeds the ordering; the coordinates placeholder does not.
        Assertions.assertEquals(1, geo.getOrderingAliases().size());
        Assertions.assertEquals(geo.getSargableAliases().get(0), geo.getOrderingAliases().get(0));
    }
}
