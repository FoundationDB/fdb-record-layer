/*
 * GeospatialRTreeIndexMaintainerFactoryTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsGeoProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.query.plan.cascades.GeospatialRTreeScanMatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.cascades.debug.DebuggerWithSymbolTables;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

/**
 * Tests for {@link GeospatialRTreeIndexMaintainerFactory#createMatchCandidates}. The default
 * {@link com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory#createMatchCandidates}
 * returns an empty list, silently making GEOSPATIAL_RTREE indexes invisible to Cascades. These tests pin the
 * override in place so a regression to that default surfaces immediately.
 */
class GeospatialRTreeIndexMaintainerFactoryTest {

    @BeforeEach
    void setUpDebugger() {
        // GeospatialRTreeIndexExpansionVisitor registers a debug counter during expansion; the debugger must
        // be set or expansion crashes before creating the candidate.
        Debugger.setDebugger(DebuggerWithSymbolTables.withSanityChecks());
        Debugger.setup();
    }

    @Test
    void createMatchCandidatesProducesUngroupedGeospatialCandidate() {
        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsGeoProto.getDescriptor());
        final Index index = new Index("cityLocation",
                field("location").nest(concat(field("latitude"), field("longitude"))),
                IndexTypes.GEOSPATIAL_RTREE);
        builder.addIndex("City", index);
        final RecordMetaData metaData = builder.build();

        final GeospatialRTreeIndexMaintainerFactory factory = new GeospatialRTreeIndexMaintainerFactory();
        final ImmutableList<MatchCandidate> candidates =
                ImmutableList.copyOf(factory.createMatchCandidates(metaData, metaData.getIndex("cityLocation"), false));

        Assertions.assertEquals(1, candidates.size(), "factory should produce exactly one match candidate");
        final MatchCandidate candidate = candidates.get(0);
        Assertions.assertTrue(candidate instanceof GeospatialRTreeScanMatchCandidate);
        final GeospatialRTreeScanMatchCandidate geo = (GeospatialRTreeScanMatchCandidate)candidate;
        Assertions.assertEquals(0, geo.getPrefixSize());
    }

    @Test
    void createMatchCandidatesProducesGroupedGeospatialCandidate() {
        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsGeoProto.getDescriptor());
        final Index index = new Index("cityLocationByCountry",
                field("location").nest(concat(field("latitude"), field("longitude"))).groupBy(field("country")),
                IndexTypes.GEOSPATIAL_RTREE);
        builder.addIndex("City", index);
        final RecordMetaData metaData = builder.build();

        final GeospatialRTreeIndexMaintainerFactory factory = new GeospatialRTreeIndexMaintainerFactory();
        final ImmutableList<MatchCandidate> candidates =
                ImmutableList.copyOf(factory.createMatchCandidates(metaData, metaData.getIndex("cityLocationByCountry"), false));

        Assertions.assertEquals(1, candidates.size(), "factory should produce exactly one match candidate");
        final MatchCandidate candidate = candidates.get(0);
        Assertions.assertTrue(candidate instanceof GeospatialRTreeScanMatchCandidate);
        final GeospatialRTreeScanMatchCandidate geo = (GeospatialRTreeScanMatchCandidate)candidate;
        Assertions.assertEquals(1, geo.getPrefixSize());
    }
}
