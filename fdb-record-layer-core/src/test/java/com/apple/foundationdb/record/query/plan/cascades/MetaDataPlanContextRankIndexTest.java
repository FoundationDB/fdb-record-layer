/*
 * MetaDataPlanContextRankIndexTest.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.TestRecordsRankProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactoryRegistryImpl;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link MetaDataPlanContext} methods with rank indexes, specifically testing:
 * <ul>
 * <li>{@link MetaDataPlanContext#forRootReference} - verifies rank indexes only create
 *     match candidates for value scans (BY_VALUE) and NOT for rank scans (BY_RANK)</li>
 * <li>{@link MetaDataPlanContext#forRecordQuery} - verifies rank indexes create both
 *     value scan candidates (BY_VALUE) and windowed scan candidates (BY_RANK)</li>
 * </ul>
 */
@Tag(Tags.RequiresFDB)
class MetaDataPlanContextRankIndexTest extends FDBRecordStoreQueryTestBase {

    private RecordMetaData setupMetaDataWithRankIndex() {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsRankProto.getDescriptor());

        // Set primary key for HeaderRankedRecord (required by the proto definition)
        metaDataBuilder.getRecordType("HeaderRankedRecord")
                .setPrimaryKey(field("header").nest(field("group"), field("id")));

        // Add a rank index on BasicRankedRecord
        metaDataBuilder.addIndex("BasicRankedRecord",
                new Index("rank_by_gender",
                        field("score").groupBy(field("gender")),
                        IndexTypes.RANK));

        // Add another rank index with different structure
        metaDataBuilder.addIndex("BasicRankedRecord",
                new Index("simple_rank_score",
                        field("score").ungrouped(),
                        IndexTypes.RANK));

        return metaDataBuilder.getRecordMetaData();
    }

    @Test
    void testForRootReferenceRankIndexOnlyCreatesValueScanCandidate() {
        try (FDBRecordContext context = openContext()) {
            final RecordMetaData metaData = setupMetaDataWithRankIndex();

            // Create and open a record store to get the state
            createOrOpenRecordStore(context, metaData);
            final RecordStoreState recordStoreState = recordStore.getRecordStoreState();

            // Create a root reference for BasicRankedRecord
            final FullUnorderedScanExpression scanExpression = new FullUnorderedScanExpression(
                    ImmutableSet.of(metaData.getRecordType("BasicRankedRecord").getName()),
                    Type.Record.fromDescriptor(metaData.getRecordType("BasicRankedRecord").getDescriptor()),
                    new AccessHints());

            final Reference rootReference = Reference.initialOf(scanExpression);

            // Create plan context using forRootReference
            final PlanContext planContext = MetaDataPlanContext.forRootReference(
                    RecordQueryPlannerConfiguration.defaultPlannerConfiguration(),
                    metaData,
                    recordStoreState,
                    IndexMaintainerFactoryRegistryImpl.instance(),
                    rootReference,
                    Optional.empty(),
                    IndexQueryabilityFilter.DEFAULT);

            // Get the match candidates
            final Set<MatchCandidate> matchCandidates = planContext.getMatchCandidates();
            // 3 rank index, each generates a ValueIndexScanMatchCandidate, and primary key generates a match candidate
            assertEquals(4, matchCandidates.size());

            // Filter to only ValueIndexScanMatchCandidate with our rank index names
            final Set<MatchCandidate> rankIndexCandidates = matchCandidates.stream()
                    .filter(candidate -> candidate.getName().equals("rank_by_gender") ||
                            candidate.getName().equals("simple_rank_score") ||
                            candidate.getName().equals("BasicRankedRecord$score"))
                    .collect(Collectors.toSet());

            // Verify that we have rank index candidates
            assertFalse(rankIndexCandidates.isEmpty(), "Should have rank index candidates");
            assertEquals(3, rankIndexCandidates.size(), "Should have exactly 2 rank index candidates");

            // Verify all are ValueIndexScanMatchCandidate (for BY_VALUE scans)
            for (MatchCandidate candidate : rankIndexCandidates) {
                assertInstanceOf(ValueIndexScanMatchCandidate.class, candidate, "Rank index candidate should be a ValueIndexScanMatchCandidate, got: " + candidate.getClass().getName());
            }

            // Verify no WindowedIndexScanMatchCandidate (BY_RANK scans) are created for rank indexes
            long windowedCandidateCount = matchCandidates.stream()
                    .filter(candidate -> candidate instanceof WindowedIndexScanMatchCandidate)
                    .count();

            assertEquals(0, windowedCandidateCount,
                    "Should not have any WindowedIndexScanMatchCandidate");
        }
    }

    @Test
    void testForRecordQueryRankIndexCreatesWindowedScanCandidate() {
        try (FDBRecordContext context = openContext()) {
            final RecordMetaData metaData = setupMetaDataWithRankIndex();

            // Create and open a record store to get the state
            createOrOpenRecordStore(context, metaData);
            final RecordStoreState recordStoreState = recordStore.getRecordStoreState();

            // Create a simple RecordQuery for BasicRankedRecord
            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("BasicRankedRecord")
                    .build();

            // Create plan context using forRecordQuery
            final PlanContext planContext = MetaDataPlanContext.forRecordQuery(
                    RecordQueryPlannerConfiguration.defaultPlannerConfiguration(),
                    metaData,
                    recordStoreState,
                    IndexMaintainerFactoryRegistryImpl.instance(),
                    query);

            // Get the match candidates
            final Set<MatchCandidate> matchCandidates = planContext.getMatchCandidates();
            assertEquals(7, matchCandidates.size());

            // Filter to ValueIndexScanMatchCandidate with our rank index names
            final Set<String> valueIndexCandidateNames = matchCandidates.stream()
                    .filter(candidate -> candidate instanceof ValueIndexScanMatchCandidate)
                    .map(MatchCandidate::getName)
                    .collect(Collectors.toSet());

            // Verify that we have ValueIndexScanMatchCandidate for rank indexes
            assertEquals(ImmutableSet.of("rank_by_gender", "simple_rank_score", "BasicRankedRecord$score"), valueIndexCandidateNames,
                    "Should have ValueIndexScanMatchCandidate for both rank indexes");

            // Filter to WindowedIndexScanMatchCandidate with our rank index names
            final Set<String> windowedIndexCandidateNames = matchCandidates.stream()
                    .filter(candidate -> candidate instanceof WindowedIndexScanMatchCandidate)
                    .map(MatchCandidate::getName)
                    .collect(Collectors.toSet());

            // Verify that we have WindowedIndexScanMatchCandidate for rank indexes (BY_RANK scans)
            assertEquals(ImmutableSet.of("rank_by_gender", "simple_rank_score", "BasicRankedRecord$score"), windowedIndexCandidateNames,
                    "Should have WindowedIndexScanMatchCandidate for both rank indexes");
        }
    }
}
