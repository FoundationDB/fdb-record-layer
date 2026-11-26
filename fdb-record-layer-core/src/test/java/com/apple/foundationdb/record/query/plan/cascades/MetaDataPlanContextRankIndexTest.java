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

import javax.annotation.Nonnull;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link MetaDataPlanContext} methods with rank indexes. A rank index offers two candidates, one per scan
 * type: a {@link ValueIndexScanMatchCandidate} for a {@code BY_VALUE} scan and a {@link WindowIndexScanMatchCandidate}
 * for a {@code BY_RANK} one. Both {@link MetaDataPlanContext#forRootReference} and
 * {@link MetaDataPlanContext#forRecordQuery} offer both, since neither candidate needs anything a {@link RecordQuery}
 * carries and a graph-planned query reaches them through the same registry.
 *
 * <p>{@link LegacyWindowedIndexScanMatchCandidate} is no longer among them. {@code WindowIndexMaintainerFactory}
 * answers for {@link com.apple.foundationdb.record.metadata.IndexTypes#RANK} and builds the window candidate;
 * {@code RankIndexMaintainerFactory}, which built the legacy one, is shadowed by it.
 */
@Tag(Tags.RequiresFDB)
class MetaDataPlanContextRankIndexTest extends FDBRecordStoreQueryTestBase {

    /**
     * The rank indexes this metadata has: two declared below and one the proto annotates on
     * {@code BasicRankedRecord.score}.
     */
    private static final Set<String> RANK_INDEX_NAMES =
            ImmutableSet.of("rank_by_gender", "simple_rank_score", "BasicRankedRecord$score");

    @Nonnull
    private static Set<String> candidateNamesOf(@Nonnull final Set<MatchCandidate> matchCandidates,
                                                @Nonnull final Class<? extends MatchCandidate> candidateClass) {
        return matchCandidates.stream()
                .filter(candidateClass::isInstance)
                .map(MatchCandidate::getName)
                .collect(Collectors.toSet());
    }

    private RecordMetaData setupMetaDataWithRankIndex() {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsRankProto.getDescriptor());

        // Set primary keys for the record types whose proto definition does not declare one
        metaDataBuilder.getRecordType("HeaderRankedRecord")
                .setPrimaryKey(field("header").nest(field("group"), field("id")));
        metaDataBuilder.getRecordType("TwoNestedRankedRecord")
                .setPrimaryKey(field("x").nest(field("a"), field("b")));

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
    void forRootReferenceARankIndexCreatesAValueAndAWindowCandidate() {
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

            // three rank indexes, each offering a candidate per scan type, and one for the primary key
            final Set<MatchCandidate> matchCandidates = planContext.getMatchCandidates();
            assertEquals(7, matchCandidates.size());

            assertEquals(RANK_INDEX_NAMES, candidateNamesOf(matchCandidates, ValueIndexScanMatchCandidate.class),
                    "every rank index should offer a BY_VALUE candidate");
            assertEquals(RANK_INDEX_NAMES, candidateNamesOf(matchCandidates, WindowIndexScanMatchCandidate.class),
                    "every rank index should offer a BY_RANK candidate");
            assertTrue(candidateNamesOf(matchCandidates, LegacyWindowedIndexScanMatchCandidate.class).isEmpty(),
                    "the legacy windowed candidate is no longer built for a rank index");
        }
    }

    @Test
    void forRecordQueryARankIndexCreatesTheSameTwoCandidates() {
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

            // the same inventory a root reference gets: a RecordQuery carries nothing either candidate needs
            final Set<MatchCandidate> matchCandidates = planContext.getMatchCandidates();
            assertEquals(7, matchCandidates.size());

            assertEquals(RANK_INDEX_NAMES, candidateNamesOf(matchCandidates, ValueIndexScanMatchCandidate.class),
                    "every rank index should offer a BY_VALUE candidate");
            assertEquals(RANK_INDEX_NAMES, candidateNamesOf(matchCandidates, WindowIndexScanMatchCandidate.class),
                    "every rank index should offer a BY_RANK candidate");
            assertTrue(candidateNamesOf(matchCandidates, LegacyWindowedIndexScanMatchCandidate.class).isEmpty(),
                    "the legacy windowed candidate is no longer built for a rank index");
        }
    }
}
