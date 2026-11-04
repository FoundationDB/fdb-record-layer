/*
 * MetaDataPlanContextSyntheticIndexTest.java
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

import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.TestRecordsNestedMapProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.JoinedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.UnnestedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactoryRegistryImpl;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests of which indexes on synthetic record types {@link MetaDataPlanContext} offers to the planner. None of these
 * tests needs a record store: the readability of an index is all the store state contributes.
 */
class MetaDataPlanContextSyntheticIndexTest {
    @Nonnull
    private static final String OUTER = "OuterRecord";
    @Nonnull
    private static final String OTHER = "OtherRecord";
    @Nonnull
    private static final String JOINED_INDEX = "joined_index";
    @Nonnull
    private static final String STORED_INDEX = "stored_index";
    @Nonnull
    private static final RecordStoreState ALL_READABLE = new RecordStoreState(null, null);

    @Test
    void syntheticIndexesOfOffersAJoinedIndexOnlyIfTheQueryReadsEveryJoinedType() {
        final RecordMetaData metaData = metaData(addOuterOtherJoined());

        assertThat(indexNames(MetaDataPlanContext.syntheticIndexesOf(metaData, ALL_READABLE, Set.of(OUTER, OTHER))))
                .containsExactly(JOINED_INDEX);
        // A query reading only one of the joined types has no joined records to read, so the index is not offered.
        assertThat(MetaDataPlanContext.syntheticIndexesOf(metaData, ALL_READABLE, Set.of(OUTER))).isEmpty();
        assertThat(MetaDataPlanContext.syntheticIndexesOf(metaData, ALL_READABLE, Set.of(OTHER))).isEmpty();
    }

    @Test
    void syntheticIndexesOfOffersASelfJoinedIndexIfTheQueryReadsTheJoinedType() {
        final RecordMetaData metaData = metaData(metaDataBuilder -> {
            final JoinedRecordTypeBuilder typeBuilder = metaDataBuilder.addJoinedRecordType("SelfJoined");
            typeBuilder.addConstituent("first", OUTER);
            typeBuilder.addConstituent("second", OUTER);
            typeBuilder.addJoin("first", field("other_id"), "second", field("rec_id"));
            metaDataBuilder.addIndex(typeBuilder, new Index(JOINED_INDEX,
                    concat(field("first").nest("rec_id"), field("second").nest("rec_id"))));
        });

        // Both constituents are the same stored type, so reading that one type reads every constituent.
        assertThat(indexNames(MetaDataPlanContext.syntheticIndexesOf(metaData, ALL_READABLE, Set.of(OUTER))))
                .containsExactly(JOINED_INDEX);
        assertThat(MetaDataPlanContext.syntheticIndexesOf(metaData, ALL_READABLE, Set.of(OTHER))).isEmpty();
    }

    @Test
    void syntheticIndexesOfOffersAnUnnestedIndexIfTheQueryReadsTheParentType() {
        final RecordMetaData metaData = metaData(metaDataBuilder -> {
            final UnnestedRecordTypeBuilder typeBuilder = metaDataBuilder.addUnnestedRecordType("UnnestedMap");
            typeBuilder.addParentConstituent("parent", metaDataBuilder.getRecordType(OUTER));
            typeBuilder.addNestedConstituent("entry", TestRecordsNestedMapProto.MapRecord.Entry.getDescriptor(),
                    "parent", field("map").nest(field("entry", FanType.FanOut)));
            metaDataBuilder.addIndex(typeBuilder, new Index("unnested_index", field("entry").nest("key")));
        });

        // The nested constituent is not a stored type: it is read through the parent, the only stored constituent.
        assertThat(indexNames(MetaDataPlanContext.syntheticIndexesOf(metaData, ALL_READABLE, Set.of(OUTER))))
                .containsExactly("unnested_index");
        assertThat(MetaDataPlanContext.syntheticIndexesOf(metaData, ALL_READABLE, Set.of(OTHER))).isEmpty();
    }

    @Test
    void syntheticIndexesOfDoesNotOfferAnIndexThatIsNotReadable() {
        final RecordMetaData metaData = metaData(addOuterOtherJoined());

        for (final IndexState state : List.of(IndexState.DISABLED, IndexState.WRITE_ONLY)) {
            final RecordStoreState recordStoreState = new RecordStoreState(null, Map.of(JOINED_INDEX, state));
            assertThat(MetaDataPlanContext.syntheticIndexesOf(metaData, recordStoreState, Set.of(OUTER, OTHER)))
                    .as("index in state %s", state)
                    .isEmpty();
        }
    }

    /**
     * An index on a synthetic record type is offered to the planner, but cannot be expanded into a match candidate
     * yet: the primary key of a synthetic record type is a list, which the expansion does not support. The expansion
     * gives up on such an index rather than failing, so offering it leaves planning as it was.
     */
    @Test
    void planContextForAQueryReadingTheJoinedTypesBuildsWithoutACandidateForTheJoinedIndex() {
        final RecordMetaData metaData = metaData(metaDataBuilder -> {
            addOuterOtherJoined().accept(metaDataBuilder);
            // A query reading both stored types is offered only the indexes on both of them.
            metaDataBuilder.addMultiTypeIndex(
                    List.of(metaDataBuilder.getRecordType(OUTER), metaDataBuilder.getRecordType(OTHER)),
                    new Index(STORED_INDEX, field("other_id")));
        });

        final PlanContext forRecordQuery = MetaDataPlanContext.forRecordQuery(
                RecordQueryPlannerConfiguration.defaultPlannerConfiguration(), metaData, ALL_READABLE,
                IndexMaintainerFactoryRegistryImpl.instance(),
                RecordQuery.newBuilder().setRecordTypes(List.of(OUTER, OTHER)).build());
        assertThat(candidateNames(forRecordQuery)).contains(STORED_INDEX).doesNotContain(JOINED_INDEX);

        final Set<String> queriedTypes = Set.of(OUTER, OTHER);
        final PlanContext forRootReference = MetaDataPlanContext.forRootReference(
                RecordQueryPlannerConfiguration.defaultPlannerConfiguration(), metaData, ALL_READABLE,
                IndexMaintainerFactoryRegistryImpl.instance(),
                Reference.initialOf(new FullUnorderedScanExpression(queriedTypes,
                        metaData.getPlannerType(queriedTypes), new AccessHints())),
                Optional.empty(), IndexQueryabilityFilter.DEFAULT);
        assertThat(candidateNames(forRootReference)).contains(STORED_INDEX).doesNotContain(JOINED_INDEX);
    }

    @Nonnull
    private static Consumer<RecordMetaDataBuilder> addOuterOtherJoined() {
        return metaDataBuilder -> {
            final JoinedRecordTypeBuilder typeBuilder = metaDataBuilder.addJoinedRecordType("OuterOtherJoined");
            typeBuilder.addConstituent("outer", OUTER);
            typeBuilder.addConstituent("other", OTHER);
            typeBuilder.addJoin("outer", field("other_id"), "other", field("other_id"));
            metaDataBuilder.addIndex(typeBuilder, new Index(JOINED_INDEX,
                    concat(field("outer").nest("rec_id"), field("other").nest("other_value"))));
        };
    }

    @Nonnull
    private static RecordMetaData metaData(@Nonnull final Consumer<RecordMetaDataBuilder> hook) {
        final RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsNestedMapProto.getDescriptor());
        hook.accept(metaDataBuilder);
        return metaDataBuilder.build();
    }

    @Nonnull
    private static List<String> indexNames(@Nonnull final List<Index> indexes) {
        return indexes.stream().map(Index::getName).collect(Collectors.toList());
    }

    @Nonnull
    private static Set<String> candidateNames(@Nonnull final PlanContext planContext) {
        return planContext.getMatchCandidates().stream().map(MatchCandidate::getName).collect(Collectors.toSet());
    }
}
