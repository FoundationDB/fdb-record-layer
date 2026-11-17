/*
 * MetaDataPlanContextSyntheticIndexAutoQATest.java
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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsNestedMapProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.JoinedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.UnnestedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.IndexMatchCandidateRegistry;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Additional tests for {@link MetaDataPlanContext} considering the indexes on synthetic record types.
 */
@Tag(Tags.RequiresFDB)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class MetaDataPlanContextSyntheticIndexAutoQATest extends FDBRecordStoreQueryTestBase {
    private static final String OUTER = "OuterRecord";
    private static final String OTHER = "OtherRecord";
    private static final String JOINED_INDEX = "joinedOtherValue";
    private static final String UNNESTED_INDEX = "unnestedEntryKey";
    private static final String NOT_QUERYABLE_INDEX = "unnestedEntryValueNotQueryable";
    private static final String MULTI_TYPE_INDEX = "multiUnnestedEntryKey";

    @Test
    void joinedIndexRequestedOnlyIfAllConstituentsQueried() {
        // a joined type's index is offered only if the query reads both OuterRecord and OtherRecord
        try (FDBRecordContext context = openContext()) {
            final RecordMetaData metaData = metaData();
            createOrOpenRecordStore(context, metaData);

            assertThat(requestedForRecordQuery(metaData, List.of(OUTER))).doesNotContain(JOINED_INDEX).contains(UNNESTED_INDEX);
            assertThat(requestedForRecordQuery(metaData, List.of(OTHER))).doesNotContain(JOINED_INDEX, UNNESTED_INDEX);
            assertThat(requestedForRecordQuery(metaData, List.of(OUTER, OTHER))).contains(JOINED_INDEX, UNNESTED_INDEX);
            assertThat(requestedForRecordQuery(metaData, List.of())).contains(JOINED_INDEX, UNNESTED_INDEX);

            assertThat(requestedForRootReference(metaData, Set.of(OUTER), IndexQueryabilityFilter.DEFAULT))
                    .doesNotContain(JOINED_INDEX).contains(UNNESTED_INDEX);
            assertThat(requestedForRootReference(metaData, Set.of(OUTER, OTHER), IndexQueryabilityFilter.DEFAULT))
                    .contains(JOINED_INDEX, UNNESTED_INDEX);
        }
    }

    @Test
    void disabledSyntheticIndexIsNotRequested() {
        // only readable synthetic indexes are offered, even if the flag is set
        try (FDBRecordContext context = openContext()) {
            final RecordMetaData metaData = metaData();
            createOrOpenRecordStore(context, metaData);
            recordStore.markIndexDisabled(UNNESTED_INDEX).join();

            assertThat(requestedForRecordQuery(metaData, List.of(OUTER, OTHER)))
                    .doesNotContain(UNNESTED_INDEX).contains(JOINED_INDEX);
            assertThat(requestedForRootReference(metaData, Set.of(OUTER, OTHER), IndexQueryabilityFilter.DEFAULT))
                    .doesNotContain(UNNESTED_INDEX).contains(JOINED_INDEX);
        }
    }

    @Test
    void syntheticIndexHonorsQueryabilityFilterAndAllowedIndexes() {
        // synthetic indexes go through the same queryability filter and allowed-index list as stored type indexes
        try (FDBRecordContext context = openContext()) {
            final RecordMetaData metaData = metaData();
            createOrOpenRecordStore(context, metaData);

            assertThat(requestedForRecordQuery(metaData, List.of(OUTER))).doesNotContain(NOT_QUERYABLE_INDEX);
            assertThat(requestedForRootReference(metaData, Set.of(OUTER), IndexQueryabilityFilter.DEFAULT))
                    .doesNotContain(NOT_QUERYABLE_INDEX);
            assertThat(requestedForRootReference(metaData, Set.of(OUTER), IndexQueryabilityFilter.TRUE))
                    .contains(NOT_QUERYABLE_INDEX);
            assertThat(requestedForRootReference(metaData, Set.of(OUTER), IndexQueryabilityFilter.FALSE))
                    .isEmpty();

            final var registry = new RecordingRegistry();
            MetaDataPlanContext.forRecordQuery(configuration(), metaData, recordStore.getRecordStoreState(), registry,
                    RecordQuery.newBuilder().setRecordType(OUTER).setAllowedIndex(UNNESTED_INDEX).build());
            assertThat(registry.requestedIndexes).containsExactly(UNNESTED_INDEX);
        }
    }

    @Nonnull
    private Set<String> requestedForRecordQuery(@Nonnull final RecordMetaData metaData,
                                                @Nonnull final Collection<String> recordTypes) {
        final var registry = new RecordingRegistry();
        MetaDataPlanContext.forRecordQuery(configuration(), metaData, recordStore.getRecordStoreState(), registry,
                RecordQuery.newBuilder().setRecordTypes(recordTypes).build());
        return registry.requestedIndexes;
    }

    @Nonnull
    private Set<String> requestedForRootReference(@Nonnull final RecordMetaData metaData,
                                                  @Nonnull final Set<String> recordTypes,
                                                  @Nonnull final IndexQueryabilityFilter filter) {
        final var registry = new RecordingRegistry();
        final Reference scan = Reference.initialOf(new FullUnorderedScanExpression(recordTypes,
                Type.Record.fromDescriptor(metaData.getRecordType(OUTER).getDescriptor()), new AccessHints()));
        MetaDataPlanContext.forRootReference(configuration(), metaData, recordStore.getRecordStoreState(), registry,
                scan, Optional.empty(), filter);
        return registry.requestedIndexes;
    }

    @Nonnull
    private static RecordMetaData metaData() {
        final RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsNestedMapProto.getDescriptor());
        final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType("OuterOtherJoined");
        joined.addConstituent("outer", OUTER);
        joined.addConstituent("other", OTHER);
        joined.addJoin("outer", field("other_id"), "other", field("other_id"));
        metaDataBuilder.addIndex(joined, new Index(JOINED_INDEX, field("other").nest("other_value")));

        final UnnestedRecordTypeBuilder unnested = addUnnested(metaDataBuilder, "UnnestedMap");
        metaDataBuilder.addIndex(unnested, new Index(UNNESTED_INDEX, field("entry").nest("key")));
        metaDataBuilder.addIndex(unnested, new Index(NOT_QUERYABLE_INDEX, field("entry").nest("value"),
                IndexTypes.VALUE, ImmutableMap.of(IndexOptions.ALLOWED_FOR_QUERY_OPTION, "false")));

        final UnnestedRecordTypeBuilder secondUnnested = addUnnested(metaDataBuilder, "SecondUnnestedMap");
        metaDataBuilder.addMultiTypeIndex(List.of(unnested, secondUnnested),
                new Index(MULTI_TYPE_INDEX, field("entry").nest("int_value")));
        return metaDataBuilder.build();
    }

    @Nonnull
    private static UnnestedRecordTypeBuilder addUnnested(@Nonnull final RecordMetaDataBuilder metaDataBuilder,
                                                         @Nonnull final String name) {
        final UnnestedRecordTypeBuilder unnested = metaDataBuilder.addUnnestedRecordType(name);
        unnested.addParentConstituent("parent", metaDataBuilder.getRecordType(OUTER));
        unnested.addNestedConstituent("entry", TestRecordsNestedMapProto.MapRecord.Entry.getDescriptor(),
                "parent", field("map").nest(field("entry", KeyExpression.FanType.FanOut)));
        return unnested;
    }

    @Nonnull
    private static RecordQueryPlannerConfiguration configuration() {
        return RecordQueryPlannerConfiguration.builder()
                .setConsiderSyntheticRecordTypeIndexes(true)
                .build();
    }

    /**
     * A registry that records the indexes it is asked to create match candidates for, and creates none.
     */
    private static final class RecordingRegistry implements IndexMatchCandidateRegistry {
        private final Set<String> requestedIndexes = new HashSet<>();

        @Override
        public Iterable<MatchCandidate> createMatchCandidates(@Nonnull final RecordMetaData metaData,
                                                              @Nonnull final Index index,
                                                              final boolean reverse) {
            requestedIndexes.add(index.getName());
            return ImmutableSet.of();
        }
    }
}
