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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsNestedMapProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.UnnestedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactoryRegistryImpl;
import com.apple.foundationdb.record.provider.foundationdb.IndexMatchCandidateRegistry;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests that {@link MetaDataPlanContext} has the planner consider the indexes on synthetic record types only if the
 * planner configuration {@linkplain RecordQueryPlannerConfiguration#shouldConsiderSyntheticRecordTypeIndexes() asks for
 * them}.
 */
@Tag(Tags.RequiresFDB)
class MetaDataPlanContextSyntheticIndexTest extends FDBRecordStoreQueryTestBase {
    private static final String OUTER_INDEX = "outerOtherId";
    private static final String UNNESTED_INDEX = "unnestedEntryKey";

    @ParameterizedTest(name = "forRecordQueryConsidersSyntheticIndexesOnlyIfConfigured[consider={0}]")
    @BooleanSource
    void forRecordQueryConsidersSyntheticIndexesOnlyIfConfigured(final boolean consider) {
        try (FDBRecordContext context = openContext()) {
            final RecordMetaData metaData = metaData();
            createOrOpenRecordStore(context, metaData);
            final var registry = new RecordingRegistry();

            MetaDataPlanContext.forRecordQuery(configuration(consider), metaData, recordStore.getRecordStoreState(),
                    registry, RecordQuery.newBuilder().setRecordType("OuterRecord").build());

            assertEquals(expectedIndexes(consider), registry.requestedIndexes);
        }
    }

    @ParameterizedTest(name = "forRootReferenceConsidersSyntheticIndexesOnlyIfConfigured[consider={0}]")
    @BooleanSource
    void forRootReferenceConsidersSyntheticIndexesOnlyIfConfigured(final boolean consider) {
        try (FDBRecordContext context = openContext()) {
            final RecordMetaData metaData = metaData();
            createOrOpenRecordStore(context, metaData);
            final var registry = new RecordingRegistry();

            MetaDataPlanContext.forRootReference(configuration(consider), metaData, recordStore.getRecordStoreState(),
                    registry, outerRecordScan(metaData), Optional.empty(), IndexQueryabilityFilter.DEFAULT);

            assertEquals(expectedIndexes(consider), registry.requestedIndexes);
        }
    }

    @Test
    void syntheticIndexFailingCandidateCreationDoesNotAffectQueriesByDefault() {
        try (FDBRecordContext context = openContext()) {
            final RecordMetaData metaData = metaData();
            createOrOpenRecordStore(context, metaData);
            final IndexMatchCandidateRegistry registry = (registryMetaData, index, reverse) -> {
                if (UNNESTED_INDEX.equals(index.getName())) {
                    throw new RecordCoreException("cannot create match candidate");
                }
                return IndexMaintainerFactoryRegistryImpl.instance().createMatchCandidates(registryMetaData, index, reverse);
            };
            final var defaultConfiguration = RecordQueryPlannerConfiguration.defaultPlannerConfiguration();

            final var recordQueryCandidates = MetaDataPlanContext.forRecordQuery(defaultConfiguration, metaData,
                    recordStore.getRecordStoreState(), registry,
                    RecordQuery.newBuilder().setRecordType("OuterRecord").build()).getMatchCandidates();
            final var rootReferenceCandidates = MetaDataPlanContext.forRootReference(defaultConfiguration, metaData,
                    recordStore.getRecordStoreState(), registry, outerRecordScan(metaData), Optional.empty(),
                    IndexQueryabilityFilter.DEFAULT).getMatchCandidates();

            assertThat(candidateNames(recordQueryCandidates)).contains(OUTER_INDEX).doesNotContain(UNNESTED_INDEX);
            assertThat(candidateNames(rootReferenceCandidates)).contains(OUTER_INDEX).doesNotContain(UNNESTED_INDEX);
        }
    }

    @Nonnull
    private static RecordMetaData metaData() {
        final RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsNestedMapProto.getDescriptor());
        metaDataBuilder.addIndex("OuterRecord", new Index(OUTER_INDEX, field("other_id")));
        final UnnestedRecordTypeBuilder unnested = metaDataBuilder.addUnnestedRecordType("UnnestedMap");
        unnested.addParentConstituent("parent", metaDataBuilder.getRecordType("OuterRecord"));
        unnested.addNestedConstituent("entry", TestRecordsNestedMapProto.MapRecord.Entry.getDescriptor(),
                "parent", field("map").nest(field("entry", KeyExpression.FanType.FanOut)));
        metaDataBuilder.addIndex(unnested, new Index(UNNESTED_INDEX, field("entry").nest(concatenateFields("key", "value"))));
        return metaDataBuilder.build();
    }

    @Nonnull
    private static RecordQueryPlannerConfiguration configuration(final boolean consider) {
        return RecordQueryPlannerConfiguration.builder()
                .setConsiderSyntheticRecordTypeIndexes(consider)
                .build();
    }

    @Nonnull
    private static Set<String> expectedIndexes(final boolean consider) {
        return consider ? Set.of(OUTER_INDEX, UNNESTED_INDEX) : Set.of(OUTER_INDEX);
    }

    @Nonnull
    private static Reference outerRecordScan(@Nonnull final RecordMetaData metaData) {
        return Reference.initialOf(new FullUnorderedScanExpression(ImmutableSet.of("OuterRecord"),
                Type.Record.fromDescriptor(metaData.getRecordType("OuterRecord").getDescriptor()),
                new AccessHints()));
    }

    @Nonnull
    private static Set<String> candidateNames(@Nonnull final Set<MatchCandidate> matchCandidates) {
        return matchCandidates.stream().map(MatchCandidate::getName).collect(ImmutableSet.toImmutableSet());
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
