/*
 * LuceneIndexScrubbingTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository;
import com.apple.foundationdb.record.lucene.directory.MockedLuceneIndexMaintainerFactory;
import com.apple.foundationdb.record.lucene.directory.TestingIndexMaintainerRegistry;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.OnlineIndexScrubber;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.util.pair.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.SIMPLE_TEXT_SUFFIXES_WITH_PRIMARY_KEY_SEGMENT_INDEX;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.createComplexDocument;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.createSimpleDocument;
import static com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository.Flags.LUCENE_MAINTAINER_SKIP_INDEX_UPDATE;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.SIMPLE_DOC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LuceneIndexScrubbingTest extends FDBLuceneTestBase {

    private TestingIndexMaintainerRegistry registry;

    @BeforeEach
    public void beforeEach() {
        registry = new TestingIndexMaintainerRegistry();
    }

    private void rebuildIndexMetaData(final FDBRecordContext context, final String document, final Index index) {
        Pair<FDBRecordStore, QueryPlanner> pair = LuceneIndexTestUtils.rebuildIndexMetaData(context, path, document, index, isUseCascadesPlanner());
        this.recordStore = pair.getLeft();
        this.planner = pair.getRight();
    }

    private static Stream<Arguments> threeBooleanArgs() {
        return Stream.of(false, true)
                .flatMap(isSynthetic -> Stream.of(false, true)
                        .flatMap(isPartitioned -> Stream.of(false, true)
                                .map(isGrouped -> Arguments.of(isSynthetic, isGrouped, isPartitioned))));
    }

    @Nonnull
    protected FDBRecordStore.Builder getStoreBuilderWithRegistry(@Nonnull FDBRecordContext context,
                                                                 @Nonnull RecordMetaDataProvider metaData,
                                                                 @Nonnull final KeySpacePath path) {
        return super.getStoreBuilder(context, metaData, path).setIndexMaintainerRegistry(registry);
    }

    @ParameterizedTest
    @MethodSource("threeBooleanArgs")
    void luceneIndexScrubMissingDataModelNoIssues(boolean isSynthetic, boolean isGrouped, boolean isPartitioned) {
        // Scrub a valid index, expect zero issues
        final long seed = 7L;

        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilderWithRegistry, pathManager)
                .setIsGrouped(isGrouped)
                .setIsSynthetic(isSynthetic)
                .setPartitionHighWatermark(isPartitioned ? 5 : 0)
                .build();

        for (int i = 0; i < 14; i++) {
            try (final FDBRecordContext context = openContext()) {
                dataModel.saveRecords(7, context, i / 6);
                context.commit();
            }
        }

        try (final FDBRecordContext context = openContext()) {
            dataModel.explicitMergeIndex(context, timer);
            context.commit();
        }
        try (final FDBRecordContext context = openContext()) {
            FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            boolean atLeastOnce = false;
            for (Map.Entry<Index, IndexState> entry : store.getAllIndexStates().entrySet()) {
                Index index = entry.getKey();
                IndexState indexState = entry.getValue();
                if (index.getType().equalsIgnoreCase("lucene") && indexState.equals(IndexState.READABLE)) {
                    atLeastOnce = true;
                    try (OnlineIndexScrubber indexScrubber = OnlineIndexScrubber.newBuilder()
                            .setRecordStore(store)
                            .setIndex(index)
                            .build()) {
                        final long missingEntriesCount = indexScrubber.scrubMissingIndexEntries();
                        assertEquals(0, missingEntriesCount);
                    }
                }
            }
            assertTrue(atLeastOnce);
        }
    }

    @Test
    void luceneIndexScrubMissingSimpleNoIssues() {
        // Scrub a valid index, expect zero issues
        Index index = SIMPLE_TEXT_SUFFIXES_WITH_PRIMARY_KEY_SEGMENT_INDEX;
        try (final FDBRecordContext context = openContext()) {
            // Write some records
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
            recordStore.saveRecord(createSimpleDocument(2222L, WAYLON + " who?", 1));
            context.commit();
        }
        try (final FDBRecordContext context = openContext()) {
            // Overwrite + add records
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(7771547L, WAYLON, 1));
            recordStore.saveRecord(createSimpleDocument(7772222L, WAYLON + " who?", 1));
            context.commit();
        }
        try (final FDBRecordContext context = openContext()) {
            // Scrub issues, assert none
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            try (OnlineIndexScrubber indexScrubber = OnlineIndexScrubber.newBuilder()
                    .setRecordStore(recordStore)
                    .setIndex(index)
                    .build()) {
                final long missingEntriesCount = indexScrubber.scrubMissingIndexEntries();
                assertEquals(0, missingEntriesCount);
            }
        }
    }


    @ParameterizedTest
    @MethodSource("threeBooleanArgs")
    void luceneIndexScrubMissingDataModel(boolean isSynthetic, boolean isGrouped, boolean isPartitioned) {
        // Scrub an index with missing entries
        final long seed = 207L;

        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilderWithRegistry, pathManager)
                .setIsGrouped(isGrouped)
                .setIsSynthetic(isSynthetic)
                .setPartitionHighWatermark(isPartitioned ? 5 : 0)
                .build();

        final InjectedFailureRepository injectedFailures = new InjectedFailureRepository();
        registry.overrideFactory(new MockedLuceneIndexMaintainerFactory(injectedFailures));

        try (final FDBRecordContext context = openContext()) {
            // Write some documents
            dataModel.saveRecordsToAllGroups(17, context);
            context.commit();
        }

        try (final FDBRecordContext context = openContext()) {
            dataModel.explicitMergeIndex(context, timer);
            context.commit();
        }

        try (final FDBRecordContext context = openContext()) {
            // By saving records with both setReverseSaveOrder true and false, we ensure that records
            // are in the oldest and most-recent partitions (if there are partitions)
            dataModel.setReverseSaveOrder(true);
            dataModel.saveRecords(7, context, 1);
            dataModel.setReverseSaveOrder(false);
            dataModel.saveRecords(7, context, 2);
            // Write few more records without updating
            injectedFailures.setFlag(LUCENE_MAINTAINER_SKIP_INDEX_UPDATE);
            dataModel.saveRecords(5, context, 4);
            dataModel.setReverseSaveOrder(true);
            dataModel.saveRecords(3, context, 1);
            dataModel.setReverseSaveOrder(false);
            dataModel.saveRecords(2, context, 3);
            injectedFailures.setFlag(LUCENE_MAINTAINER_SKIP_INDEX_UPDATE, false);
            context.commit();
        }

        try (final FDBRecordContext context = openContext()) {
            dataModel.explicitMergeIndex(context, timer);
            context.commit();
        }

        try (final FDBRecordContext context = openContext()) {
            FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            boolean atLeastOnce = false;
            for (Map.Entry<Index, IndexState> entry : store.getAllIndexStates().entrySet()) {
                Index index = entry.getKey();
                IndexState indexState = entry.getValue();
                if (index.getType().equalsIgnoreCase("lucene") && indexState.equals(IndexState.READABLE)) {
                    atLeastOnce = true;
                    try (OnlineIndexScrubber indexScrubber = OnlineIndexScrubber.newBuilder()
                            .setRecordStore(store)
                            .setIndex(index)
                            .build()) {
                        final long missingEntriesCount = indexScrubber.scrubMissingIndexEntries();
                        assertEquals(10, missingEntriesCount);
                    }
                }
            }
            assertTrue(atLeastOnce);
        }
    }

    @Test
    void luceneIndexScrubMissingSimple() {
        // Scrub an index with missing entries
        Index index = SIMPLE_TEXT_SUFFIXES_WITH_PRIMARY_KEY_SEGMENT_INDEX;

        long startTime = System.currentTimeMillis();
        try (final FDBRecordContext context = openContext()) {
            // Write some records
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.saveRecord(createComplexDocument(1623L, WAYLON, 1, startTime));
            recordStore.saveRecord(createComplexDocument(1547L, WAYLON, 1, startTime + 1000));
            recordStore.saveRecord(createComplexDocument(2222L, WAYLON + " who?", 1, startTime + 2000));
            recordStore.saveRecord(createComplexDocument(899L, ENGINEER_JOKE, 1, startTime + 3000));
            context.commit();
        }

        final InjectedFailureRepository injectedFailures = new InjectedFailureRepository();
        registry.overrideFactory(new MockedLuceneIndexMaintainerFactory(injectedFailures));

        try (final FDBRecordContext context = openContext()) {
            // Overwrite + add records without updating the index
            Pair<FDBRecordStore, QueryPlanner> pair = LuceneIndexTestUtils.rebuildIndexMetaData(context, path, SIMPLE_DOC, index, isUseCascadesPlanner(), registry);
            this.recordStore = pair.getLeft();
            this.planner = pair.getRight();
            injectedFailures.setFlag(LUCENE_MAINTAINER_SKIP_INDEX_UPDATE);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(7771547L, WAYLON, 1));
            recordStore.saveRecord(createSimpleDocument(7772222L, WAYLON + " who?", 1));
            injectedFailures.setFlag(LUCENE_MAINTAINER_SKIP_INDEX_UPDATE, false);
            context.commit();
        }

        try (final FDBRecordContext context = openContext()) {
            // Scrub issues, assert the number of issues found
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            try (OnlineIndexScrubber indexScrubber = OnlineIndexScrubber.newBuilder()
                    .setRecordStore(recordStore)
                    .setIndex(index)
                    .build()) {
                final long missingEntriesCount = indexScrubber.scrubMissingIndexEntries();
                assertEquals(3, missingEntriesCount);
            }
        }
    }
}
