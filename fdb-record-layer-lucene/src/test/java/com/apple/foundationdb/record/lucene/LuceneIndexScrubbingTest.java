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

import com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository;
import com.apple.foundationdb.record.lucene.directory.MockedLuceneIndexMaintainerFactory;
import com.apple.foundationdb.record.lucene.directory.TestingIndexMaintainerRegistry;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.OnlineIndexScrubber;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.util.pair.Pair;
import org.junit.jupiter.api.Test;

import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.SIMPLE_TEXT_SUFFIXES_WITH_PRIMARY_KEY_SEGMENT_INDEX;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.createComplexDocument;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.createSimpleDocument;
import static com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository.Flags.LUCENE_GET_PRIMARY_KEY_SEGMENT_INDEX_FORCE_NULL;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.SIMPLE_DOC;
import static org.junit.jupiter.api.Assertions.assertEquals;

class LuceneIndexScrubbingTest extends FDBLuceneTestBase {

    private void rebuildIndexMetaData(final FDBRecordContext context, final String document, final Index index) {
        Pair<FDBRecordStore, QueryPlanner> pair = LuceneIndexTestUtils.rebuildIndexMetaData(context, path, document, index, isUseCascadesPlanner());
        this.recordStore = pair.getLeft();
        this.planner = pair.getRight();
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
        final TestingIndexMaintainerRegistry registry = new TestingIndexMaintainerRegistry();
        registry.overrideFactory(new MockedLuceneIndexMaintainerFactory(injectedFailures));

        try (final FDBRecordContext context = openContext()) {
            // Overwrite + add records with an injected key segment index failure
            Pair<FDBRecordStore, QueryPlanner> pair = LuceneIndexTestUtils.rebuildIndexMetaData(context, path, SIMPLE_DOC, index, isUseCascadesPlanner(), registry);
            this.recordStore = pair.getLeft();
            this.planner = pair.getRight();
            injectedFailures.setFlag(LUCENE_GET_PRIMARY_KEY_SEGMENT_INDEX_FORCE_NULL);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(7771547L, WAYLON, 1));
            recordStore.saveRecord(createSimpleDocument(7772222L, WAYLON + " who?", 1));
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
