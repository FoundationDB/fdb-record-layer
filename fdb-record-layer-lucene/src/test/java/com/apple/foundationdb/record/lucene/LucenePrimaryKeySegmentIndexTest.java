/*
 * LucenePrimaryKeySegmentIndexTest.java
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

import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.lucene.directory.AgilityContext;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.commons.lang3.tuple.Pair;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.createSimpleDocument;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.SIMPLE_DOC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Lower level test of {@link LucenePrimaryKeySegmentIndex}.
 */
public class LucenePrimaryKeySegmentIndexTest extends FDBRecordStoreTestBase {

    enum Version {
        V1(options -> {
            options.remove(LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED);
            options.remove(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED);
            options.put(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_ENABLED, "true");
        }),
        V2(options -> {
            options.remove(LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED);
            options.remove(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_ENABLED);
            options.put(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED, "true");
        });

        private final Index simpleIndex;
        private final Index complexIndex;

        Version(Consumer<Map<String, String>> optionsBuilder) {
            this.simpleIndex = LuceneIndexTestUtils.simpleTextSuffixesIndex(optionsBuilder);
            this.complexIndex = LuceneIndexTestUtils.textAndStoredComplexIndex(optionsBuilder);
        }
    }

    @ParameterizedTest
    @EnumSource(Version.class)
    void insertDocuments(Version version) throws Exception {
        Index index = version.simpleIndex;

        final Set<Tuple> primaryKeys = saveRecords(index,
                createSimpleDocument(1623L, "Document 1", 2),
                createSimpleDocument(1624L, "Document 2", 2),
                createSimpleDocument(1547L, "NonDocument 3", 2));

        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            LuceneIndexTestValidator.validatePrimaryKeySegmentIndex(recordStore, index, Tuple.from(), null, primaryKeys);
        }
    }

    @ParameterizedTest
    @EnumSource(Version.class)
    void insertDocumentAcrossTransactions(Version version) throws Exception {
        Index index = version.simpleIndex;

        final Set<Tuple> primaryKeys = saveRecords(index, createSimpleDocument(1623L, "Document 1", 2));
        primaryKeys.addAll(saveRecords(index, createSimpleDocument(1624L, "Document 2", 2)));
        primaryKeys.addAll(saveRecords(index, createSimpleDocument(1547L, "NonDocument 3", 2)));

        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            LuceneIndexTestValidator.validatePrimaryKeySegmentIndex(recordStore, index, Tuple.from(), null, primaryKeys);
        }
    }


    @ParameterizedTest
    @EnumSource(Version.class)
    void flakyAgileContext(Version version) throws Exception {
        Index index = version.simpleIndex;

        final Set<Tuple> primaryKeys = saveRecords(index, createSimpleDocument(1623L, "Document 1", 2));
        primaryKeys.addAll(saveRecords(index, createSimpleDocument(1624L, "Document 2", 2)));
        primaryKeys.addAll(saveRecords(index, createSimpleDocument(1547L, "NonDocument 3", 2)));

        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            final LuceneIndexMaintainer indexMaintainer = (LuceneIndexMaintainer)recordStore.getIndexMaintainer(index);
            final FailCommitsAgilityContext agilityContext = new FailCommitsAgilityContext(context, index.getSubspaceKey());
            // TODO work to make this better
            final CompletionException completionException = assertThrows(CompletionException.class, () -> indexMaintainer.mergeIndexWithoutRepartitioning(agilityContext).join());
            MatcherAssert.assertThat(completionException.getCause(), Matchers.instanceOf(FailedLuceneCommit.class));
            assertEquals(1, agilityContext.commitCount);
        }

        Assertions.assertAll(
                () -> {
                    try (FDBRecordContext context = openContext()) {
                        rebuildIndexMetaData(context, SIMPLE_DOC, index);
                        LuceneIndexTestValidator.validatePrimaryKeySegmentIndex(recordStore, index, Tuple.from(), null, primaryKeys);
                    }
                },
                () -> {
                    timer.reset();
                    assertNull(timer.getCounter(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY),
                            () -> "Count: " + timer.getCounter(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY).getCount());
                    saveRecords(index,
                            createSimpleDocument(1623L, "Document 1 updated", 2),
                            createSimpleDocument(1624L, "Document 2 updated", 2),
                            createSimpleDocument(1547L, "NonDocument 3 updated", 2));
                    assertNull(timer.getCounter(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY),
                            () -> "Count: " + timer.getCounter(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY).getCount());
                    assertEquals(3, timer.getCounter(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY).getCount());
                });
    }

    @Override
    public FDBRecordContext openContext() {
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, 2.0)
                .build();
        return super.openContext(contextProps);
    }

    private Set<Tuple> saveRecords(final Index index, final TestRecordsTextProto.SimpleDocument... documents) {
        Set<Tuple> primaryKeys = new HashSet<>();
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            for (final TestRecordsTextProto.SimpleDocument document : documents) {
                primaryKeys.add(recordStore.saveRecord(document).getPrimaryKey());
            }
            context.commit();
        }
        return primaryKeys;
    }

    private void rebuildIndexMetaData(final FDBRecordContext context, final String document, final Index index) {
        Pair<FDBRecordStore, QueryPlanner> pair = LuceneIndexTestUtils.rebuildIndexMetaData(context, path, document, index, useCascadesPlanner);
        this.recordStore = pair.getLeft();
        this.planner = pair.getRight();
        recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(false);
    }


    private class FailCommitsAgilityContext extends AgilityContext.Agile {
        private final Object indexSubspaceKey;
        private int commitCount = 0;

        public FailCommitsAgilityContext(FDBRecordContext callerContext, final Object indexSubspaceKey) {
            super(callerContext, 1L, 1L);
            this.indexSubspaceKey = indexSubspaceKey;
        }

        @Override
        public void set(final byte[] key, final byte[] value) {
            final List<Object> keyItems = Tuple.fromBytes(key).getItems();
            if (keyItems.size() > 3) {
                final int size = keyItems.size();
                if (keyItems.get(size - 3).equals(indexSubspaceKey) &&
                        keyItems.get(size - 2).equals(1L) &&
                        keyItems.get(size - 1) instanceof String) {
                    if (((String)keyItems.get(size - 1)).startsWith("segments_")) {
                        commitCount++;
                        throw new FailedLuceneCommit();
                    }
                }
            }
            super.set(key, value);
        }
    }

    @SuppressWarnings("serial")
    private static class FailedLuceneCommit extends RuntimeException {
    }
}
