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

import com.apple.foundationdb.record.lucene.directory.AgileContext;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.tuple.Tuple;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.createSimpleDocument;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.SIMPLE_DOC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Lower level test of {@link LucenePrimaryKeySegmentIndex}.
 */
public class LucenePrimaryKeySegmentIndexTest extends FDBRecordStoreTestBase {

    private long idCounter = 1000L;
    private long textCounter = 1L;
    private boolean autoMerge = false;

    enum Version {
        Off(false, options -> {
            options.put(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED, "false");
            options.put(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_ENABLED, "false");
        }),
        V1(true, options -> {
            options.remove(LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED);
            options.remove(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED);
            options.put(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_ENABLED, "true");
        }),
        V2(true, options -> {
            options.remove(LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED);
            options.remove(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_ENABLED);
            options.put(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED, "true");
        });

        private final Index simpleIndex;
        private final Index complexIndex;
        public final boolean enabled;

        Version(boolean enabled, Consumer<Map<String, String>> optionsBuilder) {
            this.simpleIndex = LuceneIndexTestUtils.simpleTextSuffixesIndex(optionsBuilder);
            this.complexIndex = LuceneIndexTestUtils.textAndStoredComplexIndex(optionsBuilder);
            this.enabled = enabled;
        }
    }

    @ParameterizedTest
    @EnumSource(Version.class)
    void insertDocuments(Version version) throws Exception {
        Index index = version.simpleIndex;

        final Set<Tuple> primaryKeys = createDocuments(index, 3);

        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            LuceneIndexTestValidator.validatePrimaryKeySegmentIndex(recordStore, index, Tuple.from(), null, primaryKeys, false);
        }
    }

    @ParameterizedTest
    @EnumSource(Version.class)
    void insertDocumentAcrossTransactions(Version version) throws Exception {
        Index index = version.simpleIndex;

        final Set<Tuple> primaryKeys = createDocuments(index, 1);
        primaryKeys.addAll(createDocuments(index, 1));
        primaryKeys.addAll(createDocuments(index, 1));

        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            LuceneIndexTestValidator.validatePrimaryKeySegmentIndex(recordStore, index, Tuple.from(), null, primaryKeys, false);
        }
    }

    @ParameterizedTest
    @EnumSource(Version.class)
    void updateDocument(Version version) throws Exception {
        Index index = version.simpleIndex;

        final Set<Tuple> primaryKeys = createDocuments(index, 3);
        idCounter -= 2;
        createDocuments(index, 1);

        if (version.enabled) {
            assertEquals(0, timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY));
            assertEquals(1, timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY));
        } else {
            assertEquals(1, timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY));
            assertEquals(0, timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY));
        }

        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            LuceneIndexTestValidator.validatePrimaryKeySegmentIndex(recordStore, index, Tuple.from(), null, primaryKeys, false);
        }
    }

    public static Stream<Arguments> versionAndBoolean() {
        return Arrays.stream(Version.values())
                .flatMap(version -> Stream.of(true, false)
                        .map(bool -> Arguments.of(version, bool)));
    }

    @ParameterizedTest
    @MethodSource("versionAndBoolean")
    void manyUpdates(Version version, boolean autoMerge) throws Exception {
        Index index = version.simpleIndex;
        this.autoMerge = autoMerge;

        final Set<Tuple> primaryKeys = createDocuments(index, 3);
        for (int i = 0; i < 10; i++) {
            idCounter -= 3;
            createDocuments(index, 3);
        }

        if (!autoMerge) {
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setRecordStore(recordStore)
                    .setIndex(index)
                    .build()) {
                indexBuilder.mergeIndex();
            }
        }

        if (version.enabled) {
            assertEquals(0, timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY));
            assertEquals(30, timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY));
        } else {
            assertEquals(30, timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY));
            assertEquals(0, timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY));
        }

        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            LuceneIndexTestValidator.validatePrimaryKeySegmentIndex(recordStore, index, Tuple.from(), null, primaryKeys, false);
        }
    }

    @ParameterizedTest
    @EnumSource(Version.class)
    void deleteDocument(Version version) throws Exception {
        Index index = version.simpleIndex;

        final Set<Tuple> primaryKeys = createDocuments(index, 3);

        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            Tuple toDelete = List.copyOf(primaryKeys).get(1);
            recordStore.deleteRecord(toDelete);
            primaryKeys.remove(toDelete);
            context.commit();
        }

        if (version.enabled) {
            assertEquals(0, timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY));
            assertEquals(1, timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY));
        } else {
            assertEquals(1, timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY));
            assertEquals(0, timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY));
        }

        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            LuceneIndexTestValidator.validatePrimaryKeySegmentIndex(recordStore, index, Tuple.from(), null, primaryKeys, false);
        }
    }

    @ParameterizedTest
    @EnumSource(Version.class)
    void deleteAllDocuments(Version version) throws Exception {
        Index index = version.simpleIndex;

        final Set<Tuple> primaryKeys = createDocuments(index, 3);

        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            for (final Tuple toDelete : primaryKeys) {
                recordStore.deleteRecord(toDelete);
            }
            primaryKeys.clear();
            context.commit();
        }

        if (version.enabled) {
            assertEquals(0, timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY));
            assertEquals(3, timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY));
        } else {
            assertEquals(3, timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY));
            assertEquals(0, timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY));
        }

        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            LuceneIndexTestValidator.validatePrimaryKeySegmentIndex(recordStore, index, Tuple.from(), null, primaryKeys, false);
        }
    }


    @ParameterizedTest
    @EnumSource(Version.class)
    void deleteAllDocumentsMultipleTransactions(Version version) throws Exception {
        Index index = version.simpleIndex;

        final Set<Tuple> primaryKeys = createDocuments(index, 3);

        for (final Tuple toDelete : primaryKeys) {
            try (FDBRecordContext context = openContext()) {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                recordStore.deleteRecord(toDelete);
                context.commit();
            }
        }
        primaryKeys.clear();

        if (version.enabled) {
            assertEquals(0, timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY));
            assertEquals(3, timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY));
        } else {
            assertEquals(3, timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY));
            assertEquals(0, timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY));
        }

        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            LuceneIndexTestValidator.validatePrimaryKeySegmentIndex(recordStore, index, Tuple.from(), null, primaryKeys, false);
        }
    }

    @ParameterizedTest
    @EnumSource(Version.class)
    void flakyAgileContext(Version version) throws IOException {
        Index index = version.simpleIndex;
        final Set<Tuple> primaryKeys = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            primaryKeys.addAll(createDocuments(index, 1));
        }

        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            final LuceneIndexMaintainer indexMaintainer = (LuceneIndexMaintainer)recordStore.getIndexMaintainer(index);
            // TODO improve this as part of #2575
            final FailCommitsAgilityContext agilityContext = new FailCommitsAgilityContext(context, index.getSubspaceKey());
            try {
                assertThrows(FailedLuceneCommit.class,
                        () -> indexMaintainer.mergeIndexForTesting(Tuple.from(), null, agilityContext));
            } finally {
                agilityContext.flushAndClose();
            }
            assertEquals(1, agilityContext.commitCount);
        }
        // V1 fails here, that's the test
        Assumptions.assumeTrue(version == Version.V2);
        Assertions.assertAll(
                () -> {
                    try (FDBRecordContext context = openContext()) {
                        rebuildIndexMetaData(context, SIMPLE_DOC, index);
                        LuceneIndexTestValidator.validatePrimaryKeySegmentIndex(
                                recordStore, index, Tuple.from(), null, primaryKeys, true);
                    }
                },
                () -> {
                    timer.reset();
                    assertNull(timer.getCounter(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY),
                            () -> "Count: " + timer.getCounter(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY).getCount());
                    idCounter -= 3;
                    createDocuments(index, 3);
                    assertNull(timer.getCounter(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY),
                            () -> "Count: " + timer.getCounter(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY).getCount());
                    assertEquals(3, timer.getCounter(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY).getCount());
                });
        // retrying the merge should cleanup any extraneous mappings
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            final LuceneIndexMaintainer indexMaintainer = (LuceneIndexMaintainer)recordStore.getIndexMaintainer(index);
            indexMaintainer.mergeIndex();
        }
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            LuceneIndexTestValidator.validatePrimaryKeySegmentIndex(
                    recordStore, index, Tuple.from(), null, primaryKeys, false);
        }
    }

    @Override
    public FDBRecordContext openContext() {
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, 2.0)
                .build();
        return super.openContext(contextProps);
    }

    @Nonnull
    private Set<Tuple> createDocuments(final Index index, int count) {
        Set<Tuple> primaryKeys = new HashSet<>();
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            IntStream.range(0, count).mapToObj(i ->
                            recordStore.saveRecord(createSimpleDocument(idCounter++, "Document " + (textCounter++), 2)).getPrimaryKey())
                    .forEach(primaryKeys::add);
            context.commit();
        }
        return primaryKeys;
    }

    private void rebuildIndexMetaData(final FDBRecordContext context, final String document, final Index index) {
        Pair<FDBRecordStore, QueryPlanner> pair = LuceneIndexTestUtils.rebuildIndexMetaData(context, path, document, index, isUseCascadesPlanner());
        this.recordStore = pair.getLeft();
        this.planner = pair.getRight();
        recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(autoMerge);
    }


    private static class FailCommitsAgilityContext extends AgileContext {
        private final Object indexSubspaceKey;
        private int commitCount = 0;

        public FailCommitsAgilityContext(FDBRecordContext callerContext, final Object indexSubspaceKey) {
            super(callerContext, null, 1L, 1L);
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
