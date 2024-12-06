/*
 * LuceneScanAllEntriesTest.java
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

import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.cursors.AutoContinuingCursor;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreConcurrentTestBase;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.lucene.LuceneIndexTestDataModel.CHILD_SEARCH_TERM;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestDataModel.PARENT_SEARCH_TERM;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test for Lucene index scanning where the query contains "*:*" term matching all documents.
 */
@Tag(Tags.RequiresFDB)
public class LuceneScanAllEntriesTest extends FDBRecordStoreConcurrentTestBase {
    public static Stream<Arguments> scanArguments() {
        return Stream.of(false, true)
                .flatMap(isSynthetic -> Stream.of(false, true)
                        .flatMap(matchAllDocs -> Stream.of(false, true)
                                .flatMap(isGrouped -> Stream.of(false, true)
                                        .map(includeEmptyDoc -> Arguments.of(isSynthetic, matchAllDocs, isGrouped, includeEmptyDoc)))));
    }

    @ParameterizedTest(name = "indexScanTest({argumentsWithNames})")
    @MethodSource("scanArguments")
    public void indexScanTest(boolean isSynthetic, boolean matchAllDocs, boolean isGrouped, boolean includeEmptyDoc) throws Exception {
        final long seed = 5363275763521L;
        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilder, pathManager)
                .setIsGrouped(isGrouped)
                .setIsSynthetic(isSynthetic)
                .setPrimaryKeySegmentIndexEnabled(true)
                .setPartitionHighWatermark(10)
                .build();

        final Tuple grp1ContentDoc;
        final Tuple grp2ContentDoc;
        Tuple grp2EmptyDoc = null;
        // Populate data: 1 doc for groups 1 and 2 and one empty doc (if needed)
        try (FDBRecordContext context = openContext()) {
            final long start = Instant.now().toEpochMilli();
            final FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            grp1ContentDoc = saveRecord(isGrouped, isSynthetic, dataModel, start, store, 1);
            grp2ContentDoc = saveRecord(isGrouped, isSynthetic, dataModel, start, store, 2);
            if (includeEmptyDoc) {
                grp2EmptyDoc = saveEmptyRecord(isGrouped, isSynthetic, dataModel, start, store, 2);
            }
            commit(context);
        }

        LuceneQueryClause search = matchAllDocs
                                   ? LuceneQuerySearchClause.MATCH_ALL_DOCS_QUERY
                                   : isSynthetic
                                     ? new LuceneQuerySearchClause(LuceneQueryType.QUERY, CHILD_SEARCH_TERM, false)
                                     : new LuceneQuerySearchClause(LuceneQueryType.QUERY, PARENT_SEARCH_TERM, false);
        Set<Tuple> expectedResult = calcExpectedResults(matchAllDocs, isGrouped, includeEmptyDoc,
                grp1ContentDoc, grp2ContentDoc, grp2EmptyDoc);

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            LuceneScanBounds scanBounds = isGrouped
                                          ? LuceneIndexTestValidator.groupedSortedTextSearch(store, dataModel.index, search, null, 2)
                                          : LuceneIndexTestUtils.fullTextSearch(store, dataModel.index, search, false);

            // Run the scan with the given query and assert the results
            assertIndexEntryPrimaryKeyTuples(expectedResult,
                    store.scanIndex(dataModel.index, scanBounds, null, ScanProperties.FORWARD_SCAN));
        }
    }

    private Set<Tuple> calcExpectedResults(final boolean matchAllDocs, final boolean isGrouped, final boolean includeEmptyDoc,
                                           final Tuple grp1ContentDoc, final Tuple grp2ContentDoc, final Tuple grp2EmptyDoc) {
        // Synthetic record does not change the expected results - it just creates a record with a compound
        // key, so not needed for this method.
        if (!matchAllDocs && isGrouped) {
            return Set.of(grp2ContentDoc);
        } else if (!matchAllDocs && !isGrouped) {
            return Set.of(grp1ContentDoc, grp2ContentDoc);
        } else if (matchAllDocs && isGrouped && includeEmptyDoc) {
            return Set.of(grp2ContentDoc, grp2EmptyDoc);
        } else if (matchAllDocs && isGrouped && !includeEmptyDoc) {
            return Set.of(grp2ContentDoc);
        } else if (matchAllDocs && !isGrouped && includeEmptyDoc) {
            return Set.of(grp1ContentDoc, grp2ContentDoc, grp2EmptyDoc);
        } else if (matchAllDocs && !isGrouped && !includeEmptyDoc) {
            return Set.of(grp1ContentDoc, grp2ContentDoc);
        }

        throw new IllegalArgumentException("Cannot calculate expected result");
    }

    @ParameterizedTest
    @BooleanSource
    public void scanLargeIndexTest(boolean isGrouped) throws Exception {
        final long seed = 6437286L;
        final boolean isSynthetic = false;
        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilder, pathManager)
                .setIsGrouped(isGrouped)
                .setIsSynthetic(isSynthetic)
                .setPrimaryKeySegmentIndexEnabled(true)
                .setPartitionHighWatermark(10)
                .build();

        final long start = Instant.now().toEpochMilli();
        try (FDBRecordContext context = openContext()) {
            dataModel.saveRecords(500, start, context, 2);
            commit(context);
        }

        // This test only uses the match-all query
        LuceneQueryClause search = LuceneQuerySearchClause.MATCH_ALL_DOCS_QUERY;

        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            LuceneScanBounds scanBounds = isGrouped
                                          ? LuceneIndexTestValidator.groupedSortedTextSearch(store, dataModel.index, search, null, 2)
                                          : LuceneIndexTestUtils.fullTextSearch(store, dataModel.index, search, false);

            // Run the scan with the given query and assert the results
            final Tuple groupTuple = LuceneIndexTestDataModel.calculateGroupTuple(isGrouped, 2);
            final Set<Tuple> expectedKeys = dataModel.groupingKeyToPrimaryKeyToPartitionKey.get(groupTuple).keySet();
            assertEquals(500, expectedKeys.size());
            assertIndexEntryPrimaryKeyTuples(expectedKeys,
                    store.scanIndex(dataModel.index, scanBounds, null, ScanProperties.FORWARD_SCAN));
        }
    }

    @ParameterizedTest
    @BooleanSource
    public void scanWithContinuationsTest(boolean isGrouped) throws Exception {
        final long seed = 85373450L;
        final boolean isSynthetic = false;
        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilder, pathManager)
                .setIsGrouped(isGrouped)
                .setIsSynthetic(isSynthetic)
                .setPrimaryKeySegmentIndexEnabled(true)
                .setPartitionHighWatermark(210)
                .build();

        final long start = Instant.now().toEpochMilli();
        try (FDBRecordContext context = openContext()) {
            dataModel.saveRecords(500, start, context, 2);
            commit(context);
        }
        // Scan properties with a limit of 36
        final ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                .setReturnedRowLimit(36)
                .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                .build());

        // This test only uses the match-all query
        LuceneQueryClause search = LuceneQuerySearchClause.MATCH_ALL_DOCS_QUERY;

        try (FDBDatabaseRunner runner = fdb.newRunner()) {
            // Create a cursor that reads through all continuations
            RecordCursor<IndexEntry> cursor = new AutoContinuingCursor<>(
                    runner,
                    (context, continuation) -> {
                        final FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
                        LuceneScanBounds scanBounds = isGrouped
                                                      ? LuceneIndexTestValidator.groupedSortedTextSearch(store, dataModel.index, search, null, 2)
                                                      : LuceneIndexTestUtils.fullTextSearch(store, dataModel.index, search, false);
                        return store.scanIndex(dataModel.index, scanBounds, continuation, scanProperties);
                    });
            // Run the scan with the given query and assert the results
            final Tuple groupTuple = LuceneIndexTestDataModel.calculateGroupTuple(isGrouped, 2);
            final Set<Tuple> expectedKeys = dataModel.groupingKeyToPrimaryKeyToPartitionKey.get(groupTuple).keySet();
            assertEquals(500, expectedKeys.size());
            assertIndexEntryPrimaryKeyTuples(expectedKeys, cursor);
        }
    }


    @Nonnull
    private static Tuple saveRecord(final boolean isGrouped, final boolean isSynthetic, final LuceneIndexTestDataModel dataModel, final long start, final FDBRecordStore store, final int group) {
        return LuceneIndexTestDataModel.saveRecord(isGrouped, isSynthetic, dataModel.random, dataModel.groupingKeyToPrimaryKeyToPartitionKey, dataModel.textGenerator, start, store, group);
    }

    @Nonnull
    private static Tuple saveEmptyRecord(final boolean isGrouped, final boolean isSynthetic, final LuceneIndexTestDataModel dataModel, final long start, final FDBRecordStore store, final int group) {
        return LuceneIndexTestDataModel.saveEmptyRecord(isGrouped, isSynthetic, dataModel.random, dataModel.groupingKeyToPrimaryKeyToPartitionKey, start, store, group);
    }

    private void assertIndexEntryPrimaryKeyTuples(Set<Tuple> primaryKeys, RecordCursor<IndexEntry> cursor) {
        List<IndexEntry> indexEntries = cursor.asList().join();
        assertEquals(primaryKeys,
                indexEntries.stream().map(IndexEntry::getPrimaryKey).collect(Collectors.toSet()));
    }
}
