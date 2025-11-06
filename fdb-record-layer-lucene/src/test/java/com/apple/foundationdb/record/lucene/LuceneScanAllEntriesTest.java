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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsGroupedParentChildProto;
import com.apple.foundationdb.record.cursors.AutoContinuingCursor;
import com.apple.foundationdb.record.metadata.IndexPredicate;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreConcurrentTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintenanceFilter;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.lucene.LuceneIndexTestDataModel.CHILD_SEARCH_TERM;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestDataModel.PARENT_SEARCH_TERM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

        final Tuple group1ContentDoc;
        final Tuple group2ContentDoc;
        Tuple group2EmptyDoc = null;
        // Populate data: 1 doc for groups 1 and 2 and one empty doc (if needed)
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            group1ContentDoc = dataModel.saveRecord(store, 1);
            group2ContentDoc = dataModel.saveRecord(store, 2);
            if (includeEmptyDoc) {
                group2EmptyDoc = dataModel.saveEmptyRecord(store, 2);
            }
            commit(context);
        }

        LuceneQueryClause search = matchAllDocs
                                   ? LuceneQuerySearchClause.MATCH_ALL_DOCS_QUERY
                                   : isSynthetic
                                     ? new LuceneQuerySearchClause(LuceneQueryType.QUERY, CHILD_SEARCH_TERM, false)
                                     : new LuceneQuerySearchClause(LuceneQueryType.QUERY, PARENT_SEARCH_TERM, false);
        Set<Tuple> expectedResult = expectedResults(matchAllDocs, isGrouped, includeEmptyDoc,
                group1ContentDoc, group2ContentDoc, group2EmptyDoc);

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

    private Set<Tuple> expectedResults(final boolean matchAllDocs, final boolean isGrouped, final boolean includeEmptyDoc,
                                       final Tuple group1ContentDoc, final Tuple group2ContentDoc, final Tuple group2EmptyDoc) {
        // Synthetic record does not change the expected results - it just creates a record with a compound
        // key, so not needed for this method.
        Set<Tuple> result = new HashSet<>();

        if (!isGrouped) {
            // Grouped search searches for group2, so group1 docs are excluded
            result.add(group1ContentDoc);
        }
        // Every search includes this doc
        result.add(group2ContentDoc);
        if (matchAllDocs && includeEmptyDoc) {
            // Empty doc included only in cases it was added and the all-match query was used
            result.add(group2EmptyDoc);
        }

        return result;
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

        try (FDBRecordContext context = openContext()) {
            dataModel.saveRecords(500, context, 2);
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

        try (FDBRecordContext context = openContext()) {
            dataModel.saveRecords(500, context, 2);
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


    private void assertIndexEntryPrimaryKeyTuples(Set<Tuple> primaryKeys, RecordCursor<IndexEntry> cursor) {
        List<IndexEntry> indexEntries = cursor.asList().join();
        assertEquals(primaryKeys,
                indexEntries.stream().map(IndexEntry::getPrimaryKey).collect(Collectors.toSet()));
    }

    @ParameterizedTest
    @BooleanSource
    public void indexScanWithEvenRecNoFilterTest(boolean isGrouped) throws Exception {
        final long seed = 9876543L;
        final boolean isSynthetic = false;

        final LuceneIndexTestDataModel dataModel =
                new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilderFilterOddRecNo, pathManager)
                .setIsGrouped(isGrouped)
                .setIsSynthetic(isSynthetic)
                .setPrimaryKeySegmentIndexEnabled(true)
                .setPartitionHighWatermark(10)
                .build();

        try (FDBRecordContext context = openContext()) {
            // Save 10 records - only even recNo values should be indexed
            dataModel.saveRecords(10, context, 2);
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            List<IndexEntry> indexEntries = dataModel.findAllRecordsByQuery(context, 2);
            // We expect 5 records with even recNo values to be indexed
            assertEquals(5, indexEntries.size(), "Should have indexed only records with even recNo");
            verifyEvenRecNoOnly(indexEntries, dataModel);
        }

        try (FDBRecordContext context = openContext()) {
            // overwrite records
            dataModel.saveRecords(10, context, 2);
            commit(context);
        }
    }

    private void verifyEvenRecNoOnly(final List<IndexEntry> indexEntries, final LuceneIndexTestDataModel dataModel) {
        // Verify that all indexed records have even recNo
        for (IndexEntry entry : indexEntries) {
            Tuple primaryKey = entry.getPrimaryKey();
            // The recNo is part of the primary key - need to extract and verify it's even
            // For grouped records, structure is (group, recNo) or similar
            // For non-synthetic parent records, the recNo is in the tuple
            try (FDBRecordContext verifyContext = openContext()) {
                FDBRecordStore verifyStore = dataModel.createOrOpenRecordStore(verifyContext);
                FDBStoredRecord<?> storedRecord = verifyStore.loadRecord(primaryKey);
                assertNotNull(storedRecord, "Record should exist");
                com.google.protobuf.Message message = storedRecord.getRecord();
                com.google.protobuf.Descriptors.FieldDescriptor recNoField =
                        message.getDescriptorForType().findFieldByName("rec_no");
                long recNo = (long) message.getField(recNoField);
                assertEquals(0, recNo % 2, "All indexed records should have even recNo, but found: " + recNo);
            }
        }
    }

    @Nonnull
    private FDBRecordStore.Builder getStoreBuilderFilterOddRecNo(@Nonnull FDBRecordContext context,
                                                                   @Nonnull RecordMetaDataProvider metaData,
                                                                   @Nonnull final KeySpacePath path) {
        // Create an index maintenance filter that only indexes records with even recNo
        IndexMaintenanceFilter evenRecNoFilter = (index, rec) -> {
            Descriptors.FieldDescriptor recNoField =
                    rec.getDescriptorForType().findFieldByName("rec_no");
            if (recNoField != null && rec.hasField(recNoField)) {
                long recNo = (long) rec.getField(recNoField);
                if (recNo % 2 == 0) {
                    return IndexMaintenanceFilter.IndexValues.ALL;
                }
            }
            return IndexMaintenanceFilter.IndexValues.NONE;
        };

        return getStoreBuilder(context, metaData, path)
                .setIndexMaintenanceFilter(evenRecNoFilter);
    }

    @ParameterizedTest
    @BooleanSource
    public void indexScanWithRecNoIndexPredicateTest(boolean isGrouped) throws Exception {
        final long seed = 5432198L;

        // Create an index predicate that only indexes records with even recNo
        // We'll create a predicate: rec_no % 2 == 0
        // Since there's no modulo operator, we'll use an OR of specific even values
        final Type.Record recordType = Type.Record.fromDescriptor(
                TestRecordsGroupedParentChildProto.MyParentRecord.getDescriptor());
        final QuantifiedObjectValue recordValue = QuantifiedObjectValue.of(Quantifier.current(), recordType);
        final FieldValue recNoField = FieldValue.ofFieldName(recordValue, "rec_no");

        // Create predicate for rec_no > 1006
        final QueryPredicate filterPredicate = new ValuePredicate(recNoField, new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 1006L));

        // Convert to IndexPredicate
        final IndexPredicate indexPredicate = IndexPredicate.fromQueryPredicate(filterPredicate);

        // Build the data model using the Builder
        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilder, pathManager)
                .setIsGrouped(isGrouped)
                .setIsSynthetic(false)
                .setPrimaryKeySegmentIndexEnabled(true)
                .setPartitionHighWatermark(10)
                .setPredicate(indexPredicate)
                .build();

        try (FDBRecordContext context = openContext()) {
            dataModel.saveRecords(10, context, 2);
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            List<IndexEntry> indexEntries = dataModel.findAllRecordsByQuery(context, 2);

            // We expect 5 records with recNo > 1006 values to be indexed
            assertEquals(5, indexEntries.size(), "Should have indexed only records with even recNo");
            verifyRecNoBiggerThan(1006, indexEntries, dataModel);
        }
    }

    private void verifyRecNoBiggerThan(final long num, final List<IndexEntry> indexEntries, final LuceneIndexTestDataModel dataModel) {
        // Verify that all indexed records recNo > 1006
        for (IndexEntry entry : indexEntries) {
            Tuple primaryKey = entry.getPrimaryKey();
            // The recNo is part of the primary key - need to extract and verify it's qualifies
            // For grouped records, structure is (group, recNo) or similar
            // For non-synthetic parent records, the recNo is in the tuple
            try (FDBRecordContext verifyContext = openContext()) {
                FDBRecordStore verifyStore = dataModel.createOrOpenRecordStore(verifyContext);
                FDBStoredRecord<?> storedRecord = verifyStore.loadRecord(primaryKey);
                assertNotNull(storedRecord, "Record should exist");
                com.google.protobuf.Message message = storedRecord.getRecord();
                com.google.protobuf.Descriptors.FieldDescriptor recNoField =
                        message.getDescriptorForType().findFieldByName("rec_no");
                long recNo = (long) message.getField(recNoField);
                assertTrue(recNo > num);
            }
        }
    }

    @ParameterizedTest
    @BooleanSource
    public void indexScanWithSomeFilterThrowsExceptionTest(boolean isGrouped) {
        final long seed = 1234567L;

        final LuceneIndexTestDataModel dataModel =
                new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilderWithSomeFilter, pathManager)
                        .setIsGrouped(isGrouped)
                        .setIsSynthetic(false)
                        .setPrimaryKeySegmentIndexEnabled(true)
                        .setPartitionHighWatermark(10)
                        .build();

        // Attempt to save records - should throw RecordCoreException because SOME is not supported
        RecordCoreException exception = Assertions.assertThrows(RecordCoreException.class, () -> {
            try (FDBRecordContext context = openContext()) {
                // Try to save a single record with the SOME filter active
                // This should trigger the exception during index maintenance
                dataModel.saveRecords(1, context);
                commit(context);
            }
        });

        // Verify the exception message
        assertEquals("Lucene does not support this kind of filtering", exception.getMessage());
    }

    @Nonnull
    private FDBRecordStore.Builder getStoreBuilderWithSomeFilter(@Nonnull FDBRecordContext context,
                                                                 @Nonnull RecordMetaDataProvider metaData,
                                                                 @Nonnull final KeySpacePath path) {
        // Create an index maintenance filter that returns SOME
        // This should trigger an exception since Lucene doesn't support partial indexing
        IndexMaintenanceFilter someFilter = (index, rec) -> IndexMaintenanceFilter.IndexValues.SOME;

        return getStoreBuilder(context, metaData, path)
                .setIndexMaintenanceFilter(someFilter);
    }

    @ParameterizedTest
    @BooleanSource
    public void indexScanWithFailingFilterThrowsExceptionTest(boolean isGrouped) {
        final long seed = 7654321L;

        final LuceneIndexTestDataModel dataModel =
                new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilderWithFailingFilterFor1002L, pathManager)
                        .setIsGrouped(isGrouped)
                        .setIsSynthetic(false)
                        .setPrimaryKeySegmentIndexEnabled(true)
                        .setPartitionHighWatermark(10)
                        .build();

        // Attempt to save records - should throw RecordCoreException from the filter for recNo 1002L
        RecordCoreException exception = Assertions.assertThrows(RecordCoreException.class, () -> {
            try (FDBRecordContext context = openContext()) {
                dataModel.saveRecords(1, context, 2);
            }
        });

        Assertions.assertTrue(exception.getMessage().startsWith("Filter failed for recNo:"),
                "Exception message should indicate filter failure");
        Assertions.assertEquals(1002L, exception.getLogInfo().get("rec_no"),
                "Exception should log the rec_no that caused the failure");
        Assertions.assertEquals(dataModel.index.getName(), exception.getLogInfo().get("index"),
                "Exception should log the index name");
    }

    @Nonnull
    private FDBRecordStore.Builder getStoreBuilderWithFailingFilterFor1002L(@Nonnull FDBRecordContext context,
                                                                            @Nonnull RecordMetaDataProvider metaData,
                                                                            @Nonnull final KeySpacePath path) {
        // Create an index maintenance filter that throws an exception for specific record with recNo 1002L
        // This can be used to tests error handling when the filter itself fails
        IndexMaintenanceFilter failingFilter = (index, rec) -> {
            com.google.protobuf.Descriptors.FieldDescriptor recNoField =
                    rec.getDescriptorForType().findFieldByName("rec_no");
            if (recNoField != null && rec.hasField(recNoField)) {
                long recNo = (long) rec.getField(recNoField);
                if (recNo == 1002L) {
                    throw new RecordCoreException("Filter failed for recNo: " + recNo)
                            .addLogInfo("rec_no", recNo)
                            .addLogInfo("index", index.getName());
                }
                return IndexMaintenanceFilter.IndexValues.ALL;
            }
            return IndexMaintenanceFilter.IndexValues.NONE;
        };

        return getStoreBuilder(context, metaData, path)
                .setIndexMaintenanceFilter(failingFilter);
    }
}
