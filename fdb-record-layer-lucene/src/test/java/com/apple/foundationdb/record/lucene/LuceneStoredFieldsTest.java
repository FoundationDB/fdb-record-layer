/*
 * LuceneStoredFieldsTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.index.IndexFileNames;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.createSimpleDocument;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.COMPLEX_DOC;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.SIMPLE_DOC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for Lucene StoredFields implementation.
 */
@Tag(Tags.RequiresFDB)
public class LuceneStoredFieldsTest extends FDBRecordStoreTestBase {
    @Override
    protected RecordLayerPropertyStorage.Builder addDefaultProps(final RecordLayerPropertyStorage.Builder props) {
        return super.addDefaultProps(props)
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_COMPRESSION_ENABLED, true);
    }

    enum StoredFieldsType {
        /**
         * For testing if none of the options are specified. Currently this does not use the optimized format, but that
         * may change in the future.
         */
        Default(false, options -> {
            options.remove(LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED);
            options.remove(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED);
            options.remove(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_ENABLED);
        }),
        File(false, options -> {
            options.put(LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED, "false");
            options.put(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED, "false");
        }),
        Optimized(true, options -> {
            options.put(LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED, "true");
            options.put(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED, "false");
        }),
        PrimaryKeyV2(true, options -> {
            options.put(LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED, "false");
            options.put(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_ENABLED, "false");
            options.put(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED, "true");
        });

        private final boolean usesOptimizedStoredFields;
        private final Index simpleIndex;
        private final Index complexIndex;

        StoredFieldsType(boolean usesOptimizedStoredFields, Consumer<Map<String, String>> optionsBuilder) {
            this.usesOptimizedStoredFields = usesOptimizedStoredFields;
            this.simpleIndex = LuceneIndexTestUtils.simpleTextSuffixesIndex(optionsBuilder);
            this.complexIndex = LuceneIndexTestUtils.textAndStoredComplexIndex(optionsBuilder);
        }
    }

    @ParameterizedTest
    @EnumSource(StoredFieldsType.class)
    void testInsertDocuments(StoredFieldsType type) throws Exception {
        Index index = type.simpleIndex;

        Set<String> segments = new HashSet<>();
        final Set<Tuple> primaryKeys = new HashSet<>();
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            primaryKeys.add(recordStore.saveRecord(createSimpleDocument(1623L, "Document 1", 2)).getPrimaryKey());
            primaryKeys.add(recordStore.saveRecord(createSimpleDocument(1624L, "Document 2", 2)).getPrimaryKey());
            primaryKeys.add(recordStore.saveRecord(createSimpleDocument(1547L, "NonDocument 3", 2)).getPrimaryKey());
            context.commit();
        }
        getSegments(index, segments, SIMPLE_DOC);
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            final RecordQuery query = buildQuery("Document", Collections.emptyList(), SIMPLE_DOC);
            queryAndAssertFields(query, "text", Map.of(
                    1623L, "Document 1",
                    1624L, "Document 2"));
            try (FDBDirectory directory = new FDBDirectory(recordStore.indexSubspace(index), context, index.getOptions())) {
                if (type.usesOptimizedStoredFields) {
                    assertDocCountPerSegment(directory, List.of("_0"), List.of(3));
                    assertTrue(timer.getCounter(LuceneEvents.Waits.WAIT_LUCENE_GET_STORED_FIELDS).getCount() > 1);
                    assertTrue(timer.getCounter(LuceneEvents.SizeEvents.LUCENE_WRITE_STORED_FIELDS).getCount() >= 3);
                } else {
                    assertTotalDocCountInSegments(0, segments, directory);
                }
            }
        }
        validatePrimaryKeySegmentIndex(index, primaryKeys, SIMPLE_DOC);
    }

    @ParameterizedTest
    @EnumSource(StoredFieldsType.class)
    void testInsertMultipleTransactions(StoredFieldsType type) throws Exception {
        Index index = type.simpleIndex;

        final Set<String> segments = new HashSet<>();
        final Set<Tuple> primaryKeys = new HashSet<>();
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            primaryKeys.add(recordStore.saveRecord(createSimpleDocument(1623L, "Document 1", 2)).getPrimaryKey());
            context.commit();
        }
        getSegments(index, segments, SIMPLE_DOC);
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            primaryKeys.add(recordStore.saveRecord(createSimpleDocument(1624L, "Document 2", 2)).getPrimaryKey());
            context.commit();
        }
        getSegments(index, segments, SIMPLE_DOC);
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            primaryKeys.add(recordStore.saveRecord(createSimpleDocument(1547L, "NonDocument 3", 2)).getPrimaryKey());
            context.commit();
        }
        getSegments(index, segments, SIMPLE_DOC);
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            final RecordQuery query = buildQuery("Document", Collections.emptyList(), SIMPLE_DOC);
            queryAndAssertFields(query, "text", Map.of(
                    1623L, "Document 1",
                    1624L, "Document 2"));
            try (FDBDirectory directory = new FDBDirectory(recordStore.indexSubspace(index), context, index.getOptions())) {
                // TODO: Find a way to force a merge and make sure the old segments are gone
                if (type.usesOptimizedStoredFields) {
                    assertDocCountPerSegment(directory, List.of("_0", "_1", "_2", "_3"), List.of(1, 1, 1, 0));
                    assertTrue(timer.getCounter(LuceneEvents.Waits.WAIT_LUCENE_GET_STORED_FIELDS).getCount() > 1);
                    assertTrue(timer.getCounter(LuceneEvents.SizeEvents.LUCENE_WRITE_STORED_FIELDS).getCount() >= 3);
                } else {
                    assertTotalDocCountInSegments(0, segments, directory);
                }
            }
        }
        validatePrimaryKeySegmentIndex(index, primaryKeys, SIMPLE_DOC);
    }

    @ParameterizedTest
    @EnumSource(StoredFieldsType.class)
    void testInsertDeleteDocuments(StoredFieldsType type) throws Exception {
        Index index = type.simpleIndex;
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, 2.0)
                .build();

        Set<String> segments = new HashSet<>();
        final Set<Tuple> primaryKeys = new HashSet<>();

        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.saveRecord(createSimpleDocument(1623L, "Document 1", 2));
            // the other two will be deleted
            primaryKeys.add(recordStore.saveRecord(createSimpleDocument(1624L, "Document 2", 2)).getPrimaryKey());
            recordStore.saveRecord(createSimpleDocument(1547L, "NonDocument 3", 2));
            context.commit();
        }
        getSegments(index, segments, SIMPLE_DOC);
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.deleteRecord(Tuple.from(1623L));
            context.commit();
        }
        getSegments(index, segments, SIMPLE_DOC);
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.deleteRecord(Tuple.from(1547L));
            context.commit();
        }
        getSegments(index, segments, SIMPLE_DOC);
        try (FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            final RecordQuery query = buildQuery("Document", Collections.emptyList(), SIMPLE_DOC);
            queryAndAssertFields(query, "text", Map.of(1624L, "Document 2"));
            LuceneIndexTestUtils.mergeSegments(recordStore, index);
        }
        getSegments(index, segments, SIMPLE_DOC);
        try (FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            try (FDBDirectory directory = new FDBDirectory(recordStore.indexSubspace(index), context, index.getOptions())) {
                // After a merge, all tombstones are removed and one document remains
                if (type.usesOptimizedStoredFields) {
                    assertTotalDocCountInSegments(1, segments, directory);
                    assertTrue(timer.getCounter(LuceneEvents.Counts.LUCENE_DELETE_STORED_FIELDS).getCount() > 0);
                } else {
                    assertTotalDocCountInSegments(0, segments, directory);
                }
            }
        }
        validatePrimaryKeySegmentIndex(index, primaryKeys, SIMPLE_DOC);
    }

    @ParameterizedTest
    @EnumSource(StoredFieldsType.class)
    void testInsertDeleteDocumentsSameTransaction(StoredFieldsType type) throws Exception {
        Index index = type.simpleIndex;
        final Set<String> segments = new HashSet<>();
        final Set<Tuple> primaryKeys = new HashSet<>();
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.saveRecord(createSimpleDocument(1623L, "Document 1", 2));
            primaryKeys.add(recordStore.saveRecord(createSimpleDocument(1624L, "Document 2", 2)).getPrimaryKey());
            primaryKeys.add(recordStore.saveRecord(createSimpleDocument(1547L, "NonDocument 3", 2)).getPrimaryKey());

            recordStore.deleteRecord(Tuple.from(1623L));
            context.commit();
        }
        getSegments(index, segments, SIMPLE_DOC);
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            final RecordQuery query = buildQuery("Document", Collections.emptyList(), SIMPLE_DOC);
            queryAndAssertFields(query, "text", Map.of(1624L, "Document 2"));
        }
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            try (FDBDirectory directory = new FDBDirectory(recordStore.indexSubspace(index), context, index.getOptions())) {
                if (type.usesOptimizedStoredFields) {
                    // the delete flushes, and then deletes, but we didn't merge, so the tombstoned document should
                    // still be around
                    assertTotalDocCountInSegments(3, segments, directory);
                } else {
                    assertTotalDocCountInSegments(0, segments, directory);
                }
            }
        }
        validatePrimaryKeySegmentIndex(index, primaryKeys, SIMPLE_DOC);
    }

    @ParameterizedTest
    @EnumSource(StoredFieldsType.class)
    void testInsertUpdateDocuments(StoredFieldsType type) throws Exception {
        Index index = type.simpleIndex;
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, 2.0)
                .build();

        final Set<String> segments = new HashSet<>();

        final Set<Tuple> primaryKeys = new HashSet<>();

        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            primaryKeys.add(recordStore.saveRecord(createSimpleDocument(1623L, "Document 1", 2)).getPrimaryKey());
            primaryKeys.add(recordStore.saveRecord(createSimpleDocument(1624L, "Document 2", 2)).getPrimaryKey());
            primaryKeys.add(recordStore.saveRecord(createSimpleDocument(1547L, "NonDocument 3", 2)).getPrimaryKey());
            context.commit();
        }
        getSegments(index, segments, SIMPLE_DOC);
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.updateRecord(createSimpleDocument(1623L, "Document 3 modified", 2));
            context.commit();
        }
        getSegments(index, segments, SIMPLE_DOC);
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.updateRecord(createSimpleDocument(1624L, "Document 4 modified", 2));
            context.commit();
        }
        getSegments(index, segments, SIMPLE_DOC);
        try (FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            final RecordQuery query = buildQuery("Document", Collections.emptyList(), SIMPLE_DOC);
            queryAndAssertFields(query, "text", Map.of(
                    1623L, "Document 3 modified",
                    1624L, "Document 4 modified"));
            LuceneIndexTestUtils.mergeSegments(recordStore, index);
        }

        getSegments(index, segments, SIMPLE_DOC);
        try (FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            try (FDBDirectory directory = new FDBDirectory(recordStore.indexSubspace(index), context, index.getOptions())) {
                // After a merge, all tombstones are removed and 3 documents remain
                if (type.usesOptimizedStoredFields) {
                    assertTotalDocCountInSegments(3, segments, directory);
                    assertTrue(timer.getCounter(LuceneEvents.Counts.LUCENE_DELETE_STORED_FIELDS).getCount() > 0);
                } else {
                    assertTotalDocCountInSegments(0, segments, directory);
                }
            }
        }

        validatePrimaryKeySegmentIndex(index, primaryKeys, SIMPLE_DOC);
    }

    @ParameterizedTest
    @EnumSource(StoredFieldsType.class)
    void testDeleteAllDocuments(StoredFieldsType type) throws Exception {
        Index index = type.simpleIndex;
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, 2.0)
                .build();

        final Set<String> segments = new HashSet<>();
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.saveRecord(createSimpleDocument(1623L, "Document 1", 2));
            recordStore.saveRecord(createSimpleDocument(1624L, "Document 2", 2));
            recordStore.saveRecord(createSimpleDocument(1547L, "NonDocument 3", 2));
            context.commit();
        }
        getSegments(index, segments, SIMPLE_DOC);
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.deleteRecord(Tuple.from(1623L));
            recordStore.deleteRecord(Tuple.from(1624L));
            recordStore.deleteRecord(Tuple.from(1547L));
            context.commit();
        }
        getSegments(index, segments, SIMPLE_DOC);
        try (FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            final RecordQuery query = buildQuery("Document", Collections.emptyList(), SIMPLE_DOC);
            queryAndAssertFields(query, "text", Map.of());
            LuceneIndexTestUtils.mergeSegments(recordStore, index);
        }
        getSegments(index, segments, SIMPLE_DOC);
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            try (FDBDirectory directory = new FDBDirectory(recordStore.indexSubspace(index), context, index.getOptions())) {
                if (type.usesOptimizedStoredFields) {
                    // When deleting all docs from the index, the first segment (_0) was merged away and the last segment (_1) gets removed
                    assertDocCountPerSegment(directory, List.of("_0", "_1"), List.of(0, 0));
                    assertTrue(timer.getCounter(LuceneEvents.Counts.LUCENE_DELETE_STORED_FIELDS).getCount() > 0);
                } else {
                    assertTotalDocCountInSegments(0, segments, directory);
                }
            }
        }

        // we've deleted all the records
        validatePrimaryKeySegmentIndex(index, Set.of(), SIMPLE_DOC);
    }

    @ParameterizedTest
    @EnumSource(StoredFieldsType.class)
    void testComplexDocManyFields(StoredFieldsType type) throws Exception {
        // Use a complex index with several fields
        Index index = type.complexIndex;

        final Set<String> segments = new HashSet<>();
        final Set<Tuple> primaryKeys = new HashSet<>();

        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, index);

            primaryKeys.add(recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(
                    1623L, "Hello", "Hello 2", 5, 12, false, 7.123)).getPrimaryKey());
            primaryKeys.add(recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(
                    1624L, "Hello record", "Hello record 2", 6, 13, false, 8.123)).getPrimaryKey());
            primaryKeys.add(recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(
                    1625L, "Hello record layer", "Hello record layer 2", 7, 14, true, 9.123)).getPrimaryKey());
            context.commit();
        }
        getSegments(index, segments, COMPLEX_DOC);
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, index);
            final RecordQuery query = buildQuery("record", Collections.emptyList(), COMPLEX_DOC);
            // This doc has the PK as (Group, docID)
            queryAndAssertFieldsTuple(query, "text", Map.of(
                    Tuple.from(6, 1624L), "Hello record",
                    Tuple.from(7, 1625L), "Hello record layer"));
            // query again and compare the other fields
            queryAndAssertFieldsTuple(query, "text2", Map.of(
                    Tuple.from(6, 1624L), "Hello record 2",
                    Tuple.from(7, 1625L), "Hello record layer 2"));
            queryAndAssertFieldsTuple(query, "score", Map.of(
                    Tuple.from(6, 1624L), 13,
                    Tuple.from(7, 1625L), 14));
            queryAndAssertFieldsTuple(query, "is_seen", Map.of(
                    Tuple.from(6, 1624L), false,
                    Tuple.from(7, 1625L), true));
            queryAndAssertFieldsTuple(query, "time", Map.of(
                    Tuple.from(6, 1624L), 8.123,
                    Tuple.from(7, 1625L), 9.123));

            try (FDBDirectory directory = new FDBDirectory(recordStore.indexSubspace(index), context, index.getOptions())) {
                if (type.usesOptimizedStoredFields) {
                    assertDocCountPerSegment(directory, List.of("_0"), List.of(3));
                    assertTrue(timer.getCounter(LuceneEvents.Waits.WAIT_LUCENE_GET_STORED_FIELDS).getCount() > 5);
                    assertTrue(timer.getCounter(LuceneEvents.SizeEvents.LUCENE_WRITE_STORED_FIELDS).getCount() >= 3);
                } else {
                    assertTotalDocCountInSegments(0, segments, directory);
                }
            }
        }
        validatePrimaryKeySegmentIndex(index, primaryKeys, COMPLEX_DOC);
    }

    private RecordQuery buildQuery(final String term, final List<String> fields, final String docType) {
        final QueryComponent filter = new LuceneQueryComponent(term, fields);
        // Query for full records
        return RecordQuery.newBuilder()
                .setRecordType(docType)
                .setFilter(filter)
                .build();
    }

    /**
     * Utility helper to run a query and assert the results.
     * @param query the query to run
     * @param fieldName the field value to extract from each record
     * @param expectedValues a map of PK value to a field value to expect
     * @throws Exception in case of error
     */
    private void queryAndAssertFields(RecordQuery query, String fieldName, Map<Long, ?> expectedValues) throws Exception {
        Map<Tuple, ?> expectedValuesWithTuples = expectedValues.entrySet().stream().collect(Collectors.toMap(entry -> Tuple.from(entry.getKey()), entry -> entry.getValue()));
        queryAndAssertFieldsTuple(query, fieldName, expectedValuesWithTuples);
    }

    /**
     * Utility helper to run a query and assert the results.
     * @param query the query to run
     * @param fieldName the field value to extract from each record
     * @param expectedValues a map of PK Tuple to a field value to expect
     * @throws Exception in case of error
     */
    private void queryAndAssertFieldsTuple(RecordQuery query, String fieldName, Map<Tuple, ?> expectedValues) throws Exception {
        RecordQueryPlan plan = planner.plan(query);
        try (RecordCursor<FDBQueriedRecord<Message>> fdbQueriedRecordRecordCursor = recordStore.executeQuery(plan)) {
            List<FDBQueriedRecord<Message>> result = fdbQueriedRecordRecordCursor.asList().get();
            Map<Tuple, ?> actualValues = result.stream().collect(Collectors.toMap(record -> toPrimaryKey(record), record -> toFieldValue(record, fieldName)));

            assertEquals(expectedValues, actualValues);
        }
    }

    private Object toFieldValue(final FDBQueriedRecord<Message> record, final String fieldName) {
        final Message storedRecord = record.getStoredRecord().getRecord();
        return storedRecord.getField(storedRecord.getDescriptorForType().findFieldByName(fieldName));
    }

    private Tuple toPrimaryKey(final FDBQueriedRecord<Message> record) {
        return record.getIndexEntry().getPrimaryKey();
    }

    private void validatePrimaryKeySegmentIndex(final Index index, final Set<Tuple> primaryKeys, final String documentType) throws IOException {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, documentType, index);
            LuceneIndexTestValidator.validatePrimaryKeySegmentIndex(recordStore, index, Tuple.from(), null,
                    primaryKeys);
        }
    }

    private void getSegments(final Index index, Set<String> segments, final String documentType) {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, documentType, index);
            try (FDBDirectory directory = new FDBDirectory(recordStore.indexSubspace(index), context, index.getOptions())) {
                for (final String file : directory.listAll()) {
                    final String segmentName = IndexFileNames.parseSegmentName(file);
                    if (!segmentName.equals(file)) {
                        segments.add(segmentName);
                    }
                }
            }
        }
    }

    private static void assertTotalDocCountInSegments(final int expectedDocumentCount, final Set<String> segments, final FDBDirectory directory) {
        final Map<String, Integer> segmentCounts = segments.stream()
                .collect(Collectors.toMap(Function.identity(),
                        segmentName -> directory.scanStoredFields(segmentName).join().size()));
        assertEquals(expectedDocumentCount, segmentCounts.values().stream()
                        .mapToInt(entry -> entry)
                        .sum(),
                segmentCounts::toString);
    }

    private void assertDocCountPerSegment(FDBDirectory directory, List<String> expectedSegmentNames, List<Integer> expectedDocsPerSegment) throws Exception {
        for (int i = 0; i < expectedSegmentNames.size(); i++) {
            List<KeyValue> keyValues = directory.scanStoredFields(expectedSegmentNames.get(i)).get();
            assertEquals(expectedDocsPerSegment.get(i), keyValues.size(), expectedSegmentNames.get(i));
        }
    }

    private void rebuildIndexMetaData(final FDBRecordContext context, final String document, final Index index) {
        Pair<FDBRecordStore, QueryPlanner> pair = LuceneIndexTestUtils.rebuildIndexMetaData(context, path, document, index, useCascadesPlanner);
        this.recordStore = pair.getLeft();
        this.planner = pair.getRight();
        recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(false);
    }
}

