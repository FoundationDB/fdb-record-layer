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

import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.provider.common.text.AllSuffixesTextTokenizer;
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
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.createSimpleDocument;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.metadata.Key.Expressions.value;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.COMPLEX_DOC;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.SIMPLE_DOC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for Lucene StoredFields implementation.
 * TODO: This test is very similar to {@link LuceneStoredFieldsTest}. We should be able to combine them into one test once
 * optimized postings are enabled globally.
 */
@Tag(Tags.RequiresFDB)
public class LucenePostingsTest extends FDBRecordStoreTestBase {

    // A Copy of the SIMPLE_TEXT_SUFFIXES with the additional optimized postings flag on.
    // This can be changed to the original once we switch that on for everyone
    private static final Index SIMPLE_TEXT_SUFFIXES_WITH_OPT_POSTINGS = new Index("Simple$text_suffixes_pky",
            function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME,
                    LuceneIndexOptions.OPTIMIZED_POSTINGS_FORMAT_ENABLED, "true"));

    // An index with other types of fields and field options
    public static final Index INDEXED_COMPLEX_WITH_OPT_POSTINGS = new Index(
            "Simple$test_stored_complex",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
                    function(LuceneFunctionNames.LUCENE_TEXT, concat(field("text2"),
                            function(LuceneFunctionNames.LUCENE_FULL_TEXT_FIELD_WITH_TERM_VECTORS, value(true)),
                            function(LuceneFunctionNames.LUCENE_FULL_TEXT_FIELD_WITH_TERM_VECTOR_POSITIONS, value(true)))),
                    field("group"),
                    field("score"),
                    field("time"),
                    field("is_seen")),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(LuceneIndexOptions.OPTIMIZED_POSTINGS_FORMAT_ENABLED, "true"));


    @Override
    protected RecordLayerPropertyStorage.Builder addDefaultProps(final RecordLayerPropertyStorage.Builder props) {
        return super.addDefaultProps(props)
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_COMPRESSION_ENABLED, true);
    }

    @Test
    void testInsertDocuments() throws Exception {
        Index index = SIMPLE_TEXT_SUFFIXES_WITH_OPT_POSTINGS;

        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.saveRecord(createSimpleDocument(1623L, "Document 1", 2));
            recordStore.saveRecord(createSimpleDocument(1624L, "Document 2", 2));
            recordStore.saveRecord(createSimpleDocument(1547L, "NonDocument 3", 2));
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            final RecordQuery query = buildQuery("Document", Collections.emptyList(), SIMPLE_DOC);
            queryAndAssertFields(query, "text", Map.of(
                    1623L, "Document 1",
                    1624L, "Document 2"));
            assertTrue(timer.getCounter(LuceneEvents.Counts.LUCENE_WRITE_POSTINGS_FIELD_METADATA).getCount() >= 1);
            assertTrue(timer.getCounter(LuceneEvents.Counts.LUCENE_WRITE_POSTINGS_TERM).getCount() >= 5);
            assertTrue(timer.getCounter(LuceneEvents.Counts.LUCENE_WRITE_POSTINGS_POSITIONS).getCount() >= 6);
            assertTrue(timer.getCounter(LuceneEvents.Waits.WAIT_LUCENE_READ_POSTINGS_FIELD_METADATA).getCount() >= 1);
            assertTrue(timer.getCounter(LuceneEvents.Waits.WAIT_LUCENE_READ_POSTINGS_TERMS).getCount() >= 1);
            try (FDBDirectory directory = new FDBDirectory(recordStore.indexSubspace(index), context, index.getOptions())) {
                assertFieldCountPerSegment(directory, List.of("_0"), List.of(1));
            }
        }
    }

    @Test
    void testInsertMultipleTransactions() throws Exception {
        Index index = SIMPLE_TEXT_SUFFIXES_WITH_OPT_POSTINGS;

        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.saveRecord(createSimpleDocument(1623L, "Document 1", 2));
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.saveRecord(createSimpleDocument(1624L, "Document 2", 2));
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.saveRecord(createSimpleDocument(1547L, "NonDocument 3", 2));
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            final RecordQuery query = buildQuery("Document", Collections.emptyList(), SIMPLE_DOC);
            queryAndAssertFields(query, "text", Map.of(
                    1623L, "Document 1",
                    1624L, "Document 2"));
            assertTrue(timer.getCounter(LuceneEvents.Counts.LUCENE_WRITE_POSTINGS_FIELD_METADATA).getCount() >= 3);
            assertTrue(timer.getCounter(LuceneEvents.Counts.LUCENE_WRITE_POSTINGS_TERM).getCount() >= 6);
            assertTrue(timer.getCounter(LuceneEvents.Counts.LUCENE_WRITE_POSTINGS_POSITIONS).getCount() >= 6);
            assertTrue(timer.getCounter(LuceneEvents.Waits.WAIT_LUCENE_READ_POSTINGS_FIELD_METADATA).getCount() >= 3);
            assertTrue(timer.getCounter(LuceneEvents.Waits.WAIT_LUCENE_READ_POSTINGS_TERMS).getCount() >= 3);
            try (FDBDirectory directory = new FDBDirectory(recordStore.indexSubspace(index), context, index.getOptions())) {
                final String[] strings = directory.listAll();
                assertFieldCountPerSegment(directory, List.of("_0", "_1", "_2", "_3"), List.of(1, 1, 1, 0));
            }
        }
    }

    @Test
    void testInsertDeleteDocuments() throws Exception {
        Index index = SIMPLE_TEXT_SUFFIXES_WITH_OPT_POSTINGS;

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, 2.0)
                .build();

        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.saveRecord(createSimpleDocument(1623L, "Document 1", 2));
            recordStore.saveRecord(createSimpleDocument(1624L, "Document 2", 2));
            recordStore.saveRecord(createSimpleDocument(1547L, "NonDocument 3", 2));
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.deleteRecord(Tuple.from(1623L));
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.deleteRecord(Tuple.from(1547L));
            context.commit();
        }
        try (FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            final RecordQuery query = buildQuery("Document", Collections.emptyList(), SIMPLE_DOC);
            queryAndAssertFields(query, "text", Map.of(1624L, "Document 2"));
            LuceneIndexTestUtils.mergeSegments(recordStore, index);
            try (FDBDirectory directory = new FDBDirectory(recordStore.indexSubspace(index), context, index.getOptions())) {
                // After a merge, all tombstones are removed and one document remains
                assertFieldCountPerSegment(directory, List.of("_0", "_1", "_2"), List.of(0, 0, 1));
            }
            assertTrue(timer.getCounter(LuceneEvents.Counts.LUCENE_DELETE_POSTINGS).getCount() > 0);
        }
    }

    @Test
    void testInsertDeleteDocumentsSameTransaction() throws Exception {
        Index index = SIMPLE_TEXT_SUFFIXES_WITH_OPT_POSTINGS;

        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.saveRecord(createSimpleDocument(1623L, "Document 1", 2));
            recordStore.saveRecord(createSimpleDocument(1624L, "Document 2", 2));
            recordStore.saveRecord(createSimpleDocument(1547L, "NonDocument 3", 2));

            recordStore.deleteRecord(Tuple.from(1623L));
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            final RecordQuery query = buildQuery("Document", Collections.emptyList(), SIMPLE_DOC);
            queryAndAssertFields(query, "text", Map.of(1624L, "Document 2"));
            try (FDBDirectory directory = new FDBDirectory(recordStore.indexSubspace(index), context, index.getOptions())) {
                // After a merge, all tombstones are removed and one document remains
                assertFieldCountPerSegment(directory, List.of("_0", "_1", "_2"), List.of(0, 1, 0));
            }
        }
        assertTrue(timer.getCounter(LuceneEvents.Counts.LUCENE_DELETE_POSTINGS).getCount() > 0);
    }

    @Test
    void testInsertUpdateDocuments() throws Exception {
        Index index = SIMPLE_TEXT_SUFFIXES_WITH_OPT_POSTINGS;

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, 2.0)
                .build();

        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.saveRecord(createSimpleDocument(1623L, "Document 1", 2));
            recordStore.saveRecord(createSimpleDocument(1624L, "Document 2", 2));
            recordStore.saveRecord(createSimpleDocument(1547L, "NonDocument 3", 2));
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.updateRecord(createSimpleDocument(1623L, "Document 3 modified", 2));
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.updateRecord(createSimpleDocument(1624L, "Document 4 modified", 2));
            context.commit();
        }
        try (FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            final RecordQuery query = buildQuery("Document", Collections.emptyList(), SIMPLE_DOC);
            queryAndAssertFields(query, "text", Map.of(
                    1623L, "Document 3 modified",
                    1624L, "Document 4 modified"));
            LuceneIndexTestUtils.mergeSegments(recordStore, index);
            try (FDBDirectory directory = new FDBDirectory(recordStore.indexSubspace(index), context, index.getOptions())) {
                // All 3 segments are going to merge to one with all documents
                assertFieldCountPerSegment(directory, List.of("_0", "_1", "_2", "_3"), List.of(0, 0, 0, 1));
            }
            assertTrue(timer.getCounter(LuceneEvents.Counts.LUCENE_WRITE_POSTINGS_FIELD_METADATA).getCount() >= 4);
            assertTrue(timer.getCounter(LuceneEvents.Counts.LUCENE_WRITE_POSTINGS_TERM).getCount() >= 8);
            assertTrue(timer.getCounter(LuceneEvents.Counts.LUCENE_WRITE_POSTINGS_POSITIONS).getCount() >= 8);
            assertTrue(timer.getCounter(LuceneEvents.Waits.WAIT_LUCENE_READ_POSTINGS_FIELD_METADATA).getCount() >= 4);
            assertTrue(timer.getCounter(LuceneEvents.Waits.WAIT_LUCENE_READ_POSTINGS_TERMS).getCount() >= 3);
            assertTrue(timer.getCounter(LuceneEvents.Counts.LUCENE_DELETE_POSTINGS).getCount() >= 3);
        }
    }

    @Test
    void testDeleteAllDocuments() throws Exception {
        Index index = SIMPLE_TEXT_SUFFIXES_WITH_OPT_POSTINGS;

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, 2.0)
                .build();

        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.saveRecord(createSimpleDocument(1623L, "Document 1", 2));
            recordStore.saveRecord(createSimpleDocument(1624L, "Document 2", 2));
            recordStore.saveRecord(createSimpleDocument(1547L, "NonDocument 3", 2));
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.deleteRecord(Tuple.from(1623L));
            recordStore.deleteRecord(Tuple.from(1624L));
            recordStore.deleteRecord(Tuple.from(1547L));
            context.commit();
        }
        try (FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            final RecordQuery query = buildQuery("Document", Collections.emptyList(), SIMPLE_DOC);
            queryAndAssertFields(query, "text", Map.of());
            LuceneIndexTestUtils.mergeSegments(recordStore, index);
            try (FDBDirectory directory = new FDBDirectory(recordStore.indexSubspace(index), context, index.getOptions())) {
                // When deleting all docs from the index, the first segment (_0) was merged away and the last segment (_1) gets removed
                assertFieldCountPerSegment(directory, List.of("_0", "_1"), List.of(0, 0));
            }
            assertTrue(timer.getCounter(LuceneEvents.Counts.LUCENE_DELETE_POSTINGS).getCount() > 0);
        }
    }

    @Test
    void testComplexDocManyFields() throws Exception {
        // Use a complex index with several fields
        Index index = INDEXED_COMPLEX_WITH_OPT_POSTINGS;

        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, index);
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, "Hello", "Hello 2", 5, 12, false, 7.123));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1624L, "Hello record", "Hello record 2", 6, 13, false, 8.123));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1625L, "Hello record layer", "Hello record layer 2", 7, 14, true, 9.123));
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, index);
            RecordQuery query = buildQuery("record", Collections.emptyList(), COMPLEX_DOC);
            // This doc has the PK as (Group, docID)
            queryAndAssertFieldsTuple(query, "text", Map.of(
                    Tuple.from(6, 1624L), "Hello record",
                    Tuple.from(7, 1625L), "Hello record layer"));
            // query on the other fields
            query = buildQuery("text2:layer", Collections.emptyList(), COMPLEX_DOC);
            queryAndAssertFieldsTuple(query, "text2", Map.of(
                    Tuple.from(7, 1625L), "Hello record layer 2"));
            query = buildQuery("group:5", Collections.emptyList(), COMPLEX_DOC);
            queryAndAssertFieldsTuple(query, "group", Map.of(
                    Tuple.from(5, 1623L), 5L));
            query = buildQuery("score:12", Collections.emptyList(), COMPLEX_DOC);
            queryAndAssertFieldsTuple(query, "score", Map.of(
                    Tuple.from(5, 1623L), 12));
            query = buildQuery("is_seen:false", Collections.emptyList(), COMPLEX_DOC);
            queryAndAssertFieldsTuple(query, "is_seen", Map.of(
                    Tuple.from(5, 1623L), false,
                    Tuple.from(6, 1624L), false));

            try (FDBDirectory directory = new FDBDirectory(recordStore.indexSubspace(index), context, index.getOptions())) {
                assertFieldCountPerSegment(directory, List.of("_0"), List.of(2));
            }
            assertTrue(timer.getCounter(LuceneEvents.Waits.WAIT_LUCENE_READ_POSTINGS_TERMS).getCount() >= 3);
            assertTrue(timer.getCounter(LuceneEvents.Waits.WAIT_LUCENE_READ_POSTINGS_FIELD_METADATA).getCount() >= 3);
            assertTrue(timer.getCounter(LuceneEvents.Counts.LUCENE_WRITE_POSTINGS_FIELD_METADATA).getCount() >= 2);
            assertTrue(timer.getCounter(LuceneEvents.Counts.LUCENE_WRITE_POSTINGS_POSITIONS).getCount() >= 15);
            assertTrue(timer.getCounter(LuceneEvents.Counts.LUCENE_WRITE_POSTINGS_TERM).getCount() >= 7);
        }
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
     *
     * @param query the query to run
     * @param fieldName the field value to extract from each record
     * @param expectedValues a map of PK value to a field value to expect
     *
     * @throws Exception in case of error
     */
    private void queryAndAssertFields(RecordQuery query, String fieldName, Map<Long, ?> expectedValues) throws Exception {
        Map<Tuple, ?> expectedValuesWithTuples = expectedValues.entrySet().stream().collect(Collectors.toMap(entry -> Tuple.from(entry.getKey()), entry -> entry.getValue()));
        queryAndAssertFieldsTuple(query, fieldName, expectedValuesWithTuples);
    }

    /**
     * Utility helper to run a query and assert the results.
     *
     * @param query the query to run
     * @param fieldName the field value to extract from each record
     * @param expectedValues a map of PK Tuple to a field value to expect
     *
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

    private void assertFieldCountPerSegment(FDBDirectory directory, List<String> expectedSegmentNames, List<Integer> expectedFieldPerSegment) throws Exception {
        for (int i = 0; i < expectedSegmentNames.size(); i++) {
            long count = directory.getAllPostingFieldMetadataStream(expectedSegmentNames.get(i)).count();
            assertEquals(expectedFieldPerSegment.get(i), (int)count);
        }
    }

    private void rebuildIndexMetaData(final FDBRecordContext context, final String document, final Index index) {
        Pair<FDBRecordStore, QueryPlanner> pair = LuceneIndexTestUtils.rebuildIndexMetaData(context, path, document, index, useCascadesPlanner);
        this.recordStore = pair.getLeft();
        this.planner = pair.getRight();
    }
}

