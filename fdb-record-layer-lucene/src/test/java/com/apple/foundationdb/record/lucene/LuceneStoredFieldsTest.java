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
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.provider.common.text.AllSuffixesTextTokenizer;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.SIMPLE_TEXT_SUFFIXES;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.TEXT_AND_STORED_COMPLEX;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.createSimpleDocument;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.COMPLEX_DOC;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.SIMPLE_DOC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for Lucene StoredFields implementation.
 */
@Tag(Tags.RequiresFDB)
public class LuceneStoredFieldsTest extends FDBRecordStoreTestBase {

    // The index to use when the optimized stored fields option is disabled (this is the same as the base index, with the
    // PK index and Optimized stored fields options disabled)
    private static final Index SIMPLE_TEXT_SUFFIXES_WITHOUT_OPT_STORED_FIELDS = new Index("Simple$text_suffixes_pky",
            function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME,
                    LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED, "false"));

    public static final Index TEXT_AND_STORED_COMPLEX_WITHOUT_OPT_STORED_FIELDS = new Index(
            "Simple$test_stored_complex",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("text2")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("group")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("score")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("time")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("is_seen"))),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED, "false"));

    @Override
    protected RecordLayerPropertyStorage.Builder addDefaultProps(final RecordLayerPropertyStorage.Builder props) {
        return super.addDefaultProps(props)
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_COMPRESSION_ENABLED, true);
    }

    @ParameterizedTest
    @BooleanSource
    void testInsertDocuments(boolean useOptimizedStoredFieldsFormat) throws Exception {
        Index index = getSimpleTextIndex(useOptimizedStoredFieldsFormat);

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
            queryAndAssertFields(query, "text", Set.of(1623L, 1624L), Set.of("Document 1", "Document 2"));
            if (useOptimizedStoredFieldsFormat) {
                try (FDBDirectory directory = new FDBDirectory(recordStore.indexSubspace(index), context, index.getOptions())) {
                    assertDocCountPerSegment(directory, List.of("_0"), List.of(3));
                }
                assertTrue(timer.getCounter(LuceneEvents.Waits.WAIT_LUCENE_GET_STORED_FIELDS).getCount() > 1);
                assertTrue(timer.getCounter(LuceneEvents.Counts.LUCENE_WRITE_STORED_FIELDS).getCount() >= 3);
            }
        }
    }

    @ParameterizedTest
    @BooleanSource
    void testInsertDeleteDocuments(boolean useOptimizedStoredFieldsFormat) throws Exception {
        Index index = getSimpleTextIndex(useOptimizedStoredFieldsFormat);

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
            final RecordQuery query = buildQuery("Document", Collections.emptyList(), SIMPLE_DOC);
            queryAndAssertFields(query, "text", Set.of(1624L), Set.of("Document 2"));
        }
    }

    @ParameterizedTest
    @BooleanSource
    void testInsertDeleteDocumentsSameTransaction(boolean useOptimizedStoredFieldsFormat) throws Exception {
        Index index = getSimpleTextIndex(useOptimizedStoredFieldsFormat);

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
            queryAndAssertFields(query, "text", Set.of(1624L), Set.of("Document 2"));
        }
    }

    @ParameterizedTest
    @BooleanSource
    void testInsertUpdateDocuments(boolean useOptimizedStoredFieldsFormat) throws Exception {
        Index index = getSimpleTextIndex(useOptimizedStoredFieldsFormat);

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
            final RecordQuery query = buildQuery("Document", Collections.emptyList(), SIMPLE_DOC);
            queryAndAssertFields(query, "text", Set.of(1623L, 1624L), Set.of("Document 2", "Document 3 modified"));
        }
    }

    @ParameterizedTest
    @BooleanSource
    void testDeleteAllDocuments(boolean useOptimizedStoredFieldsFormat) throws Exception {
        Index index = getSimpleTextIndex(useOptimizedStoredFieldsFormat);

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
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            final RecordQuery query = buildQuery("Document", Collections.emptyList(), SIMPLE_DOC);
            queryAndAssertFields(query, "text", Set.of(), Set.of());
            if (useOptimizedStoredFieldsFormat) {
                try (FDBDirectory directory = new FDBDirectory(recordStore.indexSubspace(index), context, index.getOptions())) {
                    // When deleting all docs from the index, the last segment (_1) gets removed, leaving the _0 segment with the 3 tombstones
                    assertDocCountPerSegment(directory, List.of("_0", "_1"), List.of(3, 0));
                }
                assertTrue(timer.getCounter(LuceneEvents.Counts.LUCENE_DELETE_STORED_FIELDS).getCount() > 0);
            }
        }
    }

    @ParameterizedTest
    @BooleanSource
    void testRollbackInsert(boolean useOptimizedStoredFieldsFormat) throws Exception {
        Index index = getSimpleTextIndex(useOptimizedStoredFieldsFormat);

        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.saveRecord(createSimpleDocument(1623L, "Document 1", 2));
            recordStore.saveRecord(createSimpleDocument(1624L, "Document 2", 2));
            recordStore.saveRecord(createSimpleDocument(1547L, "NonDocument 3", 2));
            context.ensureActive().cancel();
        }
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            final RecordQuery query = buildQuery("Document", Collections.emptyList(), SIMPLE_DOC);
            queryAndAssertFields(query, "text", Set.of(), Set.of());
            if (useOptimizedStoredFieldsFormat) {
                try (FDBDirectory directory = new FDBDirectory(recordStore.indexSubspace(index), context, index.getOptions())) {
                    assertDocCountPerSegment(directory, List.of("_0"), List.of(0));
                }
            }
        }
    }

    @ParameterizedTest
    @BooleanSource
    void testRollbackUpdate(boolean useOptimizedStoredFieldsFormat) throws Exception {
        Index index = getSimpleTextIndex(useOptimizedStoredFieldsFormat);

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
            context.ensureActive().cancel();
        }
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            final RecordQuery query = buildQuery("Document", Collections.emptyList(), SIMPLE_DOC);
            queryAndAssertFields(query, "text", Set.of(1623L, 1624L), Set.of("Document 1", "Document 2"));
            if (useOptimizedStoredFieldsFormat) {
                try (FDBDirectory directory = new FDBDirectory(recordStore.indexSubspace(index), context, index.getOptions())) {
                    assertDocCountPerSegment(directory, List.of("_0", "_1"), List.of(3, 0));
                }
            }
        }
    }

    @ParameterizedTest
    @BooleanSource
    void testRollbackDeleteAllDocuments(boolean useOptimizedStoredFieldsFormat) throws Exception {
        Index index = getSimpleTextIndex(useOptimizedStoredFieldsFormat);

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
            context.ensureActive().cancel();
        }
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            final RecordQuery query = buildQuery("Document", Collections.emptyList(), SIMPLE_DOC);
            queryAndAssertFields(query, "text", Set.of(1623L, 1624L), Set.of("Document 1", "Document 2"));
            if (useOptimizedStoredFieldsFormat) {
                try (FDBDirectory directory = new FDBDirectory(recordStore.indexSubspace(index), context, index.getOptions())) {
                    // When deleting all docs from the index, the last segment (_1) gets removed, leaving the _0 segment with the 3 tombstones
                    assertDocCountPerSegment(directory, List.of("_0", "_1"), List.of(3, 0));
                }
                assertNull(timer.getCounter(LuceneEvents.Counts.LUCENE_DELETE_STORED_FIELDS));
            }
        }
    }

    @ParameterizedTest
    @BooleanSource
    void testComplexDocManyFields(boolean useOptimizedStoredFieldsFormat) throws Exception {
        // Use a complex index with several fields
        Index index = getComplexTextIndex(useOptimizedStoredFieldsFormat);

        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, index);
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, "Hello", "Hello 2", 5, 12, false, 7.123));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1624L, "Hello record", "Hello record 2", 6, 13, false, 8.123));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1625L, "Hello record layer", "Hello record layer 2", 7, 14, true, 9.123));
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, index);
            final RecordQuery query = buildQuery("record", Collections.emptyList(), COMPLEX_DOC);
            // This doc has the PK as (Group, docID)
            queryAndAssertFieldsTuple(query, "text", Set.of(Tuple.from(6, 1624L), Tuple.from(7, 1625L)), Set.of("Hello record", "Hello record layer"));
            // query again and compare the other fields
            queryAndAssertFieldsTuple(query, "text2", Set.of(Tuple.from(6, 1624L), Tuple.from(7, 1625L)), Set.of("Hello record 2", "Hello record layer 2"));
            queryAndAssertFieldsTuple(query, "score", Set.of(Tuple.from(6, 1624L), Tuple.from(7, 1625L)), Set.of(13, 14));
            queryAndAssertFieldsTuple(query, "is_seen", Set.of(Tuple.from(6, 1624L), Tuple.from(7, 1625L)), Set.of(false, true));
            queryAndAssertFieldsTuple(query, "time", Set.of(Tuple.from(6, 1624L), Tuple.from(7, 1625L)), Set.of(8.123, 9.123));

            if (useOptimizedStoredFieldsFormat) {
                try (FDBDirectory directory = new FDBDirectory(recordStore.indexSubspace(index), context, index.getOptions())) {
                    assertDocCountPerSegment(directory, List.of("_0"), List.of(3));
                }
                assertTrue(timer.getCounter(LuceneEvents.Waits.WAIT_LUCENE_GET_STORED_FIELDS).getCount() > 5);
                assertTrue(timer.getCounter(LuceneEvents.Counts.LUCENE_WRITE_STORED_FIELDS).getCount() >= 3);
            }
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

    private void queryAndAssertFields(RecordQuery query, String fieldName, Set<Long> expectedPrimaryKeys, Set<?> expectedFields) throws Exception {
        queryAndAssertFieldsTuple(query, fieldName, expectedPrimaryKeys.stream().map(Tuple::from).collect(Collectors.toSet()), expectedFields);
    }

    private void queryAndAssertFieldsTuple(RecordQuery query, String fieldName, Set<Tuple> expectedPrimaryKeys, Set<?> expectedFields) throws Exception {
        RecordQueryPlan plan = planner.plan(query);
        try (RecordCursor<FDBQueriedRecord<Message>> fdbQueriedRecordRecordCursor = recordStore.executeQuery(plan)) {
            List<FDBQueriedRecord<Message>> result = fdbQueriedRecordRecordCursor.asList().get();
            Set<Tuple> actualKeyTuples = result.stream().map(FDBQueriedRecord::getIndexEntry).map(IndexEntry::getPrimaryKey).collect(Collectors.toSet());
            assertEquals(expectedPrimaryKeys, actualKeyTuples);
            Set<?> fields = result.stream()
                    .map(FDBQueriedRecord::getStoredRecord)
                    .map(Objects::requireNonNull)
                    .map(FDBStoredRecord::getRecord)
                    .map(m -> m.getField(m.getDescriptorForType().findFieldByName(fieldName)))
                    .collect(Collectors.toSet());
            assertEquals(expectedFields, fields);
        }
    }

    private void assertDocCountPerSegment(FDBDirectory directory, List<String> expectedSegmentNames, List<Integer> expectedDocsPerSegment) throws Exception {
        for (int i = 0; i < expectedSegmentNames.size(); i++) {
            List<KeyValue> keyValues = directory.scanStoredFields(expectedSegmentNames.get(i)).asList().get();
            assertEquals(expectedDocsPerSegment.get(i), keyValues.size());
        }
    }

    private void rebuildIndexMetaData(final FDBRecordContext context, final String document, final Index index) {
        Pair<FDBRecordStore, QueryPlanner> pair = LuceneIndexTestUtils.rebuildIndexMetaData(context, path, document, index, useRewritePlanner);
        this.recordStore = pair.getLeft();
        this.planner = pair.getRight();
    }

    private Index getSimpleTextIndex(boolean useOptimizedStoredFieldFormat) {
        return useOptimizedStoredFieldFormat ? SIMPLE_TEXT_SUFFIXES : SIMPLE_TEXT_SUFFIXES_WITHOUT_OPT_STORED_FIELDS;
    }

    private Index getComplexTextIndex(boolean useOptimizedStoredFieldFormat) {
        return useOptimizedStoredFieldFormat ? TEXT_AND_STORED_COMPLEX : TEXT_AND_STORED_COMPLEX_WITHOUT_OPT_STORED_FIELDS;
    }
}
