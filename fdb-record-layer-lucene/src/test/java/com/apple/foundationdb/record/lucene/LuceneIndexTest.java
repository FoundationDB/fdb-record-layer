/*
 * LuceneIndexTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.provider.common.text.AllSuffixesTextTokenizer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import javax.annotation.Nonnull;
import java.util.Random;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.COMPLEX_DOC;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.SIMPLE_DOC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@code LUCENE} type indexes.
 */
@Tag(Tags.RequiresFDB)
public class LuceneIndexTest extends FDBRecordStoreTestBase {

    private static final Index SIMPLE_TEXT_SUFFIXES = new Index("Simple$text_suffixes", field("text"), LuceneIndexTypes.LUCENE,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME));

    private static final Index COMPLEX_MULTIPLE_TEXT_INDEXES = new Index("Complex$text_multipleIndexes", concatenateFields("text", "text2"), LuceneIndexTypes.LUCENE,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME));

    private static final String DYLAN = "You're an idiot, babe\n" +
                                        "It's a wonder that you still know how to breathe";

    private static final String WAYLON = "There's always one more way to do things and that's your way, and you have a right to try it at least once.";

    protected void openRecordStore(FDBRecordContext context, FDBRecordStoreTestBase.RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsTextProto.getDescriptor());
        metaDataBuilder.getRecordType(COMPLEX_DOC).setPrimaryKey(concatenateFields("group", "doc_id"));
        hook.apply(metaDataBuilder);
        recordStore = getStoreBuilder(context, metaDataBuilder.getRecordMetaData())
                .setSerializer(TextIndexTestUtils.COMPRESSING_SERIALIZER)
                .uncheckedOpen();
        setupPlanner(null);
    }

    @Nonnull
    protected FDBRecordStore.Builder getStoreBuilder(@Nonnull FDBRecordContext context, @Nonnull RecordMetaData metaData) {
        return FDBRecordStore.newBuilder()
                .setFormatVersion(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION) // set to max to test newest features (unsafe for real deployments)
                .setKeySpacePath(path)
                .setContext(context)
                .setMetaDataProvider(metaData);
    }

    private TestRecordsTextProto.SimpleDocument createSimpleDocument(long docId, String text, int group) {
        return TestRecordsTextProto.SimpleDocument.newBuilder()
                .setDocId(docId)
                .setText(text)
                .setGroup(group)
                .build();
    }

    private TestRecordsTextProto.ComplexDocument createComplexDocument(long docId, String text, String text2, int group) {
        return TestRecordsTextProto.ComplexDocument.newBuilder()
                .setDocId(docId)
                .setText(text)
                .setText2(text2)
                .setGroup(group)
                .build();
    }


    @Test
    public void simpleInsertAndSearch() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            });
            recordStore.saveRecord(createSimpleDocument(1623L, DYLAN, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE, TupleRange.allOf(Tuple.from("idiot")), null, ScanProperties.FORWARD_SCAN);
            assertEquals(1, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE,
                    TupleRange.allOf(Tuple.from("idiot")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            assertEquals(1, context.getTimer().getCounter(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
        }
    }

    @Test
    public void testContinuation() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            });
            recordStore.saveRecord(createSimpleDocument(1623L, DYLAN, 2));
            recordStore.saveRecord(createSimpleDocument(1624L, DYLAN, 2));
            recordStore.saveRecord(createSimpleDocument(1625L, DYLAN, 2));
            recordStore.saveRecord(createSimpleDocument(1626L, DYLAN, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
            assertEquals(2, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE,
                    TupleRange.allOf(Tuple.from("idiot")), Ints.toByteArray(2), ScanProperties.FORWARD_SCAN)
                    .getCount().join());
        }
    }

    @Test
    public void testLimit() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            });
            for (int i = 0; i < 200; i++) {
                recordStore.saveRecord(createSimpleDocument(1623L + i, DYLAN, 2));
            }
            assertEquals(50, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE,
                    TupleRange.allOf(Tuple.from("idiot")), null,
                    ExecuteProperties.newBuilder().setReturnedRowLimit(50).build().asScanProperties(false))
                    .getCount().join());
        }
    }

    @Test
    public void testSkip() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            });
            for (int i = 0; i < 50; i++) {
                recordStore.saveRecord(createSimpleDocument(1623L + i, DYLAN, 2));
            }
            assertEquals(40, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE,
                    TupleRange.allOf(Tuple.from("idiot")), null,
                    ExecuteProperties.newBuilder().setReturnedRowLimit(50).setSkip(10).build().asScanProperties(false))
                    .getCount().join());
        }
    }

    @Test
    public void testSkipWithLimit() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            });
            for (int i = 0; i < 50; i++) {
                recordStore.saveRecord(createSimpleDocument(1623L + i, DYLAN, 2));
            }
            assertEquals(40, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE,
                    TupleRange.allOf(Tuple.from("idiot")), null,
                    ExecuteProperties.newBuilder().setReturnedRowLimit(50).setSkip(10).build().asScanProperties(false))
                    .getCount().join());
        }
    }

    @Test
    public void testLimitWithContinuation() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            });
            for (int i = 0; i < 200; i++) {
                recordStore.saveRecord(createSimpleDocument(1623L + i, DYLAN, 2));
            }
            assertEquals(48, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE,
                    TupleRange.allOf(Tuple.from("idiot")), Ints.toByteArray(2),
                    ExecuteProperties.newBuilder().setReturnedRowLimit(50).build().asScanProperties(false))
                    .getCount().join());
        }
    }

    @Test
    public void testMultipleFieldSearch() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(COMPLEX_DOC, COMPLEX_MULTIPLE_TEXT_INDEXES);
            });
            recordStore.saveRecord(createComplexDocument(1623L, DYLAN, "john_leach@apple.com", 2));
            recordStore.saveRecord(createComplexDocument(1547L, WAYLON, "hering@gmail.com", 2));
            assertEquals(1, recordStore.scanIndex(COMPLEX_MULTIPLE_TEXT_INDEXES, IndexScanType.BY_LUCENE,
                    TupleRange.allOf(Tuple.from("text:\"idiot\" AND text2:\"john_leach@apple.com\"")),
                    null, ScanProperties.FORWARD_SCAN).getCount().join());
        }
    }

    @Test
    public void testFuzzySearchWithDefaultEdit2() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(COMPLEX_DOC, COMPLEX_MULTIPLE_TEXT_INDEXES);
            });
            recordStore.saveRecord(createComplexDocument(1623L, DYLAN, "john_leach@apple.com", 2));
            recordStore.saveRecord(createComplexDocument(1547L, WAYLON, "hering@gmail.com", 2));
            assertEquals(1, recordStore.scanIndex(COMPLEX_MULTIPLE_TEXT_INDEXES, IndexScanType.BY_LUCENE,
                    TupleRange.allOf(Tuple.from("text:\"idiot\" AND text2:jonleach@apple.com\\~")),
                    null, ScanProperties.FORWARD_SCAN).getCount().join());
        }
    }

    @Test
    public void simpleInsertDeleteAndSearch() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            });
            recordStore.saveRecord(createSimpleDocument(1623L, DYLAN, 2));
            recordStore.saveRecord(createSimpleDocument(1624L, DYLAN, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 2));
            assertEquals(2, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE,
                    TupleRange.allOf(Tuple.from("idiot")), null, ScanProperties.FORWARD_SCAN).getCount().join());
            assertTrue(recordStore.deleteRecord(Tuple.from(1624L)));
            assertEquals(1, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE,
                    TupleRange.allOf(Tuple.from("idiot")), null, ScanProperties.FORWARD_SCAN).getCount().join());
        }
    }

    @Test
    public void testCommit() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            });
            recordStore.saveRecord(createSimpleDocument(1623L, DYLAN, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
            assertEquals(1, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE,
                    TupleRange.allOf(Tuple.from("idiot")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            });
            assertEquals(1, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE,
                    TupleRange.allOf(Tuple.from("idiot")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
        }
    }

    @Test
    public void testRollback() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            });
            recordStore.saveRecord(createSimpleDocument(1623L, DYLAN, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
            assertEquals(1, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE,
                    TupleRange.allOf(Tuple.from("idiot")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            context.ensureActive().cancel();
        }
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            });
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
            assertEquals(0, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE,
                    TupleRange.allOf(Tuple.from("idiot")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            recordStore.saveRecord(createSimpleDocument(1623L, DYLAN, 2));
            assertEquals(1, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, IndexScanType.BY_LUCENE,
                    TupleRange.allOf(Tuple.from("idiot")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());

        }
    }

    @Test
    public void testDataLoad() {
        FDBRecordContext context = openContext();
        for (int i = 0; i < 2000; i++) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            });
            String[] randomWords = generateRandomWords(500);
            final TestRecordsTextProto.SimpleDocument dylan = TestRecordsTextProto.SimpleDocument.newBuilder()
                    .setDocId(i)
                    .setText(randomWords[1])
                    .setGroup(2)
                    .build();
            recordStore.saveRecord(dylan);
            if (i % 50 == 0) {
                commit(context);
                context = openContext();
            }
        }
        context.close();
    }

    public static String[] generateRandomWords(int numberOfWords) {
        assert numberOfWords > 0 : "Number of words have to be greater than 0";
        StringBuilder builder = new StringBuilder();
        Random random = new Random();
        char[] word = null;
        for (int i = 0; i < numberOfWords; i++) {
            word = new char[random.nextInt(8) + 3]; // words of length 3 through 10. (1 and 2 letter words are boring.)
            for (int j = 0; j < word.length; j++) {
                word[j] = (char)('a' + random.nextInt(26));
            }
            if (i != numberOfWords - 1) {
                builder.append(word).append(" ");
            }
        }
        String[] returnValue = new String[2];
        returnValue[0] = new String(word);
        returnValue[1] = builder.toString();
        return returnValue;
    }
}
