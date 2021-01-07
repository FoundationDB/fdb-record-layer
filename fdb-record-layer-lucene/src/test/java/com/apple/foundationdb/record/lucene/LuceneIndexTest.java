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

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.common.text.AllSuffixesTextTokenizer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableMap;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import static com.apple.foundationdb.record.lucene.LuceneIndexScanTypes.BY_LUCENE_QUERY;
import static com.apple.foundationdb.record.lucene.LuceneIndexScanTypes.BY_LUCENE_QUERY_WITH_POSITIONS;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.COMPLEX_DOC;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.SIMPLE_DOC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@code FULL_TEXT} type indexes.
 */
@Tag(Tags.RequiresFDB)
public class LuceneIndexTest extends FDBRecordStoreTestBase {

    private static final Index SIMPLE_TEXT_SUFFIXES = new Index("Simple$text_suffixes", field("text"), IndexTypes.FULL_TEXT,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME));

    private static final Index COMPLEX_MULTIPLE_TEXT_INDEXES = new Index("Complex$text_multipleIndexes", concatenateFields("text", "text2"), IndexTypes.FULL_TEXT,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME));

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

    @Test
    public void simpleInsertAndSearch() {
        final TestRecordsTextProto.SimpleDocument dylan = TestRecordsTextProto.SimpleDocument.newBuilder()
                .setDocId(1623L)
                .setText("You're an idiot, babe\n" +
                         "It's a wonder that you still know how to breathe")
                .setGroup(2)
                .build();
        final TestRecordsTextProto.SimpleDocument waylon = TestRecordsTextProto.SimpleDocument.newBuilder()
                .setDocId(1547L)
                .setText("There's always one more way to do things and that's your way, and you have a right to try it at least once.")
                .build();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            });
            recordStore.saveRecord(dylan);
            recordStore.saveRecord(waylon);
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, BY_LUCENE_QUERY, TupleRange.allOf(Tuple.from("idiot")), null, ScanProperties.FORWARD_SCAN);
            assertEquals(1, indexEntries.getCount().join());
            RecordCursor<IndexEntry> indexEntriesWithPositions = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, BY_LUCENE_QUERY_WITH_POSITIONS, TupleRange.allOf(Tuple.from("idiot")), null, ScanProperties.FORWARD_SCAN);
            assertEquals(1, indexEntriesWithPositions.getCount().join());
        }
    }

    @Test
    public void testMultipleFieldSearch() {
        final TestRecordsTextProto.ComplexDocument dylan = TestRecordsTextProto.ComplexDocument.newBuilder()
                .setDocId(1623L)
                .setText("You're an idiot, babe\n" +
                         "It's a wonder that you still know how to breathe")
                .setGroup(2)
                .setText2("john_leach@apple.com")
                .build();
        final TestRecordsTextProto.ComplexDocument waylon = TestRecordsTextProto.ComplexDocument.newBuilder()
                .setDocId(1547L)
                .setText("There's always one more way to do things and that's your way, and you have a right to try it at least once.")
                .setText2("hering@mail.com")
                .build();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(COMPLEX_DOC, COMPLEX_MULTIPLE_TEXT_INDEXES);
            });
            recordStore.saveRecord(dylan);
            recordStore.saveRecord(waylon);
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(COMPLEX_MULTIPLE_TEXT_INDEXES, BY_LUCENE_QUERY, TupleRange.allOf(Tuple.from("text:\"idiot\" AND text2:\"john_leach@apple.com\"")), null, ScanProperties.FORWARD_SCAN);
            assertEquals(1, indexEntries.getCount().join());
        }
    }


    @Test
    public void simpleInsertDeleteAndSearch() {
        final TestRecordsTextProto.SimpleDocument dylan = TestRecordsTextProto.SimpleDocument.newBuilder()
                .setDocId(1623L)
                .setText("You're an idiot, babe\n" +
                         "It's a wonder that you still know how to breathe")
                .setGroup(2)
                .build();
        final TestRecordsTextProto.SimpleDocument dylan2 = TestRecordsTextProto.SimpleDocument.newBuilder()
                .setDocId(1624L)
                .setText("You're an idiot, babe\n" +
                         "It's a wonder that you still know how to breathe")
                .setGroup(2)
                .build();
        final TestRecordsTextProto.SimpleDocument waylon = TestRecordsTextProto.SimpleDocument.newBuilder()
                .setDocId(1547L)
                .setText("There's always one more way to do things and that's your way, and you have a right to try it at least once.")
                .build();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            });
            recordStore.saveRecord(dylan);
            recordStore.saveRecord(dylan2);
            recordStore.saveRecord(waylon);
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, BY_LUCENE_QUERY, TupleRange.allOf(Tuple.from("idiot")), null, ScanProperties.FORWARD_SCAN);
            assertEquals(2, indexEntries.getCount().join());
            assertTrue(recordStore.deleteRecord(Tuple.from(1624L)));
            indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, BY_LUCENE_QUERY, TupleRange.allOf(Tuple.from("idiot")), null, ScanProperties.FORWARD_SCAN);
            assertEquals(1, indexEntries.getCount().join());
        }
    }

    @Test
    public void testOverflow() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            });
            Directory directory = new FDBDirectory(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), recordStore.ensureContextActive());
            IndexOutput indexOutput = directory.createOutput("OVERFLOW", null);
            Random rd = new Random();
            byte[] in = new byte[10 * 10 * 1024];
            rd.nextBytes(in);
            indexOutput.writeBytes(in, in.length);
            indexOutput.close();
            IndexInput indexInput = directory.openInput("OVERFLOW", null);
            byte[] out = new byte[10 * 10 * 1024];
            indexInput.readBytes(out, 0, out.length);
            assertTrue(Arrays.equals(in, out));
        }
    }

    @Test
    public void testSpanBlocks() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            });
            Directory directory = new FDBDirectory(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), recordStore.ensureContextActive());
            IndexOutput indexOutput = directory.createOutput("OVERFLOW", null);
            Random rd = new Random();
            byte[] in = new byte[10 * 10 * 1024];
            rd.nextBytes(in);
            indexOutput.writeBytes(in, in.length);
            indexOutput.close();
            IndexInput indexInput = directory.openInput("OVERFLOW", null);
            byte[] span = new byte[10];
            indexInput.seek(FDBDirectory.DEFAULT_BLOCK_SIZE - 2);
            indexInput.readBytes(span, 0, 10);
            assertEquals(ByteBuffer.wrap(in, FDBDirectory.DEFAULT_BLOCK_SIZE - 2, 10), ByteBuffer.wrap(span));
        }
    }

    @Test
    public void testAtBlockBoundary() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            });
            Directory directory = new FDBDirectory(recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), recordStore.ensureContextActive());
            IndexOutput indexOutput = directory.createOutput("OVERFLOW", null);
            Random rd = new Random();
            byte[] in = new byte[10 * 10 * 1024];
            rd.nextBytes(in);
            indexOutput.writeBytes(in, in.length);
            indexOutput.close();
            IndexInput indexInput = directory.openInput("OVERFLOW", null);
            byte[] span = new byte[10];
            indexInput.seek(FDBDirectory.DEFAULT_BLOCK_SIZE);
            indexInput.readBytes(span, 0, 10);
            assertEquals(ByteBuffer.wrap(in, FDBDirectory.DEFAULT_BLOCK_SIZE, 10), ByteBuffer.wrap(span));
        }
    }

    public void loadTestData() throws Exception {
        FDBRecordContext context = openContext();
        BufferedReader bufferedReader = new BufferedReader(new FileReader("/Users/john_leach/Downloads/counts.txt"));
        String line = bufferedReader.readLine();
        int i = 0;
        openRecordStore(context, metaDataBuilder -> {
            metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
            metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
        });
        while (line != null) {
            final TestRecordsTextProto.SimpleDocument dylan = TestRecordsTextProto.SimpleDocument.newBuilder()
                    .setDocId(i)
                    .setText(line)
                    .setGroup(2)
                    .build();
            recordStore.saveRecord(dylan);
            if (i % 100 == 0) {
                commit(context);
                context = openContext();
                openRecordStore(context, metaDataBuilder -> {
                    metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                    metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
                });
                System.out.println("Finished -> " + i);
            }
            i++;
            line = bufferedReader.readLine();
        }
        context = openContext();
        openRecordStore(context, metaDataBuilder -> {
            metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
            metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
        });
        for (int k = 0; k < 100000; k++) {
            final long execTime = System.currentTimeMillis();
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, BY_LUCENE_QUERY, TupleRange.allOf(Tuple.from("regional")), null, ScanProperties.FORWARD_SCAN);
            assertEquals(10, indexEntries.getCount().join());
            commit(context);
            context = openContext();
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            });
            System.out.println("query -> " + (System.currentTimeMillis() - execTime));
        }
        context.close();
    }

    // @Test
    public void testConcurrency() {
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
