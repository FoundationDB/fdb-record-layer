/*
 * SortCursorTests.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.cursors;

import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.DynamicMessageRecordSerializer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.SortedRecordSerializer;
import com.apple.foundationdb.record.sorting.FileSortAdapter;
import com.apple.foundationdb.record.sorting.FileSortCursor;
import com.apple.foundationdb.record.sorting.MemorySortAdapter;
import com.apple.foundationdb.record.sorting.MemorySortCursor;
import com.apple.foundationdb.record.sorting.MemorySorter;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link MemorySortCursor} and {@link FileSortCursor}.
 */
@Tag(Tags.RequiresFDB)
public class SortCursorTests extends FDBRecordStoreTestBase {
    final KeyExpression num2Field = Key.Expressions.field("num_value_2");

    private SortedRecordSerializer<Message> serializer;

    private List<Integer> sortedNums;

    @BeforeEach
    public void setupRecords() throws Exception {
        sortedNums = new ArrayList<>(100);
        Random random = new Random(3456);
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            serializer = new SortedRecordSerializer<>(DynamicMessageRecordSerializer.instance(), recordStore.getRecordMetaData(), recordStore.getTimer());

            for (int i = 0; i < 100; i++) {
                int num = random.nextInt();
                sortedNums.add(num);

                TestRecords1Proto.MySimpleRecord.Builder record = TestRecords1Proto.MySimpleRecord.newBuilder();
                record.setRecNo(i);
                record.setNumValue2(num);
                recordStore.saveRecord(record.build());
            }
            commit(context);
        }
        Collections.sort(sortedNums);
    }

    abstract class MemoryAdapterBase implements MemorySortAdapter<Tuple, FDBStoredRecord<Message>> {
        @Override
        public int compare(Tuple o1, Tuple o2) {
            return o1.compareTo(o2);
        }

        @Nonnull
        @Override
        public Tuple generateKey(FDBStoredRecord<Message> record) {
            return num2Field.evaluateSingleton(record).toTuple();
        }

        @Nonnull
        @Override
        public byte[] serializeKey(final Tuple key) {
            return key.pack();
        }

        @Override
        public boolean isSerializedOrderReversed() {
            return false;
        }

        @Nonnull
        @Override
        public Tuple deserializeKey(@Nonnull final byte[] key) {
            return Tuple.fromBytes(key);
        }

        @Nonnull
        @Override
        public byte[] serializeValue(final FDBStoredRecord<Message> record) {
            return serializer.serialize(record);
        }

        @Nonnull
        @Override
        public FDBStoredRecord<Message> deserializeValue(@Nonnull final byte[] bytes) {
            return serializer.deserialize(bytes).getStoredRecord();
        }

        @Nonnull
        @Override
        public MemorySorter.SizeLimitMode getSizeLimitMode() {
            return MemorySorter.SizeLimitMode.DISCARD;
        }
    }

    @Test
    public void memorySort() throws Exception {
        final Function<byte[], RecordCursor<FDBStoredRecord<Message>>> scanRecords =
                continuation -> recordStore.scanRecords(null, null, EndpointType.TREE_START, EndpointType.TREE_END, continuation, ScanProperties.FORWARD_SCAN);
        final MemoryAdapterBase adapter = new MemoryAdapterBase() {
            @Override
            public int getMaxMapSize() {
                return 20;
            }
        };

        List<Integer> resultNums;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            try (RecordCursor<FDBStoredRecord<Message>> cursor = MemorySortCursor.create(adapter, scanRecords, timer, null)) {
                resultNums = cursor.map(r -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(r.getRecord()).getNumValue2()).asList().get();
            }
        }
        assertEquals(sortedNums.subList(0, 20), resultNums);
    }

    @Test
    public void memorySortContinuations() throws Exception {
        final Function<byte[], RecordCursor<FDBStoredRecord<Message>>> scanRecords =
                continuation -> {
                    final ExecuteProperties executeProperties = ExecuteProperties.newBuilder().setScannedRecordsLimit(20).build();
                    return recordStore.scanRecords(null, null, EndpointType.TREE_START, EndpointType.TREE_END, continuation, new ScanProperties(executeProperties));
                };
        final MemoryAdapterBase adapter = new MemoryAdapterBase() {
            @Override
            public int getMaxMapSize() {
                return 10;
            }
        };

        List<Integer> resultNums = new ArrayList<>();
        byte[] continuation = null;
        int transactionCount = 0;
        
        do {
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context);
                try (RecordCursor<FDBStoredRecord<Message>> cursor = MemorySortCursor.create(adapter, scanRecords, timer, continuation)) {
                    while (true) {
                        RecordCursorResult<FDBStoredRecord<Message>> result = cursor.getNext();
                        if (result.hasNext()) {
                            int num2 = TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(result.get().getRecord()).getNumValue2();
                            resultNums.add(num2);
                        } else {
                            continuation = result.getContinuation().toBytes();
                            break;
                        }
                    }
                }
                transactionCount++;
            }
        } while (continuation != null);
        assertEquals(110, transactionCount);
        assertEquals(sortedNums, resultNums);
    }

    abstract class FileSortAdapterBase extends MemoryAdapterBase implements FileSortAdapter<Tuple, FDBStoredRecord<Message>> {
        @Nonnull
        @Override
        public MemorySorter.SizeLimitMode getSizeLimitMode() {
            return MemorySorter.SizeLimitMode.STOP;
        }

        @Nonnull
        @Override
        public File generateFilename() throws IOException {
            return File.createTempFile("fdb", ".bin");
        }

        @Override
        public void writeValue(@Nonnull final FDBStoredRecord<Message> record, @Nonnull final CodedOutputStream stream) throws IOException {
            serializer.write(record, stream);
        }

        @Override
        public FDBStoredRecord<Message> readValue(@Nonnull final CodedInputStream stream) throws IOException {
            return serializer.read(stream).getStoredRecord();
        }

        @Override
        public boolean isCompressed() {
            return false;
        }

        @Nullable
        @Override
        public java.security.Key getEncryptionKey() {
            return null;
        }

        @Nullable
        @Override
        public SecureRandom getSecureRandom() {
            return null;
        }
    }

    private FileSortAdapterBase fileSortMemoryAdapter() {
        return new FileSortAdapterBase() {
            @Override
            public int getMinFileSize() {
                return 200;
            }

            @Override
            public int getMaxNumFiles() {
                return 5;
            }

            @Override
            public int getRecordsPerSection() {
                return 200;
            }

            @Override
            public int getMaxMapSize() {
                return 250;
            }
        };
    }

    private FileSortAdapterBase fileSortFilesAdapter() {
        return new FileSortAdapterBase() {
            @Override
            public int getMinFileSize() {
                return 10;
            }

            @Override
            public int getMaxNumFiles() {
                return 5;
            }

            @Override
            public int getRecordsPerSection() {
                return 10;
            }

            @Override
            public int getMaxMapSize() {
                return 10;
            }
        };
    }

    private FileSortAdapterBase fileSortEncryptedAdapter() throws Exception {
        final SecureRandom secureRandom = new SecureRandom();
        final KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(128, secureRandom);
        final SecretKey secretKey = keyGen.generateKey();
        return new FileSortAdapterBase() {
            @Override
            public int getMinFileSize() {
                return 10;
            }

            @Override
            public int getMaxNumFiles() {
                return 5;
            }

            @Override
            public int getRecordsPerSection() {
                return 10;
            }

            @Override
            public int getMaxMapSize() {
                return 10;
            }

            @Override
            public boolean isCompressed() {
                return true;
            }

            @Nullable
            @Override
            public java.security.Key getEncryptionKey() {
                return secretKey;
            }

            @Nullable
            @Override
            public SecureRandom getSecureRandom() {
                return secureRandom;
            }
        };
    }

    @Test
    public void fileSortMemory() throws Exception {
        final Function<byte[], RecordCursor<FDBStoredRecord<Message>>> scanRecords =
                continuation -> recordStore.scanRecords(null, null, EndpointType.TREE_START, EndpointType.TREE_END, continuation, ScanProperties.FORWARD_SCAN);
        List<Integer> resultNums;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            try (RecordCursor<FDBStoredRecord<Message>> cursor = FileSortCursor.create(fileSortMemoryAdapter(), scanRecords, timer, null, 0, Integer.MAX_VALUE)) {
                resultNums = cursor.map(r -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(r.getRecord()).getNumValue2()).asList().get();
            }
        }
        assertEquals(sortedNums, resultNums);
    }

    @Test
    public void fileSortFiles() throws Exception {
        final Function<byte[], RecordCursor<FDBStoredRecord<Message>>> scanRecords =
                continuation -> recordStore.scanRecords(null, null, EndpointType.TREE_START, EndpointType.TREE_END, continuation, ScanProperties.FORWARD_SCAN);
        List<Integer> resultNums;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            try (RecordCursor<FDBStoredRecord<Message>> cursor = FileSortCursor.create(fileSortFilesAdapter(), scanRecords, timer, null, 0, Integer.MAX_VALUE)) {
                resultNums = cursor.map(r -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(r.getRecord()).getNumValue2()).asList().get();
            }
        }
        assertEquals(sortedNums, resultNums);
    }

    @Test
    public void fileSortSkip() throws Exception {
        final Function<byte[], RecordCursor<FDBStoredRecord<Message>>> scanRecords =
                continuation -> recordStore.scanRecords(null, null, EndpointType.TREE_START, EndpointType.TREE_END, continuation, ScanProperties.FORWARD_SCAN);
        List<Integer> resultNums;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            try (RecordCursor<FDBStoredRecord<Message>> cursor = FileSortCursor.create(fileSortFilesAdapter(), scanRecords, timer, null, 13, 8)) {
                resultNums = cursor.map(r -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(r.getRecord()).getNumValue2()).asList().get();
            }
        }
        assertEquals(sortedNums.subList(13, 21), resultNums);
    }

    @Test
    public void fileSortEncrypted() throws Exception {
        final Function<byte[], RecordCursor<FDBStoredRecord<Message>>> scanRecords =
                continuation -> recordStore.scanRecords(null, null, EndpointType.TREE_START, EndpointType.TREE_END, continuation, ScanProperties.FORWARD_SCAN);
        List<Integer> resultNums;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            try (RecordCursor<FDBStoredRecord<Message>> cursor = FileSortCursor.create(fileSortEncryptedAdapter(), scanRecords, timer, null, 0, Integer.MAX_VALUE)) {
                resultNums = cursor.map(r -> TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(r.getRecord()).getNumValue2()).asList().get();
            }
        }
        assertEquals(sortedNums, resultNums);
    }

}
