/*
 * TransformedRecordSerializerTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.common;

import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecords1Proto.MySimpleRecord;
import com.apple.foundationdb.record.TestRecords1Proto.RecordTypeUnion;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Strings;
import com.google.common.primitives.Bytes;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.zip.Deflater;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link TransformedRecordSerializer}.
 */
public class TransformedRecordSerializerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransformedRecordSerializerTest.class);
    // Below is sonnet 108 by William Shakespeare, which is text that has been placed in the public domain.
    private static final String SONNET_108 =
            "  What's in the brain that ink may character,\n" +
            "  Which hath not figured to thee my true spirit,\n" +
            "  What's new to speak, what now to register,\n" +
            "  That may express my love, or thy dear merit?\n" +
            "  Nothing sweet boy, but yet like prayers divine,\n" +
            "  I must each day say o'er the very same,\n" +
            "  Counting no old thing old, thou mine, I thine,\n" +
            "  Even as when first I hallowed thy fair name.\n" +
            "  So that eternal love in love's fresh case,\n" +
            "  Weighs not the dust and injury of age,\n" +
            "  Nor gives to necessary wrinkles place,\n" +
            "  But makes antiquity for aye his page,\n" +
            "    Finding the first conceit of love there bred,\n" +
            "    Where time and outward form would show it dead.";
    private static RecordMetaData metaData;
    private StoreTimer storeTimer;

    @BeforeAll
    public static void setUpMetaData() {
        metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
    }

    public TransformedRecordSerializerTest() {
        storeTimer = new StoreTimer();
    }

    @BeforeEach
    public void resetTimer() {
        storeTimer.reset();
    }

    private void logMetrics(@Nonnull String staticMessage, @Nullable Object...keysAndValues) {
        KeyValueLogMessage message = KeyValueLogMessage.build(staticMessage, keysAndValues);
        message.addKeysAndValues(storeTimer.getKeysAndValues());
        LOGGER.info(message.toString());
        resetTimer();
    }

    private byte[] serialize(@Nonnull RecordSerializer<? super MySimpleRecord> serializer, MySimpleRecord record) {
        return serializer.serialize(metaData, metaData.getRecordType("MySimpleRecord"), record, storeTimer);
    }

    private <M extends Message> M deserialize(@Nonnull RecordSerializer<M> serializer, @Nonnull Tuple primaryKey, byte[] serialized) {
        return serializer.deserialize(metaData, primaryKey, serialized, storeTimer);
    }

    @Test
    public void readOldFormat() {
        MySimpleRecord simpleRecord = MySimpleRecord.newBuilder().setRecNo(1066L).setStrValueIndexed("Hello").build();
        RecordTypeUnion unionRecord = RecordTypeUnion.newBuilder().setMySimpleRecord(simpleRecord).build();
        byte[] serialized = serialize(DynamicMessageRecordSerializer.instance(), simpleRecord);
        assertArrayEquals(unionRecord.toByteArray(), serialized);

        // Without anything specified
        TransformedRecordSerializer<Message> serializer = TransformedRecordSerializer.newDefaultBuilder().build();
        Message deserialized = deserialize(serializer, Tuple.from(1066L), serialized);
        assertEquals(simpleRecord, deserialized);

        // With compression
        serializer = TransformedRecordSerializer.newDefaultBuilder().setCompressWhenSerializing(true).build();
        deserialized = deserialize(serializer, Tuple.from(1066L), serialized);
        assertEquals(simpleRecord, deserialized);
    }

    @Test
    public void noTransformations() {
        TransformedRecordSerializer<Message> serializer = TransformedRecordSerializer.newDefaultBuilder().build();

        MySimpleRecord simpleRecord = MySimpleRecord.newBuilder().setRecNo(1066L).setStrValueIndexed("Hello").build();
        RecordTypeUnion unionRecord = RecordTypeUnion.newBuilder().setMySimpleRecord(simpleRecord).build();
        byte[] serialized = serialize(serializer, simpleRecord);
        assertEquals(TransformedRecordSerializer.ENCODING_CLEAR, serialized[0]);
        assertArrayEquals(unionRecord.toByteArray(), Arrays.copyOfRange(serialized, 1, serialized.length));
        Message deserialized = deserialize(serializer, Tuple.from(1066L), serialized);
        assertEquals(simpleRecord, deserialized);

        logMetrics("metrics with no transformations");
    }

    @Test
    public void compressWhenSerializing() {
        TransformedRecordSerializer<Message> serializer = TransformedRecordSerializer.newDefaultBuilder()
                .setCompressWhenSerializing(true)
                .setCompressionLevel(Deflater.HUFFMAN_ONLY)
                .build();

        // There should be no compression actually added for a small record like this
        MySimpleRecord smallRecord = MySimpleRecord.newBuilder().setRecNo(1066L).build();
        RecordTypeUnion smallUnionRecord = RecordTypeUnion.newBuilder().setMySimpleRecord(smallRecord).build();
        byte[] serialized = serialize(serializer, smallRecord);
        assertEquals(TransformedRecordSerializer.ENCODING_CLEAR, serialized[0]);
        assertArrayEquals(smallUnionRecord.toByteArray(), Arrays.copyOfRange(serialized, 1, serialized.length));
        Message deserialized = deserialize(serializer, Tuple.from(1066L), serialized);
        assertEquals(smallRecord, deserialized);

        assertEquals(storeTimer.getCount(RecordSerializer.Counts.ESCHEW_RECORD_COMPRESSION), 1);
        assertEquals(storeTimer.getCount(RecordSerializer.Counts.RECORD_BYTES_BEFORE_COMPRESSION),
                storeTimer.getCount(RecordSerializer.Counts.RECORD_BYTES_AFTER_COMPRESSION));

        logMetrics("metrics with failed compression");

        // There should definitely be compression from a record like this
        MySimpleRecord largeRecord = MySimpleRecord.newBuilder().setRecNo(1066L).setStrValueIndexed(Strings.repeat("foo", 1000)).build();
        final RecordTypeUnion largeUnionRecord = RecordTypeUnion.newBuilder().setMySimpleRecord(largeRecord).build();
        serialized = serialize(serializer, largeRecord);
        assertThat(storeTimer.getCount(RecordSerializer.Counts.RECORD_BYTES_BEFORE_COMPRESSION),
                greaterThan(storeTimer.getCount(RecordSerializer.Counts.RECORD_BYTES_AFTER_COMPRESSION)));
        assertEquals(TransformedRecordSerializer.ENCODING_COMPRESSED, serialized[0]);
        int rawLength = largeUnionRecord.toByteArray().length;
        assertEquals(rawLength, ByteBuffer.wrap(serialized, 2, 4).order(ByteOrder.BIG_ENDIAN).getInt());
        deserialized = deserialize(serializer, Tuple.from(1066L), serialized);
        assertEquals(largeRecord, deserialized);

        logMetrics("metrics with successful compression (repeated foo)",
                "raw_length", rawLength, "compressed_length", serialized.length);

        // There should be a medium amount of compression from this record
        MySimpleRecord mediumRecord = MySimpleRecord.newBuilder().setRecNo(1066L).setStrValueIndexed(SONNET_108).build();
        RecordTypeUnion mediumUnionRecord = RecordTypeUnion.newBuilder().setMySimpleRecord(mediumRecord).build();
        serialized = serialize(serializer, mediumRecord);
        assertEquals(TransformedRecordSerializer.ENCODING_COMPRESSED, serialized[0]);
        rawLength = mediumUnionRecord.toByteArray().length;
        assertEquals(rawLength, ByteBuffer.wrap(serialized, 2, 4).order(ByteOrder.BIG_ENDIAN).getInt());
        deserialized = deserialize(serializer, Tuple.from(1066L), serialized);
        assertEquals(mediumRecord, deserialized);

        logMetrics("metrics with successful compression (sonnet 108)",
                "raw_length", rawLength, "compressed_length", serialized.length);
    }

    @Test
    public void unknownCompressionVersion() {
        RecordSerializationException e = assertThrows(RecordSerializationException.class, () -> {
            TransformedRecordSerializer<Message> serializer = TransformedRecordSerializer.newDefaultBuilder().setCompressWhenSerializing(true).build();
            MySimpleRecord simpleRecord = MySimpleRecord.newBuilder().setRecNo(1066L).setStrValueIndexed(SONNET_108).build();
            byte[] serialized = serialize(serializer, simpleRecord);
            serialized[1] += 1; // Bump the compression version to an unknown value.
            deserialize(serializer, Tuple.from(1066L), serialized);
        });
        assertThat(e.getMessage(), containsString("unknown compression version"));
    }

    @Test
    public void decompressionError() {
        RecordSerializationException e = assertThrows(RecordSerializationException.class, () -> {
            TransformedRecordSerializer<Message> serializer = TransformedRecordSerializer.newDefaultBuilder().setCompressWhenSerializing(true).build();
            MySimpleRecord simpleRecord = MySimpleRecord.newBuilder().setRecNo(1066L).setStrValueIndexed(SONNET_108).build();
            byte[] serialized = serialize(serializer, simpleRecord);
            serialized[serialized.length - 1] += 1; // Corrupt the compressed data
            deserialize(serializer, Tuple.from(1066L), serialized);
        });
        assertThat(e.getMessage(), containsString("decompression error"));
    }

    @Test
    public void buildWithoutSettingEncryption() {
        assertThrows(RecordCoreArgumentException.class, () -> TransformedRecordSerializer.newDefaultBuilder().setEncryptWhenSerializing(true).build());
    }

    @Test
    public void decryptWithoutSettingEncryption() {
        List<Integer> codes = Arrays.asList(TransformedRecordSerializer.ENCODING_ENCRYPTED, TransformedRecordSerializer.ENCODING_ENCRYPTED | TransformedRecordSerializer.ENCODING_COMPRESSED);
        for (int code : codes) {
            RecordSerializationException e = assertThrows(RecordSerializationException.class, () -> {
                TransformedRecordSerializer<Message> serializer = TransformedRecordSerializer.newDefaultBuilder().build();
                byte[] serialized = new byte[10];
                serialized[0] = (byte)code;
                deserialize(serializer, Tuple.from(1066L), serialized);
            });
            assertThat(e.getMessage(), containsString("this serializer cannot decrypt"));
        }
    }

    @Test
    public void unrecognizedEncoding() {
        RecordSerializationException e = assertThrows(RecordSerializationException.class, () -> {
            TransformedRecordSerializer<Message> serializer = TransformedRecordSerializer.newDefaultBuilder().build();
            byte[] serialized = new byte[10];
            serialized[0] = 0x0f;
            deserialize(serializer, Tuple.from(1066L), serialized);
        });
        assertThat(e.getMessage(), containsString("unrecognized transformation encoding"));
        assertEquals(15, e.getLogInfo().get("encoding"));
    }

    @Test
    public void encryptWhenSerializing() throws Exception {
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(128);
        SecretKey key = keyGen.generateKey();
        TransformedRecordSerializer<Message> serializer = TransformedRecordSerializerJCE.newDefaultBuilder()
                .setEncryptWhenSerializing(true)
                .setEncryptionKey(key)
                .build();

        MySimpleRecord mediumRecord = MySimpleRecord.newBuilder().setRecNo(1066L).setStrValueIndexed(SONNET_108).build();
        assertTrue(Bytes.indexOf(mediumRecord.toByteArray(), "brain".getBytes()) >= 0, "should contain clear text");
        byte[] serialized = serialize(serializer, mediumRecord);
        assertEquals(TransformedRecordSerializer.ENCODING_ENCRYPTED, serialized[0]);
        assertFalse(Bytes.indexOf(serialized, "brain".getBytes()) >= 0, "should not contain clear text");
        Message deserialized = deserialize(serializer, Tuple.from(1066L), serialized);
        assertEquals(mediumRecord, deserialized);
    }
}
