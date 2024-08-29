/*
 * TransformedRecordSerializerTest.java
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

package com.apple.foundationdb.record.provider.common;

import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecords1Proto.MySimpleRecord;
import com.apple.foundationdb.record.TestRecords1Proto.RecordTypeUnion;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Strings;
import com.google.common.primitives.Bytes;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
import java.util.stream.Stream;
import java.util.zip.Deflater;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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

    private void logMetrics(@Nonnull String staticMessage, @Nullable Object... keysAndValues) {
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

    private <M extends Message> void validateSerialization(@Nonnull RecordSerializer<M> serializer, M record, byte[] serialized) {
        serializer.validateSerialization(metaData, metaData.getRecordType("MySimpleRecord"), record, serialized, storeTimer);
    }

    @Test
    public void readOldFormat() {
        MySimpleRecord simpleRecord = MySimpleRecord.newBuilder().setRecNo(1066L).setStrValueIndexed("Hello").build();
        RecordTypeUnion unionRecord = RecordTypeUnion.newBuilder().setMySimpleRecord(simpleRecord).build();
        byte[] serialized = serialize(DynamicMessageRecordSerializer.instance(), simpleRecord);
        assertArrayEquals(unionRecord.toByteArray(), serialized);

        // Without anything specified
        TransformedRecordSerializer<Message> serializer = TransformedRecordSerializer.newDefaultBuilder().build();
        validateSerialization(serializer, simpleRecord, serialized);

        // With compression
        serializer = TransformedRecordSerializer.newDefaultBuilder().setCompressWhenSerializing(true).setWriteValidationRatio(1.0).build();
        validateSerialization(serializer, simpleRecord, serialized);
    }

    @Test
    public void noTransformations() {
        TransformedRecordSerializer<Message> serializer = TransformedRecordSerializer.newDefaultBuilder().setWriteValidationRatio(1.0).build();

        MySimpleRecord simpleRecord = MySimpleRecord.newBuilder().setRecNo(1066L).setStrValueIndexed("Hello").build();
        RecordTypeUnion unionRecord = RecordTypeUnion.newBuilder().setMySimpleRecord(simpleRecord).build();
        byte[] serialized = serialize(serializer, simpleRecord);
        assertEquals(TransformedRecordSerializer.ENCODING_CLEAR, serialized[0]);
        assertArrayEquals(unionRecord.toByteArray(), Arrays.copyOfRange(serialized, 1, serialized.length));

        logMetrics("metrics with no transformations");
    }

    static Stream<Arguments> smallRecords() {
        return Stream.of(
                Arguments.of(MySimpleRecord.newBuilder().setRecNo(74545L).build(), Tuple.from(74545L)),
                Arguments.of(MySimpleRecord.newBuilder().setRecNo(1L).build(), Tuple.from(1L))
        );
    }

    @ParameterizedTest
    @MethodSource("smallRecords")
    public void compressSmallRecordWhenSerializing(@Nonnull final MySimpleRecord smallRecord, @Nonnull final Tuple primaryKey) {
        TransformedRecordSerializer<Message> serializer = TransformedRecordSerializer.newDefaultBuilder()
                .setCompressWhenSerializing(true)
                .setCompressionLevel(Deflater.HUFFMAN_ONLY)
                .setWriteValidationRatio(1.0)
                .build();

        // There should be no compression actually added for a small record like this
        RecordTypeUnion smallUnionRecord = RecordTypeUnion.newBuilder().setMySimpleRecord(smallRecord).build();
        byte[] serialized = serialize(serializer, smallRecord);
        assertEquals(TransformedRecordSerializer.ENCODING_CLEAR, serialized[0]);
        assertArrayEquals(smallUnionRecord.toByteArray(), Arrays.copyOfRange(serialized, 1, serialized.length));
        Message deserialized = deserialize(serializer, primaryKey, serialized);
        assertEquals(smallRecord, deserialized);

        assertEquals(storeTimer.getCount(RecordSerializer.Counts.ESCHEW_RECORD_COMPRESSION), 1);
        assertEquals(storeTimer.getCount(RecordSerializer.Counts.RECORD_BYTES_BEFORE_COMPRESSION),
                storeTimer.getCount(RecordSerializer.Counts.RECORD_BYTES_AFTER_COMPRESSION));
    }

    static Stream<Arguments> longRecords() {
        return Stream.of(
                Arguments.of(MySimpleRecord.newBuilder().setRecNo(1066L).setStrValueIndexed(Strings.repeat("foo", 1000)).build(), Tuple.from(1066L)),
                Arguments.of(MySimpleRecord.newBuilder().setRecNo(1066L).setStrValueIndexed(SONNET_108).build(), Tuple.from(1066L))
        );
    }

    @ParameterizedTest
    @MethodSource("longRecords")
    public void compressLongRecordWhenSerializing(@Nonnull final MySimpleRecord longRecord, @Nonnull final Tuple primaryKey) {
        TransformedRecordSerializer<Message> serializer = TransformedRecordSerializer.newDefaultBuilder()
                .setCompressWhenSerializing(true)
                .setCompressionLevel(Deflater.HUFFMAN_ONLY)
                .setWriteValidationRatio(1.0)
                .build();

        // There should definitely be compression from a record like this
        final RecordTypeUnion largeUnionRecord = RecordTypeUnion.newBuilder().setMySimpleRecord(longRecord).build();
        byte[] serialized = serialize(serializer, longRecord);
        assertThat(storeTimer.getCount(RecordSerializer.Counts.RECORD_BYTES_BEFORE_COMPRESSION),
                greaterThan(storeTimer.getCount(RecordSerializer.Counts.RECORD_BYTES_AFTER_COMPRESSION)));
        assertEquals(TransformedRecordSerializer.ENCODING_COMPRESSED, serialized[0]);
        int rawLength = largeUnionRecord.toByteArray().length;
        assertEquals(rawLength, ByteBuffer.wrap(serialized, 2, 4).order(ByteOrder.BIG_ENDIAN).getInt());
        Message deserialized = deserialize(serializer, primaryKey, serialized);
        assertEquals(longRecord, deserialized);

        logMetrics("metrics with successful compression (repeated foo)",
                "raw_length", rawLength, "compressed_length", serialized.length);
    }

    @Test
    public void decompressWithoutAdler() {
        final TransformedRecordSerializer<Message> serializer = TransformedRecordSerializer.newDefaultBuilder()
                .setCompressWhenSerializing(true)
                .setCompressionLevel(Deflater.BEST_COMPRESSION)
                .setWriteValidationRatio(1.0)
                .build();

        final MySimpleRecord simpleRecord = MySimpleRecord.newBuilder().setRecNo(1215L).setStrValueIndexed(SONNET_108).build();
        byte[] serialized = serialize(serializer, simpleRecord);
        assertThat("flag indicating checksum should be present in serialized data", serialized[8] & 1, not(equalTo(0)));
        assertTrue(isCompressed(serialized));

        // A bug meant that we previously weren't including the Adler-32 checksum in compressed data. Simulate
        // that behavior here by compressing the record, without including the checksum.
        // See: https://github.com/FoundationDB/fdb-record-layer/issues/2691
        Deflater deflater = new Deflater(Deflater.BEST_COMPRESSION);
        byte[] withoutAdler = new byte[serialized.length];
        System.arraycopy(serialized, 0, withoutAdler, 0, 6);
        try {
            byte[] underlying = serialize(serializer.untransformed(), simpleRecord);
            deflater.setInput(underlying);
            // Bug was here: this should call deflater.finish() to ensure the checksum is added
            deflater.deflate(withoutAdler, 6, withoutAdler.length - 6, Deflater.SYNC_FLUSH);
            assertEquals(underlying.length, deflater.getBytesRead());
        } finally {
            deflater.end();
        }
        assertThat("flag indicating checksum should be absent in data without checksum", withoutAdler[8] & 1, equalTo(0));

        // The checksum goes at the end, so validate that the data all match except for at the end
        boolean checksumDifferent = false;
        for (int i = 0; i < serialized.length; i++) {
            if (i != 8) {
                if (i < serialized.length - 4) {
                    assertEquals(serialized[i], withoutAdler[i], "byte " + i + " should match with and without checksum");
                } else {
                    checksumDifferent |= (serialized[i] != withoutAdler[i]);
                }
            }
        }
        assertTrue(checksumDifferent, "Checksum footer should differ in different serializations");

        // Data without checksum should still deserialize into the original record
        validateSerialization(serializer, simpleRecord, withoutAdler);
    }

    @Test
    public void unknownCompressionVersion() {
        TransformedRecordSerializer<Message> serializer = TransformedRecordSerializer.newDefaultBuilder()
                .setCompressWhenSerializing(true)
                .build();
        MySimpleRecord simpleRecord = MySimpleRecord.newBuilder().setRecNo(1066L).setStrValueIndexed(SONNET_108).build();
        byte[] serialized = serialize(serializer, simpleRecord);
        assertTrue(isCompressed(serialized));
        serialized[1] += 1; // Bump the compression version to an unknown value.

        RecordSerializationException e = assertThrows(RecordSerializationException.class,
                () -> deserialize(serializer, Tuple.from(1066L), serialized));
        assertThat(e.getMessage(), containsString("unknown compression version"));
        RecordSerializationValidationException validationException = assertThrows(RecordSerializationValidationException.class,
                () -> validateSerialization(serializer, simpleRecord, serialized));
        assertThat(validationException.getMessage(), containsString("cannot deserialize record"));
        assertThat(validationException.getCause().getMessage(), containsString("unknown compression version"));
    }

    @Test
    public void decompressionError() {
        TransformedRecordSerializer<Message> serializer = TransformedRecordSerializer.newDefaultBuilder().setCompressWhenSerializing(true).build();
        MySimpleRecord simpleRecord = MySimpleRecord.newBuilder().setRecNo(1066L).setStrValueIndexed(SONNET_108).build();
        byte[] serialized = serialize(serializer, simpleRecord);
        serialized[serialized.length - 1] += 1; // Corrupt the compressed data

        RecordSerializationException e = assertThrows(RecordSerializationException.class,
                () -> deserialize(serializer, Tuple.from(1066L), serialized));
        assertThat(e.getMessage(), containsString("decompression error"));

        RecordSerializationValidationException validationException = assertThrows(RecordSerializationValidationException.class,
                () -> validateSerialization(serializer, simpleRecord, serialized));
        assertThat(validationException.getMessage(), containsString("cannot deserialize record"));
        assertThat(validationException.getCause().getMessage(), containsString("decompression error"));
    }

    @Test
    public void incorrectUncompressedSize() {
        TransformedRecordSerializer<Message> serializer = TransformedRecordSerializer.newDefaultBuilder()
                .setCompressionLevel(9)
                .setCompressWhenSerializing(true)
                .setWriteValidationRatio(1.0)
                .build();

        final MySimpleRecord simpleRecord = MySimpleRecord.newBuilder()
                .setRecNo(1066L)
                .setStrValueIndexed(SONNET_108)
                .build();
        byte[] serialized = serialize(serializer, simpleRecord);
        assertTrue(isCompressed(serialized));
        int innerSize = ByteBuffer.wrap(serialized, 2, Integer.BYTES).order(ByteOrder.BIG_ENDIAN).getInt();

        RecordSerializer<Message> innerSerializer = serializer.untransformed();
        byte[] innerSerialized = serialize(innerSerializer, simpleRecord);
        assertEquals(innerSerialized.length, innerSize);
        assertThat(innerSerialized.length, greaterThan(serialized.length));

        updateSize(serialized, innerSize * 2);
        RecordSerializationException tooSmallException = assertThrows(RecordSerializationException.class,
                () -> deserialize(serializer, Tuple.from(1066L), serialized));
        assertThat(tooSmallException.getMessage(), containsString("decompressed record too small"));
        assertThat(tooSmallException.getLogInfo(), both(hasEntry(LogMessageKeys.EXPECTED.toString(), (Object) (innerSize * 2))).and(hasEntry(LogMessageKeys.ACTUAL.toString(), innerSize)));

        updateSize(serialized, innerSize / 2);
        RecordSerializationException tooLargeException = assertThrows(RecordSerializationException.class,
                () -> deserialize(serializer, Tuple.from(1066L), serialized));
        assertThat(tooLargeException.getMessage(), containsString("decompressed record too large"));
        assertThat(tooLargeException.getLogInfo(), hasEntry(LogMessageKeys.EXPECTED.toString(), innerSize / 2));
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
                .setWriteValidationRatio(1.0)
                .build();

        MySimpleRecord mediumRecord = MySimpleRecord.newBuilder().setRecNo(1066L).setStrValueIndexed(SONNET_108).build();
        assertTrue(Bytes.indexOf(mediumRecord.toByteArray(), "brain".getBytes()) >= 0, "should contain clear text");
        byte[] serialized = serialize(serializer, mediumRecord);
        assertEquals(TransformedRecordSerializer.ENCODING_ENCRYPTED, serialized[0]);
        assertFalse(Bytes.indexOf(serialized, "brain".getBytes()) >= 0, "should not contain clear text");
        Message deserialized = deserialize(serializer, Tuple.from(1066L), serialized);
        assertEquals(mediumRecord, deserialized);
    }

    @Test
    public void validateWithIncorrectInner() {
        TransformedRecordSerializer<MySimpleRecord> serializer = TransformedRecordSerializer.newBuilder(new ModifyingRecordSerializer())
                .setCompressionLevel(9)
                .setCompressWhenSerializing(true)
                .setWriteValidationRatio(1.0)
                .build();
        final MySimpleRecord simpleRecord = MySimpleRecord.newBuilder().setRecNo(1066L).setNumValue2(42).build();

        RecordSerializationValidationException validationError = assertThrows(RecordSerializationValidationException.class,
                () -> serialize(serializer, simpleRecord));
        assertThat(validationError.getMessage(), containsString("record serialization mismatch"));

        TransformedRecordSerializer<MySimpleRecord> serializer2 = TransformedRecordSerializer.newBuilder(new ModifyingRecordSerializer())
                .setCompressionLevel(2)
                .setCompressWhenSerializing(true)
                .setWriteValidationRatio(0.0)
                .build();
        byte[] serialized = serialize(serializer2, simpleRecord);
        assertThrows(RecordSerializationValidationException.class,
                () -> validateSerialization(serializer, simpleRecord, serialized));
        assertThat(validationError.getMessage(), containsString("record serialization mismatch"));

        validateSerialization(serializer, simpleRecord.toBuilder().setNumValue2(43).build(), serialized);
    }

    /**
     * Validate that we catch 1 bit flips in compression.
     */
    @Test
    public void corruptAnyBit() {
        final TransformedRecordSerializer<Message> serializer = TransformedRecordSerializer.newDefaultBuilder()
                .setCompressWhenSerializing(true)
                .setCompressionLevel(9)
                .setWriteValidationRatio(1.0)
                .build();
        final MySimpleRecord simpleRecord = MySimpleRecord.newBuilder()
                .setRecNo(1415L)
                .setStrValueIndexed(SONNET_108 + "\n" + SONNET_108)
                .build();
        byte[] serialized = serialize(serializer, simpleRecord);
        assertTrue(isCompressed(serialized));

        // Serializer inserts: 1 header + 1 version + 4 length bytes
        // Compressed blob begin at position 6
        for (int i = 6; i < serialized.length; i++) {
            for (int b = 0; b < Byte.SIZE; b++) {
                byte mask = (byte) (1 << b);
                serialized[i] ^= mask;

                try {
                    // If validateSerialization passes, then flipping the bit does not change
                    // the underlying result, so do not flag this as a problem
                    validateSerialization(serializer, simpleRecord, serialized);
                } catch (RecordSerializationValidationException err) {
                    assertNotNull(err.getCause());
                    assertThat(err.getCause(), instanceOf(RecordSerializationException.class));
                    assertThat(err.getCause().getMessage(), either(containsString("decompression error"))
                            .or(containsString("decompressed record too small"))
                            .or(containsString("decompressed record too large"))
                    );
                }

                serialized[i] ^= mask;
            }
        }
    }

    private boolean isCompressed(byte[] serialized) {
        byte headerByte = serialized[0];
        return (headerByte & TransformedRecordSerializer.ENCODING_PROTO_MESSAGE_FIELD) == 0
                && (headerByte & TransformedRecordSerializer.ENCODING_COMPRESSED) != 0;
    }

    private int getUncompressedSize(byte[] serialized) {
        return ByteBuffer.wrap(serialized, 2, Integer.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .getInt();
    }

    private void updateSize(byte[] serialized, int newSize) {
        ByteBuffer.wrap(serialized, 2, Integer.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(newSize);
    }

    /**
     * Test {@link RecordSerializer} that modifies the contents of the record during serialization. This allows
     * us to test that we catch faulty implementations that modify records if the {@link TransformedRecordSerializer#writeValidationRatio}
     * is configured to check for writes that mishandle.
     */
    private static class ModifyingRecordSerializer implements RecordSerializer<MySimpleRecord> {
        @Nonnull
        private static TypedRecordSerializer<MySimpleRecord, TestRecords1Proto.RecordTypeUnion, TestRecords1Proto.RecordTypeUnion.Builder> underlying = new TypedRecordSerializer<>(
                TestRecords1Proto.RecordTypeUnion.getDescriptor().findFieldByName("_MySimpleRecord"),
                RecordTypeUnion::newBuilder,
                RecordTypeUnion::hasMySimpleRecord,
                RecordTypeUnion::getMySimpleRecord,
                RecordTypeUnion.Builder::setMySimpleRecord
        );

        public ModifyingRecordSerializer() {
        }

        @Nonnull
        @Override
        public byte[] serialize(@Nonnull final RecordMetaData metaData, @Nonnull final RecordType recordType, @Nonnull final MySimpleRecord rec, @Nullable final StoreTimer timer) {
            MySimpleRecord modified = rec.toBuilder().setNumValue2(rec.getNumValue2() + 1).build();
            return underlying.serialize(metaData, recordType, modified, timer);
        }

        @Nonnull
        @Override
        public MySimpleRecord deserialize(@Nonnull final RecordMetaData metaData, @Nonnull final Tuple primaryKey, @Nonnull final byte[] serialized, @Nullable final StoreTimer timer) {
            return underlying.deserialize(metaData, primaryKey, serialized, timer);
        }

        @Nonnull
        @Override
        public RecordSerializer<Message> widen() {
            throw new UnsupportedOperationException("cannot widen this serializer");
        }
    }
}
