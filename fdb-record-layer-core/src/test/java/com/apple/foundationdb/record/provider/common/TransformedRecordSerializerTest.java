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
import com.apple.test.BooleanSource;
import com.apple.test.ParameterizedTestUtils;
import com.apple.test.RandomSeedSource;
import com.apple.test.RandomizedTestUtils;
import com.google.common.base.Strings;
import com.google.common.collect.Streams;
import com.google.common.primitives.Bytes;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
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
    public static final long PRIMARY_KEY_REC_NO = 1066;
    public static final Tuple PRIMARY_KEY = Tuple.from(PRIMARY_KEY_REC_NO);
    private static RecordMetaData metaData;
    private final StoreTimer storeTimer;

    @BeforeAll
    static void setUpMetaData() {
        metaData = RecordMetaData.build(TestRecords1Proto.getDescriptor());
    }

    public TransformedRecordSerializerTest() {
        storeTimer = new StoreTimer();
    }

    @BeforeEach
    void resetTimer() {
        storeTimer.reset();
    }

    private void logMetrics(@Nonnull String staticMessage, @Nullable Object... keysAndValues) {
        KeyValueLogMessage message = KeyValueLogMessage.build(staticMessage, keysAndValues);
        message.addKeysAndValues(storeTimer.getKeysAndValues());
        LOGGER.info(message.toString());
        resetTimer();
    }

    private byte[] serialize(@Nonnull RecordSerializer<? super MySimpleRecord> serializer, MySimpleRecord rec) {
        return serializer.serialize(metaData, metaData.getRecordType("MySimpleRecord"), rec, storeTimer);
    }

    private <M extends Message> M deserialize(@Nonnull RecordSerializer<M> serializer, @Nonnull Tuple primaryKey, byte[] serialized) {
        return serializer.deserialize(metaData, primaryKey, serialized, storeTimer);
    }

    private <M extends Message> void validateSerialization(@Nonnull RecordSerializer<M> serializer, M rec, byte[] serialized) {
        serializer.validateSerialization(metaData, metaData.getRecordType("MySimpleRecord"), rec, serialized, storeTimer);
    }

    @Test
    void readOldFormat() {
        MySimpleRecord simpleRecord = MySimpleRecord.newBuilder().setRecNo(PRIMARY_KEY_REC_NO).setStrValueIndexed("Hello").build();
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
    void noTransformations() {
        TransformedRecordSerializer<Message> serializer = TransformedRecordSerializer.newDefaultBuilder().setWriteValidationRatio(1.0).build();

        MySimpleRecord simpleRecord = MySimpleRecord.newBuilder().setRecNo(PRIMARY_KEY_REC_NO).setStrValueIndexed("Hello").build();
        RecordTypeUnion unionRecord = RecordTypeUnion.newBuilder().setMySimpleRecord(simpleRecord).build();
        byte[] serialized = serialize(serializer, simpleRecord);
        assertEquals(TransformedRecordSerializerPrefix.PREFIX_CLEAR, serialized[0]);
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
    void compressSmallRecordWhenSerializing(@Nonnull final MySimpleRecord smallRecord, @Nonnull final Tuple primaryKey) {
        TransformedRecordSerializer<Message> serializer = TransformedRecordSerializer.newDefaultBuilder()
                .setCompressWhenSerializing(true)
                .setCompressionLevel(Deflater.HUFFMAN_ONLY)
                .setWriteValidationRatio(1.0)
                .build();

        // There should be no compression actually added for a small record like this
        RecordTypeUnion smallUnionRecord = RecordTypeUnion.newBuilder().setMySimpleRecord(smallRecord).build();
        byte[] serialized = serialize(serializer, smallRecord);
        assertEquals(TransformedRecordSerializerPrefix.PREFIX_CLEAR, serialized[0]);
        assertArrayEquals(smallUnionRecord.toByteArray(), Arrays.copyOfRange(serialized, 1, serialized.length));
        Message deserialized = deserialize(serializer, primaryKey, serialized);
        assertEquals(smallRecord, deserialized);

        assertEquals(1, storeTimer.getCount(RecordSerializer.Counts.ESCHEW_RECORD_COMPRESSION));
        assertEquals(storeTimer.getCount(RecordSerializer.Counts.RECORD_BYTES_BEFORE_COMPRESSION),
                storeTimer.getCount(RecordSerializer.Counts.RECORD_BYTES_AFTER_COMPRESSION));
    }

    static Stream<Arguments> longRecords() {
        return Stream.of(
                Arguments.of(MySimpleRecord.newBuilder().setRecNo(PRIMARY_KEY_REC_NO).setStrValueIndexed(Strings.repeat("foo", 1000)).build(), PRIMARY_KEY),
                Arguments.of(MySimpleRecord.newBuilder().setRecNo(PRIMARY_KEY_REC_NO).setStrValueIndexed(SONNET_108).build(), PRIMARY_KEY)
        );
    }

    @ParameterizedTest
    @MethodSource("longRecords")
    void compressLongRecordWhenSerializing(@Nonnull final MySimpleRecord longRecord, @Nonnull final Tuple primaryKey) {
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
        assertEquals(TransformedRecordSerializerPrefix.PREFIX_COMPRESSED, serialized[0]);
        int rawLength = largeUnionRecord.toByteArray().length;
        assertEquals(rawLength, getUncompressedSize(serialized));
        Message deserialized = deserialize(serializer, primaryKey, serialized);
        assertEquals(longRecord, deserialized);

        logMetrics("metrics with successful compression (repeated foo)",
                "raw_length", rawLength, "compressed_length", serialized.length);
    }

    @Test
    void decompressWithoutAdler() {
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
    void unknownCompressionVersion() {
        TransformedRecordSerializer<Message> serializer = TransformedRecordSerializer.newDefaultBuilder()
                .setCompressWhenSerializing(true)
                .build();
        MySimpleRecord simpleRecord = MySimpleRecord.newBuilder().setRecNo(PRIMARY_KEY_REC_NO).setStrValueIndexed(SONNET_108).build();
        byte[] serialized = serialize(serializer, simpleRecord);
        assertTrue(isCompressed(serialized));
        serialized[1] += 1; // Bump the compression version to an unknown value.

        RecordSerializationException e = assertThrows(RecordSerializationException.class,
                () -> deserialize(serializer, PRIMARY_KEY, serialized));
        assertThat(e.getMessage(), containsString("unknown compression version"));
        RecordSerializationValidationException validationException = assertThrows(RecordSerializationValidationException.class,
                () -> validateSerialization(serializer, simpleRecord, serialized));
        assertThat(validationException.getMessage(), containsString("cannot deserialize record"));
        assertThat(validationException.getCause().getMessage(), containsString("unknown compression version"));
    }

    @Test
    void decompressionError() {
        TransformedRecordSerializer<Message> serializer = TransformedRecordSerializer.newDefaultBuilder().setCompressWhenSerializing(true).build();
        MySimpleRecord simpleRecord = MySimpleRecord.newBuilder().setRecNo(PRIMARY_KEY_REC_NO).setStrValueIndexed(SONNET_108).build();
        byte[] serialized = serialize(serializer, simpleRecord);
        serialized[serialized.length - 1] += 1; // Corrupt the compressed data

        RecordSerializationException e = assertThrows(RecordSerializationException.class,
                () -> deserialize(serializer, PRIMARY_KEY, serialized));
        assertThat(e.getMessage(), containsString("decompression error"));

        RecordSerializationValidationException validationException = assertThrows(RecordSerializationValidationException.class,
                () -> validateSerialization(serializer, simpleRecord, serialized));
        assertThat(validationException.getMessage(), containsString("cannot deserialize record"));
        assertThat(validationException.getCause().getMessage(), containsString("decompression error"));
    }

    @Test
    void incorrectUncompressedSize() {
        TransformedRecordSerializer<Message> serializer = TransformedRecordSerializer.newDefaultBuilder()
                .setCompressionLevel(9)
                .setCompressWhenSerializing(true)
                .setWriteValidationRatio(1.0)
                .build();

        final MySimpleRecord simpleRecord = MySimpleRecord.newBuilder()
                .setRecNo(PRIMARY_KEY_REC_NO)
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
                () -> deserialize(serializer, PRIMARY_KEY, serialized));
        assertThat(tooSmallException.getMessage(), containsString("decompressed record too small"));
        assertThat(tooSmallException.getLogInfo(), both(hasEntry(LogMessageKeys.EXPECTED.toString(), (Object) (innerSize * 2))).and(hasEntry(LogMessageKeys.ACTUAL.toString(), innerSize)));

        updateSize(serialized, innerSize / 2);
        RecordSerializationException tooLargeException = assertThrows(RecordSerializationException.class,
                () -> deserialize(serializer, PRIMARY_KEY, serialized));
        assertThat(tooLargeException.getMessage(), containsString("decompressed record too large"));
        assertThat(tooLargeException.getLogInfo(), hasEntry(LogMessageKeys.EXPECTED.toString(), innerSize / 2));
    }

    @Test
    void buildWithoutSettingEncryption() {
        final TransformedRecordSerializer.Builder<Message> builder = TransformedRecordSerializer.newDefaultBuilder().setEncryptWhenSerializing(true);
        assertThrows(RecordCoreArgumentException.class, builder::build);
    }

    @Test
    void decryptWithoutSettingEncryption() {
        final List<Integer> codes = Arrays.asList(TransformedRecordSerializerPrefix.PREFIX_ENCRYPTED, TransformedRecordSerializerPrefix.PREFIX_ENCRYPTED | TransformedRecordSerializerPrefix.PREFIX_COMPRESSED);
        final TransformedRecordSerializer<Message> serializer = TransformedRecordSerializer.newDefaultBuilder().build();
        for (int code : codes) {
            byte[] serialized = new byte[10];
            serialized[0] = (byte)code;
            RecordSerializationException e = assertThrows(RecordSerializationException.class,
                    () -> deserialize(serializer, PRIMARY_KEY, serialized));
            assertThat(e.getMessage(), containsString("this serializer cannot decrypt"));
        }
    }

    @Test
    void unrecognizedEncoding() {
        final TransformedRecordSerializer<Message> serializer = TransformedRecordSerializer.newDefaultBuilder().build();
        byte[] serialized = new byte[10];
        serialized[0] = 0x0f;
        RecordSerializationException e = assertThrows(RecordSerializationException.class, () -> deserialize(serializer, PRIMARY_KEY, serialized));
        assertThat(e.getMessage(), containsString("unrecognized transformation encoding"));
        assertEquals(15L, e.getLogInfo().get("encoding"));
    }

    @ParameterizedTest
    @BooleanSource
    void encryptWhenSerializing(boolean compressToo) throws Exception {
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(128);
        SecretKey key = keyGen.generateKey();
        TransformedRecordSerializer<Message> serializer = TransformedRecordSerializerJCE.newDefaultBuilder()
                .setEncryptWhenSerializing(true)
                .setEncryptionKey(key)
                .setCompressWhenSerializing(compressToo)
                .setCompressionLevel(9)
                .setWriteValidationRatio(1.0)
                .build();

        MySimpleRecord mediumRecord = MySimpleRecord.newBuilder().setRecNo(PRIMARY_KEY_REC_NO).setStrValueIndexed(SONNET_108).build();
        assertTrue(Bytes.indexOf(mediumRecord.toByteArray(), "brain".getBytes()) >= 0, "should contain clear text");
        byte[] serialized = serialize(serializer, mediumRecord);
        assertEquals(compressToo ? TransformedRecordSerializerPrefix.PREFIX_COMPRESSED_THEN_ENCRYPTED
                                 : TransformedRecordSerializerPrefix.PREFIX_ENCRYPTED,
                serialized[0]);
        assertFalse(Bytes.indexOf(serialized, "brain".getBytes()) >= 0, "should not contain clear text");
        Message deserialized = deserialize(serializer, PRIMARY_KEY, serialized);
        assertEquals(mediumRecord, deserialized);
    }

    @Test
    void validateWithIncorrectInner() {
        TransformedRecordSerializer<MySimpleRecord> serializer = TransformedRecordSerializer.newBuilder(new ModifyingRecordSerializer())
                .setCompressionLevel(9)
                .setCompressWhenSerializing(true)
                .setWriteValidationRatio(1.0)
                .build();
        final MySimpleRecord simpleRecord = MySimpleRecord.newBuilder().setRecNo(PRIMARY_KEY_REC_NO).setNumValue2(42).build();

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
    void corruptAnyBit() {
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

    @ParameterizedTest
    @RandomSeedSource
    void varintEncoding(long seed) {
        final Random random = new Random(seed);
        for (int pass = 0; pass < 100; pass++) {
            final long varint = random.nextLong();
            final byte[] protobuf = MySimpleRecord.newBuilder().setRecNo(varint).build().toByteArray();
            assertEquals(MySimpleRecord.REC_NO_FIELD_NUMBER << 3, protobuf[0]);
            final byte[] protovar = Arrays.copyOfRange(protobuf, 1, protobuf.length);
            final int length = protovar.length;
            assertEquals(length, TransformedRecordSerializerPrefix.varintSize(varint));
            final byte[] encoded = new byte[length];
            final int encodedLength = TransformedRecordSerializerPrefix.writeVarint(encoded, varint);
            assertEquals(length, encodedLength);
            assertArrayEquals(protovar, encoded);
            final TransformedRecordSerializerState state = new TransformedRecordSerializerState(protovar);
            final long decoded = TransformedRecordSerializerPrefix.readVarint(state, PRIMARY_KEY);
            assertEquals(varint, decoded);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {6, 10})
    void malformedVarintEncoding(int length) {
        final TransformedRecordSerializer<Message> serializer = TransformedRecordSerializer.newDefaultBuilder().build();
        byte[] serialized = new byte[length];
        Arrays.fill(serialized, (byte)0xFF);
        RecordSerializationException e = assertThrows(RecordSerializationException.class, () -> deserialize(serializer, PRIMARY_KEY, serialized));
        assertThat(e.getMessage(), containsString(length > 64 / 7 ? "transformation prefix too long"
                                                                  : "transformation prefix malformed"));
    }

    @Test
    void invalidKeyNumberEncoding() {
        final TransformedRecordSerializer<Message> serializer = TransformedRecordSerializer.newDefaultBuilder().build();
        byte[] serialized = new byte[10];
        TransformedRecordSerializerPrefix.writeVarint(serialized,
                TransformedRecordSerializerPrefix.PREFIX_ENCRYPTED + ((long)Integer.MAX_VALUE + 1 << 3));
        RecordSerializationException e = assertThrows(RecordSerializationException.class, () -> deserialize(serializer, PRIMARY_KEY, serialized));
        assertThat(e.getMessage(), containsString("unrecognized transformation encoding"));
    }

    @SuppressWarnings("UnstableApiUsage")
    public static Stream<Arguments> randomAndCompressed() {
        return Streams.zip(
                RandomizedTestUtils.randomSeeds(0xC0DE6EEDL, 0x6EEDC0DEL),
                Stream.of(Boolean.TRUE, Boolean.FALSE),
                Arguments::arguments);
    }

    @ParameterizedTest
    @MethodSource("randomAndCompressed")
    void encryptRollingKeys(long seed, boolean compressToo) throws Exception {
        RollingTestKeyManager keyManager = new RollingTestKeyManager(seed);
        TransformedRecordSerializer<Message> serializer = TransformedRecordSerializerJCE.newDefaultBuilder()
                .setEncryptWhenSerializing(true)
                .setKeyManager(keyManager)
                .setCompressWhenSerializing(compressToo)
                .setWriteValidationRatio(1.0)
                .build();

        List<MySimpleRecord> records = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            records.add(MySimpleRecord.newBuilder()
                        .setRecNo(1000 + i)
                        .setNumValue2(i)
                        .setStrValueIndexed(SONNET_108)
                        .build());
        }

        List<byte[]> serialized = new ArrayList<>();
        for (MySimpleRecord rec : records) {
            serialized.add(serialize(serializer, rec));
        }

        assertThat(keyManager.numberOfKeys(), greaterThan(5));

        List<Message> deserialized = new ArrayList<>();
        for (int i = 0; i < serialized.size(); i++) {
            deserialized.add(deserialize(serializer, Tuple.from(1000L + i), serialized.get(i)));
        }

        assertEquals(records, deserialized);
    }

    @Test
    void cannotDecryptUnknownKey() throws Exception {
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(128);
        SecretKey key = keyGen.generateKey();
        SecureRandom random = new SecureRandom();
        TransformedRecordSerializer<Message> serializer = TransformedRecordSerializerJCE.newDefaultBuilder()
                .setEncryptWhenSerializing(true)
                .setKeyManager(new SerializationKeyManager() {
                    @Override
                    public int getSerializationKey() {
                        return 2;
                    }

                    @Override
                    public Key getKey(final int keyNumber) {
                        return key;
                    }

                    @Override
                    public String getCipher(final int keyNumber) {
                        return CipherPool.DEFAULT_CIPHER;
                    }

                    @Override
                    public Random getRandom(final int keyNumber) {
                        return random;
                    }
                })
                .setWriteValidationRatio(1.0)
                .build();

        MySimpleRecord simpleRecord = MySimpleRecord.newBuilder().setRecNo(PRIMARY_KEY_REC_NO).setStrValueIndexed("Hello").build();
        byte[] serialized = serialize(serializer, simpleRecord);
        TransformedRecordSerializer<Message> deserializer = TransformedRecordSerializerJCE.newDefaultBuilder()
                .setEncryptionKey(key)
                .build();
        RecordSerializationException e = assertThrows(RecordSerializationException.class,
                () -> deserialize(deserializer, PRIMARY_KEY, serialized));
        assertThat(e.getMessage(), containsString("only provide key number 0"));
    }

    @ParameterizedTest
    @BooleanSource
    void cannotDecryptWithoutKey(boolean jce) throws Exception {
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(128);
        TransformedRecordSerializer<Message> serializer = TransformedRecordSerializerJCE.newDefaultBuilder()
                .setEncryptWhenSerializing(true)
                .setEncryptionKey(keyGen.generateKey())
                .setWriteValidationRatio(1.0)
                .build();
        MySimpleRecord simpleRecord = MySimpleRecord.newBuilder().setRecNo(PRIMARY_KEY_REC_NO).setStrValueIndexed("Hello").build();
        byte[] serialized = serialize(serializer, simpleRecord);
        TransformedRecordSerializer<Message> deserializer;
        if (jce) {
            deserializer = TransformedRecordSerializerJCE.newDefaultBuilder()
                .setWriteValidationRatio(1.0)
                .build();
        } else {
            deserializer = TransformedRecordSerializer.newDefaultBuilder()
                .setWriteValidationRatio(1.0)
                .build();
        }
        RecordSerializationException e = assertThrows(RecordSerializationException.class,
                () -> deserialize(deserializer, PRIMARY_KEY, serialized));
        assertThat(e.getMessage(), containsString(jce ? "missing encryption key or provider during decryption"
                                                      : "this serializer cannot decrypt"));
    }

    @Test
    void cannotEncryptAfterClearKey() throws Exception {
        RollingTestKeyManager keyManager = new RollingTestKeyManager(0);
        TransformedRecordSerializerJCE.Builder<Message> builder = TransformedRecordSerializerJCE.newDefaultBuilder()
                .setEncryptWhenSerializing(true)
                .setKeyManager(keyManager);
        builder.clearKeyManager();
        RecordCoreArgumentException e = assertThrows(RecordCoreArgumentException.class, builder::build);
        assertThat(e.getMessage(), containsString("cannot encrypt when serializing if encryption key is not set"));
    }

    @Test
    void keyDoesNotMatchAlgorithm() throws Exception {
        KeyGenerator keyGen = KeyGenerator.getInstance("DES");
        keyGen.init(56);
        try {
            TransformedRecordSerializer<Message> serializer = TransformedRecordSerializerJCE.newDefaultBuilder()
                    .setEncryptWhenSerializing(true)
                    .setEncryptionKey(keyGen.generateKey())
                    .setWriteValidationRatio(1.0)
                    .build();
            MySimpleRecord simpleRecord = MySimpleRecord.newBuilder().setRecNo(PRIMARY_KEY_REC_NO).setStrValueIndexed("Hello").build();
            RecordSerializationException e = assertThrows(RecordSerializationException.class,
                    () -> serialize(serializer, simpleRecord));
            assertThat(e.getMessage(), containsString("encryption error"));
            assertThat(e.getCause(), instanceOf(InvalidKeyException.class));
            assertThat(e.getCause().getMessage(), containsString("Wrong algorithm"));
        } finally {
            // We have put something inconsistent in.
            CipherPool.invalidateAll();
        }
    }

    @Test
    void changeAlgorithm() throws Exception {
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(128);
        TransformedRecordSerializer<Message> serializer = TransformedRecordSerializerJCE.newDefaultBuilder()
                .setEncryptWhenSerializing(true)
                .setEncryptionKey(keyGen.generateKey())
                .setWriteValidationRatio(1.0)
                .build();
        MySimpleRecord simpleRecord = MySimpleRecord.newBuilder().setRecNo(PRIMARY_KEY_REC_NO).setStrValueIndexed("Hello").build();
        byte[] serialized = serialize(serializer, simpleRecord);
        KeyGenerator keyGen2 = KeyGenerator.getInstance("DES");
        keyGen2.init(56);
        TransformedRecordSerializer<Message> deserializer = TransformedRecordSerializerJCE.newDefaultBuilder()
                .setEncryptWhenSerializing(true)
                 .setCipherName("DES")
                .setEncryptionKey(keyGen2.generateKey())
                .setWriteValidationRatio(1.0)
                .build();
        RecordSerializationException e = assertThrows(RecordSerializationException.class,
                () -> deserialize(deserializer, PRIMARY_KEY, serialized));
        assertThat(e.getMessage(), containsString("decryption error"));
    }

    public static Stream<Arguments> compressedAndOrEncrypted() {
        return ParameterizedTestUtils.cartesianProduct(
                ParameterizedTestUtils.booleans("compressed"),
                ParameterizedTestUtils.booleans("encrypted"));
    }

    @ParameterizedTest
    @MethodSource("compressedAndOrEncrypted")
    void typed(boolean compressed, boolean encrypted) throws Exception {
        RecordSerializer<MySimpleRecord> typedSerializer = new TypedRecordSerializer<>(
                TestRecords1Proto.RecordTypeUnion.getDescriptor().findFieldByNumber(TestRecords1Proto.RecordTypeUnion._MYSIMPLERECORD_FIELD_NUMBER),
                TestRecords1Proto.RecordTypeUnion::newBuilder,
                TestRecords1Proto.RecordTypeUnion::hasMySimpleRecord,
                TestRecords1Proto.RecordTypeUnion::getMySimpleRecord,
                TestRecords1Proto.RecordTypeUnion.Builder::setMySimpleRecord);
        MySimpleRecord rec = MySimpleRecord.newBuilder().setRecNo(PRIMARY_KEY_REC_NO).setStrValueIndexed(SONNET_108).build();

        if (encrypted) {
            KeyGenerator keyGen = KeyGenerator.getInstance("AES");
            keyGen.init(128);
            SecretKey key = keyGen.generateKey();
            typedSerializer = TransformedRecordSerializerJCE.newBuilder(typedSerializer)
                .setEncryptWhenSerializing(true)
                .setEncryptionKey(key)
                .setCompressWhenSerializing(compressed)
                .setWriteValidationRatio(1.0)
                .build();
        } else if (compressed) {
            typedSerializer = TransformedRecordSerializer.newBuilder(typedSerializer)
                .setCompressWhenSerializing(true)
                .setWriteValidationRatio(1.0)
                .build();
        }

        byte[] typedSerialized = serialize(typedSerializer, rec);
        RecordSerializer<Message> untypedSerializer = typedSerializer.widen();
        byte[] untypedSerialized = serialize(untypedSerializer, rec);

        MySimpleRecord typedDeserialized = deserialize(typedSerializer, PRIMARY_KEY, typedSerialized);
        assertEquals(rec, typedDeserialized);
        typedDeserialized = deserialize(typedSerializer, PRIMARY_KEY, untypedSerialized);
        assertEquals(rec, typedDeserialized);

        Message untypedDeserialized = deserialize(untypedSerializer, PRIMARY_KEY, typedSerialized);
        assertEquals(rec, untypedDeserialized);
        untypedDeserialized = deserialize(untypedSerializer, PRIMARY_KEY, untypedSerialized);
        assertEquals(rec, untypedDeserialized);
    }

    @Test
    void defaultKeyManagerKey() throws Exception {
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(128);
        TransformedRecordSerializerJCE<Message> serializer = TransformedRecordSerializerJCE.newDefaultBuilder()
                .setEncryptWhenSerializing(true)
                .setEncryptionKey(keyGen.generateKey())
                .setWriteValidationRatio(1.0)
                .build();
        SerializationKeyManager keyManager = serializer.keyManager;
        assertNotNull(keyManager);
        assertEquals(0, keyManager.getSerializationKey());

        RecordSerializationException e = assertThrows(RecordSerializationException.class,
                () -> keyManager.getKey(1));
        assertThat(e.getMessage(), containsString("only provide key number 0"));

        e = assertThrows(RecordSerializationException.class,
                () -> keyManager.getCipher(1));
        assertThat(e.getMessage(), containsString("only provide key number 0"));

        e = assertThrows(RecordSerializationException.class,
                () -> keyManager.getRandom(1));
        assertThat(e.getMessage(), containsString("only provide key number 0"));
    }

    @Test
    void invalidKeyManagerBuilder() throws Exception {
        TransformedRecordSerializerJCE.Builder<Message> builder = TransformedRecordSerializerJCE.newDefaultBuilder();
        builder.setEncryptWhenSerializing(true);

        RecordCoreArgumentException e = assertThrows(RecordCoreArgumentException.class, builder::build);
        assertThat(e.getMessage(), containsString("cannot encrypt when serializing if encryption key is not set"));

        RollingTestKeyManager keyManager = new RollingTestKeyManager(0);
        builder.setKeyManager(keyManager);

        builder.setCipherName(CipherPool.DEFAULT_CIPHER);
        e = assertThrows(RecordCoreArgumentException.class, builder::build);
        assertThat(e.getMessage(), containsString("cannot specify both key manager and cipher name"));

        builder.clearEncryption();
        builder.setEncryptionKey(keyManager.getKey(keyManager.getSerializationKey()));
        e = assertThrows(RecordCoreArgumentException.class, builder::build);
        assertThat(e.getMessage(), containsString("cannot specify both key manager and encryption key"));
    }

    private boolean isCompressed(byte[] serialized) {
        byte headerByte = serialized[0];
        return headerByte == TransformedRecordSerializerPrefix.PREFIX_COMPRESSED ||
                headerByte == TransformedRecordSerializerPrefix.PREFIX_COMPRESSED_THEN_ENCRYPTED;
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
        private static final TypedRecordSerializer<MySimpleRecord, TestRecords1Proto.RecordTypeUnion, TestRecords1Proto.RecordTypeUnion.Builder> underlying = new TypedRecordSerializer<>(
                TestRecords1Proto.RecordTypeUnion.getDescriptor().findFieldByName("_MySimpleRecord"),
                RecordTypeUnion::newBuilder,
                RecordTypeUnion::hasMySimpleRecord,
                RecordTypeUnion::getMySimpleRecord,
                RecordTypeUnion.Builder::setMySimpleRecord
        );

        public ModifyingRecordSerializer() {
            /* handled by parent */
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
