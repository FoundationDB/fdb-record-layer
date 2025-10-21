/*
 * LuceneCompressionTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.lucene.directory.LuceneSerializer;
import com.apple.foundationdb.record.provider.common.FixedZeroKeyManager;
import com.apple.foundationdb.record.provider.common.SerializationKeyManager;
import com.apple.foundationdb.record.util.RandomSecretUtil;
import com.apple.foundationdb.record.util.RandomUtil;
import com.apple.test.ParameterizedTestUtils;
import com.apple.test.RandomizedTestUtils;
import com.apple.test.Tags;
import com.google.common.collect.Streams;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.crypto.SecretKey;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

/**
 * Test for Lucene data compression/decompression and encryption/decryption validation.
 */
@Tag(Tags.RequiresFDB)
class LuceneSerializerTest {
    @Test
    void testEncodingWithoutCompression() throws Exception {
        final LuceneSerializer serializer = getSerializer(false, true, false, null);
        final ByteString content = RandomUtil.randomByteString(ThreadLocalRandom.current(), 100);
        final LuceneFileSystemProto.LuceneFileReference reference = LuceneFileSystemProto.LuceneFileReference.newBuilder()
                .setId(1)
                .setSize(20L)
                .setBlockSize(10L)
                .setContent(content)
                .build();
        final byte[] originalValue = reference.toByteArray();
        final byte[] encodedValue = serializer.encode(originalValue);
        final byte[] decodedValue = serializer.decode(encodedValue);

        final byte[] expectedEncodedValue = new byte[originalValue.length + 1];
        System.arraycopy(originalValue, 0, expectedEncodedValue, 1, originalValue.length);
        // The encoded data is just original data with an encoding byte at the beginning
        // Compression is skipped because it doesn't help to reduce the size
        Assertions.assertArrayEquals(expectedEncodedValue, encodedValue);
        // Decoded value is expected to be equal to the original one
        Assertions.assertArrayEquals(originalValue, decodedValue);

        final LuceneFileSystemProto.LuceneFileReference decompressedReference = LuceneFileSystemProto.LuceneFileReference.parseFrom(decodedValue);
        // The deserialized file reference is equal to the original one
        Assertions.assertEquals(1, decompressedReference.getId());
        Assertions.assertEquals(20L, decompressedReference.getSize());
        Assertions.assertEquals(10L, decompressedReference.getBlockSize());
        Assertions.assertEquals(content, decompressedReference.getContent());
    }

    @Test
    void testEncodingWithCompression() throws Exception {
        final LuceneSerializer serializer = getSerializer(false, true, false, null);
        final String duplicateMsg = "abcdefghijklmnopqrstuvwxyz";
        final String content = "content_" + duplicateMsg + "_" + duplicateMsg;
        final LuceneFileSystemProto.LuceneFileReference reference = LuceneFileSystemProto.LuceneFileReference.newBuilder()
                .setId(1)
                .setSize(20L)
                .setBlockSize(10L)
                .setContent(ByteString.copyFromUtf8(content))
                .build();
        final byte[] value = reference.toByteArray();
        final byte[] encodedValue = serializer.encode(value);
        final byte[] decodedValue = serializer.decode(encodedValue);

        // The encoded value's size is smaller than the original one due to compression
        Assertions.assertTrue(value.length > encodedValue.length);
        // Decoded value is expected to be equal to the original one
        Assertions.assertArrayEquals(value, decodedValue);

        final LuceneFileSystemProto.LuceneFileReference decompressedReference = LuceneFileSystemProto.LuceneFileReference.parseFrom(decodedValue);
        // The deserialized file reference is equal to the original one
        Assertions.assertEquals(1, decompressedReference.getId());
        Assertions.assertEquals(20L, decompressedReference.getSize());
        Assertions.assertEquals(10L, decompressedReference.getBlockSize());
        Assertions.assertEquals(ByteString.copyFromUtf8(content), decompressedReference.getContent());
    }

    public static Stream<Arguments> randomAndCompressed() {
        return ParameterizedTestUtils.cartesianProduct(
                RandomizedTestUtils.randomSeeds(),
                ParameterizedTestUtils.booleans("compressToo")
        );
    }

    @ParameterizedTest
    @MethodSource("randomAndCompressed")
    void testEncodingWithEncryption(long seed, boolean compressToo) throws Exception {
        Random random = new Random(seed);
        final LuceneSerializer serializer = getSerializer(false, compressToo, true, random);
        final ByteString content = RandomUtil.randomByteString(random, 100);
        final LuceneFileSystemProto.LuceneFileReference reference = LuceneFileSystemProto.LuceneFileReference.newBuilder()
                .setId(1)
                .setSize(20L)
                .setBlockSize(10L)
                .setContent(content)
                .build();
        final byte[] value = reference.toByteArray();
        final byte[] encodedValue = serializer.encode(value);
        final byte[] decodedValue = serializer.decode(encodedValue);
        Assertions.assertArrayEquals(value, decodedValue);
        final LuceneFileSystemProto.LuceneFileReference decryptedReference = LuceneFileSystemProto.LuceneFileReference.parseFrom(decodedValue);
        Assertions.assertEquals(content, decryptedReference.getContent());
    }

    @ParameterizedTest
    @MethodSource("encodedCompressedAndEncrypted")
    void testStoredFieldsEncoding(boolean encode, boolean compress, boolean encrypt, long seed) throws Exception {
        testFieldProtobuf(encode, compress, encrypt, seed,
                () -> {
                    final String storedField = RandomUtil.randomAlphanumericString(ThreadLocalRandom.current(), 20);
                    final LuceneStoredFieldsProto.LuceneStoredFields.Builder builder = LuceneStoredFieldsProto.LuceneStoredFields.newBuilder();
                    builder.addStoredFieldsBuilder().setFieldNumber(5).setStringValue(storedField);
                    return builder.build();
                },
                LuceneStoredFieldsProto.LuceneStoredFields::parseFrom);
    }

    @ParameterizedTest
    @MethodSource("encodedCompressedAndEncrypted")
    void testStoredFieldsKeyOnly(boolean encode, boolean compress, boolean encrypt, long seed) throws Exception {
        testFieldProtobuf(encode, compress, encrypt, seed,
                () -> {
                    final ByteString key = RandomUtil.randomByteString(ThreadLocalRandom.current(), 20);
                    final LuceneStoredFieldsProto.LuceneStoredFields.Builder builder = LuceneStoredFieldsProto.LuceneStoredFields.newBuilder();
                    builder.setPrimaryKey(key);
                    return builder.build();
                },
                LuceneStoredFieldsProto.LuceneStoredFields::parseFrom);
    }

    @ParameterizedTest
    @MethodSource("encodedCompressedAndEncrypted")
    void testFieldInfosEncoding(boolean encode, boolean compress, boolean encrypt, long seed) throws Exception {
        testFieldProtobuf(encode, compress, encrypt, seed,
                () -> {
                    final String name = RandomUtil.randomAlphanumericString(ThreadLocalRandom.current(), 10);
                    final LuceneFieldInfosProto.FieldInfos.Builder builder = LuceneFieldInfosProto.FieldInfos.newBuilder();
                    builder.addFieldInfoBuilder().setName(name).setNumber(2);
                    return builder.build();
                },
                LuceneFieldInfosProto.FieldInfos::parseFrom);
    }

    @ParameterizedTest
    @MethodSource("encodedCompressedAndEncrypted")
    void testStoredFieldsEmpty(boolean encode, boolean compress, boolean encrypt, long seed) throws Exception {
        testFieldProtobuf(encode, compress, encrypt, seed,
                LuceneStoredFieldsProto.LuceneStoredFields::getDefaultInstance,
                LuceneStoredFieldsProto.LuceneStoredFields::parseFrom);
    }

    @ParameterizedTest
    @MethodSource("encodedCompressedAndEncrypted")
    void testFieldInfosEmpty(boolean encode, boolean compress, boolean encrypt, long seed) throws Exception {
        testFieldProtobuf(encode, compress, encrypt, seed,
                LuceneFieldInfosProto.FieldInfos::getDefaultInstance,
                LuceneFieldInfosProto.FieldInfos::parseFrom);
    }

    static Stream<Arguments> encodedCompressedAndEncrypted() {
        return Streams.concat(
                // Non-encrypted cases.
                Stream.of(
                        Arguments.of(false, false, false, 0),
                        Arguments.of(true, false, false, 0),
                        Arguments.of(true, true, false, 0)
                ),
                // Encoded, encrypted, maybe compressed, random seeds.
                ParameterizedTestUtils.cartesianProduct(
                        Stream.of(Named.of("encode", true)),
                        ParameterizedTestUtils.booleans("compress"),
                        Stream.of(Named.of("encrypt", true)),
                        RandomizedTestUtils.randomSeeds()
                ));
    }

    @FunctionalInterface
    private interface MessageBuilder {
        Message build() throws Exception;
    }

    @FunctionalInterface
    private interface MessageParser {
        Message parse(byte[] bytes) throws Exception;
    }

    private void testFieldProtobuf(boolean encode, boolean compress,
                                   boolean encrypt, long seed,
                                   MessageBuilder build, MessageParser parse) throws Exception {
        final Random random = new Random(seed);
        final LuceneSerializer serializer = getSerializer(encode, compress, encrypt, random);
        final Message built = build.build();
        final byte[] value = built.toByteArray();
        final byte[] encodedValue = serializer.encodeFieldProtobuf(value);
        final byte[] decodedValue = serializer.decodeFieldProtobuf(encodedValue);
        Assertions.assertArrayEquals(value, decodedValue);
        final Message parsed = parse.parse(decodedValue);
        Assertions.assertEquals(built, parsed);
    }

    private LuceneSerializer getSerializer(boolean encodeFieldProtobuf, boolean compress,
                                           boolean encrypt, Random randomIfEncrypting) {
        final SerializationKeyManager keyManager;
        if (encrypt) {
            SecretKey key = RandomSecretUtil.randomSecretKey(randomIfEncrypting);
            keyManager = new FixedZeroKeyManager(key, null, randomIfEncrypting);
        } else {
            keyManager = null;
        }
        return new LuceneSerializer(compress, encrypt, keyManager, encodeFieldProtobuf);
    }
}
