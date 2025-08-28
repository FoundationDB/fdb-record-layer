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
import com.apple.foundationdb.record.util.RandomUtil;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Test for Lucene data compression/decompression and encryption/decryption validation.
 */
@Tag(Tags.RequiresFDB)
class LuceneSerializerTest {
    @Test
    void testEncodingWithoutCompression() throws InvalidProtocolBufferException {
        final LuceneSerializer serializer = new LuceneSerializer(null);
        final ByteString content = RandomUtil.randomByteString(ThreadLocalRandom.current(), 100);
        final LuceneFileSystemProto.LuceneFileReference reference = LuceneFileSystemProto.LuceneFileReference.newBuilder()
                .setId(1)
                .setSize(20L)
                .setBlockSize(10L)
                .setContent(content)
                .build();
        final byte[] originalValue = reference.toByteArray();
        final byte[] encodedValue = serializer.encode(originalValue, true, false);
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
    void testEncodingWithCompression() throws InvalidProtocolBufferException {
        final LuceneSerializer serializer = new LuceneSerializer(null);
        final String duplicateMsg = "abcdefghijklmnopqrstuvwxyz";
        final String content = "content_" + duplicateMsg + "_" + duplicateMsg;
        final LuceneFileSystemProto.LuceneFileReference reference = LuceneFileSystemProto.LuceneFileReference.newBuilder()
                .setId(1)
                .setSize(20L)
                .setBlockSize(10L)
                .setContent(ByteString.copyFromUtf8(content))
                .build();
        final byte[] value = reference.toByteArray();
        final byte[] encodedValue = serializer.encode(value, true, false);
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

    @ParameterizedTest
    @BooleanSource
    void testEncodingWithEncryption(boolean compressToo) throws Exception {
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(128);
        SecretKey key = keyGen.generateKey();
        final SerializationKeyManager keyManager = new FixedZeroKeyManager(key, null, null);
        final LuceneSerializer serializer = new LuceneSerializer(keyManager);
        final ByteString content = RandomUtil.randomByteString(ThreadLocalRandom.current(), 100);
        final LuceneFileSystemProto.LuceneFileReference reference = LuceneFileSystemProto.LuceneFileReference.newBuilder()
                .setId(1)
                .setSize(20L)
                .setBlockSize(10L)
                .setContent(content)
                .build();
        final byte[] value = reference.toByteArray();
        final byte[] encodedValue = serializer.encode(value, compressToo, true);
        final byte[] decodedValue = serializer.decode(encodedValue);
        Assertions.assertArrayEquals(value, decodedValue);
        final LuceneFileSystemProto.LuceneFileReference decryptedReference = LuceneFileSystemProto.LuceneFileReference.parseFrom(decodedValue);
        Assertions.assertEquals(content, decryptedReference.getContent());
    }
}
