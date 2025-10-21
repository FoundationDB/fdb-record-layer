/*
 * LuceneSerializer.java
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

package com.apple.foundationdb.record.lucene.directory;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.lucene.LuceneLogMessageKeys;
import com.apple.foundationdb.record.provider.common.CipherPool;
import com.apple.foundationdb.record.provider.common.CompressedAndEncryptedSerializerState;
import com.apple.foundationdb.record.provider.common.SerializationKeyManager;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * Serialize a Lucene directory block to/from an FDB key-value byte array.
 * Supports compression and encryption.
 */
public class LuceneSerializer {
    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneSerializer.class);
    private static final int ENCODING_ENCRYPTED = 1;
    private static final int ENCODING_COMPRESSED = 2;
    private static final byte COMPRESSION_VERSION_FOR_HIGH_COMPRESSION = 0;
    private static final int ENCRYPTION_KEY_SHIFT = 3;

    private final boolean compressionEnabled;
    private final boolean encryptionEnabled;
    @Nullable
    private final SerializationKeyManager keyManager;
    private final boolean fieldProtobufPrefixEnabled;

    public LuceneSerializer(boolean compressionEnabled,
                            boolean encryptionEnabled, @Nullable SerializationKeyManager keyManager,
                            boolean fieldProtobufPrefixEnabled) {
        this.compressionEnabled = compressionEnabled;
        this.encryptionEnabled = encryptionEnabled;
        this.keyManager = keyManager;
        this.fieldProtobufPrefixEnabled = fieldProtobufPrefixEnabled;
    }

    public boolean isCompressionEnabled() {
        return compressionEnabled;
    }

    public boolean isEncryptionEnabled() {
        return encryptionEnabled;
    }

    public boolean isFieldProtobufPrefixEnabled() {
        return fieldProtobufPrefixEnabled;
    }

    @Nullable
    public SerializationKeyManager getKeyManager() {
        return keyManager;
    }

    @Nullable
    public byte[] encode(@Nullable byte[] data) {
        if (data == null) {
            return null;
        }

        final CompressedAndEncryptedSerializerState state = new CompressedAndEncryptedSerializerState();
        long prefix = 0;
        if (compressionEnabled) {
            prefix |= ENCODING_COMPRESSED;
            state.setCompressed(true);
        }
        if (encryptionEnabled) {
            if (keyManager == null) {
                throw new RecordCoreException("cannot encrypt Lucene blocks without keys");
            }
            int key = keyManager.getSerializationKey();
            prefix |= ENCODING_ENCRYPTED | ((key & 0xFFFFFFFFL) << ENCRYPTION_KEY_SHIFT);
            state.setEncrypted(true);
            state.setKeyNumber(key);
        }

        byte[] encoded;
        try {
            final ByteBuffersDataOutput decodedDataOutput = new ByteBuffersDataOutput();
            // Placeholder for the code byte at beginning, will be modified in output byte array if needed
            decodedDataOutput.writeVLong(prefix);
            final int prefixLength = (int)decodedDataOutput.size();
            encoded = compressIfNeeded(state, decodedDataOutput, data, prefixLength);
            encoded = encryptIfNeeded(state, encoded, prefixLength);
        } catch (IOException | GeneralSecurityException ex) {
            throw new RecordCoreException("Lucene data encoding failure", ex);
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(KeyValueLogMessage.of("Encoded lucene data",
                    LuceneLogMessageKeys.COMPRESSION_SUPPOSED, compressionEnabled,
                    LuceneLogMessageKeys.ENCRYPTION_SUPPOSED, encryptionEnabled,
                    LuceneLogMessageKeys.COMPRESSED_EVENTUALLY, state.isCompressed(),
                    LuceneLogMessageKeys.ENCRYPTED_EVENTUALLY, state.isEncrypted(),
                    LuceneLogMessageKeys.ORIGINAL_DATA_SIZE, data.length,
                    LuceneLogMessageKeys.ENCODED_DATA_SIZE, encoded.length));
        }

        return encoded;
    }

    @Nullable
    public byte[] decode(@Nullable byte[] data) {
        if (data == null) {
            return null;
        }

        if (data.length < 1) {
            throw new RecordCoreException("Invalid data")
                    .addLogInfo(LuceneLogMessageKeys.DATA_VALUE, data);
        }

        final CompressedAndEncryptedSerializerState state = new CompressedAndEncryptedSerializerState();
        byte[] decoded;
        try {
            final ByteArrayDataInput encodedDataInput = new ByteArrayDataInput(data);
            final long prefix = encodedDataInput.readVLong();

            state.setCompressed((prefix & ENCODING_COMPRESSED) != 0);
            state.setEncrypted((prefix & ENCODING_ENCRYPTED) != 0);
            state.setKeyNumber((int)(prefix >> ENCRYPTION_KEY_SHIFT));

            decryptIfNeeded(state, encodedDataInput);
            decompressIfNeeded(state, encodedDataInput);

            decoded = new byte[encodedDataInput.length() - encodedDataInput.getPosition()];
            encodedDataInput.readBytes(decoded, 0, decoded.length);
        } catch (IOException | GeneralSecurityException ex) {
            throw new RecordCoreException("Lucene data decoding failure", ex);
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(KeyValueLogMessage.of("Decoded lucene data",
                    LuceneLogMessageKeys.COMPRESSED_EVENTUALLY, state.isCompressed(),
                    LuceneLogMessageKeys.ENCRYPTED_EVENTUALLY, state.isEncrypted(),
                    LuceneLogMessageKeys.ENCODED_DATA_SIZE, data.length,
                    LuceneLogMessageKeys.ORIGINAL_DATA_SIZE, decoded.length));
        }

        return decoded;
    }

    @Nonnull
    private static byte[] compressIfNeeded(@Nonnull CompressedAndEncryptedSerializerState state, @Nonnull ByteBuffersDataOutput encodedDataOutput,
                                           @Nonnull byte[] uncompressedData, int prefixLength)
            throws IOException {
        if (!state.isCompressed()) {
            return fallBackToUncompressed(state, uncompressedData, encodedDataOutput.toArrayCopy(), prefixLength);
        }

        try (Compressor compressor = CompressionMode.HIGH_COMPRESSION.newCompressor()) {
            encodedDataOutput.writeByte(COMPRESSION_VERSION_FOR_HIGH_COMPRESSION);
            encodedDataOutput.writeVInt(uncompressedData.length);
            compressor.compress(uncompressedData, 0, uncompressedData.length, encodedDataOutput);
            final byte[] compressedData = encodedDataOutput.toArrayCopy();

            // Compress only if it helps to shorten the bytes
            if (compressedData.length < uncompressedData.length) {
                state.setCompressed(true);
                return compressedData;
            } else {
                return fallBackToUncompressed(state, uncompressedData, compressedData, prefixLength);
            }
        }
    }

    private static byte[] fallBackToUncompressed(@Nonnull CompressedAndEncryptedSerializerState state, @Nonnull byte[] originalData,
                                                 @Nonnull byte[] encodedData, int prefixLength) {
        final byte[] encoded = new byte[originalData.length + prefixLength];
        System.arraycopy(encodedData, 0, encoded, 0, prefixLength);
        // This bit is always in the lowest (first) byte, even if the prefix is longer.
        encoded[0] &= ~ENCODING_COMPRESSED;
        System.arraycopy(originalData, 0, encoded, prefixLength, originalData.length);
        state.setCompressed(false);
        return encoded;
    }

    private void decompressIfNeeded(@Nonnull CompressedAndEncryptedSerializerState state, @Nonnull ByteArrayDataInput encodedDataInput)
            throws IOException {
        if (!state.isCompressed()) {
            return;
        }

        final byte version = encodedDataInput.readByte();
        if (version != COMPRESSION_VERSION_FOR_HIGH_COMPRESSION) {
            throw new RecordCoreException("Un-supported compression version")
                    .addLogInfo(LuceneLogMessageKeys.COMPRESSION_VERSION, version);
        }

        BytesRef ref = new BytesRef();
        int originalLength = encodedDataInput.readVInt();
        Decompressor decompressor = CompressionMode.HIGH_COMPRESSION.newDecompressor();
        decompressor.decompress(encodedDataInput, originalLength, 0, originalLength, ref);
        encodedDataInput.reset(ref.bytes, ref.offset, ref.length);
    }

    private byte[] encryptIfNeeded(@Nonnull CompressedAndEncryptedSerializerState state, @Nonnull byte[] encoded, int prefixLength) throws GeneralSecurityException {
        if (!state.isEncrypted()) {
            return encoded;
        }

        final byte[] encrypted;
        final byte[] ivData = new byte[CipherPool.IV_SIZE];
        keyManager.getRandom(state.getKeyNumber()).nextBytes(ivData);
        final IvParameterSpec iv = new IvParameterSpec(ivData);
        final Cipher cipher = CipherPool.borrowCipher(keyManager.getCipher(state.getKeyNumber()));
        try {
            cipher.init(Cipher.ENCRYPT_MODE, keyManager.getKey(state.getKeyNumber()), iv);
            encrypted = cipher.doFinal(encoded, prefixLength, encoded.length - prefixLength);
        } finally {
            CipherPool.returnCipher(cipher);
        }
        final int totalSize = prefixLength + CipherPool.IV_SIZE + encrypted.length;
        final byte[] withIv = new byte[totalSize];
        System.arraycopy(encoded, 0, withIv, 0, prefixLength);
        System.arraycopy(iv.getIV(), 0, withIv, prefixLength, CipherPool.IV_SIZE);
        System.arraycopy(encrypted, 0, withIv, prefixLength + CipherPool.IV_SIZE, encrypted.length);
        return withIv;
    }

    private void decryptIfNeeded(@Nonnull CompressedAndEncryptedSerializerState state, @Nonnull ByteArrayDataInput encodedDataInput)
            throws GeneralSecurityException {
        if (!state.isEncrypted()) {
            return;
        }

        if (keyManager == null) {
            throw new RecordCoreException("cannot decrypt Lucene blocks without keys");
        }

        final byte[] ivData = new byte[CipherPool.IV_SIZE];
        encodedDataInput.readBytes(ivData, 0, CipherPool.IV_SIZE);
        final IvParameterSpec iv = new IvParameterSpec(ivData);

        final byte[] decrypted;
        final byte[] encrypted = new byte[encodedDataInput.length() - encodedDataInput.getPosition()];
        encodedDataInput.readBytes(encrypted, 0, encrypted.length);
        final Cipher cipher = CipherPool.borrowCipher(keyManager.getCipher(state.getKeyNumber()));
        try {
            cipher.init(Cipher.DECRYPT_MODE, keyManager.getKey(state.getKeyNumber()), iv);
            decrypted = cipher.doFinal(encrypted);
        } finally {
            CipherPool.returnCipher(cipher);
        }
        encodedDataInput.reset(decrypted);
    }

    @Nullable
    public byte[] encodeFieldProtobuf(@Nullable byte[] bytes) {
        if (fieldProtobufPrefixEnabled) {
            return encode(bytes);
        } else {
            return bytes;
        }
    }

    @Nullable
    public byte[] decodeFieldProtobuf(@Nullable byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        if (isProtobufMessageWithoutPrefix(bytes)) {
            return bytes;
        }
        return decode(bytes);
    }

    // This can be removed once it is guaranteed that all indexes are using the encoded format.
    // Only works for Protobuf messages all of whose fields are themselves length-delimited,
    // such as LuceneStoredFields (StoredField or bytes) or FieldInfos (FieldInfo).
    private boolean isProtobufMessageWithoutPrefix(@Nonnull byte[] bytes) {
        if (bytes.length < 1) {
            return true;    // No room for prefix; empty message.
        }
        final int byte0 = bytes[0];
        final int fieldTypeOrPrefixFlags = byte0 & 7;
        // Either Protobuf LEN or ENCODING_COMPRESSED.
        if (fieldTypeOrPrefixFlags != 2) {
            return false;
        }
        final int fieldNumberOrKeyNumber = byte0 >> 3;
        // ENCODING_COMPRESSED will never have a key; 0 is not a valid field number.
        if (fieldNumberOrKeyNumber == 0) {
            return false;
        }
        return true;
    }
}
