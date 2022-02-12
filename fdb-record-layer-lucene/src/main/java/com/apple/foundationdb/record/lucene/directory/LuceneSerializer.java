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
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.LuceneLogMessageKeys;
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
import java.util.Arrays;

public class LuceneSerializer {
    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneSerializer.class);
    private static final int ENCODING_ENCRYPTED = 1;
    private static final int ENCODING_COMPRESSED = 2;
    private static final byte COMPRESSION_VERSION_FOR_HIGH_COMPRESSION = 0;

    private static class EncodingState {
        private boolean compressed;
        private boolean encrypted;

        private EncodingState() {
            this.compressed = false;
            this.encrypted = false;
        }

        private void setCompressed(boolean compressed) {
            this.compressed = compressed;
        }

        private void setEncrypted(boolean encrypted) {
            this.encrypted = encrypted;
        }

        private boolean isCompressed() {
            return compressed;
        }

        private boolean isEncrypted() {
            return encrypted;
        }
    }

    @Nullable
    public static byte[] encode(@Nullable byte[] data, boolean compress, boolean encrypt) {
        if (data == null) {
            return null;
        }

        final EncodingState state = new EncodingState();
        final ByteBuffersDataOutput decodedDataOutput = new ByteBuffersDataOutput();
        // Placeholder for the code byte at beginning, will be modified in output byte array if needed
        decodedDataOutput.writeByte((byte) 0);

        byte[] encoded = compressIfNeeded(compress, data, decodedDataOutput, state, 1);

        int code = 0;
        if (state.isCompressed()) {
            code = code | ENCODING_COMPRESSED;
        }
        // Encryption will be supported in future
        if (state.isEncrypted()) {
            code = code | ENCODING_ENCRYPTED;
        }
        encoded[0] = (byte) code;
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(KeyValueLogMessage.of("Encoded lucene data",
                    LuceneLogMessageKeys.COMPRESSION_SUPPOSED, compress,
                    LuceneLogMessageKeys.ENCRYPTION_SUPPOSED, encrypt,
                    LuceneLogMessageKeys.COMPRESSED_EVENTUALLY, state.isCompressed(),
                    LuceneLogMessageKeys.ENCRYPTED_EVENTUALLY, state.isEncrypted(),
                    LuceneLogMessageKeys.ORIGINAL_DATA_SIZE, data.length,
                    LuceneLogMessageKeys.ENCODED_DATA_SIZE, encoded.length));
        }
        return encoded;
    }

    @Nullable
    public static byte[] decode(@Nullable byte[] data) {
        if (data == null) {
            return null;
        }

        if (data.length < 2) {
            throw new RecordCoreException("Invalid data")
                    .addLogInfo(LuceneLogMessageKeys.DATA_VALUE, data);
        }

        final byte encoding = data[0];
        final boolean encrypted = (encoding & ENCODING_ENCRYPTED) == ENCODING_ENCRYPTED;
        final boolean compressed = (encoding & ENCODING_COMPRESSED) == ENCODING_COMPRESSED;

        byte[] decoded = decompressIfNeeded(compressed, data, 1);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(KeyValueLogMessage.of("Decoded lucene data",
                    LuceneLogMessageKeys.COMPRESSED_EVENTUALLY, compressed,
                    LuceneLogMessageKeys.ENCRYPTED_EVENTUALLY, encrypted,
                    LuceneLogMessageKeys.ENCODED_DATA_SIZE, data.length,
                    LuceneLogMessageKeys.ORIGINAL_DATA_SIZE, decoded.length));
        }
        return decoded;
    }

    @Nonnull
    private static byte[] compressIfNeeded(boolean compressionNeeded, @Nonnull byte[] data, @Nonnull ByteBuffersDataOutput encodedDataOutput,
                                           @Nonnull EncodingState state, int offset) {
        if (!compressionNeeded) {
            return fallBackToUncompressed(encodedDataOutput.toArrayCopy(), data, state, offset);
        }

        try (Compressor compressor = CompressionMode.HIGH_COMPRESSION.newCompressor()) {
            encodedDataOutput.writeByte(COMPRESSION_VERSION_FOR_HIGH_COMPRESSION);
            encodedDataOutput.writeVInt(data.length);
            compressor.compress(data, 0, data.length, encodedDataOutput);
            final byte[] compressedData = encodedDataOutput.toArrayCopy();

            // Compress only if it helps to shorten the bytes
            if (compressedData.length < data.length) {
                state.setCompressed(true);
                return compressedData;
            } else {
                return fallBackToUncompressed(compressedData, data, state, offset);
            }
        } catch (Exception e) {
            throw new RecordCoreException("Lucene data compression failure", e);
        }
    }

    private static byte[] fallBackToUncompressed(@Nonnull byte[] compressedData, @Nonnull byte[] originalData,
                                                 @Nonnull EncodingState state, int offset) {
        state.setCompressed(false);
        final byte[] encoded = new byte[originalData.length + offset];
        System.arraycopy(compressedData, 0, encoded, 0, offset);
        System.arraycopy(originalData, 0, encoded, offset, originalData.length);
        return encoded;
    }

    @Nonnull
    private static byte[] decompressIfNeeded(boolean decompressionNeeded, @Nonnull byte[] data, int offset) {
        if (!decompressionNeeded) {
            // Return the array to be the final output for decoding, so the offset bytes need to be removed
            return Arrays.copyOfRange(data, offset, data.length);
        }

        if (data.length < 2 + offset) {
            throw new RecordCoreException("Invalid data for decompression")
                    .addLogInfo(LogMessageKeys.DIR_VALUE, data);
        }
        byte version = data[offset];
        if (version != COMPRESSION_VERSION_FOR_HIGH_COMPRESSION) {
            throw new RecordCoreException("Un-supported compression version")
                    .addLogInfo(LuceneLogMessageKeys.COMPRESSION_VERSION, version);
        }

        try {
            BytesRef ref = new BytesRef();
            ByteArrayDataInput input = new ByteArrayDataInput(data, offset + 1, data.length - offset - 1);
            int originalLength = input.readVInt();
            Decompressor decompressor = CompressionMode.HIGH_COMPRESSION.newDecompressor();
            decompressor.decompress(input, originalLength, 0, originalLength, ref);
            return Arrays.copyOfRange(ref.bytes, ref.offset, ref.length);
        } catch (Exception e) {
            throw new RecordCoreException("Lucene data decompression failure", e);
        }
    }
}
