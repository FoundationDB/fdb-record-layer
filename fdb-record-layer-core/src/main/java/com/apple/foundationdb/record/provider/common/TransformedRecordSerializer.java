/*
 * TransformedRecordSerializer.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * A {@link RecordSerializer} implementation that transforms the bytes produced
 * by another serializer before returning (and likewise performs the reverse
 * transformation before deserializing). At the moment, the transformations
 * are encryption and compression, but other transformations could be
 * added in the future.
 *
 * <p>
 * This serializer will begin each serialized string with a one-byte prefix
 * containing information about which transformations were performed. This
 * way, when deserializing, it can detect which transformations were applied
 * so it knows which ones it needs to use to restore the original record.
 * This also allows it to do things like decide not to compress records, even
 * if compression is turned on, if the compression algorithm actually produces
 * a byte array that is larger than the raw record, for example.
 * </p>
 *
 * <p>
 * This base class does not itself support encryption: an exception will be thrown
 * when trying to construct a serializer with encryption enabled or when encountering
 * a serialized record that requires decryption. Subclasses, such as {@link TransformedRecordSerializerJCE},
 * implement encryption and have additional state like secret keys.
 * </p>
 *
 * <p>
 * It is designed to be compatible with the {@link DynamicMessageRecordSerializer}
 * insofar as it can deserialize records written by that serializer, though
 * records serialized by this class cannot be read by instances of that class.
 * In the future, we might remove that capability (when all existing data
 * have been migrated to use this class).
 * </p>
 *
 * @param <M> type of {@link Message} that underlying records will use
 */
@API(API.Status.UNSTABLE)
public class TransformedRecordSerializer<M extends Message> implements RecordSerializer<M> {
    @VisibleForTesting
    protected static final int ENCODING_ENCRYPTED = 1;
    @VisibleForTesting
    protected static final int ENCODING_CLEAR = 2;
    @VisibleForTesting
    protected static final int ENCODING_COMPRESSED = 4;
    // TODO: Can remove this after transition to write everything with _CLEAR.
    protected static final int ENCODING_PROTO_MESSAGE_FIELD = 0x02;
    protected static final int ENCODING_PROTO_TYPE_MASK = 0x07;
    protected static final int DEFAULT_COMPRESSION_LEVEL = Deflater.BEST_COMPRESSION;
    protected static final int MIN_COMPRESSION_VERSION = 1;
    protected static final int MAX_COMPRESSION_VERSION = 1;

    @Nonnull
    protected final RecordSerializer<M> inner;
    protected final boolean compressWhenSerializing;
    protected final int compressionLevel;
    protected final boolean encryptWhenSerializing;

    protected TransformedRecordSerializer(@Nonnull RecordSerializer<M> inner,
                                          boolean compressWhenSerializing,
                                          int compressionLevel,
                                          boolean encryptWhenSerializing) {
        this.inner = inner;
        this.compressWhenSerializing = compressWhenSerializing;
        this.compressionLevel = compressionLevel;
        this.encryptWhenSerializing = encryptWhenSerializing;
    }

    @SpotBugsSuppressWarnings("EI_EXPOSE_REP")
    protected static class TransformState {
        public boolean compressed;
        public boolean encrypted;

        @Nonnull public byte[] data;
        public int offset;
        public int length;

        public TransformState(@Nonnull byte[] data) {
            this(data, 0, data.length);
        }

        public TransformState(@Nonnull byte[] data, int offset, int length) {
            this.compressed = false;
            this.encrypted = false;
            this.data = data;
            this.offset = offset;
            this.length = length;
        }

        @Nonnull
        public byte[] getDataArray() {
            if (offset == 0 && length == data.length) {
                return data;
            } else {
                byte[] newData = Arrays.copyOfRange(data, offset, offset + length);
                offset = 0;
                length = newData.length;
                data = newData;
                return newData;
            }
        }


        public void setDataArray(@Nonnull byte[] data) {
            setDataArray(data, 0, data.length);
        }

        public void setDataArray(@Nonnull byte[] data, int offset, int length) {
            this.data = data;
            this.offset = offset;
            this.length = length;
        }
    }

    protected void compress(@Nonnull TransformState state, @Nullable StoreTimer timer) {
        long startTime = System.nanoTime();

        byte[] compressed = new byte[state.length];

        // Write compression version number and uncompressed size as these
        // meta-data are needed when decompressing.
        compressed[0] = (byte) MAX_COMPRESSION_VERSION;
        ByteBuffer.wrap(compressed, 1, 4).order(ByteOrder.BIG_ENDIAN).putInt(state.length);

        // Actually compress. If we end up filling the buffer, then just
        // return the uncompressed value because it's pointless to compress
        // if we actually increase the amount of data.
        Deflater compressor = new Deflater(compressionLevel);
        compressor.setInput(state.data, state.offset, state.length);
        int compressedLength = compressor.deflate(compressed, 5, compressed.length - 5, Deflater.FULL_FLUSH);
        compressor.end();
        if (compressedLength == compressed.length - 5) {
            state.compressed = false;
        } else {
            state.compressed = true;
            state.setDataArray(compressed, 0, compressedLength + 5);
        }

        if (timer != null) {
            timer.recordSinceNanoTime(Events.COMPRESS_SERIALIZED_RECORD, startTime);
            if (!state.compressed) {
                timer.increment(Counts.ESCHEW_RECORD_COMPRESSION);
            }
        }
    }

    protected void encrypt(@Nonnull TransformState state, @Nullable StoreTimer timer) throws GeneralSecurityException {
        throw new RecordSerializationException("this serializer cannot encrypt");
    }

    @Nonnull
    @Override
    public byte[] serialize(@Nonnull RecordMetaData metaData,
                            @Nonnull RecordType recordType,
                            @Nonnull M record,
                            @Nullable StoreTimer timer) {
        byte[] innerSerialized = inner.serialize(metaData, recordType, record, timer);

        TransformState state = new TransformState(innerSerialized);

        if (compressWhenSerializing) {
            compress(state, timer);
        }

        if (encryptWhenSerializing) {
            try {
                encrypt(state, timer);
            } catch (GeneralSecurityException ex) {
                throw new RecordSerializationException("encryption error", ex)
                        .addLogInfo("recordType", recordType.getName())
                        .addLogInfo(LogMessageKeys.META_DATA_VERSION, metaData.getVersion());
            }
        }

        int code;
        if (state.compressed || state.encrypted) {
            code = 0;
            if (state.compressed) {
                code = code | ENCODING_COMPRESSED;
            }
            if (state.encrypted) {
                code = code | ENCODING_ENCRYPTED;
            }
        } else {
            code = ENCODING_CLEAR;
        }

        int size = state.length + 1;
        byte[] serialized = new byte[size];
        serialized[0] = (byte) code;
        System.arraycopy(state.data, state.offset, serialized, 1, state.length);

        return serialized;
    }

    protected void decompress(@Nonnull TransformState state, @Nullable StoreTimer timer) throws DataFormatException {
        long startTime = System.nanoTime();

        // At the moment, there is only one compression version, so
        // we after we've verified it is in the right range, we
        // can just move on. If we ever introduce a new format version,
        // we will need to make this code more complicated.
        int compressionVersion = state.data[state.offset];
        if (compressionVersion < MIN_COMPRESSION_VERSION || compressionVersion > MAX_COMPRESSION_VERSION) {
            throw new RecordSerializationException("unknown compression version")
                    .addLogInfo("compressionVersion", compressionVersion);
        }

        int decompressedLength = ByteBuffer.wrap(state.data, state.offset + 1, 4).order(ByteOrder.BIG_ENDIAN).getInt();
        byte[] decompressed = new byte[decompressedLength];

        Inflater decompressor = new Inflater();
        decompressor.setInput(state.data, state.offset + 5, state.length - 5);
        decompressor.inflate(decompressed);
        decompressor.end();
        state.setDataArray(decompressed);

        if (timer != null) {
            timer.recordSinceNanoTime(Events.DECOMPRESS_SERIALIZED_RECORD, startTime);
        }
    }

    protected void decrypt(@Nonnull TransformState state, @Nullable StoreTimer timer) throws GeneralSecurityException {
        throw new RecordSerializationException("this serializer cannot decrypt");
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.PreserveStackTrace")
    public M deserialize(@Nonnull RecordMetaData metaData,
                         @Nonnull Tuple primaryKey,
                         @Nonnull byte[] serialized,
                         @Nullable StoreTimer timer) {
        int encoding = serialized[0];
        if (encoding != ENCODING_CLEAR && (encoding & ENCODING_PROTO_TYPE_MASK) == ENCODING_PROTO_MESSAGE_FIELD) {
            // TODO: Can remove this after transition to write everything with _CLEAR.
            return inner.deserialize(metaData, primaryKey, serialized, timer);
        } else {
            TransformState state = new TransformState(serialized, 1, serialized.length - 1);
            if (encoding != ENCODING_CLEAR) {
                if ((encoding & ENCODING_COMPRESSED) == ENCODING_COMPRESSED) {
                    state.compressed = true;
                }
                if ((encoding & ENCODING_ENCRYPTED) == ENCODING_ENCRYPTED) {
                    state.encrypted = true;
                }
                if ((encoding & ~(ENCODING_COMPRESSED | ENCODING_ENCRYPTED)) != 0) {
                    throw new RecordSerializationException("unrecognized transformation encoding")
                            .addLogInfo(LogMessageKeys.META_DATA_VERSION, metaData.getVersion())
                            .addLogInfo(LogMessageKeys.PRIMARY_KEY, primaryKey)
                            .addLogInfo("encoding", encoding);
                }
            }
            if (state.encrypted) {
                try {
                    decrypt(state, timer);
                } catch (RecordCoreException ex) {
                    throw ex.addLogInfo(LogMessageKeys.META_DATA_VERSION, metaData.getVersion())
                            .addLogInfo(LogMessageKeys.PRIMARY_KEY, primaryKey);
                } catch (GeneralSecurityException ex) {
                    throw new RecordSerializationException("decryption error", ex)
                            .addLogInfo(LogMessageKeys.META_DATA_VERSION, metaData.getVersion())
                            .addLogInfo(LogMessageKeys.PRIMARY_KEY, primaryKey);
                }
            }
            if (state.compressed) {
                try {
                    decompress(state, timer);
                } catch (RecordCoreException ex) {
                    throw ex.addLogInfo(LogMessageKeys.META_DATA_VERSION, metaData.getVersion())
                            .addLogInfo(LogMessageKeys.PRIMARY_KEY, primaryKey);
                } catch (DataFormatException ex) {
                    throw new RecordSerializationException("decompression error", ex)
                            .addLogInfo(LogMessageKeys.META_DATA_VERSION, metaData.getVersion())
                            .addLogInfo(LogMessageKeys.PRIMARY_KEY, primaryKey);
                }
            }
            return inner.deserialize(metaData, primaryKey, state.getDataArray(), timer);
        }
    }

    @Nonnull
    @Override
    public RecordSerializer<Message> widen() {
        return new TransformedRecordSerializer<>(inner.widen(), compressWhenSerializing, compressionLevel, encryptWhenSerializing);
    }

    @Nonnull
    public RecordSerializer<M> untransformed() {
        return inner;
    }

    /**
     * Creates a new {@link Builder TransformedRecordSerializer.Builder} instance
     * that is backed by the default serializer for {@link Message}s, namely
     * a {@link DynamicMessageRecordSerializer}. Methods on the returned
     * <code>Builder</code> instance can be used to specify which transformations
     * to apply after using the default serializer.
     *
     * @return <code>Builder</code> instance backed by a {@link DynamicMessageRecordSerializer}
     */
    public static Builder<Message> newDefaultBuilder() {
        return newBuilder(DynamicMessageRecordSerializer.instance());
    }

    /**
     * Creates a new {@link Builder TransformedRecordSerializer.Builder} instance around
     * the given serializer. Methods on the <code>Builder</code> instance can be used to
     * specify which transformations after using the provided serializer.
     *
     * @param inner {@link RecordSerializer} to use before/after applying transformations
     * @param <M> type of {@link Message} that underlying records will use
     * @return <code>Builder</code> instance that can be used to specify transformations
     */
    public static <M extends Message> Builder<M> newBuilder(@Nonnull RecordSerializer<M> inner) {
        return new Builder<>(inner);
    }

    /**
     * Builder class for the {@link TransformedRecordSerializer} class. The methods
     * on this builder allows the user to specify parameters that can then be
     * used to specify which transformations should be applied before serializing
     * the record. It can also be used to specify parameters that will then be
     * applied to serialized data before deserializing, though the prefix of
     * records themselves are what specify the needed transformations.
     *
     * @param <M> type of {@link Message} that underlying records will use
     */
    public static class Builder<M extends Message> {
        @Nonnull
        protected final RecordSerializer<M> inner;
        protected boolean compressWhenSerializing;
        protected int compressionLevel = DEFAULT_COMPRESSION_LEVEL;
        protected boolean encryptWhenSerializing;

        protected Builder(@Nonnull RecordSerializer<M> inner) {
            this.inner = inner;
        }

        /**
         * Whether to compress records after serializing. Should
         * compression and encryption both be set, then the data
         * will be compressed before they are encrypted. By default,
         * records are not compressed. The compression level
         * can be specified with {@link #setCompressionLevel(int)}.
         * @param compressWhenSerializing <code>true</code> if records should be compressed and <code>false</code> otherwise
         * @return this <code>Builder</code>
         */
        public Builder<M> setCompressWhenSerializing(boolean compressWhenSerializing) {
            this.compressWhenSerializing = compressWhenSerializing;
            return this;
        }

        /**
         * Compression level to use if compressing. These should be the
         * same levels as used by the {@link Deflater} class (which
         * are the same levels as used by zlib). The higher the level,
         * the better the compression will be but the slower or more
         * CPU intensive it will be. The default level if none is
         * set here is {@value DEFAULT_COMPRESSION_LEVEL}. Setting
         * this does <i>not</i> automatically enable compression when
         * serializing.
         * @param level the compression level (0-9)
         * @return this <code>Builder</code>
         * @see Deflater
         */
        public Builder<M> setCompressionLevel(int level) {
            this.compressionLevel = level;
            return this;
        }

        /**
         * Whether to encrypt records after serializing. Should
         * compression and encryption both be set, then data
         * will be compressed before they are encrypted. By
         * default, records are not encrypted. The base {@link TransformedRecordSerializer} class
         * does not support encryption, so {@link #build} will throw on exception.
         * But subclasses such as {@link TransformedRecordSerializerJCE} do support it.
         * When using those builders and enabling encryption, the user must also call appropriate methods
         * to specify the encryption key.
         * @param encryptWhenSerializing <code>true</code> if records should be encrypted and <code>false</code> otherwise
         * @return this <code>Builder</code>
         */
        public Builder<M> setEncryptWhenSerializing(boolean encryptWhenSerializing) {
            this.encryptWhenSerializing = encryptWhenSerializing;
            return this;
        }

        /**
         * Construct a {@link TransformedRecordSerializer} from the
         * parameters specified by this builder. If one has enabled
         * encryption at serialization time, then this will fail
         * with an {@link RecordCoreArgumentException}.
         * @return a {@link TransformedRecordSerializer} from this <code>Builder</code>
         * @throws RecordCoreArgumentException if encryption is enabled when serializing but no encryption is specified
         */
        public TransformedRecordSerializer<M> build() {
            if (encryptWhenSerializing) {
                throw new RecordCoreArgumentException("cannot encrypt when serializing using this class");
            }
            return new TransformedRecordSerializer<>(
                    inner,
                    compressWhenSerializing,
                    compressionLevel,
                    encryptWhenSerializing
            );
        }
    }
}
