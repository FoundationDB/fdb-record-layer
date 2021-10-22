/*
 * RecordSerializer.java
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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A converter between a Protobuf record and a byte string stored in one or more values in the FDB key-value store.
 * @param <M> type used to represent stored records
 */
@API(API.Status.MAINTAINED)
public interface RecordSerializer<M extends Message> {

    /**
     * Convert a Protobuf record to bytes. While Protobuf messages provide
     * their own {@link Message#toByteArray()} method for serialization,
     * record stores may elect to first wrap the raw Protobuf record in another
     * wrapping type or perform additional transformations like encrypt or
     * compress records. Implementors of this interface can control how
     * exactly which transformations are done, with the main constraint being
     * that whatever operation is done should be reversible by calling the
     * {@link #deserialize(RecordMetaData, Tuple, byte[], StoreTimer) deserialize()}
     * method on those bytes. Implementors should also be careful as they
     * evolve this method that the <code>deserialize</code> method is still able
     * to read older data unless they are certain that any record store that
     * used the older implementation has since been cleared out or migrated
     * to a newer format.
     *
     * @param metaData the store's meta-data
     * @param recordType the record type of the message
     * @param rec the Protobuf record to serialize
     * @param timer a timer used to instrument serialization
     * @return the serialized record
     */
    @Nonnull
    byte[] serialize(@Nonnull RecordMetaData metaData, @Nonnull RecordType recordType,
                     @Nonnull M rec, @Nullable StoreTimer timer);

    /**
     * Convert a byte array to a Protobuf record. This should be the inverse of the
     * {@link #serialize(RecordMetaData, RecordType, Message, StoreTimer) serialize()}
     * method.
     *
     * @param metaData the store's meta-data
     * @param primaryKey the primary key of the record
     * @param serialized the serialized bytes
     * @param timer a timer used to instrument deserialization
     * @return the deserialized record
     */
    @Nonnull
    M deserialize(@Nonnull RecordMetaData metaData, @Nonnull Tuple primaryKey,
                  @Nonnull byte[] serialized, @Nullable StoreTimer timer);

    /**
     * Convert this typed record serializer to an untyped one.
     *
     * <ul>
     * <li>If this serializer wraps another serializer, for example, because it does compressions, then
     * {@code widen} that serializer and return the result wrapped equivalently.</li>
     * <li>If this serializer parses messages in a type-sensitive way, return the closest general purpose way.</li>
     * <li>If this serializer is inherently bound to its type parameter, throw an exception.</li>
     * </ul>
     *
     * <p>
     * If a {@link com.apple.foundationdb.record.provider.foundationdb.FDBTypedRecordStore} is created without calling
     * {@link com.apple.foundationdb.record.provider.foundationdb.FDBTypedRecordStore.Builder#setUntypedSerializer}, then
     * in order to make the untyped record store that is paired with the typed store (used, for instance, to rebuild indexes),
     * the given typed serializer will be widened by calling this method.
     * @return a new serializer that works the same way but handles all record types.
     */
    @Nonnull
    RecordSerializer<Message> widen();

    /**
     * Instrumentation events related to record serialization.
     */
    @API(API.Status.UNSTABLE)
    enum Events implements StoreTimer.DetailEvent {
        /** The amount of time spent serializing a Protobuf record to bytes. */
        SERIALIZE_PROTOBUF_RECORD("serialize protobuf record"),
        /** The amount of time spent deserializing a Protobuf record from bytes. */
        DESERIALIZE_PROTOBUF_RECORD("deserialize protobuf record"),
        /** The amount of time spent compressing serialized bytes. */
        COMPRESS_SERIALIZED_RECORD("compress serialized record"),
        /** The amount of time spent decompressing serialized bytes. */
        DECOMPRESS_SERIALIZED_RECORD("decompress serialized record"),
        /** The amount of time spent encrypting serialized bytes. */
        ENCRYPT_SERIALIZED_RECORD("encrypt serialized record"),
        /** The amount of time spent decrypting serialized bytes. */
        DECRYPT_SERIALIZED_RECORD("decrypt serialized record"),
        ;

        private final String title;
        private final String logKey;

        Events(String title, String logKey) {
            this.title = title;
            this.logKey = (logKey != null) ? logKey : StoreTimer.DetailEvent.super.logKey();
        }

        Events(String title) {
            this(title, null);
        }


        @Override
        public String title() {
            return title;
        }

        @Override
        @Nonnull
        public String logKey() {
            return this.logKey;
        }
    }

    /**
     * Instrumentation counts related to record serialization.
     */
    @API(API.Status.UNSTABLE)
    enum Counts implements StoreTimer.Count {
        /** The number of times that record compression was not effective and the record was kept uncompressed. */
        ESCHEW_RECORD_COMPRESSION("eschew record compression"),
        /** Total record bytes for which compression was attempted. */
        RECORD_BYTES_BEFORE_COMPRESSION("record bytes before compression"),
        /** Total record bytes after compression was attempted. */
        RECORD_BYTES_AFTER_COMPRESSION("record bytes after compression"),
        ;

        private final String title;
        private final boolean isSize;
        private final String logKey;

        Counts(String title, boolean isSize, String logKey) {
            this.title = title;
            this.isSize = false;
            this.logKey = (logKey != null) ? logKey : StoreTimer.Count.super.logKey();
        }

        Counts(String title, boolean isSize) {
            this(title, isSize, null);
        }

        Counts(String title) {
            this(title, false, null);
        }

        @Override
        public String title() {
            return title;
        }

        @Override
        @Nonnull
        public String logKey() {
            return this.logKey;
        }

        @Override
        public boolean isSize() {
            return isSize;
        }
    }
}
