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
public interface RecordSerializer<M extends Message> {
    @Nonnull
    public byte[] serialize(@Nonnull RecordMetaData metaData,
                            @Nonnull RecordType recordType,
                            @Nonnull M record,
                            @Nullable StoreTimer timer);

    @Nonnull
    public M deserialize(@Nonnull RecordMetaData metaData,
                         @Nonnull Tuple primaryKey,
                         @Nonnull byte[] serialized,
                         @Nullable StoreTimer timer);

    /**
     * Instrumentation events related to record serialization.
     */
    public enum Events implements StoreTimer.DetailEvent {
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
        DECRYPT_SERIALIZED_RECORD("decrypt serialized record");

        private final String title;
        Events(String title) {
            this.title = title;
        }

        @Override
        public String title() {
            return title;
        }
    }

    /**
     * Instrumentation counts related to record serialization.
     */
    public enum Counts implements StoreTimer.Count {
        /** The number of times that record compression was not effective and the record was kept uncompressed. */
        ESCHEW_RECORD_COMPRESSION("eschew record compression");

        private final String title;
        private final boolean isSize;

        Counts(String title) {
            this(title, false);
        }

        Counts(String title, boolean isSize) {
            this.title = title;
            this.isSize = false;
        }

        @Override
        public String title() {
            return title;
        }

        @Override
        public boolean isSize() {
            return isSize;
        }
    }
}
