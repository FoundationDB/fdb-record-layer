/*
 * TypeCheckingRecordSerializer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Supplier;

public class TypeCheckingRecordSerializer<M extends Message> implements RecordSerializer<M> {
    @Nonnull
    private final RecordSerializer<? super M> underlying;
    @Nonnull
    private final Class<M> messageClass;
    @Nonnull
    private final Supplier<? extends Message.Builder> builderSupplier;

    private TypeCheckingRecordSerializer(@Nonnull RecordSerializer<? super  M> underlying, @Nonnull Class<M> messageClass, @Nonnull Supplier<? extends Message.Builder> builderSupplier) {
        this.underlying = underlying;
        this.messageClass = messageClass;
        this.builderSupplier = builderSupplier;
    }

    @Nonnull
    @Override
    public byte[] serialize(@Nonnull RecordMetaData metaData, @Nonnull RecordType recordType, @Nonnull M record, @Nullable StoreTimer timer) {
        return underlying.serialize(metaData, recordType, record, timer);
    }

    @Nonnull
    @Override
    public M deserialize(@Nonnull RecordMetaData metaData, @Nonnull Tuple primaryKey, @Nonnull byte[] serialized, @Nullable StoreTimer timer) {
        Message msg = underlying.deserialize(metaData, primaryKey, serialized, timer);
        return ProtoUtils.cast(msg, messageClass, builderSupplier);
    }

    @Nonnull
    @Override
    public RecordSerializer<Message> widen() {
        return underlying.widen();
    }

    public static <M extends Message> TypeCheckingRecordSerializer<M> of(@Nonnull RecordSerializer<? super M> serializer, @Nonnull Class<M> messageClass, @Nonnull Supplier<? extends Message.Builder> builderSupplier) {
        return new TypeCheckingRecordSerializer<>(serializer, messageClass, builderSupplier);
    }
}
