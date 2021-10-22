/*
 * MessageBuilderRecordSerializerBase.java
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
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.UninitializedMessageException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Supplier;

/**
 * Base class for record serializers that use a supplied union message builder method reference to reconstruct records.
 * @param <M> generated Protobuf class for the record message type
 * @param <U> generated Protobuf class for the union message
 * @param <B> generated Protobuf class for the union message's builder
 * @see DynamicMessageRecordSerializer
 */
@API(API.Status.UNSTABLE)
public abstract class MessageBuilderRecordSerializerBase<M extends Message, U extends Message, B extends Message.Builder> implements RecordSerializer<M> {
    @Nonnull
    private final Supplier<B> builderSupplier;

    public MessageBuilderRecordSerializerBase(@Nonnull Supplier<B> builderSupplier) {
        this.builderSupplier = builderSupplier;
    }

    @Nonnull
    @Override
    public byte[] serialize(@Nonnull RecordMetaData metaData,
                            @Nonnull RecordType recordType,
                            @Nonnull M rec,
                            @Nullable StoreTimer timer) {
        long startTime = System.nanoTime();
        try {
            Descriptors.Descriptor unionDescriptor = metaData.getUnionDescriptor();
            B unionBuilder = builderSupplier.get();
            if (unionBuilder.getDescriptorForType() != unionDescriptor) {
                throw new RecordSerializationException("Builder does not match union type")
                        .addLogInfo("recordType", recordType.getName())
                        .addLogInfo("unionDescriptorFullName", metaData.getUnionDescriptor().getFullName())
                        .addLogInfo(LogMessageKeys.META_DATA_VERSION, metaData.getVersion());
            }
            setUnionField(metaData, recordType, unionBuilder, rec);
            @SuppressWarnings("unchecked")
            U storedRecord = (U) unionBuilder.build();
            return storedRecord.toByteArray();
        } finally {
            if (timer != null) {
                timer.recordSinceNanoTime(Events.SERIALIZE_PROTOBUF_RECORD, startTime);
            }
        }
    }

    protected abstract void setUnionField(@Nonnull RecordMetaData metaData,
                                          @Nonnull RecordType recordType,
                                          @Nonnull B unionBuilder,
                                          @Nonnull M rec);

    @Nonnull
    @Override
    @SuppressWarnings({"unchecked", "squid:S1193", "PMD.AvoidInstanceofChecksInCatchClause", // exception type checking is less clunky
                       "PMD.PreserveStackTrace"})
    public M deserialize(@Nonnull RecordMetaData metaData,
                         @Nonnull Tuple primaryKey,
                         @Nonnull byte[] serialized,
                         @Nullable StoreTimer timer) {
        long startTime = System.nanoTime();
        try {
            Descriptors.Descriptor unionDescriptor = metaData.getUnionDescriptor();
            B unionBuilder = builderSupplier.get();
            if (unionBuilder.getDescriptorForType() != unionDescriptor) {
                throw new RecordSerializationException("Builder does not match union type")
                        .addLogInfo(LogMessageKeys.PRIMARY_KEY, primaryKey)
                        .addLogInfo("unionDescriptorFullName", metaData.getUnionDescriptor().getFullName())
                        .addLogInfo(LogMessageKeys.META_DATA_VERSION, metaData.getVersion());
            }
            U storedRecord;
            try {
                storedRecord = (U) unionBuilder.mergeFrom(serialized).build();
            } catch (InvalidProtocolBufferException | UninitializedMessageException ex) {
                InvalidProtocolBufferException iex;
                if (ex instanceof InvalidProtocolBufferException) {
                    iex = (InvalidProtocolBufferException) ex;
                } else {
                    iex = ((UninitializedMessageException) ex).asInvalidProtocolBufferException();
                }
                throw new RecordSerializationException("Error reading from byte array", iex)
                        .addLogInfo(LogMessageKeys.PRIMARY_KEY, primaryKey)
                        .addLogInfo(LogMessageKeys.META_DATA_VERSION, metaData.getVersion());
            }
            return getUnionField(unionDescriptor, storedRecord);
        } finally {
            if (timer != null) {
                timer.recordSinceNanoTime(Events.DESERIALIZE_PROTOBUF_RECORD, startTime);
            }
        }
    }

    @Nonnull
    protected abstract M getUnionField(@Nonnull Descriptors.Descriptor unionDescriptor,
                                       @Nonnull U storedRecord);

    @Nonnull
    @Override
    public RecordSerializer<Message> widen() {
        return new MessageBuilderRecordSerializer(builderSupplier::get);
    }
}
