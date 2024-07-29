/*
 * DynamicMessageRecordSerializer.java
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
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Serialize records using default Protobuf serialization using {@link DynamicMessage}.
 */
@API(API.Status.UNSTABLE)
public class DynamicMessageRecordSerializer implements RecordSerializer<Message> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicMessageRecordSerializer.class);

    private static final DynamicMessageRecordSerializer INSTANCE = new DynamicMessageRecordSerializer();

    @Nonnull
    public static RecordSerializer<Message> instance() {
        return INSTANCE;
    }

    @SpotBugsSuppressWarnings(value = "SING_SINGLETON_HAS_NONPRIVATE_CONSTRUCTOR",
            justification = "Singleton is optimization as base class has no state. Subclasses are allowed and may not be singletons")
    protected DynamicMessageRecordSerializer() {
    }

    @Nonnull
    @Override
    public RecordSerializer<Message> widen() {
        return this;
    }

    @Nonnull
    @Override
    public byte[] serialize(@Nonnull RecordMetaData metaData,
                            @Nonnull RecordType recordType,
                            @Nonnull Message rec,
                            @Nullable StoreTimer timer) {
        long startTime = System.nanoTime();
        try {
            // Wrap in union message, if needed.
            Message storedRecord = rec;
            Descriptors.Descriptor unionDescriptor = metaData.getUnionDescriptor();
            if (unionDescriptor != null) {
                DynamicMessage.Builder unionBuilder = DynamicMessage.newBuilder(unionDescriptor);
                Descriptors.FieldDescriptor unionField = metaData.getUnionFieldForRecordType(recordType);
                unionBuilder.setField(unionField, rec);
                storedRecord = unionBuilder.build();
            }
            return serializeToBytes(storedRecord);
        } finally {
            if (timer != null) {
                timer.recordSinceNanoTime(Events.SERIALIZE_PROTOBUF_RECORD, startTime);
            }
        }
    }

    @Nonnull
    protected byte[] serializeToBytes(@Nonnull Message storedRecord) {
        return storedRecord.toByteArray();
    }

    @Nonnull
    @Override
    @SpotBugsSuppressWarnings("RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
    public Message deserialize(@Nonnull final RecordMetaData metaData,
                               @Nonnull final Tuple primaryKey,
                               @Nonnull final byte[] serialized,
                               @Nullable StoreTimer timer) {
        final long startTime = System.nanoTime();
        try {
            final Descriptors.Descriptor unionDescriptor = metaData.getUnionDescriptor();
            final DynamicMessage unionMessage = deserializeUnion(unionDescriptor, primaryKey, serialized, metaData.getVersion());
            return getUnionField(unionMessage, primaryKey).getValue();
        } finally {
            if (timer != null) {
                timer.recordSinceNanoTime(Events.DESERIALIZE_PROTOBUF_RECORD, startTime);
            }
        }
    }

    @Nonnull
    protected DynamicMessage deserializeUnion(@Nonnull final Descriptors.Descriptor unionDescriptor,
                                              @Nonnull final Tuple primaryKey,
                                              @Nonnull final byte[] serialized,
                                              int metaDataVersion) {
        final DynamicMessage unionMessage = deserializeFromBytes(unionDescriptor, serialized);
        final Map<Descriptors.FieldDescriptor, Object> allFields = unionMessage.getAllFields();
        final Map<Integer, UnknownFieldSet.Field> unknownFields = unionMessage.getUnknownFields().asMap();
        if (!(allFields.size() == 1 && unknownFields.isEmpty())) {
            final String detailedMessage;
            if (!unknownFields.isEmpty()) {
                detailedMessage = " because there are unknown fields";
            } else if (allFields.size() > 1) {
                detailedMessage = " because there are extra known fields";
            } else {
                detailedMessage = " because there are no fields";
            }
            final String message = "Could not deserialize union message" + detailedMessage;
            final RecordSerializationException ex = new RecordSerializationException(message)
                    .addLogInfo("unknownFields", unknownFields.keySet())
                    .addLogInfo("fields", getFieldNames(allFields.keySet()))
                    .addLogInfo("primaryKey", primaryKey)
                    .addLogInfo("metaDataVersion", metaDataVersion);
            throw ex;
        }
        return unionMessage;
    }

    @Nonnull
    protected DynamicMessage deserializeFromBytes(@Nonnull Descriptors.Descriptor storedDescriptor,
                                                  @Nonnull byte[] serialized) {
        try {
            return DynamicMessage.parseFrom(storedDescriptor, serialized);
        } catch (InvalidProtocolBufferException ex) {
            throw new RecordSerializationException("Error reading from byte array", ex)
                    .addLogInfo("recordType", storedDescriptor.getName());
        }
    }

    @Nonnull
    protected Map.Entry<Descriptors.FieldDescriptor, DynamicMessage> getUnionField(@Nonnull final DynamicMessage unionMessage,
                                                                                   @Nonnull final Tuple primaryKey) {
        final Map.Entry<Descriptors.FieldDescriptor, Object> entry = unionMessage.getAllFields().entrySet().iterator().next();
        final DynamicMessage message = (DynamicMessage)entry.getValue();
        if (!message.getUnknownFields().asMap().isEmpty()) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn(KeyValueLogMessage.of("Deserialized message has unknown fields",
                        LogMessageKeys.PRIMARY_KEY, primaryKey,
                        LogMessageKeys.RECORD_TYPE, message.getDescriptorForType().getName(),
                        LogMessageKeys.UNKNOWN_FIELDS, message.getUnknownFields().asMap().keySet()));
            }
        }
        return Pair.of(entry.getKey(), message);
    }

    @Nonnull
    private Set<String> getFieldNames(Set<Descriptors.FieldDescriptor> fieldDescriptors) {
        return fieldDescriptors.stream().map(Descriptors.FieldDescriptor::getName).collect(Collectors.toSet());
    }

}
