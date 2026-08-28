/*
 * MessageBuilderRecordSerializer.java
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
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.List;
import java.util.function.Supplier;

/**
 * Serialize records using default Protobuf serialization using the supplied message builder for the union message type.
 */
@API(API.Status.UNSTABLE)
public class MessageBuilderRecordSerializer extends MessageBuilderRecordSerializerBase<Message, Message, Message.Builder> {
    public MessageBuilderRecordSerializer(Supplier<Message.Builder> builderSupplier) {
        super(builderSupplier);
    }

    @Override
    protected void setUnionField(RecordMetaData metaData,
                                 RecordType recordType,
                                 Message.Builder unionBuilder,
                                 Message rec) {
        Descriptors.FieldDescriptor unionField = metaData.getUnionFieldForRecordType(recordType);
        unionBuilder.setField(unionField, rec);
    }

    @Override
    protected Message getUnionField(Descriptors.Descriptor unionDescriptor,
                                    Message storedRecord) {
        final List<Descriptors.OneofDescriptor> oneofs = unionDescriptor.getOneofs();
        if (!oneofs.isEmpty()) {
            Descriptors.FieldDescriptor unionField = storedRecord.getOneofFieldDescriptor(oneofs.get(0));
            if (unionField != null) {
                return (Message)storedRecord.getField(unionField);
            }
        } else {
            for (Descriptors.FieldDescriptor unionField : unionDescriptor.getFields()) {
                if (storedRecord.hasField(unionField)) {
                    return (Message)storedRecord.getField(unionField);
                }
            }
        }
        throw new RecordSerializationException("Union record does not have any fields")
                .addLogInfo("unionDescriptorFullName", unionDescriptor.getFullName())
                .addLogInfo("recordType", storedRecord.getDescriptorForType().getName());
    }

    @Override
    public RecordSerializer<Message> widen() {
        return this;
    }

}
