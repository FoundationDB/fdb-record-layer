/*
 * MessageTuple.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.TupleFieldsProto;
import com.apple.foundationdb.record.metadata.expressions.TupleFieldsHelper;
import com.apple.foundationdb.relational.api.exceptions.InvalidColumnReferenceException;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.util.List;
import java.util.stream.Collectors;

@API(API.Status.EXPERIMENTAL)
public class MessageTuple extends AbstractRow {
    private final Message message;

    public MessageTuple(Message m) {
        this.message = m;
    }

    @Override
    public int getNumFields() {
        return message.getDescriptorForType().getFields().size();
    }

    @Override
    public Object getObject(int position) throws InvalidColumnReferenceException {
        if (position < 0 || position >= getNumFields()) {
            throw InvalidColumnReferenceException.getExceptionForInvalidPositionNumber(position);
        }
        Descriptors.FieldDescriptor fieldDescriptor = message.getDescriptorForType().getFields().get(position);
        if (fieldDescriptor.isRepeated()) {
            final var list = (List<?>) message.getField(message.getDescriptorForType().getFields().get(position));
            return list.stream().map(MessageTuple::sanitizeField).collect(Collectors.toList());
        }
        if (message.hasField(fieldDescriptor)) {
            final var field = message.getField(message.getDescriptorForType().getFields().get(position));
            return sanitizeField(field);
        } else {
            return null;
        }
    }

    public static Object sanitizeField(final Object field) {
        if (field instanceof Message && ((Message) field).getDescriptorForType().equals(TupleFieldsProto.UUID.getDescriptor())) {
            return TupleFieldsHelper.fromProto((Message) field, TupleFieldsProto.UUID.getDescriptor());
        }
        if (field instanceof Descriptors.EnumValueDescriptor) {
            return ((Descriptors.EnumValueDescriptor) field).getName();
        }
        if (field instanceof ByteString) {
            return ((ByteString) field).toByteArray();
        }
        return field;
    }

    @SuppressWarnings("unchecked")
    public <M extends Message> M parseMessage() {
        return (M) message;
    }

    @Override
    public String toString() {
        return "(" + message.toString() + ")";
    }
}
