/*
 * ProtobufDdlUtil.java
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

package com.apple.foundationdb.relational.api.ddl;

import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;

import java.sql.Types;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class ProtobufDdlUtil {

    private ProtobufDdlUtil() {
    }

    public static Type.Record recordFromDescriptor(Descriptors.Descriptor descriptor) throws RelationalException {
        /*
         * This exists because we have to make sure that fields are found by name, but sorted by their index,
         * otherwise metadata will not find the correct columns by position.
         */

        Map<String, Descriptors.FieldDescriptor> descriptorLookupMap = descriptor.getFields().stream()
                .collect(Collectors.toMap(Descriptors.FieldDescriptor::getName, Function.identity()));

        TreeMap<String, Descriptors.FieldDescriptor> orderedFieldMap = new TreeMap<>((o1, o2) -> {
            if (o1 == null) {
                if (o2 == null) {
                    return 0;
                } else {
                    return -1;
                } //sort nulls first; shouldn't happen here but it's a good habit
            } else if (o2 == null) {
                return 1;
            } else {
                Descriptors.FieldDescriptor field1 = descriptorLookupMap.get(o1);
                Descriptors.FieldDescriptor field2 = descriptorLookupMap.get(o2);
                return Integer.compare(field1.getIndex(), field2.getIndex());
            }
        });
        orderedFieldMap.putAll(descriptorLookupMap);
        return Type.Record.fromFieldDescriptorsMap(orderedFieldMap);
    }

    public static String getTypeName(DescriptorProtos.FieldDescriptorProto descriptor) {
        String type = "";
        switch (descriptor.getType()) {
            case TYPE_INT32:
            case TYPE_INT64:
                type += "INT64";
                break;
            case TYPE_FLOAT:
            case TYPE_DOUBLE:
                type += "DOUBLE";
                break;
            case TYPE_BOOL:
                type += "BOOLEAN";
                break;
            case TYPE_STRING:
                type += "STRING";
                break;
            case TYPE_BYTES:
                type += "BYTES";
                break;
            case TYPE_MESSAGE:
                type +=  descriptor.getTypeName();
                break;
            case TYPE_ENUM:
            //TODO(Bfines) figure this one out
            default:
                throw new IllegalStateException("Unexpected descriptor java type <" + descriptor.getType());
        }

        if (descriptor.getLabel() == DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED) {
            type += " array";
        }

        return type;
    }

    public static String getTypeName(Descriptors.FieldDescriptor descriptor) {
        String type = "";
        switch (descriptor.getJavaType()) {
            case INT:
            case LONG:
                type += "INT64";
                break;
            case FLOAT:
            case DOUBLE:
                type += "DOUBLE";
                break;
            case BOOLEAN:
                type += "BOOLEAN";
                break;
            case STRING:
                type += "STRING";
                break;
            case BYTE_STRING:
                type += "BYTES";
                break;
            case MESSAGE:
                type += descriptor.getMessageType().getName();
                break;
            default:
                throw new IllegalStateException("Unexpected java type :" + descriptor.getJavaType());
        }
        if (descriptor.isRepeated()) {
            type += " array";
        }

        return type.toUpperCase(Locale.ROOT);
    }

    public static int getSqlType(Descriptors.FieldDescriptor field) {
        if (field.isRepeated()) {
            return Types.ARRAY;
        }
        switch (field.getJavaType()) {
            case INT:
            case LONG:
                return Types.BIGINT;
            case FLOAT:
            case DOUBLE:
                return Types.DOUBLE;
            case BOOLEAN:
                return Types.BOOLEAN;
            case STRING:
                return Types.VARCHAR;
            case BYTE_STRING:
                return Types.VARBINARY;
            case MESSAGE:
                return Types.STRUCT;
            default:
                throw new IllegalStateException("Unexpected java type " + field.getJavaType());
        }
    }

    public static Type.Record recordFromFieldDescriptors(List<Descriptors.FieldDescriptor> fields) {
        Map<String, Descriptors.FieldDescriptor> descriptorLookupMap = fields.stream()
                .collect(Collectors.toMap(Descriptors.FieldDescriptor::getName, Function.identity()));

        TreeMap<String, Descriptors.FieldDescriptor> orderedFieldMap = new TreeMap<>((o1, o2) -> {
            if (o1 == null) {
                if (o2 == null) {
                    return 0;
                } else {
                    return -1;
                } //sort nulls first; shouldn't happen here but it's a good habit
            } else if (o2 == null) {
                return 1;
            } else {
                Descriptors.FieldDescriptor field1 = descriptorLookupMap.get(o1);
                Descriptors.FieldDescriptor field2 = descriptorLookupMap.get(o2);
                return Integer.compare(field1.getIndex(), field2.getIndex());
            }
        });
        orderedFieldMap.putAll(descriptorLookupMap);
        return Type.Record.fromFieldDescriptorsMap(orderedFieldMap);
    }
}
