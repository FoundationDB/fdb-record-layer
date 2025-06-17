/*
 * TypeRenamer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TypeRenamer {
    @Nonnull
    private final Function<String, String> renamer;

    public TypeRenamer(@Nonnull final Function<String, String> renamer) {
        this.renamer = renamer;
    }

    public void modify(@Nonnull RecordMetaDataProto.MetaData.Builder metaDataBuilder,
                       @Nonnull Descriptors.FileDescriptor[] dependencies) {
        final DescriptorProtos.FileDescriptorProto.Builder protoFile = metaDataBuilder.getRecordsBuilder();
        final Descriptors.FileDescriptor fileDescriptor = RecordMetaDataBuilder.buildFileDescriptor(metaDataBuilder.getRecords(), dependencies);
        final Descriptors.Descriptor unionDescriptor = RecordMetaDataBuilder.fetchUnionDescriptor(fileDescriptor);
        final Set<String> unionFieldTypes = unionDescriptor.getFields().stream()
                .map(field -> field.getMessageType().getName()).collect(Collectors.toSet());
        Map<String, String> nameMapping = new HashMap<>();
        for (var messageType : protoFile.getMessageTypeBuilderList()) {
            if (unionFieldTypes.contains(messageType.getName())) {
                if (nameMapping.containsKey(messageType.getName())) {
                    throw new MetaDataException("Duplicate type name")
                            .addLogInfo(LogMessageKeys.RECORD_TYPE, messageType.getName());
                }
                final String newName = renamer.apply(messageType.getName());
                nameMapping.put(messageType.getName(), newName);
                messageType.setName(newName);
            }
        }
        for (var messageType : protoFile.getMessageTypeBuilderList()) {
            renameFieldTypes(messageType, nameMapping);
        }

        for (var recordType : metaDataBuilder.getRecordTypesBuilderList()) {
            final String newName = getNewName(nameMapping, recordType.getName());
            recordType.setName(newName);
        }
        for (var index : metaDataBuilder.getIndexesBuilderList()) {
            final List<String> newTypes = index.getRecordTypeList().stream()
                    .map(recordType -> getNewName(nameMapping, recordType))
                    .collect(Collectors.toList());
            index.clearRecordType()
                    .addAllRecordType(newTypes);
        }
        for (var unnested : metaDataBuilder.getUnnestedRecordTypesBuilderList()) {
            for (var constituent : unnested.getNestedConstituentsBuilderList()) {
                // The nested constituents would most likely be nested types, not record types, and thus would not be
                // renamed
                if (constituent.getParent().isEmpty()) {
                    constituent.setTypeName(getNewName(nameMapping, constituent.getTypeName()));
                } else {
                    final String newName = nameMapping.get(constituent.getTypeName());
                    if (newName != null) {
                        constituent.setTypeName(getNewName(nameMapping, constituent.getTypeName()));
                    }
                }
            }
        }
        for (var joined : metaDataBuilder.getJoinedRecordTypesBuilderList()) {
            for (var constituent : joined.getJoinConstituentsBuilderList()) {
                constituent.setRecordType(getNewName(nameMapping, constituent.getRecordType()));
            }
        }
        // TODO error if there are userDefined functions
    }

    @Nonnull
    private static String getNewName(final Map<String, String> nameMapping, final String oldName) {
        final String newName = nameMapping.get(oldName);
        if (newName == null) {
            throw new MetaDataException("RecordType references ")
                    .addLogInfo(LogMessageKeys.RECORD_TYPE, oldName);
        }
        return newName;
    }

    private static void renameFieldTypes(@Nonnull final DescriptorProtos.DescriptorProto.Builder messageType,
                                         @Nonnull final Map<String, String> nameMapping) {
        for (final DescriptorProtos.FieldDescriptorProto.Builder field : messageType.getFieldBuilderList()) {
            if (field.getType() == DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE
                    && nameMapping.containsKey(field.getTypeName())) {
                field.setTypeName(nameMapping.get(field.getTypeName()));
            }
        }
        for (final DescriptorProtos.DescriptorProto.Builder nestedType : messageType.getNestedTypeBuilderList()) {
            renameFieldTypes(nestedType, nameMapping);
        }
    }
}
