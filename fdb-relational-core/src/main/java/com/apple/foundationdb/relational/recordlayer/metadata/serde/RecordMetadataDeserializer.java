/*
 * RecordMetadataDeserializer.java
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

package com.apple.foundationdb.relational.recordlayer.metadata.serde;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.metadata.DataTypeUtils;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.util.Assert;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public class RecordMetadataDeserializer {

    @Nonnull
    private final RecordMetaData recordMetaData;

    public RecordMetadataDeserializer(@Nonnull RecordMetaData recordMetaData) {
        this.recordMetaData = recordMetaData;
    }

    public RecordLayerSchemaTemplate getSchemaTemplate(@Nonnull final String schemaTemplateName, long version) {
        // iterate _only_ over the record types registered in the union descriptor to avoid potentially-expensive
        // deserialization of other descriptors that can never be used by the user.
        final var unionDescriptor = recordMetaData.getUnionDescriptor();

        final var registeredTypes = unionDescriptor.getFields();
        final var schemaTemplateBuilder = RecordLayerSchemaTemplate.newBuilder();
        for (final var registeredType : registeredTypes) {
            switch (registeredType.getType()) {
                case MESSAGE:
                    schemaTemplateBuilder.addTable(generateTable(registeredType.getName()));
                    break;
                case ENUM:
                    // todo (yhatem) this is temporary, we rely on rec layer types to deserliaze protobuf descriptors.
                    final var recordLayerType = new Type.Enum(false, Type.Enum.enumValuesFromProto(registeredType.getEnumType().getValues()), registeredType.getName());
                    schemaTemplateBuilder.addAuxiliaryType((DataType.Named) DataTypeUtils.toRelationalType(recordLayerType));
                    break;
                default:
                    Assert.failUnchecked(String.format("Unexpected type '%s' found in union descriptor!", registeredType.getType()));
                    break;
            }
        }
        return schemaTemplateBuilder
                .setVersion(version)
                .setName(schemaTemplateName)
                .build();
    }

    @Nonnull
    private RecordLayerTable generateTable(@Nonnull final String tableName) {
        final var recordType = recordMetaData.getRecordType(tableName);
        // todo (yhatem) we rely on the record type for deserialization from ProtoBuf for now, later on
        //      we will avoid this step by having our own deserializers.
        final var recordLayerType = Type.Record.fromFieldsWithName(recordType.getName(), false, Type.Record.fromDescriptor(recordType.getDescriptor()).getFields());
        return RecordLayerTable.from(
                recordLayerType,
                recordType.getPrimaryKey(),
                recordType.getIndexes().stream().map(index -> RecordLayerIndex.from(tableName, index)).collect(Collectors.toSet()));
    }
}
