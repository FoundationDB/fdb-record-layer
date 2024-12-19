/*
 * LocatableResolverMetaDataProvider.java
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

package com.apple.foundationdb.relational.recordlayer.storage;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.ResolverStateProto;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverResult;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

final class LocatableResolverMetaDataProvider implements RecordMetaDataProvider {
    private static volatile LocatableResolverMetaDataProvider memoizedInstance;

    static final String INTERNING_TYPE_NAME = "Interning";
    static final String RESOLVER_STATE_TYPE_NAME = "ResolverState";

    static final String KEY_FIELD_NAME = "key";
    static final String VALUE_FIELD_NAME = "value";
    static final String META_DATA_FIELD_NAME = "meta_data";
    static final String VERSION_FIELD_NAME = "version";
    static final String LOCK_FIELD_NAME = "lock";

    static final String REVERSE_INDEX_NAME = "reverse_interning";

    private static final DataType.EnumType WRITE_LOCK_ENUM = DataType.EnumType.from("WriteLock",
            List.of(
                    DataType.EnumType.EnumValue.of("UNLOCKED", 1),
                    DataType.EnumType.EnumValue.of("WRITE_LOCKED", 2),
                    DataType.EnumType.EnumValue.of("RETIRED", 3)
            ), true);

    private static final RecordLayerSchemaTemplate SCHEMA_TEMPLATE = RecordLayerSchemaTemplate.newBuilder()
            .setName("locatable_resolver")
            .setVersion(1)
            .addTable(RecordLayerTable.newBuilder(false)
                    .setName(INTERNING_TYPE_NAME)
                    .addColumn(RecordLayerColumn.newBuilder()
                            .setDataType(DataType.Primitives.STRING.type())
                            .setName(KEY_FIELD_NAME)
                            .build())
                    .addColumn(RecordLayerColumn.newBuilder()
                            .setDataType(DataType.Primitives.LONG.type())
                            .setName(VALUE_FIELD_NAME)
                            .build())
                    .addColumn(RecordLayerColumn.newBuilder()
                            .setDataType(DataType.Primitives.NULLABLE_BYTES.type())
                            .setName(META_DATA_FIELD_NAME)
                            .build())
                    .addPrimaryKeyPart(List.of(KEY_FIELD_NAME))
                    .addIndex(RecordLayerIndex.newBuilder()
                            .setName(REVERSE_INDEX_NAME)
                            .setTableName(INTERNING_TYPE_NAME)
                            .setIndexType(IndexTypes.VALUE)
                            .setKeyExpression(Key.Expressions.field(VALUE_FIELD_NAME))
                            .setUnique(true)
                            .build())
                    .build())
            .addAuxiliaryType(WRITE_LOCK_ENUM)
            .addTable(RecordLayerTable.newBuilder(false)
                    .setName(RESOLVER_STATE_TYPE_NAME)
                    .addColumn(RecordLayerColumn.newBuilder()
                            .setDataType(WRITE_LOCK_ENUM)
                            .setName(LOCK_FIELD_NAME)
                            .build())
                    .addColumn(RecordLayerColumn.newBuilder()
                            .setDataType(DataType.Primitives.INTEGER.type())
                            .setName(VERSION_FIELD_NAME)
                            .build())
                    .build())
            .build();

    @Nonnull
    private final RecordMetaData metaData;
    @Nonnull
    private final Object interningTypeKey;
    @Nonnull
    private final Object resolverStateTypeKey;

    private LocatableResolverMetaDataProvider(@Nonnull RecordMetaData metaData) {
        this.metaData = metaData;
        interningTypeKey = metaData.getRecordType(INTERNING_TYPE_NAME).getRecordTypeKey();
        resolverStateTypeKey = metaData.getRecordType(RESOLVER_STATE_TYPE_NAME).getRecordTypeKey();
    }

    Message wrapInterning(@Nonnull String key, long value, @Nullable byte[] metaData) {
        Descriptors.Descriptor descriptor = this.metaData.getRecordType(INTERNING_TYPE_NAME).getDescriptor();
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName(KEY_FIELD_NAME), key)
                .setField(descriptor.findFieldByName(VALUE_FIELD_NAME), value);
        if (metaData != null) {
            builder.setField(descriptor.findFieldByName(META_DATA_FIELD_NAME), ByteString.copyFrom(metaData));
        }
        return builder.build();
    }

    @Nullable
    Message wrapResolverResult(@Nonnull String key, @Nullable ResolverResult result) {
        if (result == null) {
            return null;
        }
        return wrapInterning(key, result.getValue(), result.getMetadata());
    }

    @Nonnull
    Message wrapResolverState(ResolverStateProto.State state) {
        Descriptors.Descriptor descriptor = metaData.getRecordType(RESOLVER_STATE_TYPE_NAME).getDescriptor();
        return DynamicMessage.newBuilder(descriptor)
                .setField(descriptor.findFieldByName(VERSION_FIELD_NAME), state.getVersion())
                .setField(descriptor.findFieldByName("lock"), descriptor.getFile().findEnumTypeByName(WRITE_LOCK_ENUM.getName()).findValueByName(state.getLock().name()))
                .build();
    }

    static LocatableResolverMetaDataProvider instance() throws RelationalException {
        if (memoizedInstance == null) {
            synchronized (LocatableResolverMetaDataProvider.class) {
                if (memoizedInstance == null) {
                    RecordMetaData metaData = SCHEMA_TEMPLATE.toRecordMetadata();
                    memoizedInstance = new LocatableResolverMetaDataProvider(metaData);
                }
            }
        }
        return memoizedInstance;
    }

    public static SchemaTemplate getSchemaTemplate() {
        return SCHEMA_TEMPLATE;
    }

    @Nonnull
    @Override
    public RecordMetaData getRecordMetaData() {
        return metaData;
    }

    @Nonnull
    public Object getInterningTypeKey() {
        return interningTypeKey;
    }

    @Nonnull
    public Object getResolverStateTypeKey() {
        return resolverStateTypeKey;
    }
}
