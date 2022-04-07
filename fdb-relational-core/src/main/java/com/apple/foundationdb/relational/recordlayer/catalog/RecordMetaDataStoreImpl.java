/*
 * RecordMetaDataStoreImpl.java
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

package com.apple.foundationdb.relational.recordlayer.catalog;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.generated.CatalogData;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.InvalidProtocolBufferException;

// potentially implements interface RecordMetaDataStore
public final class RecordMetaDataStoreImpl {
    public static RecordMetaDataProvider buildRecordMetaData(CatalogData.Schema schema) throws InvalidProtocolBufferException, RelationalException {
        // set up ExtensionRegistry for de-serialize bytes
        com.google.protobuf.ExtensionRegistry registry =
                com.google.protobuf.ExtensionRegistry.newInstance();
        registry.add(com.apple.foundationdb.record.RecordMetaDataOptionsProto.field);
        registry.add(com.apple.foundationdb.record.RecordMetaDataOptionsProto.record);
        // de-serialize bytes FileDescriptorProto
        DescriptorProtos.FileDescriptorProto schemaFileDescriptor = DescriptorProtos.FileDescriptorProto.parseFrom(schema.getRecord(), registry);
        RecordMetaDataProto.MetaData metaData = RecordMetaDataProto.MetaData.newBuilder().setRecords(schemaFileDescriptor).build();
        RecordMetaDataBuilder recordMetaDataBuilder = RecordMetaData.newBuilder().setRecords(metaData);
        // set primary key
        for (CatalogData.Table table : schema.getTablesList()) {
            recordMetaDataBuilder = setPrimaryKey(table, recordMetaDataBuilder);
        }
        return recordMetaDataBuilder.build();
    }

    private static RecordMetaDataBuilder setPrimaryKey(CatalogData.Table table, RecordMetaDataBuilder recordMetaDataBuilder) throws RelationalException {
        try {
            recordMetaDataBuilder.getRecordType(table.getName()).setPrimaryKey(KeyExpression.fromProto(table.getPrimaryKey()));
            return recordMetaDataBuilder;
        } catch (MetaDataException ex) {
            throw new RelationalException(ErrorCode.UNDEFINED_TABLE, ex);
        } catch (RecordCoreException ex) {
            throw new RelationalException(ErrorCode.INVALID_PARAMETER, ex);
        }
    }

    private RecordMetaDataStoreImpl() {
    }
}
