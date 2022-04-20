/*
 * CatalogMetaDataProvider.java
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
import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.generated.CatalogData;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.URI;
import java.util.Locale;

import javax.annotation.Nonnull;

public class CatalogMetaDataProvider implements RecordMetaDataProvider {
    //TODO(bfines) there should probably be a cleaner way to deal with this
    private static final ExtensionRegistry DEFAULT_EXTENSION_REGISTRY;

    static {
        ExtensionRegistry defaultExtensionRegistry = ExtensionRegistry.newInstance();
        RecordMetaDataOptionsProto.registerAllExtensions(defaultExtensionRegistry);
        DEFAULT_EXTENSION_REGISTRY = defaultExtensionRegistry.getUnmodifiable();
    }

    private final StoreCatalog storeCatalog;
    private final URI dbUri;
    private final String schemaName;
    private final Transaction txn;

    private volatile RecordMetaData cachedMetaData;

    public CatalogMetaDataProvider(@Nonnull StoreCatalog storeCatalog,
                                   @Nonnull URI dbUri,
                                   @Nonnull String schemaName,
                                   @Nonnull Transaction txn) {
        this.storeCatalog = storeCatalog;
        this.dbUri = dbUri;
        this.schemaName = schemaName;
        this.txn = txn;
    }

    @Nonnull
    @Override
    public RecordMetaData getRecordMetaData() {
        RecordMetaData metaData = cachedMetaData;
        if (metaData == null) {
            try {
                //TODO(bfines) handling the recordType key is not correct here, because it requires the Table list to always be in the same order
                int typeKey = 0;
                final CatalogData.Schema schema = storeCatalog.loadSchema(txn, dbUri, schemaName);
                DescriptorProtos.FileDescriptorProto fdProto = DescriptorProtos.FileDescriptorProto.parseFrom(schema.getRecord(), DEFAULT_EXTENSION_REGISTRY);

                Descriptors.FileDescriptor fileDesc = Descriptors.FileDescriptor.buildFrom(fdProto,
                        new Descriptors.FileDescriptor[]{RecordMetaDataOptionsProto.getDescriptor()});
                final RecordMetaDataBuilder recordMetaDataBuilder = RecordMetaData.newBuilder().setRecords(fileDesc);
                recordMetaDataBuilder.setVersion(schema.getSchemaVersion());
                //add in the primary keys for each table
                for (CatalogData.Table tbl : schema.getTablesList()) {
                    final RecordTypeBuilder recordType = recordMetaDataBuilder.getRecordType(tbl.getName());
                    recordType.setPrimaryKey(KeyExpression.fromProto(tbl.getPrimaryKey()));
                    recordType.setRecordTypeKey(typeKey);
                    for (CatalogData.Index idx : tbl.getIndexesList()) {
                        RecordMetaDataProto.Index indexDef = idx.getIndexDef();
                        //RecordLayer wants lower-case values for type, and just in case the parser doesn't
                        // obey that, we don't want to break because of it. So here we are double-checking it.
                        indexDef = indexDef.toBuilder().setType(indexDef.getType().toLowerCase(Locale.ROOT)).build();
                        recordMetaDataBuilder.addIndex(tbl.getName(), new Index(indexDef));
                    }
                    typeKey++;
                }
                metaData = recordMetaDataBuilder.build();
                cachedMetaData = metaData;
            } catch (RelationalException e) {
                //TODO(bfines): vomit, but I think this is probably the correct type, because RecordLayer is
                // the thing that's doing the processing here
                throw new RecordCoreException(e);
            } catch (Descriptors.DescriptorValidationException | InvalidProtocolBufferException e) {
                //should never happen
                throw new RuntimeException(new RelationalException(ErrorCode.INTERNAL_ERROR, e));
            }
        }
        return metaData;
    }
}
