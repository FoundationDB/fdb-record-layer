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
import com.apple.foundationdb.relational.recordlayer.catalog.systables.DatabaseInfoSystemTable;
import com.apple.foundationdb.relational.recordlayer.catalog.systables.SchemaSystemTable;
import com.apple.foundationdb.relational.recordlayer.catalog.systables.SystemTableRegistry;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.URI;
import java.util.Locale;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.relational.recordlayer.catalog.RecordLayerStoreCatalogImpl.SYS_DB;

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
                //add in the primary keys for each table
                for (CatalogData.Table tbl : schema.getTablesList()) {
                    final RecordTypeBuilder recordType = recordMetaDataBuilder.getRecordType(tbl.getName());
                    recordType.setPrimaryKey(KeyExpression.fromProto(RecordMetaDataProto.KeyExpression.parseFrom(tbl.getPrimaryKey())));
                    setRecordTypeKey(recordType, schema.getDatabaseId(), schema.getSchemaName(), tbl.getName(), typeKey);
                    for (CatalogData.Index idx : tbl.getIndexesList()) {
                        RecordMetaDataProto.Index indexDef = RecordMetaDataProto.Index.parseFrom(idx.getIndexDef());
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

    /**
     * This is a temporary solution until we have a more reliable way of setting the record type keys in a deterministic
     * way.
     *
     * @param recordTypeBuilder The record type build.
     * @param databaseId The database id.
     * @param schemaName The schema name.
     * @param tableName The table name.
     * @param typeKey The type key to use for user tables.
     */
    private void setRecordTypeKey(@Nonnull RecordTypeBuilder recordTypeBuilder,
                                  @Nonnull final String databaseId,
                                  @Nonnull final String schemaName,
                                  @Nonnull final String tableName,
                                  int typeKey) {
        if (databaseId.equals(SYS_DB) && "catalog".equals(schemaName) && tableName.equals(SchemaSystemTable.TABLE_NAME)) {
            recordTypeBuilder.setRecordTypeKey(SystemTableRegistry.SCHEMA_RECORD_TYPE_KEY);
        } else if (databaseId.equals(SYS_DB) && "catalog".equals(schemaName) && tableName.equals(DatabaseInfoSystemTable.TABLE_NAME)) {
            recordTypeBuilder.setRecordTypeKey(SystemTableRegistry.DATABASE_INFO_RECORD_TYPE_KEY);
        } else {
            recordTypeBuilder.setRecordTypeKey(typeKey);
        }
    }
}
