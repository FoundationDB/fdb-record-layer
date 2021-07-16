/*
 * RecordLayerDatabase.java
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

import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.relational.api.OperationOption;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalException;
import com.apple.foundationdb.relational.api.catalog.DatabaseSchema;
import com.apple.foundationdb.relational.api.catalog.RelationalDatabase;
import com.apple.foundationdb.relational.recordlayer.catalog.RecordMetaDataStore;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.HashMap;
import java.util.Map;

@NotThreadSafe
public class RecordLayerDatabase implements RelationalDatabase {
    private final RecordMetaDataStore metaDataProvider;
    private final FDBRecordStoreBase.UserVersionChecker userVersionChecker;
    private final int formatVersion;

    private final SerializerRegistry serializerRegistry;
    private final KeySpacePath ksPath;

    private RecordStoreConnection connection;

    private final Map<String, RecordLayerSchema> schemas = new HashMap<>();

    public RecordLayerDatabase(RecordMetaDataStore metaDataProvider,
                               FDBRecordStoreBase.UserVersionChecker userVersionChecker,
                               int formatVersion,
                               SerializerRegistry serializerRegistry,
                               KeySpacePath dbPathPrefix) {
        this.metaDataProvider = metaDataProvider;
        this.userVersionChecker = userVersionChecker;
        this.formatVersion = formatVersion;
        this.serializerRegistry = serializerRegistry;
        this.ksPath = dbPathPrefix;
    }

    void setConnection(@Nonnull RecordStoreConnection conn){
        this.connection = conn;
    }

    @Override
    public @Nonnull DatabaseSchema loadSchema(@Nonnull String schemaId, @Nonnull Options options) throws RelationalException{
        RecordLayerSchema schema = schemas.get(schemaId);
        boolean putBack = false;
        if(schema==null){
            schema = new RecordLayerSchema(schemaId,this,connection);
            putBack = true;
        }
        if(options.hasOption(OperationOption.FORCE_VERIFY_DDL)){
            if(!this.connection.inActiveTransaction()){
                this.connection.beginTransaction();
                try{
                    schema.loadStore();
                } finally{
                    this.connection.rollback();
                }
            }else{
                schema.loadStore();
            }
        }

        if(putBack){
            schemas.put(schemaId,schema);
        }
        return schema;
    }

    @Override
    public void close() throws RelationalException {
        for(RecordLayerSchema schema: schemas.values()){
            schema.close();
        }
        schemas.clear();
    }

    FDBRecordStore loadStore(FDBRecordContext txn, String storeName) {
        //TODO(bfines) error handling if this store doesn't exist

        //TODO(bfines) this is probably not right in general
        final KeySpacePath storePath = ksPath.add(storeName);

        return FDBRecordStore.newBuilder()
                .setKeySpacePath(storePath)
                .setSerializer(serializerRegistry.loadSerializer(storePath))
                .setMetaDataProvider(metaDataProvider.loadMetaData(storeName))
                .setUserVersionChecker(userVersionChecker)
                .setFormatVersion(formatVersion)
                .setContext(txn)
                .createOrOpen();
    }

    /* ****************************************************************************************************************/
    /* private helper methods*/
    FDBRecordStore loadRecordStore(String schemaId) {
        try{
            return loadStore(this.connection.transaction.unwrap(FDBRecordContext.class), schemaId);
        }catch(MetaDataException mde){
            throw new RelationalException(mde.getMessage(),RelationalException.ErrorCode.UNKNOWN_SCHEMA);
        }
    }

    void clearDatabase(@Nonnull FDBRecordContext context) {
        FDBRecordStore.deleteStore(context, ksPath);
    }

}
