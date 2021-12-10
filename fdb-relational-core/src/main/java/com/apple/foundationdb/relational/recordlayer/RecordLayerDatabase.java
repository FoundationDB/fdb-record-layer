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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.RecordStoreDoesNotExistException;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.NoSuchDirectoryException;
import com.apple.foundationdb.relational.api.OperationOption;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.catalog.RelationalDatabase;
import com.apple.foundationdb.relational.recordlayer.catalog.RecordLayerCatalog;
import com.apple.foundationdb.relational.recordlayer.catalog.RecordMetaDataStore;
import com.google.common.base.Throwables;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

@NotThreadSafe
public class RecordLayerDatabase implements RelationalDatabase {
    private final FDBDatabase fdbDb;
    private final RecordMetaDataStore metaDataStore;
    private final FDBRecordStoreBase.UserVersionChecker userVersionChecker;
    private final int formatVersion;

    private final SerializerRegistry serializerRegistry;
    private final KeySpacePath ksPath;

    private RecordStoreConnection connection;

    private final Map<String, RecordLayerSchema> schemas = new HashMap<>();

    private final RecordLayerCatalog catalog;

    @Nullable
    private final FDBStoreTimer storeTimer;

    public RecordLayerDatabase(FDBDatabase fdbDb,
                               RecordMetaDataStore metaDataStore,
                               FDBRecordStoreBase.UserVersionChecker userVersionChecker,
                               int formatVersion,
                               SerializerRegistry serializerRegistry,
                               KeySpacePath dbPathPrefix,
                               @Nullable FDBStoreTimer storeTimer,
                               RecordLayerCatalog catalog) {
        this.fdbDb = fdbDb;
        this.metaDataStore = metaDataStore;
        this.userVersionChecker = userVersionChecker;
        this.formatVersion = formatVersion;
        this.serializerRegistry = serializerRegistry;
        this.ksPath = dbPathPrefix;
        this.storeTimer = storeTimer;
        this.catalog = catalog;
    }

    void setConnection(@Nonnull RecordStoreConnection conn) {
        this.connection = conn;
    }

    @Override
    public @Nonnull RecordLayerSchema loadSchema(@Nonnull String schemaId, @Nonnull Options options) throws RelationalException {
        RecordLayerSchema schema = schemas.get(schemaId);
        boolean putBack = false;
        if (schema == null) {
            // The SchemaExistenceCheck from the options is only taken when the schema is created firstly
            // It is an immutable parameter for the schema and the options for the following operations on that schema are ignored
            schema = new RecordLayerSchema(schemaId, this, connection, options);
            putBack = true;
        }
        if (options.hasOption(OperationOption.FORCE_VERIFY_DDL)) {
            if (!this.connection.inActiveTransaction()) {
                this.connection.beginTransaction();
                try {
                    schema.loadStore();
                } finally {
                    this.connection.rollback();
                }
            } else {
                schema.loadStore();
            }
        }

        if (putBack) {
            schemas.put(schemaId, schema);
        }
        return schema;
    }

    @Override
    public void close() throws RelationalException {
        for (RecordLayerSchema schema : schemas.values()) {
            schema.close();
        }
        schemas.clear();
    }

    FDBRecordStore loadStore(@Nonnull FDBRecordContext txn, @Nonnull String storeName, @Nonnull FDBRecordStoreBase.StoreExistenceCheck existenceCheck) {
        //TODO(bfines) error handling if this store doesn't exist

        final KeySpacePath storePath = ksPath.add(storeName);

        try {
            return FDBRecordStore.newBuilder()
                    .setKeySpacePath(storePath)
                    .setSerializer(serializerRegistry.loadSerializer(storePath))
                    //TODO(bfines) replace this schema template with an actual mapping structure based on the storePath
                    .setMetaDataProvider(metaDataStore.loadMetaData(KeySpaceUtils.pathToUri(storePath)))
                    .setUserVersionChecker(userVersionChecker)
                    .setFormatVersion(formatVersion)
                    .setContext(txn)
                    .createOrOpen(existenceCheck);
        } catch (RecordCoreException rce) {
            Throwable cause = Throwables.getRootCause(rce);
            if (cause instanceof RecordStoreDoesNotExistException) {
                throw new RelationalException("Schema does not exist. Schema: <" + storeName + ">", RelationalException.ErrorCode.SCHEMA_NOT_FOUND, cause);
            } else {
                throw new RelationalException("Schema <" + storeName + "> cannot be found", RelationalException.ErrorCode.UNKNOWN_SCHEMA, cause);
            }
        }
    }

    FDBDatabase getFDBDatabase() {
        return fdbDb;
    }

    KeySpace getKeySpace() {
        return catalog.getKeySpace();
    }

    /* ****************************************************************************************************************/
    /* private helper methods */

    FDBRecordStore loadRecordStore(@Nonnull String schemaId, @Nonnull FDBRecordStoreBase.StoreExistenceCheck existenceCheck) {
        try {
            return loadStore(this.connection.transaction.unwrap(FDBRecordContext.class), schemaId, existenceCheck);
        } catch (NoSuchDirectoryException nsde) {
            throw new RelationalException("Unknown schema <" + schemaId + ">", RelationalException.ErrorCode.UNKNOWN_SCHEMA, nsde);
        } catch (MetaDataException mde) {
            throw new RelationalException(mde.getMessage(), RelationalException.ErrorCode.UNKNOWN_SCHEMA, mde);
        }
    }

    void clearDatabase(@Nonnull FDBRecordContext context) {
        FDBRecordStore.deleteStore(context, ksPath);
    }

    public URI getPath() {
        return KeySpaceUtils.pathToUri(ksPath);
    }

    @Nullable
    FDBStoreTimer getStoreTimer() {
        return storeTimer;
    }
}
