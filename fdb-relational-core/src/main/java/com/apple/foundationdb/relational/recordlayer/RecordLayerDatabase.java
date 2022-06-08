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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.RecordStoreDoesNotExistException;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.NoSuchDirectoryException;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.TransactionConfig;
import com.apple.foundationdb.relational.api.TransactionManager;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.ddl.ConstantActionFactory;
import com.apple.foundationdb.relational.api.ddl.DdlQueryFactory;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.InvalidTypeException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.catalog.CachedMetaDataStore;
import com.apple.foundationdb.relational.recordlayer.catalog.RecordMetaDataStore;
import com.apple.foundationdb.relational.recordlayer.catalog.StoreCatalog;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import com.google.common.base.Throwables;

import java.net.URI;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class RecordLayerDatabase extends AbstractDatabase {
    private final FdbConnection fdbDb;
    private final RecordMetaDataStore metaDataStore;
    private final StoreCatalog storeCatalog;
    private final FDBRecordStoreBase.UserVersionChecker userVersionChecker;
    private final int formatVersion;

    private final SerializerRegistry serializerRegistry;
    private final KeySpacePath ksPath;

    public RecordLayerDatabase(FdbConnection fdbDb,
                               RecordMetaDataStore metaDataStore,
                               StoreCatalog storeCatalog,
                               FDBRecordStoreBase.UserVersionChecker userVersionChecker,
                               int formatVersion,
                               SerializerRegistry serializerRegistry,
                               KeySpacePath dbPathPrefix,
                               @Nonnull final ConstantActionFactory constantActionFactory,
                               @Nonnull final DdlQueryFactory ddlQueryFactory) {
        super(constantActionFactory, ddlQueryFactory);
        this.fdbDb = fdbDb;
        this.metaDataStore = new CachedMetaDataStore(metaDataStore);
        this.storeCatalog = storeCatalog;
        this.userVersionChecker = userVersionChecker;
        this.formatVersion = formatVersion;
        this.serializerRegistry = serializerRegistry;
        this.ksPath = dbPathPrefix;
    }

    @Override
    public RelationalConnection connect(@Nullable Transaction transaction, @Nonnull TransactionConfig txnConfig) throws RelationalException {
        if (transaction != null && !(transaction instanceof RecordContextTransaction)) {
            throw new InvalidTypeException("Invalid Transaction type to use to connect to FDB");
        }
        EmbeddedRelationalConnection conn = new EmbeddedRelationalConnection(this, storeCatalog, transaction);
        setConnection(conn);
        return conn;
    }

    @Override
    public TransactionManager getTransactionManager() {
        return getFDBDatabase().getTransactionManager();
    }

    @Override
    public void close() throws RelationalException {
        for (RecordLayerSchema schema : schemas.values()) {
            schema.close();
        }
        schemas.clear();
    }

    @SuppressWarnings("PMD.PreserveStackTrace") //we actually do, the PMD linter just doesn't seem to be able to tell
    FDBRecordStore loadStore(@Nonnull Transaction txn, @Nonnull String schemaName, @Nonnull FDBRecordStoreBase.StoreExistenceCheck existenceCheck) throws RelationalException {
        //TODO(bfines) error handling if this store doesn't exist

        KeySpacePath storePath;
        try {
            storePath = ksPath.add("schema", schemaName);
        } catch (NoSuchDirectoryException nsde) {
            throw new RelationalException("Uninitialized Catalog", ErrorCode.INTERNAL_ERROR, nsde);
        } catch (MetaDataException mde) {
            throw new RelationalException(mde.getMessage(), ErrorCode.UNKNOWN_SCHEMA, mde);
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }

        try {
            return FDBRecordStore.newBuilder()
                    .setKeySpacePath(storePath)
                    .setSerializer(serializerRegistry.loadSerializer(storePath))
                    //TODO(bfines) replace this schema template with an actual mapping structure based on the storePath
                    .setMetaDataProvider(metaDataStore.loadMetaData(txn, KeySpaceUtils.pathToUri(this.ksPath), schemaName))
                    .setUserVersionChecker(userVersionChecker)
                    .setFormatVersion(formatVersion)
                    .setContext(txn.unwrap(FDBRecordContext.class))
                    .createOrOpen(existenceCheck);
        } catch (RecordCoreException rce) {
            Throwable cause = Throwables.getRootCause(rce);
            if (cause instanceof RecordStoreDoesNotExistException) {
                throw new RelationalException("Schema does not exist. Schema: <" + schemaName + ">", ErrorCode.SCHEMA_NOT_FOUND, cause);
            } else {
                throw new RelationalException("Schema <" + schemaName + "> cannot be found", ErrorCode.UNKNOWN_SCHEMA, cause);
            }
        }
    }

    FdbConnection getFDBDatabase() {
        return fdbDb;
    }

    /* ****************************************************************************************************************/
    /* private helper methods */

    @Override
    public FDBRecordStore loadRecordStore(@Nonnull String schemaId, @Nonnull FDBRecordStoreBase.StoreExistenceCheck existenceCheck) throws RelationalException {
        return loadStore(this.connection.transaction, schemaId, existenceCheck);
    }

    @Override
    public URI getURI() {
        return KeySpaceUtils.pathToUri(ksPath);
    }

}
