/*
 * RecordLayerDatabase.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.TransactionManager;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.ddl.DdlQueryFactory;
import com.apple.foundationdb.relational.api.ddl.MetadataOperationsFactory;
import com.apple.foundationdb.relational.api.exceptions.InvalidTypeException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.catalog.CachedMetaDataStore;
import com.apple.foundationdb.relational.recordlayer.catalog.RecordMetaDataStore;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;
import com.apple.foundationdb.relational.recordlayer.storage.BackingRecordStore;
import com.apple.foundationdb.relational.recordlayer.storage.StoreConfig;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.net.URI;
import java.sql.SQLException;

@NotThreadSafe
@API(API.Status.EXPERIMENTAL)
public class RecordLayerDatabase extends AbstractDatabase {
    private final FdbConnection fdbDb;
    private final RecordMetaDataStore metaDataStore;
    private final StoreCatalog storeCatalog;
    private final RecordLayerConfig recordLayerConfig;

    private final RelationalKeyspaceProvider.RelationalDatabasePath databasePath;

    @Nullable
    private final String defaultSchema;

    /**
     * Create a new RecordLayerDatabase instance.
     *
     * @param fdbDb                 the connection to FDB
     * @param metaDataStore         a store to fetch RecordMetaData objects from
     * @param storeCatalog          the catalog when we need to do catalog lookups
     * @param config                general RecordLayer configurations
     * @param databasePath          the path to the database that this represents
     * @param metadataOperationsFactory a factory for constant actions
     * @param ddlQueryFactory       a factory for DDL queries
     * @param planCache             a plan cache to use, or null if no caching should be enabled.
     * @param defaultSchema         if not null, then a pre-validated schema to start all connections with.
     * @param options               any database-level options.
     */
    public RecordLayerDatabase(FdbConnection fdbDb,
                               RecordMetaDataStore metaDataStore,
                               StoreCatalog storeCatalog,
                               RecordLayerConfig config,
                               RelationalKeyspaceProvider.RelationalDatabasePath databasePath,
                               @Nonnull final MetadataOperationsFactory metadataOperationsFactory,
                               @Nonnull final DdlQueryFactory ddlQueryFactory,
                               @Nullable RelationalPlanCache planCache,
                               @Nullable String defaultSchema,
                               @Nonnull Options options) {
        super(metadataOperationsFactory, ddlQueryFactory, planCache, options);
        this.fdbDb = fdbDb;
        this.metaDataStore = new CachedMetaDataStore(metaDataStore);
        this.storeCatalog = storeCatalog;
        this.recordLayerConfig = config;
        this.databasePath = databasePath;
        this.defaultSchema = defaultSchema;
    }

    @Override
    public RelationalConnection connect(@Nullable Transaction transaction) throws RelationalException {
        if (transaction != null && !(transaction instanceof RecordContextTransaction)) {
            throw new InvalidTypeException("Invalid Transaction type to use to connect to FDB");
        }
        EmbeddedRelationalConnection conn = new EmbeddedRelationalConnection(this, storeCatalog, transaction, options);
        setConnection(conn);
        if (defaultSchema != null) {
            /*
             * If we have set a default schema here, then we should have already validated that it is correct and exists
             * and everything, so we don't duplicate the checking here.
             */
            try {
                conn.setSchema(defaultSchema, false);
            } catch (SQLException e) {
                throw new RelationalException(e);
            }
        }
        return conn;
    }

    @Override
    public TransactionManager getTransactionManager() {
        return getFDBDatabase().getTransactionManager();
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public void close() throws RelationalException {
        for (RecordLayerSchema schema : schemas.values()) {
            schema.close();
        }
        schemas.clear();
    }

    BackingRecordStore loadStore(@Nonnull Transaction txn, @Nonnull String schemaName, @Nonnull FDBRecordStoreBase.StoreExistenceCheck existenceCheck) throws RelationalException {
        StoreConfig storeConfig = StoreConfig.create(recordLayerConfig, schemaName, databasePath, metaDataStore, txn);
        return BackingRecordStore.load(txn, storeConfig, existenceCheck);
    }

    FdbConnection getFDBDatabase() {
        return fdbDb;
    }

    /* ****************************************************************************************************************/
    /* private helper methods */

    @Override
    public BackingRecordStore loadRecordStore(@Nonnull String schemaId, @Nonnull FDBRecordStoreBase.StoreExistenceCheck existenceCheck) throws RelationalException {
        return loadStore(getCurrentTransaction(), schemaId, existenceCheck);
    }

    @Override
    public URI getURI() {
        return databasePath.toUri();
    }
}
