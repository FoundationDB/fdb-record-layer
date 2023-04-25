/*
 * RecordLayerStorageCluster.java
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

import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.StorageCluster;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.TransactionManager;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.catalog.RelationalDatabase;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.catalog.CatalogMetaDataStore;
import com.apple.foundationdb.relational.recordlayer.ddl.RecordLayerCatalogQueryFactory;
import com.apple.foundationdb.relational.recordlayer.ddl.RecordLayerMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Represents a single FDBCluster, and all of it's associated logical components (Catalog, TransactionManager, etc).
 */
public class RecordLayerStorageCluster implements StorageCluster {
    private final StoreCatalog catalog;
    private final RecordLayerConfig rlConfiguration;
    private final FdbConnection fdb;
    private final KeySpace keySpace;
    private final RecordLayerMetadataOperationsFactory ddlFactory;

    @Nullable
    private final RelationalPlanCache planCache;

    public RecordLayerStorageCluster(FdbConnection connection,
                                     KeySpace keySpace,
                                     RecordLayerConfig rlConfig,
                                     StoreCatalog storeCatalog,
                                     RelationalPlanCache planCache,
                                     RecordLayerMetadataOperationsFactory ddlFactory) {
        //TODO(bfines) we shouldn't use FDBStoreTimer, we should use our own abstraction that can be easily disabled
        this.fdb = connection;
        this.keySpace = keySpace;
        this.catalog = storeCatalog;
        this.rlConfiguration = rlConfig;
        this.ddlFactory = ddlFactory;
        this.planCache = planCache;
    }

    private Map<String, String> parseConnectionQueryString(@Nullable String queryStr) {
        if (queryStr == null) {
            return Collections.emptyMap();
        }

        Map<String, String> queryFields = new HashMap<>();
        String[] connQuery = queryStr.split(",");
        for (String connStr : connQuery) {
            String[] kvPair = connStr.split("=");
            if (kvPair.length < 2) {
                continue; //skip those that aren't in the form key=value
            }
            queryFields.put(kvPair[0].toUpperCase(Locale.ROOT), kvPair[1]);
        }
        return queryFields;
    }

    /**
     * Load the database from the cluster.
     *
     * @param url the path to the database
     * @return the database at that path, or {@code null} if no database with that path exists in this cluster
     */

    @SpotBugsSuppressWarnings(value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE", justification = "false positive. https://github.com/spotbugs/spotbugs/issues/1694")
    @Override
    @Nullable
    public RelationalDatabase loadDatabase(@Nonnull URI url,
                                         @Nonnull Options connOptions) throws RelationalException {
        Map<String, String> connectionOptions = parseConnectionQueryString(url.getQuery());
        String presetSchema = connectionOptions.get("SCHEMA");
        try (Transaction txn = getTransactionManager().createTransaction(Options.NONE)) {
            if (presetSchema != null) {
                //if the schema does not exist, then an error will be thrown, and the schema can't
                //exist if the database doesn't, so we get two calls for the price of one
                try {
                    catalog.loadSchema(txn, url, presetSchema);
                } catch (RelationalException ve) {
                    if (ve.getErrorCode() == ErrorCode.UNDEFINED_SCHEMA) {
                        //this could be because the database doesn't exist, OR it could be because the schema
                        //doesn't exist within this database. Let's find out which
                        if (!catalog.doesDatabaseExist(txn, url)) {
                            return null; // the database doesn't exist either
                        }
                    }
                    throw ve;
                }
            } else if (!catalog.doesDatabaseExist(txn, url)) {
                return null;
            }
        }
        final var databasePath = RelationalKeyspaceProvider.toDatabasePath(url, keySpace);
        final var ddlQueryFactory = new RecordLayerCatalogQueryFactory(catalog);

        return new RecordLayerDatabase(fdb, new CatalogMetaDataStore(catalog),
                catalog,
                rlConfiguration,
                databasePath,
                ddlFactory,
                ddlQueryFactory,
                planCache,
                presetSchema,
                connOptions);
    }

    @Override
    public TransactionManager getTransactionManager() {
        return fdb.getTransactionManager();
    }
}
