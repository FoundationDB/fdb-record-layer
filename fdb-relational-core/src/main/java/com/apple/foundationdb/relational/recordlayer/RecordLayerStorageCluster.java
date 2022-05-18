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
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.StorageCluster;
import com.apple.foundationdb.relational.api.TransactionManager;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplateCatalog;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.catalog.RelationalDatabase;
import com.apple.foundationdb.relational.api.ddl.CatalogQueryFactory;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.catalog.CatalogMetaDataStore;
import com.apple.foundationdb.relational.recordlayer.ddl.RecordLayerConstantActionFactory;

import java.net.URI;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Represents a single FDBCluster, and all of it's associated logical components (Catalog, TransactionManager, etc).
 */
public class RecordLayerStorageCluster implements StorageCluster {
    private final StoreCatalog catalog;
    private final RecordLayerConfig rlConfiguration;
    private final FdbConnection fdb;
    private final KeySpace keySpace;
    private final SchemaTemplateCatalog schemaTemplateCatalog;

    public RecordLayerStorageCluster(FdbConnection connection,
                                     KeySpace keySpace,
                                     RecordLayerConfig rlConfig,
                                     StoreCatalog storeCatalog,
                                     SchemaTemplateCatalog schemaTemplateCatalog) {
        //TODO(bfines) we shouldn't use FDBStoreTimer, we should use our own abstraction that can be easily disabled
        this.fdb = connection;
        this.keySpace = keySpace;
        this.catalog = storeCatalog;
        this.schemaTemplateCatalog = schemaTemplateCatalog;
        this.rlConfiguration = rlConfig;
    }

    /**
     * Load the database from the cluster.
     *
     * @param url the path to the database
     * @return the database at that path, or {@code null} if no database with that path exists in this cluster
     */
    @Override
    @Nullable
    public RelationalDatabase loadDatabase(@Nonnull URI url,
                                         @Nonnull Options connOptions) throws RelationalException {
        //TODO(bfines) validate that this is actually connected to the correct cluster for that location
        // to do that, we will want to use the Catalog to pull database information
        KeySpacePath ksPath = KeySpaceUtils.uriToPath(url, keySpace);

        final var constantActionFactory = new RecordLayerConstantActionFactory.Builder()
                .setRlConfig(rlConfiguration)
                .setBaseKeySpace(keySpace)
                .setTemplateCatalog(schemaTemplateCatalog)
                .setStoreCatalog(catalog)
                .build();

        final var ddlQueryFactory = new CatalogQueryFactory(catalog, schemaTemplateCatalog);

        return new RecordLayerDatabase(fdb, new CatalogMetaDataStore(catalog),
                catalog,
                rlConfiguration.getUserVersionChecker(),
                rlConfiguration.getFormatVersion(),
                rlConfiguration.getSerializerRegistry(),
                ksPath, constantActionFactory, ddlQueryFactory);
    }

    @Override
    public TransactionManager getTransactionManager() {
        return fdb.getTransactionManager();
    }
}
