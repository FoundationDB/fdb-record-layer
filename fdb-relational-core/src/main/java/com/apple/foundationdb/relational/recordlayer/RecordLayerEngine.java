/*
 * RecordLayerEngine.java
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

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.relational.api.EmbeddedRelationalEngine;
import com.apple.foundationdb.relational.api.StorageCluster;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplateCatalog;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.catalog.RecordLayerStoreCatalogImpl;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class RecordLayerEngine {

    public static EmbeddedRelationalEngine makeEngine(@Nonnull RecordLayerConfig cfg,
                                                    @Nonnull List<FDBDatabase> databases,
                                                    @Nonnull KeySpace baseKeySpace,
                                                    @Nonnull Supplier<SchemaTemplateCatalog> templateCatalogSupplier) throws RelationalException {
        return makeEngine(cfg, databases, baseKeySpace, templateCatalogSupplier, null);
    }

    public static EmbeddedRelationalEngine makeEngine(@Nonnull RecordLayerConfig cfg,
                                                    @Nonnull List<FDBDatabase> databases,
                                                    @Nonnull KeySpace baseKeySpace,
                                                    @Nonnull Supplier<SchemaTemplateCatalog> templateCatalogSupplier,
                                                    @Nullable FDBStoreTimer timer) throws RelationalException {

        SchemaTemplateCatalog templateCatalog = templateCatalogSupplier.get();
        //TODO(bfines) we need to move StoreCatalog to a per-cluster thing
        RecordLayerStoreCatalogImpl schemaCatalog = new RecordLayerStoreCatalogImpl(baseKeySpace);

        List<StorageCluster> clusters = databases.stream().map(db ->
                new RecordLayerStorageCluster(new DirectFdbConnection(db, timer), baseKeySpace, cfg, schemaCatalog, templateCatalog)).collect(Collectors.toList());
        try (Transaction txn = clusters.get(0).getTransactionManager().createTransaction()) {
            schemaCatalog.initialize(txn);
            txn.commit();
        }

        return new EmbeddedRelationalEngine(clusters);
    }

    private RecordLayerEngine() {
    }
}
