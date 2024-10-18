/*
 * RelationalSQLLine.java
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

package com.apple.foundationdb.relational.cli.sqlline;

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.relational.api.EmbeddedRelationalDriver;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metrics.NoOpMetricRegistry;
import com.apple.foundationdb.relational.recordlayer.DirectFdbConnection;
import com.apple.foundationdb.relational.recordlayer.RecordLayerConfig;
import com.apple.foundationdb.relational.recordlayer.RecordLayerEngine;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;
import com.apple.foundationdb.relational.recordlayer.catalog.StoreCatalogProvider;
import com.apple.foundationdb.relational.recordlayer.ddl.RecordLayerMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;
import org.apache.commons.lang3.ArrayUtils;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;

@SuppressWarnings("PMD.AvoidPrintStackTrace")
public class RelationalSQLLine {

    public static void main(String[] args) throws SQLException {
        FDBDatabase fdbDb = FDBDatabaseFactory.instance().getDatabase();
        DirectFdbConnection fdbConnection = new DirectFdbConnection(fdbDb, NoOpMetricRegistry.INSTANCE);
        RelationalKeyspaceProvider.instance().registerDomainIfNotExists("FRL");
        KeySpace keySpace = RelationalKeyspaceProvider.instance().getKeySpace();
        StoreCatalog storeCatalog;
        try (Transaction txn = fdbConnection.getTransactionManager().createTransaction(Options.NONE)) {
            storeCatalog = StoreCatalogProvider.getCatalog(txn, keySpace);
            txn.commit();
        } catch (RelationalException e) {
            throw new RuntimeException(e);
        }
        RecordLayerConfig rlConfig = RecordLayerConfig.getDefault();
        RecordLayerMetadataOperationsFactory ddlFactory = new RecordLayerMetadataOperationsFactory.Builder()
                .setRlConfig(rlConfig)
                .setBaseKeySpace(keySpace)
                .setStoreCatalog(storeCatalog).build();
        final var embeddedRelationalDriver = new EmbeddedRelationalDriver(RecordLayerEngine.makeEngine(
                rlConfig,
                Collections.singletonList(fdbDb),
                keySpace,
                storeCatalog,
                NoOpMetricRegistry.INSTANCE,
                ddlFactory,
                RelationalPlanCache.buildWithDefaults()));

        DriverManager.registerDriver(embeddedRelationalDriver);

        // Now start SQLLine
        try {
            args = ArrayUtils.addAll(new String[] { "-ac", "com.apple.foundationdb.relational.cli.sqlline.Customize"}, args);
            sqlline.SqlLine.main(args);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DriverManager.deregisterDriver(embeddedRelationalDriver);
        }
    }
}
