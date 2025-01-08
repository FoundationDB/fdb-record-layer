/*
 * EmbeddedRelationalExtension.java
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

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.provider.foundationdb.APIVersion;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.relational.api.EmbeddedRelationalDriver;
import com.apple.foundationdb.relational.api.EmbeddedRelationalEngine;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalDriver;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.catalog.StoreCatalogProvider;
import com.apple.foundationdb.relational.recordlayer.ddl.RecordLayerMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;

import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.function.Supplier;

@API(API.Status.EXPERIMENTAL)
public class EmbeddedRelationalExtension implements RelationalExtension, BeforeEachCallback, AfterEachCallback {

    private final KeySpace keySpace;
    private final Supplier<RecordLayerMetadataOperationsFactory.Builder> ddlFactoryBuilder;
    private RelationalDriver driver;
    private EmbeddedRelationalEngine engine;
    private final MetricRegistry storeTimer = new MetricRegistry();

    public EmbeddedRelationalExtension() {
        this(RecordLayerMetadataOperationsFactory::defaultFactory);
    }

    public EmbeddedRelationalExtension(Supplier<RecordLayerMetadataOperationsFactory.Builder> ddlFactory) {
        final RelationalKeyspaceProvider keyspaceProvider = RelationalKeyspaceProvider.instance();
        keyspaceProvider.registerDomainIfNotExists("TEST");
        this.keySpace = keyspaceProvider.getKeySpace();
        this.ddlFactoryBuilder = ddlFactory;
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        if (driver != null) {
            DriverManager.deregisterDriver(driver);
            driver = null;
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        setup();
    }

    private void setup() throws RelationalException, SQLException {
        RecordLayerConfig rlCfg = RecordLayerConfig.getDefault();
        //here we are extending the StorageCluster so that we can track which internal Databases were
        // connected to and we can validate that they were all closed properly

        // This needs to be done prior to the first call to factory.getDatabase()
        FDBDatabaseFactory.instance().setAPIVersion(APIVersion.API_VERSION_7_1);

        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        StoreCatalog storeCatalog;
        try (var txn = new DirectFdbConnection(database).getTransactionManager().createTransaction(Options.NONE)) {
            storeCatalog = StoreCatalogProvider.getCatalog(txn, keySpace);
            txn.commit();
        }

        RecordLayerMetadataOperationsFactory ddlFactory = ddlFactoryBuilder.get()
                .setBaseKeySpace(keySpace)
                .setRlConfig(rlCfg)
                .setStoreCatalog(storeCatalog)
                .build();
        engine = RecordLayerEngine.makeEngine(
                rlCfg,
                Collections.singletonList(database),
                keySpace,
                storeCatalog,
                storeTimer,
                ddlFactory,
                RelationalPlanCache.buildWithDefaults());
        driver = new EmbeddedRelationalDriver(engine);
        DriverManager.registerDriver(driver);
    }

    public EmbeddedRelationalEngine getEngine() {
        return engine;
    }

    public RelationalDriver getDriver() {
        return driver;
    }
}
