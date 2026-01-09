/*
 * EmbeddedRelationalExtension.java
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

import com.apple.foundationdb.record.provider.foundationdb.APIVersion;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FormatVersion;
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
import com.apple.foundationdb.test.FDBTestEnvironment;
import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;

public class EmbeddedRelationalExtension implements RelationalExtension, BeforeEachCallback, AfterEachCallback {

    @Nonnull
    private final KeySpace keySpace;

    @Nullable
    private RelationalDriver driver;

    @Nullable
    private EmbeddedRelationalEngine engine;

    @Nonnull
    private final MetricRegistry storeTimer;

    @Nonnull
    private final Options options;

    public EmbeddedRelationalExtension() {
        this(Options.none());
    }

    public EmbeddedRelationalExtension(@Nonnull final Options options) {
        final RelationalKeyspaceProvider keyspaceProvider = RelationalKeyspaceProvider.instance();
        keyspaceProvider.registerDomainIfNotExists("TEST");
        this.keySpace = keyspaceProvider.getKeySpace();
        this.storeTimer = new MetricRegistry();
        this.options = options;
    }

    @Override
    public void afterEach(ExtensionContext ignored) throws Exception {
        if (driver != null) {
            DriverManager.deregisterDriver(driver);
            driver = null;
        }
    }

    @Override
    public void beforeEach(ExtensionContext ignored) throws Exception {
        setup();
    }

    private void setup() throws RelationalException, SQLException {
        // here we are extending the StorageCluster so that we can track which internal Databases were
        // connected to and we can validate that they were all closed properly

        // This needs to be done prior to the first call to factory.getDatabase()
        FDBDatabaseFactory.instance().setAPIVersion(APIVersion.API_VERSION_7_1);

        final var database = FDBDatabaseFactory.instance().getDatabase(FDBTestEnvironment.randomClusterFile());
        final StoreCatalog storeCatalog;
        try (var connection = new DirectFdbConnection(database);
                var txn = connection.getTransactionManager().createTransaction(Options.NONE)) {
            storeCatalog = StoreCatalogProvider.getCatalog(txn, keySpace);
            txn.commit();
        }

        final var config = new RecordLayerConfig.RecordLayerConfigBuilder()
                .setFormatVersion(FormatVersion.getMaximumSupportedVersion())
                .build();
        final var ddlFactory = RecordLayerMetadataOperationsFactory.defaultFactory()
                .setBaseKeySpace(keySpace)
                .setRlConfig(config)
                .setStoreCatalog(storeCatalog)
                .build();
        engine = RecordLayerEngine.makeEngine(
                config,
                Collections.singletonList(database),
                keySpace,
                storeCatalog,
                storeTimer,
                ddlFactory,
                RelationalPlanCache.buildWithDefaults());
        driver = new EmbeddedRelationalDriver(engine);
        DriverManager.registerDriver(driver);
    }

    @Nullable
    public EmbeddedRelationalEngine getEngine() {
        return engine;
    }

    @Nullable
    public RelationalDriver getDriver() {
        return driver;
    }

    @Nonnull
    public static Resource newAsResource() throws Exception {
        return new Resource(new EmbeddedRelationalExtension());
    }

    @Nonnull
    public static Resource newAsResource(@Nonnull final Options options) throws Exception {
        return new Resource(new EmbeddedRelationalExtension(options));
    }

    public static final class Resource implements Closeable {

        @Nonnull
        private final EmbeddedRelationalExtension underlyingExtension;

        Resource(@Nonnull final EmbeddedRelationalExtension underlyingExtension) throws Exception {
            this.underlyingExtension = underlyingExtension;
            this.underlyingExtension.beforeEach(null);
        }

        @Override
        public void close() {
            try {
                underlyingExtension.afterEach(null);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Nonnull
        public EmbeddedRelationalExtension getUnderlyingExtension() {
            return underlyingExtension;
        }
    }
}
