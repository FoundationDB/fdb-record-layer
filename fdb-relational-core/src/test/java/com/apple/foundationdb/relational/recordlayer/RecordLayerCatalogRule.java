/*
 * RecordLayerCatalogRule.java
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

import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.catalog.Catalog;
import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.api.catalog.RelationalDatabase;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.net.URI;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

public class RecordLayerCatalogRule implements BeforeEachCallback, AfterEachCallback, Catalog {
    private final Supplier<KeySpace> keySpaceSupplier;
    private FDBDatabase fdbDatabase;

    private RecordLayerEngine engine;

    private final List<RecordLayerDatabase> databases = new LinkedList<>();

    private final Map<String, Object> metrics = new HashMap<>();

    public RecordLayerCatalogRule() {
        this.keySpaceSupplier = this::getKeySpaceForSetup;
    }

    public RecordLayerCatalogRule(Supplier<KeySpace> keySpaceSupplier) {
        this.keySpaceSupplier = keySpaceSupplier;
    }

    @Override
    public void afterEach(ExtensionContext context) {
        try (FDBRecordContext ctx = fdbDatabase.openContext()) {
            for (RecordLayerDatabase db :databases) {
                db.clearDatabase(ctx);
            }
        }

        engine.deregisterDriver();
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        KeySpace keySpace = keySpaceSupplier.get();
        fdbDatabase = FDBDatabaseFactory.instance().getDatabase();

        engine = new RecordLayerEngine(dbPath -> fdbDatabase,
                new MapRecordMetaDataStore(),
                (oldUserVersion, oldMetaDataVersion, metaData) -> CompletableFuture.completedFuture(oldUserVersion),
                new TestSerializerRegistry(),
                keySpace, new TestStoreTimer(metrics));
        engine.registerDriver();
    }

    @Override
    @Nonnull
    public SchemaTemplate getSchemaTemplate(@Nonnull String templateId) throws RelationalException {
        return engine.getCatalog().getSchemaTemplate(templateId);
    }

    @Nonnull
    @Override
    public RelationalDatabase getDatabase(@Nonnull URI dbUrl) throws RelationalException {
        try {
            return engine.getCatalog().getDatabase(dbUrl);
        } catch (RelationalException ve) {
            if (ve.getErrorCode().equals(RelationalException.ErrorCode.INVALID_PATH)) {
                throw new RelationalException("Database is unknown or does not exist: <" + dbUrl + ">", RelationalException.ErrorCode.UNDEFINED_DATABASE, ve);
            } else {
                throw ve;
            }
        }
    }

    @Override
    public void deleteDatabase(@Nonnull URI dbUrl) throws RelationalException {
        try (final Transaction txn = new RecordContextTransaction(fdbDatabase.openContext())) {
            engine.getConstantActionFactory().getDeleteDatabaseContantAction(dbUrl, Options.create()).execute(txn);
            txn.commit();
        }
    }

    @Override
    public RelationalResultSet listDatabases(@Nonnull Transaction transaction) throws RelationalException {
        return engine.getCatalog().listDatabases(transaction);
    }

    private KeySpace getKeySpaceForSetup() {
        KeySpaceDirectory dbDirectory = new KeySpaceDirectory("dbid", KeySpaceDirectory.KeyType.STRING);
        return new KeySpace(dbDirectory);
    }

    public void createDatabase(URI dbUri, DatabaseTemplate dbTemplate) {
        try (final Transaction txn = new RecordContextTransaction(fdbDatabase.openContext())) {
            engine.getConstantActionFactory().getCreateDatabaseConstantAction(dbUri, dbTemplate, Options.create()).execute(txn);
            txn.commit();
        }
    }

    public void createSchemaTemplate(RecordLayerTemplate template) {
        try (final Transaction txn = new RecordContextTransaction(fdbDatabase.openContext())) {
            engine.getConstantActionFactory().getCreateSchemaTemplateConstantAction(template, Options.create()).execute(txn);
            txn.commit();
        }
    }

    public Map<String, Object> getMetrics() {
        return metrics;
    }

    private static class TestStoreTimer extends FDBStoreTimer {
        private final Map<String, Object> metrics;

        TestStoreTimer(Map<String, Object> metrics) {
            this.metrics = metrics;
        }

        @Override
        public void record(StoreTimer.Event event, long timeDifference) {
            super.record(event, timeDifference);
            metrics.put(event.name(), timeDifference);
        }
    }
}
