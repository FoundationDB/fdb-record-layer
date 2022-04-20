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

import com.apple.foundationdb.record.provider.common.DynamicMessageRecordSerializer;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.relational.api.EmbeddedRelationalEngine;
import com.apple.foundationdb.relational.api.catalog.InMemorySchemaTemplateCatalog;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class EmbeddedRelationalExtension implements BeforeEachCallback, AfterEachCallback, BeforeAllCallback, AfterAllCallback {
    private final Supplier<KeySpace> keySpaceSupplier;
    private EmbeddedRelationalEngine engine;
    private final TestStoreTimer storeTimer = new TestStoreTimer();

    public EmbeddedRelationalExtension() {
        this.keySpaceSupplier = this::createNewKeySpace;
    }

    public EmbeddedRelationalExtension(Supplier<KeySpace> keySpaceSupplier) {
        this.keySpaceSupplier = keySpaceSupplier;
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        tearDown();
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        tearDown();
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        setup();
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        setup();
    }

    private void tearDown() {
        engine.deregisterDriver();
    }

    private void setup() throws RelationalException {
        RecordLayerConfig rlCfg = new RecordLayerConfig(
                (oldUserVersion, oldMetaDataVersion, metaData) -> CompletableFuture.completedFuture(oldUserVersion),
                path -> DynamicMessageRecordSerializer.instance(),
                1);
        //here we are extending the StorageCluster so that we can track which internal Databases were
        // connected to and we can validate that they were all closed properly

        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase();
        final KeySpace keySpace = keySpaceSupplier.get();
        engine = RecordLayerEngine.makeEngine(rlCfg, Collections.singletonList(database), keySpace, InMemorySchemaTemplateCatalog::new, storeTimer);
        engine.registerDriver(); //register the engine driver
    }

    private KeySpace createNewKeySpace() {
        KeySpaceDirectory dbDirectory = new KeySpaceDirectory("dbid", KeySpaceDirectory.KeyType.STRING);
        KeySpaceDirectory schemaDir = new KeySpaceDirectory("schema", KeySpaceDirectory.KeyType.STRING);
        dbDirectory.addSubdirectory(schemaDir);
        KeySpaceDirectory catalogDirectory = new KeySpaceDirectory("catalog", KeySpaceDirectory.KeyType.NULL);
        return new KeySpace(dbDirectory, catalogDirectory);
    }

    public EmbeddedRelationalEngine getEngine() {
        return engine;
    }

    public Map<String, Object> getStoreTimerMetrics() {
        return storeTimer.metrics;
    }

    private static class TestStoreTimer extends FDBStoreTimer {
        private final Map<String, Object> metrics = new ConcurrentHashMap<>();

        TestStoreTimer() {
        }

        @Override
        public synchronized void record(StoreTimer.Event event, long timeDifference) {
            super.record(event, timeDifference);
            metrics.put(event.name(), timeDifference);
        }
    }
}
