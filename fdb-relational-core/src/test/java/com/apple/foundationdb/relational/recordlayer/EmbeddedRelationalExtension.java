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
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.relational.api.EmbeddedRelationalEngine;
import com.apple.foundationdb.relational.api.catalog.InMemorySchemaTemplateCatalog;

import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class EmbeddedRelationalExtension implements RelationalExtension, BeforeEachCallback, AfterEachCallback {
    private final Supplier<KeySpace> keySpaceSupplier;
    private EmbeddedRelationalEngine engine;
    private final MetricRegistry storeTimer = new MetricRegistry();

    public EmbeddedRelationalExtension() {
        this.keySpaceSupplier = this::createNewKeySpace;
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        engine.deregisterDriver();
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
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

}
