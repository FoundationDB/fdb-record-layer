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

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.catalog.Catalog;
import com.apple.foundationdb.relational.recordlayer.catalog.DatabaseLocator;
import com.apple.foundationdb.relational.recordlayer.catalog.MutableRecordMetaDataStore;
import com.apple.foundationdb.relational.recordlayer.catalog.RecordLayerCatalog;
import com.apple.foundationdb.relational.recordlayer.ddl.ConstantActionFactory;
import com.apple.foundationdb.relational.recordlayer.ddl.RecordLayerConstantActionFactory;

import javax.annotation.Nullable;

/**
 * A holder object for all the dependencies that need to be implemented by users.
 * What is passed to the constructor are the things that need to be implemented/provided
 * by the user of the Relational library--everything else is computed or determined based off
 * these interfaces.
 */
public class RecordLayerEngine {
    private final DatabaseLocator databaseFinder;
    private final MutableRecordMetaDataStore metaDataStore;
    private final FDBRecordStoreBase.UserVersionChecker userVersionChecker;
    private final SerializerRegistry serializerRegistry;
    private final KeySpace keySpace;
    @Nullable
    private final FDBStoreTimer storeTimer;

    /*Internal objects*/
    private final Catalog catalog;
    private final ConstantActionFactory constantActionFactory;
    private final RecordLayerDriver driver;

    public RecordLayerEngine(DatabaseLocator databaseFinder,
                             MutableRecordMetaDataStore metaDataStore,
                             FDBRecordStoreBase.UserVersionChecker userVersionChecker,
                             SerializerRegistry serializerRegistry,
                             KeySpace keySpace,
                             @Nullable FDBStoreTimer storeTimer) {
        this.databaseFinder = databaseFinder;
        this.metaDataStore = metaDataStore;
        this.userVersionChecker = userVersionChecker;
        this.serializerRegistry = serializerRegistry;
        this.keySpace = keySpace;
        this.storeTimer = storeTimer;

        this.catalog = new RecordLayerCatalog.Builder()
                .setDatabaseLocator(databaseFinder)
                .setKeySpace(keySpace)
                .setMetadataProvider(metaDataStore)
                .setSerializerRegistry(serializerRegistry)
                .setUserVersionChecker(userVersionChecker)
                .setStoreTimer(storeTimer)
                .build();

        this.constantActionFactory = new RecordLayerConstantActionFactory.Builder()
                .setMetaDataStore(metaDataStore)
                .setSerializerRegistry(serializerRegistry)
                .setBaseKeySpace(keySpace)
                .setUserVersionChecker(userVersionChecker)
                .build();
        this.driver = new RecordLayerDriver(this);

    }

    public void registerDriver() {
        Relational.registerDriver(driver);
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public ConstantActionFactory getConstantActionFactory() {
        return constantActionFactory;
    }

    public String getScheme() {
        return "embed";
    }

    public void deregisterDriver() {
        Relational.deregisterDriver(driver);
    }
}
