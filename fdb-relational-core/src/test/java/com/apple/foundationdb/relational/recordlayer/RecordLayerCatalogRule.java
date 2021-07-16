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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.Restaurant;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.relational.api.RelationalException;
import com.apple.foundationdb.relational.api.catalog.Catalog;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.api.catalog.RelationalDatabase;
import com.apple.foundationdb.relational.recordlayer.catalog.RecordLayerCatalog;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class RecordLayerCatalogRule implements BeforeEachCallback, AfterEachCallback, Catalog {
    private FDBDatabase fdbDatabase;
    private RecordLayerCatalog catalog;
    private final List<RecordLayerDatabase> databases = new LinkedList<>();

    @Override
    public void afterEach(ExtensionContext context) {
        try(FDBRecordContext ctx = fdbDatabase.openContext()) {
            for(RecordLayerDatabase db :databases) {
                db.clearDatabase(ctx);
            }
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        final KeySpace keySpace = getKeySpaceForSetup();

        final RecordLayerCatalog.Builder catalogBuilder = new RecordLayerCatalog.Builder();
        catalogBuilder.setKeySpace(keySpace)
                .setMetadataProvider(id -> getRecordMetadataBuilder().build())
                .setSerializerRegistry(new TestSerializerRegistry())
                .setUserVersionChecker((oldUserVersion, oldMetaDataVersion, metaData) -> CompletableFuture.completedFuture(oldUserVersion));

        catalog = catalogBuilder.build();
        fdbDatabase = FDBDatabaseFactory.instance().getDatabase();
    }

    @Override
    @Nonnull
    public SchemaTemplate getSchemaTemplate(@Nonnull String templateId) throws RelationalException {
        return catalog.getSchemaTemplate(templateId);
    }

    @Override
    @Nonnull
    public RecordLayerDatabase getDatabase(@Nonnull List<Object> url) throws RelationalException {
        RelationalDatabase relationalDatabase = catalog.getDatabase(url);
        assert relationalDatabase instanceof RecordLayerDatabase : "The returned relationalDatabase should be a RecordLayerDatabase object";
        RecordLayerDatabase recordLayerDatabase = (RecordLayerDatabase) relationalDatabase;
        databases.add(recordLayerDatabase);
        return recordLayerDatabase;
    }

    public FDBDatabase getFdbDatabase() {
        return fdbDatabase;
    }

    private static RecordMetaDataBuilder getRecordMetadataBuilder() {
        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(Restaurant.getDescriptor());
        builder.getRecordType("RestaurantRecord").setPrimaryKey(Key.Expressions.field("name"));
        return builder;
    }

    private KeySpace getKeySpaceForSetup() {
        KeySpaceDirectory rootDirectory = new KeySpaceDirectory("/", KeySpaceDirectory.KeyType.NULL);
        KeySpaceDirectory dbDirectory = new KeySpaceDirectory("dbid", KeySpaceDirectory.KeyType.STRING);
        KeySpaceDirectory schemaDirectory = new KeySpaceDirectory("test", KeySpaceDirectory.KeyType.NULL);
        dbDirectory = dbDirectory.addSubdirectory(schemaDirectory);
        rootDirectory = rootDirectory.addSubdirectory(dbDirectory);

        return new KeySpace(rootDirectory);
    }
}
