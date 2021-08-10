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
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.relational.api.RelationalException;
import com.apple.foundationdb.relational.api.catalog.Catalog;
import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.api.catalog.RelationalDatabase;
import com.apple.foundationdb.relational.recordlayer.catalog.MutableRecordMetaDataStore;
import com.apple.foundationdb.relational.recordlayer.catalog.DatabaseLocator;
import com.apple.foundationdb.relational.recordlayer.catalog.RecordLayerCatalog;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class RecordLayerCatalogRule implements BeforeEachCallback, AfterEachCallback, Catalog {
    private MapRecordMetaDataStore metaDataStore;
    private FDBDatabase fdbDatabase;
    private RecordLayerCatalog catalog;
    private KeySpace keySpace;

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
        keySpace = getKeySpaceForSetup();

        metaDataStore = new MapRecordMetaDataStore();
        RecordLayerCatalog.Builder catalogBuilder = new RecordLayerCatalog.Builder();
        catalogBuilder = catalogBuilder.setKeySpace(keySpace)
                .setMetadataProvider(metaDataStore)
                .setSerializerRegistry(new TestSerializerRegistry())
                .setDatabaseLocator(dbPath -> fdbDatabase)
                .setUserVersionChecker((oldUserVersion, oldMetaDataVersion, metaData) -> CompletableFuture.completedFuture(oldUserVersion));

        catalog = catalogBuilder.build();
        fdbDatabase = FDBDatabaseFactory.instance().getDatabase();
    }

    @Override
    @Nonnull
    public SchemaTemplate getSchemaTemplate(@Nonnull URI templateId) throws RelationalException {
        return catalog.getSchemaTemplate(templateId);
    }

    @Nonnull
    @Override
    public RelationalDatabase getDatabase(@Nonnull URI url) throws RelationalException {
        return catalog.getDatabase(url);
    }

    private static RecordMetaDataBuilder getRecordMetadataBuilder() {
        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(Restaurant.getDescriptor());
        builder.getRecordType("RestaurantRecord").setPrimaryKey(Key.Expressions.field("name"));
        return builder;
    }

    private KeySpace getKeySpaceForSetup() {
        KeySpaceDirectory rootDirectory = new KeySpaceDirectory("/", KeySpaceDirectory.KeyType.NULL);
        KeySpaceDirectory dbDirectory = new KeySpaceDirectory("dbid", KeySpaceDirectory.KeyType.STRING);
        rootDirectory = rootDirectory.addSubdirectory(dbDirectory);
        //add the templates subdirectory
        rootDirectory.addSubdirectory(new KeySpaceDirectory("templates", KeySpaceDirectory.KeyType.LONG,123));
        return new KeySpace(rootDirectory);
    }

    public void createDatabase(URI dbUri, DatabaseTemplate dbTemplate) {
        //URI is of the format /<dbid>, and each schema should be in /<dbid>/<schemaid>
        KeySpacePath dbPath = KeySpaceUtils.uriToPath(dbUri,keySpace);

        final KeySpaceDirectory dbDirectory = dbPath.getDirectory();
        //get an FDBDatabase from the locator, so that we can open a transaction against it
//        final FDBDatabase fdbDatabase = catalog.getLocator().locateDatabase(dbPath);
        RecordContextTransaction context = new RecordContextTransaction(fdbDatabase.openContext());

        for (Map.Entry<String, String> schemaData : dbTemplate.getSchemaToTemplateNameMap().entrySet()) {
            final KeySpaceDirectory schemaDirectory = dbDirectory.addSubdirectory(new KeySpaceDirectory(schemaData.getKey(), KeySpaceDirectory.KeyType.NULL));
            URI templateUri = URI.create("/123/" + schemaData.getValue());

            //create the schema from the template
            URI schemaUri = URI.create(dbUri.getPath()+"/"+schemaData.getKey());

            catalog.createSchema(schemaUri,templateUri,context);
        }
        context.commit();
    }

    public void createSchemaTemplate(RecordLayerTemplate template) {
        metaDataStore.setSchemaTemplateMetaData(URI.create("/123" + template.getUniqueName().getPath()), template);
    }
}
