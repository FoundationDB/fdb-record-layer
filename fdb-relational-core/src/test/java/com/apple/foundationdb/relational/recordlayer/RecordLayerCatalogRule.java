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
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.relational.api.RelationalException;
import com.apple.foundationdb.relational.api.catalog.Catalog;
import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.api.catalog.RelationalDatabase;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.annotation.Nonnull;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class RecordLayerCatalogRule implements BeforeEachCallback, AfterEachCallback, Catalog {
    FDBDatabase fdbDatabase;
    private final String keySpacePrefix;

    private TestRecordLayerCatalog catalog;
    private KeySpaceDirectory keySpace;

    private final FDBRecordStoreBase.UserVersionChecker checker;
    private final SerializerRegistry registry;

    private final List<KeySpacePath> databases = new LinkedList<>();


    public RecordLayerCatalogRule(String catalogPrefix,FDBRecordStoreBase.UserVersionChecker checker, SerializerRegistry registry) {
        this.checker = checker;
        this.keySpacePrefix = catalogPrefix;
        this.registry = registry;
    }

    @Override
    public void afterEach(ExtensionContext context) {
        try(FDBRecordContext ctx = fdbDatabase.openContext()){
            for(KeySpacePath db :databases) {
                FDBRecordStore.deleteStore(ctx, db);
            }
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        fdbDatabase = FDBDatabaseFactory.instance().getDatabase();

        keySpace = new KeySpaceDirectory(keySpacePrefix,KeySpaceDirectory.KeyType.STRING,"");

        this.catalog = new TestRecordLayerCatalog(fdbDatabase,new MapRecordMetaDataStore(),checker,registry);
    }

    @Nonnull
    public SchemaTemplate getSchemaTemplate(@Nonnull String templateId) throws RelationalException {
        return catalog.getSchemaTemplate(templateId);
    }

    @Nonnull
    public RelationalDatabase getDatabase(@Nonnull List<Object> url) throws RelationalException {
        return catalog.getDatabase(url);
    }

    public void setSchemaTemplate(RecordLayerTemplate template){
        catalog.createSchemaTemplate(template);
    }

    public void loadDatabase(String dbid, DatabaseTemplate dbTemplate) {
        keySpace.addSubdirectory(new KeySpaceDirectory(dbid, KeySpaceDirectory.KeyType.STRING, dbid));
        KeySpacePath dbPath = new KeySpace(keySpace).path(keySpacePrefix).add(dbid);
        catalog.createDatabase(dbPath);

        final KeySpaceDirectory dbDir = keySpace.getSubdirectory(dbid);
        for(Map.Entry<String,String> schemaData : dbTemplate.getSchemaToTemplateNameMap().entrySet()){
            dbDir.addSubdirectory(new KeySpaceDirectory(schemaData.getKey(), KeySpaceDirectory.KeyType.STRING, schemaData.getKey()));
            KeySpacePath schemaPath = dbPath.add(schemaData.getKey());

            SchemaTemplate schemaTemplate = catalog.getSchemaTemplate(schemaData.getValue());
            catalog.createSchema(schemaPath,schemaTemplate);
        }
        databases.add(dbPath);
    }
}
