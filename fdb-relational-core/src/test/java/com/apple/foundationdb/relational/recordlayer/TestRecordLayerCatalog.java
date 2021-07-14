/*
 * TestRecordLayerCatalog.java
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

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.relational.api.RelationalException;
import com.apple.foundationdb.relational.api.catalog.Catalog;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.api.catalog.RelationalDatabase;
import com.apple.foundationdb.relational.recordlayer.catalog.MutableRecordMetaDataStore;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A simple catalog implementation that can be used for easy testing
 */
public class TestRecordLayerCatalog implements Catalog {
    private final ConcurrentMap<String,RelationalDatabase> dbMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String,RecordLayerTemplate> schemaTemplates = new ConcurrentHashMap<>();

    private final FDBDatabase fdbDatabase;
    private final FDBRecordStoreBase.UserVersionChecker userVersionChecker;
    private final SerializerRegistry serializerRegistry;
    private final MutableRecordMetaDataStore metaDataStore;

    public TestRecordLayerCatalog(FDBDatabase fdbDatabase,
                                  MutableRecordMetaDataStore metaDataStore,
                                  FDBRecordStoreBase.UserVersionChecker userVersionChecker,
                                  SerializerRegistry serializerRegistry) {
        this.metaDataStore = metaDataStore;
        this.fdbDatabase = fdbDatabase;
        this.userVersionChecker = userVersionChecker;
        this.serializerRegistry = serializerRegistry;
    }

    @Nonnull
    @Override
    public SchemaTemplate getSchemaTemplate(@Nonnull String templateId) throws RelationalException {
        SchemaTemplate template = schemaTemplates.get(templateId);
        if(template==null){
            throw new RelationalException("Cannot find Schema template for id <"+templateId+">", RelationalException.ErrorCode.UNKNOWN_SCHEMA);
        }
        return template;
    }

    @Nonnull
    @Override
    public RelationalDatabase getDatabase(@Nonnull List<Object> url) throws RelationalException {
        StringBuilder stringBuilder = new StringBuilder();
        url.forEach(o -> {
            stringBuilder.append("/");
            stringBuilder.append(o);
        });
        final String dbId = stringBuilder.toString();

        RelationalDatabase frl = dbMap.get(dbId);
        if(frl==null){
            throw new RelationalException(dbId,RelationalException.ErrorCode.UNKNOWN_DATABASE);
        }
        return frl;
    }

    public void createDatabase(KeySpacePath dbPath){
        RelationalDatabase db = new RecordLayerDatabase(metaDataStore,userVersionChecker,8,serializerRegistry,dbPath);

        final RelationalDatabase old = dbMap.putIfAbsent("/"+dbPath.getDirectoryName(), db);
        if(old!=null && old !=db){
            throw new IllegalStateException("DB with key </"+dbPath.getDirectoryName()+"> already exists!");
        }
    }

    public void createSchema(KeySpacePath schemaPath,SchemaTemplate metaData){
        RecordLayerTemplate template= (RecordLayerTemplate)metaData;
        try(FDBRecordContext ctx = fdbDatabase.openContext()) {
            FDBRecordStore.newBuilder()
                    .setKeySpacePath(schemaPath)
                    .setMetaDataProvider(template)
                    .setContext(ctx).createOrOpen();
            ctx.commit();
        }
        metaDataStore.putMetadata(schemaPath.getDirectoryName(),template);
    }

    public void createSchemaTemplate(RecordLayerTemplate template){
        this.schemaTemplates.put(template.getUniqueName(),template);
    }
}
