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

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalException;
import com.apple.foundationdb.relational.api.catalog.Catalog;
import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.api.catalog.RelationalDatabase;
import com.apple.foundationdb.relational.recordlayer.ddl.ConstantActionFactory;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class RecordLayerCatalogRule implements BeforeEachCallback, AfterEachCallback, Catalog {
    private FDBDatabase fdbDatabase;

    private Catalog catalog;
    private ConstantActionFactory constantActionFactory;

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
        KeySpace keySpace = getKeySpaceForSetup();
        fdbDatabase = FDBDatabaseFactory.instance().getDatabase();

        RecordLayerEngine engine = new RecordLayerEngine(dbPath -> fdbDatabase,
                new MapRecordMetaDataStore(),
                (oldUserVersion, oldMetaDataVersion, metaData) -> CompletableFuture.completedFuture(oldUserVersion),
                new TestSerializerRegistry(),
                keySpace,
                URI.create("/t"));
        catalog = engine.getCatalog();
        constantActionFactory = engine.getConstantActionFactory();
    }

    @Override
    @Nonnull
    public SchemaTemplate getSchemaTemplate(@Nonnull URI templateId) throws RelationalException {
        return catalog.getSchemaTemplate(templateId);
    }

    @Nonnull
    @Override
    public RelationalDatabase getDatabase(@Nonnull URI url) throws RelationalException {
        try {
            return catalog.getDatabase(url);
        }catch(RelationalException ve){
            if(ve.getErrorCode().equals(RelationalException.ErrorCode.INVALID_PATH)){
                throw new RelationalException("Database <"+url+"> is unknown or does not exist", RelationalException.ErrorCode.UNDEFINED_DATABASE,ve);
            }else{
                throw ve;
            }
        }
    }

    private KeySpace getKeySpaceForSetup() {
//        KeySpaceDirectory dbDirectory = new KeySpaceDirectory("dbid", KeySpaceDirectory.KeyType.NULL);
        //add the templates subdirectory
        KeySpaceDirectory templatesDirectory = new KeySpaceDirectory("templates", KeySpaceDirectory.KeyType.STRING,"T");
        return new KeySpace(templatesDirectory);
    }

    public void createDatabase(URI dbUri, DatabaseTemplate dbTemplate) {
        try(final Transaction txn= new RecordContextTransaction(fdbDatabase.openContext())){
            constantActionFactory.getCreateDatabaseConstantAction(dbUri,dbTemplate, Options.create()).execute(txn);
            txn.commit();
        }
    }

    public void createSchemaTemplate(RecordLayerTemplate template) {
        try(final Transaction txn= new RecordContextTransaction(fdbDatabase.openContext())){
            constantActionFactory.getCreateSchemaTemplateConstantAction(template, Options.create()).execute(txn);
            txn.commit();
        }
    }
}
