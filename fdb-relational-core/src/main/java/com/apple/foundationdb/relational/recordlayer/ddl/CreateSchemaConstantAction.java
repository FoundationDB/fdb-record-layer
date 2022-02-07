/*
 * CreateSchemaConstantAction.java
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

package com.apple.foundationdb.relational.recordlayer.ddl;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.KeySpaceUtils;
import com.apple.foundationdb.relational.recordlayer.SerializerRegistry;
import com.apple.foundationdb.relational.recordlayer.catalog.MutableRecordMetaDataStore;

import java.net.URI;

public class CreateSchemaConstantAction implements ConstantAction {
    private final URI schemaUrl;
    private final String templateId;
    private final KeySpace keySpace;

    private final MutableRecordMetaDataStore metaDataStore;
    private final SerializerRegistry serializerRegistry;
    private final FDBRecordStoreBase.UserVersionChecker userVersionChecker;
    private final int formatVersion;

    public CreateSchemaConstantAction(URI schemaUrl,
                                      String templateId,
                                      KeySpace keySpace,
                                      MutableRecordMetaDataStore metaDataStore,
                                      SerializerRegistry serializerRegistry,
                                      FDBRecordStoreBase.UserVersionChecker userVersionChecker,
                                      int formatVersion) {
        this.schemaUrl = schemaUrl;
        this.templateId = templateId;
        this.keySpace = keySpace;
        this.metaDataStore = metaDataStore;
        this.serializerRegistry = serializerRegistry;
        this.userVersionChecker = userVersionChecker;
        this.formatVersion = formatVersion;
    }

    @Override
    public void execute(Transaction txn) throws RelationalException {
        //TODO(bfines) error handling
        KeySpacePath schemaPath = KeySpaceUtils.getSchemaPath(schemaUrl, keySpace);
        FDBRecordContext ctx = txn.unwrap(FDBRecordContext.class);

        //create the metadata
        metaDataStore.assignSchemaToTemplate(schemaUrl, templateId);

        FDBRecordStore.newBuilder()
                .setKeySpacePath(schemaPath)
                .setSerializer(serializerRegistry.loadSerializer(schemaPath))
                .setMetaDataProvider(metaDataStore.loadMetaData(schemaUrl))
                .setUserVersionChecker(userVersionChecker)
                .setFormatVersion(formatVersion)
                .setContext(ctx)
                .createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_EXISTS);
    }
}
