/*
 * KeySpaceRule.java
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
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.function.Consumer;

public class KeySpaceRule implements BeforeEachCallback, AfterEachCallback {
    //TODO(bfines) we need to do some extra work here to make sure that we have a running FDB instance etc.
    KeySpace keySpace;
    FDBDatabase fdbDatabase;
    RecordMetaData metadata;
    KeySpacePath ksPath;

    private final String keySpacePrefix;
    private final String schema;
    private Consumer<RecordMetaDataBuilder> metadataFunc;

    public KeySpaceRule(String keySpacePrefix, String schema, Consumer<RecordMetaDataBuilder> metadataFunc) {
        this.keySpacePrefix = keySpacePrefix;
        this.schema = schema;
        this.metadataFunc = metadataFunc;
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        try (FDBRecordContext ctx = fdbDatabase.openContext()) {
            FDBRecordStore.deleteStore(ctx, ksPath);
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        fdbDatabase = FDBDatabaseFactory.instance().getDatabase();

        final KeySpaceDirectory rootDir = new KeySpaceDirectory(keySpacePrefix, KeySpaceDirectory.KeyType.STRING, "");
        rootDir.addSubdirectory(new KeySpaceDirectory(schema, KeySpaceDirectory.KeyType.STRING, ""));
        keySpace = new KeySpace(rootDir);
        ksPath = keySpace.path(keySpacePrefix).add(schema);
        final RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder();
        metadataFunc.accept(metaDataBuilder);

        metadata = metaDataBuilder.build();
        try (FDBRecordContext ctx = fdbDatabase.openContext()) {
            FDBRecordStore.newBuilder()
                    .setKeySpacePath(ksPath)
                    .setMetaDataProvider(metadata)
                    .setContext(ctx).createOrOpen();
            ctx.commit();
        }
    }

    public RecordStoreConnection openDirectConnection() {
        throw new UnsupportedOperationException("Not Implemented in the Relational layer");
        //        RecordStoreConnection recStoreConn =  new RecordStoreConnection(fdbDatabase,
        //                (schema, txn) -> FDBRecordStore.newBuilder().setMetaDataProvider(metadata)
        //                        .setKeySpacePath(ksPath)
        //                        .setContext(txn).open());
        //
        //        recStoreConn.setSchema(schema);
        //        return recStoreConn;
    }
}
