/*
 * EmbeddedRelationalBenchmark.java
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
import com.apple.foundationdb.record.provider.common.DynamicMessageRecordSerializer;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import org.openjdk.jmh.annotations.TearDown;

import java.net.URI;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public abstract class EmbeddedRelationalBenchmark {
    RecordLayerEngine engine;
    KeySpace keySpace;
    FDBDatabase fdbDatabase;

    final String restaurantRecord = "RestaurantRecord";

    Map<Integer, URI> createdDatabases = new ConcurrentHashMap<>();

    private static class TestStoreTimer extends FDBStoreTimer {
        private final Map<String, Object> metrics;

        TestStoreTimer(Map<String, Object> metrics) {
            this.metrics = metrics;
        }

        @Override
        public void record(Event event, long timeDifference) {
            super.record(event, timeDifference);
            metrics.put(event.name(), timeDifference);
        }
    }

    URI getUri(String dbName, boolean fullyQualified) {
        if (fullyQualified) {
            return URI.create("rlsc:embed:" + dbName);
        } else {
            return URI.create(dbName);
        }
    }

    void createDatabase(DatabaseTemplate dbTemplate, URI dbUri, int dbId) throws RelationalException {
        try (final Transaction txn = new RecordContextTransaction(fdbDatabase.openContext())) {
            engine.getConstantActionFactory().getCreateDatabaseConstantAction(dbUri, dbTemplate, com.apple.foundationdb.relational.api.Options.create()).execute(txn);
            txn.commit();
        }
        createdDatabases.put(dbId, dbUri);
    }

    private void createSchemaTemplate(RecordLayerTemplate template) throws RelationalException {
        try (final Transaction txn = new RecordContextTransaction(fdbDatabase.openContext())) {
            engine.getConstantActionFactory().getCreateSchemaTemplateConstantAction(template, com.apple.foundationdb.relational.api.Options.create()).execute(txn);
            txn.commit();
        }
    }

    public void deleteDatabase(URI dbUri) throws RelationalException {
        try (final Transaction txn = new RecordContextTransaction(fdbDatabase.openContext())) {
            engine.getConstantActionFactory().getDeleteDatabaseContantAction(dbUri, com.apple.foundationdb.relational.api.Options.create()).execute(txn);
            txn.commit();
        }
    }

    public void setUp() throws RelationalException, SQLException {
        KeySpaceDirectory dbDirectory = new KeySpaceDirectory("dbid", KeySpaceDirectory.KeyType.STRING);
        keySpace = new KeySpace(dbDirectory);
        fdbDatabase = FDBDatabaseFactory.instance().getDatabase();
        engine = new RecordLayerEngine(dbPath -> fdbDatabase,
                new MapRecordMetaDataStore(),
                (oldUserVersion, oldMetaDataVersion, metaData) -> CompletableFuture.completedFuture(oldUserVersion),
                storePath -> DynamicMessageRecordSerializer.instance(),
                keySpace, new TestStoreTimer(new HashMap<>()));
        engine.registerDriver();

        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(Restaurant.getDescriptor());
        builder.getRecordType(restaurantRecord).setPrimaryKey(Key.Expressions.field("rest_no"));
        createSchemaTemplate(new RecordLayerTemplate(restaurantRecord, builder.build()));
    }

    @TearDown
    public void tearDown() throws RelationalException {
        for (URI dbUri: createdDatabases.values()) {
            deleteDatabase(dbUri);
        }
        createdDatabases.clear();
        engine.deregisterDriver();
    }
}
