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

import com.apple.foundationdb.record.provider.common.DynamicMessageRecordSerializer;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.relational.api.EmbeddedRelationalEngine;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;
import com.apple.foundationdb.relational.api.catalog.InMemorySchemaTemplateCatalog;
import com.apple.foundationdb.relational.api.ddl.DdlConnection;
import com.apple.foundationdb.relational.api.ddl.DdlStatement;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.catalog.RecordLayerStoreCatalogImpl;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

public abstract class EmbeddedRelationalBenchmark {
    private static final String restaurantSchema =
            "CREATE STRUCT Location (address string, latitude string, longitude string);" +
                    "CREATE STRUCT RestaurantReview (reviewer int64, rating int64);" +
                    "CREATE STRUCT RestaurantTag (tag string, weight int64);" +
                    "CREATE STRUCT ReviewerStats (start_date int64, school_name string, hometown string);" +
                    "CREATE TABLE RestaurantRecord (rest_no int64, name string, location Location, reviews RestaurantReview ARRAY, tags RestaurantTag ARRAY, customer string ARRAY PRIMARY KEY(rest_no));" +
                    "CREATE TABLE RestaurantReviewer (id int64, name string, email string, stats ReviewerStats PRIMARY KEY(id));" +
                    "CREATE VALUE INDEX record_type_covering on RestaurantRecord(rest_no) INCLUDE (name);" +
                    "CREATE VALUE INDEX record_name_idx on RestaurantRecord(name);" +
                    "CREATE VALUE INDEX reviewer_name_idx on RestaurantReviewer(name) ";

    static final String restaurantRecord = "RestaurantRecord";

    @State(Scope.Benchmark)
    public static class Driver {
        EmbeddedRelationalEngine engine;
        KeySpace keySpace;
        FdbConnection fdbDatabase;

        @Setup(Level.Trial)
        public void up() throws RelationalException {
            KeySpaceDirectory dbDirectory = new KeySpaceDirectory("dbid", KeySpaceDirectory.KeyType.STRING);
            keySpace = new KeySpace(dbDirectory);
            final FDBDatabase fdbDb = FDBDatabaseFactory.instance().getDatabase();
            fdbDatabase = new DirectFdbConnection(fdbDb, new TestStoreTimer(new HashMap<>()));
            RecordLayerConfig rlConfig = new RecordLayerConfig(
                    (oldUserVersion, oldMetaDataVersion, metaData) -> CompletableFuture.completedFuture(oldUserVersion),
                    storePath -> DynamicMessageRecordSerializer.instance(),
                    1
            );
            RecordLayerStoreCatalogImpl catalog = new RecordLayerStoreCatalogImpl(keySpace);
            try (Transaction txn = fdbDatabase.getTransactionManager().createTransaction()) {
                catalog.initialize(txn);
                txn.commit();
            }
            engine = RecordLayerEngine.makeEngine(rlConfig, Collections.singletonList(fdbDb), keySpace, InMemorySchemaTemplateCatalog::new);
            engine.registerDriver();

            createSchemaTemplate();
        }

        private void createSchemaTemplate() throws RelationalException {
            try (DdlConnection conn = engine.getDdlConnection(); DdlStatement statement = conn.createStatement()) {
                statement.execute("CREATE SCHEMA TEMPLATE restaurant_template AS { " + restaurantSchema + "}");
                conn.commit();
            }
        }

        @TearDown(Level.Trial)
        public void down() throws RelationalException {
            engine.deregisterDriver();
        }
    }

    @State(Scope.Thread)
    public static class ThreadScopedDatabases {
        List<String> databases = new ArrayList<>();

        @TearDown(Level.Iteration)
        public void down(Driver driver) throws RelationalException {
            deleteDatabases(databases, driver);
        }

        public void createDatabase(Driver driver, DatabaseTemplate dbTemplate, String dbName) throws RelationalException {
            EmbeddedRelationalBenchmark.createDatabase(driver, dbTemplate, getUri(dbName, false));
            databases.add(dbName);
        }
    }

    @State(Scope.Benchmark)
    public static class BenchmarkScopedDatabases {
        List<String> databases = new ArrayList<>();

        @TearDown(Level.Trial)
        public void down(Driver driver) throws RelationalException {
            deleteDatabases(databases, driver);
        }

        public void createMultipleDatabases(
                Driver driver,
                DatabaseTemplate dbTemplate,
                int dbCount,
                Function<Integer, String> dbName,
                Consumer<URI> populateDatabase) throws RelationalException {
            try {
                IntStream.range(0, dbCount).parallel().forEach(i ->
                {
                    try {
                        EmbeddedRelationalBenchmark.createDatabase(driver, dbTemplate, getUri(dbName.apply(i), false));
                        populateDatabase.accept(getUri(dbName.apply(i), true));
                    } catch (RelationalException e) {
                        throw e.toUncheckedWrappedException();
                    }
                });
            } catch (UncheckedRelationalException e) {
                throw e.unwrap();
            }
            for (int i = 0; i < dbCount; ++i) {
                databases.add(dbName.apply(i));
            }
        }
    }

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

    static URI getUri(String dbName, boolean fullyQualified) {
        if (fullyQualified) {
            return URI.create("jdbc:embed:" + dbName);
        } else {
            return URI.create(dbName);
        }
    }

    private static void createDatabase(Driver driver, DatabaseTemplate dbTemplate, URI dbUri) throws RelationalException {
        try (DdlConnection conn = driver.engine.getDdlConnection(); DdlStatement statement = conn.createStatement()) {
            statement.execute("CREATE DATABASE " + dbUri.getPath() + ";");
            conn.setDatabase(dbUri);
            for (Map.Entry<String, String> schemaTemplateEntry : dbTemplate.getSchemaToTemplateNameMap().entrySet()) {
                statement.execute("CREATE SCHEMA " + schemaTemplateEntry.getKey() + " WITH TEMPLATE " + schemaTemplateEntry.getValue());
            }
            conn.commit();
        }
    }

    private static void deleteDatabase(URI dbUri, Driver driver) throws RelationalException {
        try (DdlConnection conn = driver.engine.getDdlConnection(); DdlStatement statement = conn.createStatement()) {
            statement.execute("DROP DATABASE " + dbUri.getPath());
            conn.commit();
        }
    }

    private static void deleteDatabases(Collection<String> databases, Driver driver) throws RelationalException {
        try {
            databases.parallelStream().forEach(dbName -> {
                try {
                    deleteDatabase(getUri(dbName, false), driver);
                } catch (RelationalException e) {
                    throw e.toUncheckedWrappedException();
                }
            });
        } catch (UncheckedRelationalException e) {
            throw e.unwrap();
        }
        databases.clear();
    }
}
