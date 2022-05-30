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
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.relational.api.EmbeddedRelationalEngine;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;
import com.apple.foundationdb.relational.api.catalog.InMemorySchemaTemplateCatalog;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metrics.NoOpMetricRegistry;
import com.apple.foundationdb.relational.recordlayer.catalog.RecordLayerStoreCatalogImpl;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import java.net.URI;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

public abstract class EmbeddedRelationalBenchmark {
    private static final String templateDefinition =
            "CREATE STRUCT Location (address string, latitude string, longitude string);" +
                    "CREATE STRUCT RestaurantReview (reviewer int64, rating int64);" +
                    "CREATE STRUCT RestaurantTag (tag string, weight int64);" +
                    "CREATE STRUCT ReviewerStats (start_date int64, school_name string, hometown string);" +
                    "CREATE TABLE RestaurantRecord (rest_no int64, name string, location Location, reviews RestaurantReview ARRAY, tags RestaurantTag ARRAY, customer string ARRAY PRIMARY KEY(rest_no));" +
                    "CREATE TABLE RestaurantReviewer (id int64, name string, email string, stats ReviewerStats PRIMARY KEY(id));" +
                    //"CREATE VALUE INDEX record_type_covering on RestaurantRecord(rest_no) INCLUDE (name);" +
                    "CREATE VALUE INDEX record_name_idx on RestaurantRecord(name);" +
                    "CREATE VALUE INDEX reviewer_name_idx on RestaurantReviewer(name) ";

    static final String restaurantRecordTable = "RestaurantRecord";

    static final String schemaTemplateName = "RestaurantTemplate";

    public static class Driver {
        EmbeddedRelationalEngine engine;
        KeySpace keySpace;
        FdbConnection fdbDatabase;

        public void up() throws RelationalException, SQLException {
            KeySpaceDirectory dbDirectory = new KeySpaceDirectory("dbid", KeySpaceDirectory.KeyType.STRING);
            dbDirectory.addSubdirectory(new KeySpaceDirectory("schema", KeySpaceDirectory.KeyType.STRING));
            KeySpaceDirectory catalogDir = new KeySpaceDirectory("catalog", KeySpaceDirectory.KeyType.NULL);
            keySpace = new KeySpace(dbDirectory, catalogDir);
            final FDBDatabase fdbDb = FDBDatabaseFactory.instance().getDatabase();
            fdbDatabase = new DirectFdbConnection(fdbDb, NoOpMetricRegistry.INSTANCE);
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

        public void down() throws RelationalException {
            engine.deregisterDriver();
        }

        private void createSchemaTemplate() throws RelationalException, SQLException {
            try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.create())) {
                conn.setSchema("catalog");
                try (Statement statement = conn.createStatement()) {
                    statement.executeUpdate("CREATE SCHEMA TEMPLATE " + schemaTemplateName + " AS { " + templateDefinition + "}");
                }
            }
        }
    }

    @State(Scope.Thread)
    public static class ThreadScopedDatabases {
        List<String> databases = new ArrayList<>();

        @TearDown(Level.Iteration)
        public void down() throws RelationalException {
            deleteDatabases(databases);
        }

        public void createDatabase(DatabaseTemplate dbTemplate, String dbName) throws RelationalException, SQLException {
            EmbeddedRelationalBenchmark.createDatabase(dbTemplate, getUri(dbName, false));
            databases.add(dbName);
        }
    }

    public static class BenchmarkScopedDatabases {
        List<String> databases = new ArrayList<>();

        public void createMultipleDatabases(
                DatabaseTemplate dbTemplate,
                int dbCount,
                Function<Integer, String> dbName,
                Consumer<URI> populateDatabase) throws RelationalException {
            try {
                IntStream.range(0, dbCount).parallel().forEach(i ->
                {
                    try {
                        EmbeddedRelationalBenchmark.createDatabase(dbTemplate, getUri(dbName.apply(i), false));
                        populateDatabase.accept(getUri(dbName.apply(i), true));
                    } catch (RelationalException e) {
                        throw e.toUncheckedWrappedException();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
            } catch (UncheckedRelationalException e) {
                throw e.unwrap();
            }
            for (int i = 0; i < dbCount; ++i) {
                databases.add(dbName.apply(i));
            }
        }

        public void deleteDatabases() throws RelationalException {
            EmbeddedRelationalBenchmark.deleteDatabases(databases);
        }
    }

    static URI getUri(String dbName, boolean fullyQualified) {
        if (fullyQualified) {
            return URI.create("jdbc:embed:" + dbName);
        } else {
            return URI.create(dbName);
        }
    }

    private static void createDatabase(DatabaseTemplate dbTemplate, URI dbUri) throws RelationalException, SQLException {
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.create())) {
            conn.setSchema("catalog");
            try (Statement statement = conn.createStatement()) {
                statement.executeUpdate("CREATE DATABASE '" + dbUri.getPath() + "'");
                for (Map.Entry<String, String> schemaTemplateEntry : dbTemplate.getSchemaToTemplateNameMap().entrySet()) {
                    statement.executeUpdate("CREATE SCHEMA '" + dbUri.getPath() + "/" + schemaTemplateEntry.getKey() + "' WITH TEMPLATE " + schemaTemplateEntry.getValue());
                }
            }
        }
    }

    private static void deleteDatabase(URI dbUri) throws RelationalException, SQLException {
        try (RelationalConnection conn = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.create())) {
            conn.setSchema("catalog");
            try (Statement statement = conn.createStatement()) {
                statement.executeUpdate("DROP DATABASE '" + dbUri.getPath() + "'");
            }
        }
    }

    private static void deleteDatabases(Collection<String> databases) throws RelationalException {
        try {
            databases.parallelStream().forEach(dbName -> {
                try {
                    deleteDatabase(getUri(dbName, false));
                } catch (RelationalException e) {
                    throw e.toUncheckedWrappedException();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (UncheckedRelationalException e) {
            throw e.unwrap();
        }
        databases.clear();
    }
}
