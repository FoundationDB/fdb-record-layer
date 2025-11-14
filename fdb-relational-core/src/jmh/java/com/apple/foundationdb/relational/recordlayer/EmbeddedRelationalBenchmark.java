/*
 * EmbeddedRelationalBenchmark.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.relational.api.EmbeddedRelationalDriver;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalDriver;
import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metrics.NoOpMetricRegistry;
import com.apple.foundationdb.relational.recordlayer.catalog.StoreCatalogProvider;
import com.apple.foundationdb.relational.recordlayer.ddl.RecordLayerMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

public abstract class EmbeddedRelationalBenchmark {
    private static final String templateDefinition =
            "CREATE TYPE AS STRUCT \"Location\" (\"address\" string, \"latitude\" string, \"longitude\" string) " +
                    "CREATE TYPE AS STRUCT \"RestaurantReview\" (\"reviewer\" bigint, \"rating\" bigint) " +
                    "CREATE TYPE AS STRUCT \"RestaurantTag\" (\"tag\" string, \"weight\" bigint) " +
                    "CREATE TYPE AS STRUCT \"ReviewerStats\" (\"start_date\" bigint, \"school_name\" string, \"hometown\" string) " +
                    "CREATE TABLE \"RestaurantRecord\" (\"rest_no\" bigint, \"name\" string, \"location\" \"Location\", \"reviews\" \"RestaurantReview\" ARRAY, \"tags\" \"RestaurantTag\" ARRAY, \"customer\" string ARRAY, PRIMARY KEY(\"rest_no\")) " +
                    "CREATE TABLE \"RestaurantReviewer\" (\"id\" bigint, \"name\" string, \"email\" string, \"stats\" \"ReviewerStats\", PRIMARY KEY(\"id\")) " +

                    "CREATE INDEX \"record_name_idx\" ON \"RestaurantRecord\"(\"name\") " +
                    "CREATE INDEX \"reviewer_name_idx\" ON \"RestaurantReviewer\"(\"name\") ";

    static final String restaurantRecordTable = "RestaurantRecord";

    static final String schemaTemplateName = "RestaurantTemplate";

    public static class Driver {
        RelationalDriver driver;
        KeySpace keySpace;
        FdbConnection fdbDatabase;

        private final String templateName;
        private final String templateDef;

        private final RelationalPlanCache planCache;
        public StoreCatalog catalog;


        public Driver() {
            this(schemaTemplateName, templateDefinition);
        }

        public Driver(RelationalPlanCache planCache) {
            this(schemaTemplateName,templateDefinition,planCache);
        }

        public Driver(String templateName, String templateDef) {
            this.templateName = templateName;
            this.templateDef = templateDef;
            this.planCache = null;
        }

        public Driver(String templateName, String templateDef, RelationalPlanCache planCache) {
            this.templateName = templateName;
            this.templateDef = templateDef;
            this.planCache = planCache;
        }

        public void up() throws RelationalException, SQLException {
            final RelationalKeyspaceProvider keyspaceProvider = RelationalKeyspaceProvider.instance();
            keyspaceProvider.registerDomainIfNotExists("BENCHMARKS");
            keySpace = keyspaceProvider.getKeySpace();
            final FDBDatabase fdbDb = FDBDatabaseFactory.instance().getDatabase();
            fdbDatabase = new DirectFdbConnection(fdbDb, NoOpMetricRegistry.INSTANCE);

            RecordLayerConfig rlConfig = RecordLayerConfig.getDefault();
            try (Transaction txn = fdbDatabase.getTransactionManager().createTransaction(Options.NONE)) {
                catalog = StoreCatalogProvider.getCatalog(txn, keySpace);
                txn.commit();
            }

            RecordLayerMetadataOperationsFactory ddlFactory = new RecordLayerMetadataOperationsFactory.Builder()
                    .setRlConfig(rlConfig)
                    .setBaseKeySpace(keySpace)
                    .setStoreCatalog(catalog).build();
            this.driver = new EmbeddedRelationalDriver(RecordLayerEngine.makeEngine(
                    rlConfig,
                    Collections.singletonList(fdbDb),
                    keySpace,
                    catalog,
                    null,
                    ddlFactory,
                    planCache));
            DriverManager.registerDriver(driver);

            createSchemaTemplate();
        }

        // Disabling the driver to be de-registered owing to TODO
        public void down() throws RelationalException {
            // engine.deregisterDriver();
        }

        private void createSchemaTemplate() throws SQLException {
            try (final var conn = DriverManager.getConnection("jdbc:embed:/__SYS")) {
                conn.setSchema("CATALOG");
                try (final var statement = conn.createStatement()) {
                    statement.executeUpdate("CREATE SCHEMA TEMPLATE \"" + templateName + "\" " + templateDef);
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

        public void createDatabase(URI path, String templateName, String... schemas) throws RelationalException, SQLException {
            EmbeddedRelationalBenchmark.createDatabase(path, templateName, schemas);
            databases.add(path.getPath());
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
        try (final var conn = DriverManager.getConnection("jdbc:embed:/__SYS")) {
            conn.setSchema("CATALOG");
            try (final var statement = conn.createStatement()) {
                statement.executeUpdate("CREATE DATABASE \"" + dbUri + "\"");
                for (Map.Entry<String, String> schemaTemplateEntry : dbTemplate.getSchemaToTemplateNameMap().entrySet()) {
                    statement.executeUpdate("CREATE SCHEMA \"" + dbUri + "/" + schemaTemplateEntry.getKey() + "\" WITH TEMPLATE \"" + schemaTemplateEntry.getValue() + "\"");
                }
            }
        }
    }

    public static void createDatabase(@Nonnull URI dbUri, String templateName, String... schemas) throws RelationalException, SQLException {
        try (final var conn = DriverManager.getConnection("jdbc:embed:/__SYS")) {
            conn.setSchema("CATALOG");
            try (final var statement = conn.createStatement()) {
                try {
                    statement.executeUpdate("CREATE DATABASE \"" + dbUri.getPath() + "\"");
                } catch (SQLException se) {
                    if (se.getSQLState().equals(ErrorCode.DATABASE_ALREADY_EXISTS.getErrorCode())) {
                        statement.executeUpdate("DROP DATABASE \"" + dbUri.getPath() + "\"");
                        statement.executeUpdate("CREATE DATABASE \"" + dbUri.getPath() + "\"");
                    } else {
                        throw se;
                    }
                }
                for (String schema : schemas) {
                    statement.executeUpdate("CREATE SCHEMA \"" + dbUri + "/" + schema + "\" WITH TEMPLATE \"" + templateName + "\"");
                }
            }
        }
    }

    public static void deleteDatabase(URI dbUri) throws RelationalException, SQLException {
        try (final var conn = DriverManager.getConnection("jdbc:embed:/__SYS")) {
            conn.setSchema("CATALOG");
            try (final var statement = conn.createStatement()) {
                statement.executeUpdate("DROP DATABASE \"" + dbUri + "\"");
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
