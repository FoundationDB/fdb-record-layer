/*
 * BackingLocatableResolverStoreTest.java
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

package com.apple.foundationdb.relational.recordlayer.storage;

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.LocatableResolver;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolvedKeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverCreateHooks;
import com.apple.foundationdb.record.provider.foundationdb.layers.interning.ScopedInterningLayer;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.StorageCluster;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.TransactionManager;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.catalog.RelationalDatabase;
import com.apple.foundationdb.relational.api.ddl.NoOpQueryFactory;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.memory.InMemoryCatalog;
import com.apple.foundationdb.relational.recordlayer.AbstractDatabase;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.ddl.NoOpMetadataOperationsFactory;
import com.apple.foundationdb.relational.utils.ResultSetAssert;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Tests of the {@link BackingLocatableResolverStore}.
 */
public class BackingLocatableResolverStoreTest {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    private StorageCluster storageCluster;
    private KeySpacePath path;

    @BeforeEach
    void setUp() {
        storageCluster = relationalExtension.getEngine()
                .getStorageClusters()
                .stream()
                .findFirst()
                .orElseThrow();
        KeySpace keySpace = new KeySpace(new KeySpaceDirectory("test", KeySpaceDirectory.KeyType.STRING, "lr_store_test")
                .addSubdirectory(new KeySpaceDirectory("test_id", KeySpaceDirectory.KeyType.UUID)));
        path = keySpace.path("test")
                .add("test_id", UUID.randomUUID());
    }

    @AfterEach
    void tearDown() throws RelationalException {
        try (FDBRecordContext context = createTransaction().unwrap(FDBRecordContext.class)) {
            path.deleteAllData(context);
            context.commit();
        }
        path = null;
    }

    private static class TransactionBoundLocatableResolverDatabase extends AbstractDatabase {
        private final URI dbPath;
        private final TransactionManager transactionManager;
        private final LocatableResolver resolver;
        private final ResolverCreateHooks resolverCreateHooks;
        private final StoreCatalog catalog;
        private final SchemaTemplate schemaTemplate;
        private Transaction txn;
        private boolean ownsTransaction;

        public TransactionBoundLocatableResolverDatabase(@Nonnull URI dbPath,
                                                         @Nonnull StorageCluster cluster,
                                                         @Nonnull LocatableResolver resolver,
                                                         @Nonnull ResolverCreateHooks createHooks) {
            super(NoOpMetadataOperationsFactory.INSTANCE, NoOpQueryFactory.INSTANCE, null);
            this.dbPath = dbPath;
            this.transactionManager = cluster.getTransactionManager();
            this.resolver = resolver;
            this.resolverCreateHooks = createHooks;
            this.catalog = new InMemoryCatalog();
            this.schemaTemplate = LocatableResolverMetaDataProvider.getSchemaTemplate();
        }

        @Override
        public RelationalConnection connect(@Nullable Transaction sharedTransaction) throws RelationalException {
            Transaction transaction;
            if (sharedTransaction == null) {
                transaction = transactionManager.createTransaction(Options.NONE);
                ownsTransaction = true;
            } else {
                transaction = sharedTransaction;
            }
            if (!catalog.doesDatabaseExist(transaction, getURI())) {
                catalog.createDatabase(transaction, getURI());
                catalog.saveSchema(transaction, schemaTemplate.generateSchema(getURI().getPath(), "dl"), false);
            }
            this.txn = transaction;
            EmbeddedRelationalConnection connection = new EmbeddedRelationalConnection(this, catalog, transaction, Options.NONE);
            setConnection(connection);
            return connection;
        }

        @Override
        public void close() throws RelationalException {
            if (txn != null && ownsTransaction) {
                txn.close();
            }
            txn = null;
        }

        @Override
        public BackingStore loadRecordStore(@Nonnull String schemaId, @Nonnull FDBRecordStoreBase.StoreExistenceCheck existenceCheck) throws RelationalException {
            return BackingLocatableResolverStore.create(resolver, resolverCreateHooks, txn);
        }

        @Override
        public URI getURI() {
            return dbPath;
        }

        @Override
        public TransactionManager getTransactionManager() {
            return transactionManager;
        }
    }

    private Transaction createTransaction() throws RelationalException {
        return storageCluster.getTransactionManager().createTransaction(Options.NONE);
    }

    private ScopedInterningLayer createScopedInterningLayer() throws RelationalException {
        try (Transaction transaction = createTransaction()) {
            FDBRecordContext context = transaction.unwrap(FDBRecordContext.class);
            FDBDatabase database = context.getDatabase();
            ResolvedKeySpacePath resolvedPath = path.toResolvedPath(context);
            return new ScopedInterningLayer(database, resolvedPath);
        }
    }

    private RelationalDatabase createScopedInterningDatabase() throws RelationalException {
        return createScopedInterningDatabase(ResolverCreateHooks.getDefault());
    }

    private RelationalDatabase createScopedInterningDatabase(ResolverCreateHooks createHooks) throws RelationalException {
        ScopedInterningLayer interningLayer = createScopedInterningLayer();
        return new TransactionBoundLocatableResolverDatabase(URI.create("blah/blah"), storageCluster, interningLayer, createHooks);
    }

    @Test
    void basicResolve() throws RelationalException, SQLException {
        RelationalDatabase db = createScopedInterningDatabase();
        resolveMappings(db, 1);
    }

    @Test
    void resolveMultiple() throws RelationalException, SQLException {
        RelationalDatabase db = createScopedInterningDatabase();
        Map<String, Long> mappings = resolveMappings(db, 20);

        try (RelationalConnection connection = db.connect(null)) {
            connection.setSchema("dl");
            try (RelationalStatement statement = connection.createStatement()) {
                for (Map.Entry<String, Long> entry : mappings.entrySet()) {
                    KeySet key = new KeySet()
                            .setKeyColumn("key", entry.getKey());
                    try (RelationalResultSet resultSet = statement.executeGet("Interning", key, Options.NONE)) {
                        ResultSetAssert.assertThat(resultSet)
                                .hasNextRow()
                                .row()
                                .hasValue("key", entry.getKey())
                                .hasValue("value", entry.getValue());
                        ResultSetAssert.assertThat(resultSet)
                                .hasNoNextRow();
                    }
                }
            }
        }
    }

    @Test
    void resolveReverse() throws RelationalException, SQLException {
        RelationalDatabase db = createScopedInterningDatabase();
        Map<String, Long> mappings = resolveMappings(db, 15);

        try (RelationalConnection connection = db.connect(null)) {
            connection.setSchema("dl");
            try (RelationalStatement statement = connection.createStatement()) {
                Options options = Options.builder()
                        .withOption(Options.Name.INDEX_HINT, LocatableResolverMetaDataProvider.REVERSE_INDEX_NAME)
                        .build();
                for (Map.Entry<String, Long> entry : mappings.entrySet()) {
                    KeySet keySet = new KeySet()
                            .setKeyColumn("value", entry.getValue());
                    try (RelationalResultSet resultSet = statement.executeGet("Interning", keySet, options)) {
                        ResultSetAssert.assertThat(resultSet)
                                .hasNextRow()
                                .row()
                                .hasValue("key", entry.getKey())
                                .hasValue("value", entry.getValue());
                        ResultSetAssert.assertThat(resultSet)
                                .hasNoNextRow();
                    }
                }
            }
        }
    }

    @Test
    void listMappings() throws RelationalException, SQLException {
        RelationalDatabase db = createScopedInterningDatabase();
        Map<String, Long> mappings = resolveMappings(db, 20);

        try (RelationalConnection connection = db.connect(null)) {
            connection.setSchema("dl");
            try (RelationalStatement statement = connection.createStatement()) {
                Set<String> found = new HashSet<>();
                try (RelationalResultSet resultSet = statement.executeScan("Interning", new KeySet(), Options.NONE)) {
                    while (resultSet.next()) {
                        String key = resultSet.getString("key");
                        long value = resultSet.getLong("value");
                        Assertions.assertThat(mappings)
                                .containsEntry(key, value);
                        found.add(key);
                    }
                }
                Assertions.assertThat(found)
                        .containsExactlyElementsOf(mappings.keySet());
            }
        }
    }

    @Test
    void listMappingsWithContinuation() throws RelationalException, SQLException {
        RelationalDatabase db = createScopedInterningDatabase();
        Map<String, Long> mappings = resolveMappings(db, 100);

        try (RelationalConnection connection = db.connect(null)) {
            connection.setSchema("dl");
            final int pageSize = 10;
            Map<String, Long> found = new HashMap<>();
            Continuation continuation = Continuation.BEGIN;

            while (!continuation.atEnd()) {
                try (RelationalStatement statement = connection.createStatement()) {
                    Options options = Options.builder()
                            .withOption(Options.Name.CONTINUATION_PAGE_SIZE, pageSize)
                            .withOption(Options.Name.CONTINUATION, continuation)
                            .build();
                    try (RelationalResultSet resultSet = statement.executeScan("Interning", new KeySet(), options)) {
                        int currentCount = 0;
                        while (currentCount < pageSize && found.size() < mappings.size()) {
                            ResultSetAssert.assertThat(resultSet)
                                    .hasNextRow();
                            currentCount++;
                            String key = resultSet.getString("key");
                            long value = resultSet.getLong("value");
                            Assertions.assertThat(found)
                                    .doesNotContainKey(key);
                            Assertions.assertThat(mappings)
                                    .containsEntry(key, value);
                            found.put(key, value);
                        }
                        ResultSetAssert.assertThat(resultSet)
                                .hasNoNextRow();
                        continuation = resultSet.getContinuation();
                    }
                }
            }

            Assertions.assertThat(found)
                    .containsExactlyEntriesOf(mappings);
        }
    }

    @Test
    void withCreateHooks() throws RelationalException, SQLException {
        // Use resolver create hooks to insert the name into the meta-data field
        ResolverCreateHooks.MetadataHook metadataHook = name -> name.getBytes(StandardCharsets.UTF_8);
        ResolverCreateHooks hooks = new ResolverCreateHooks(Collections.emptyList(), metadataHook);
        RelationalDatabase db = createScopedInterningDatabase(hooks);
        Map<String, Long> mappings = resolveMappings(db, 10);

        try (RelationalConnection connection = db.connect(null)) {
            connection.setSchema("dl");
            try (RelationalStatement statement = connection.createStatement()) {
                for (Map.Entry<String, Long> entry : mappings.entrySet()) {
                    KeySet keySet = new KeySet()
                            .setKeyColumn("key", entry.getKey());
                    try (RelationalResultSet resultSet = statement.executeGet("Interning", keySet, Options.NONE)) {
                        ResultSetAssert.assertThat(resultSet)
                                .hasNextRow()
                                .row()
                                .hasValue("key", entry.getKey())
                                .hasValue("value", entry.getValue());
                        byte[] metaData = resultSet.getBytes("meta_data");
                        Assertions.assertThat(metaData)
                                .containsExactly(entry.getKey().getBytes(StandardCharsets.UTF_8));
                        ResultSetAssert.assertThat(resultSet)
                                .hasNoNextRow();
                    }
                }
            }
        }

        // Change the meta-data hooks to instead put the length of the name
        ResolverCreateHooks.MetadataHook newMetaDataHook = name -> ("" + name.length()).getBytes(StandardCharsets.UTF_8);
        ResolverCreateHooks newHooks = new ResolverCreateHooks(Collections.emptyList(), newMetaDataHook);
        RelationalDatabase newDb = createScopedInterningDatabase(newHooks);
        Map<String, Long> newMappings = resolveMappings(newDb, 20);
        Assertions.assertThat(newMappings)
                .containsAllEntriesOf(mappings);

        // Go through and validate that any entry which was written in the first pass retains its original
        // meta-data value, and that values written in the second pass get the new meta-data value
        try (RelationalConnection connection = newDb.connect(null)) {
            connection.setSchema("dl");
            try (RelationalStatement statement = connection.createStatement()) {
                for (Map.Entry<String, Long> entry : newMappings.entrySet()) {
                    KeySet keySet = new KeySet()
                            .setKeyColumn("key", entry.getKey());
                    try (RelationalResultSet resultSet = statement.executeGet("Interning", keySet, Options.NONE)) {
                        ResultSetAssert.assertThat(resultSet)
                                .hasNextRow()
                                .row()
                                .hasValue("key", entry.getKey())
                                .hasValue("value", entry.getValue());
                        byte[] metaData = resultSet.getBytes("meta_data");
                        if (mappings.containsKey(entry.getKey())) {
                            Assertions.assertThat(metaData)
                                    .containsExactly(entry.getKey().getBytes(StandardCharsets.UTF_8));
                        } else {
                            Assertions.assertThat(metaData)
                                    .containsExactly(("" + entry.getKey().length()).getBytes(StandardCharsets.UTF_8));
                        }
                        ResultSetAssert.assertThat(resultSet)
                                .hasNoNextRow();
                    }
                }
            }
        }
    }

    @Test
    void resolverState() throws RelationalException, SQLException {
        RelationalDatabase db = createScopedInterningDatabase();
        ScopedInterningLayer interningLayer = createScopedInterningLayer();

        try (RelationalConnection connection = db.connect(null)) {
            connection.setSchema("dl");
            try (RelationalStatement statement = connection.createStatement()) {
                long version = interningLayer.getVersion(null).join();
                assertVersionMatches(statement, version);

                interningLayer.incrementVersion().join();
                interningLayer.getDatabase().clearCaches();
                assertVersionMatches(statement, version + 1L);

                interningLayer.enableWriteLock().join();
                interningLayer.getDatabase().clearCaches();
                assertVersionMatches(statement, version + 1L);

                interningLayer.disableWriteLock().join();
                interningLayer.getDatabase().clearCaches();
                assertVersionMatches(statement, version + 1L);

                interningLayer.retireLayer().join();
                interningLayer.getDatabase().clearCaches();
                assertVersionMatches(statement, version + 1L);
            }
        }
    }

    private Map<String, Long> resolveMappings(RelationalDatabase db, int count) throws RelationalException, SQLException {
        Map<String, Long> mappings = new HashMap<>();
        try (RelationalConnection connection = db.connect(null)) {
            connection.setSchema("dl");
            try (RelationalStatement statement = connection.createStatement()) {
                for (int i = 0; i < count; i++) {
                    String name = String.format("val_%02d", i);
                    KeySet key = new KeySet()
                            .setKeyColumn("key", name);
                    try (RelationalResultSet resultSet = statement.executeGet("Interning", key, Options.NONE)) {
                        ResultSetAssert.assertThat(resultSet)
                                .hasNextRow();
                        mappings.put(name, resultSet.getLong("value"));
                    }
                }

                Assertions.assertThat(mappings.values())
                        .as("all resolved values should be unique")
                        .doesNotHaveDuplicates();

                for (Map.Entry<String, Long> entry : mappings.entrySet()) {
                    KeySet key = new KeySet()
                            .setKeyColumn("key", entry.getKey());
                    try (RelationalResultSet resultSet = statement.executeGet("Interning", key, Options.NONE)) {
                        ResultSetAssert.assertThat(resultSet)
                                .hasNextRow()
                                .row()
                                .hasValue("key", entry.getKey())
                                .hasValue("value", entry.getValue());
                        ResultSetAssert.assertThat(resultSet)
                                .hasNoNextRow();
                    }
                }
            }
        }

        return mappings;
    }

    private void assertVersionMatches(RelationalStatement statement, long version) throws SQLException {
        try (RelationalResultSet resultSet = statement.executeGet("ResolverState", new KeySet(), Options.NONE)) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .row()
                    .hasValue("version", version);
            ResultSetAssert.assertThat(resultSet)
                    .hasNoNextRow();
        }

        try (RelationalResultSet resultSet = statement.executeScan("ResolverState", new KeySet(), Options.NONE)) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .row()
                    .hasValue("version", version);
            ResultSetAssert.assertThat(resultSet)
                    .hasNoNextRow();
        }
    }
}
