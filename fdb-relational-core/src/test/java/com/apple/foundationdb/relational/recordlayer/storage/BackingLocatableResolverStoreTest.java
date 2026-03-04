/*
 * BackingLocatableResolverStoreTest.java
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
import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.StorageCluster;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.TransactionManager;
import com.apple.foundationdb.relational.api.catalog.RelationalDatabase;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.ddl.NoOpQueryFactory;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.memory.InMemoryCatalog;
import com.apple.foundationdb.relational.recordlayer.AbstractDatabase;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.ddl.NoOpMetadataOperationsFactory;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * Tests of the {@link BackingLocatableResolverStore}.
 */
public class BackingLocatableResolverStoreTest {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    private StorageCluster storageCluster;
    private KeySpacePath path;
    private URI dbPath;
    private StoreCatalog catalog;

    @BeforeEach
    void setUp() throws RelationalException {
        storageCluster = relationalExtension.getEngine()
                .getStorageClusters()
                .stream()
                .findFirst()
                .orElseThrow();
        KeySpace keySpace = new KeySpace(new KeySpaceDirectory("test", KeySpaceDirectory.KeyType.STRING, "lr_store_test")
                .addSubdirectory(new KeySpaceDirectory("test_id", KeySpaceDirectory.KeyType.UUID)));
        path = keySpace.path("test")
                .add("test_id", UUID.randomUUID());

        catalog = new InMemoryCatalog();
        dbPath = URI.create("blah/blah");
        try (Transaction transaction = createTransaction()) {
            SchemaTemplate template = LocatableResolverMetaDataProvider.getSchemaTemplate();
            catalog.saveSchema(transaction, template.generateSchema(dbPath.toString(), "dl"), true);
            transaction.commit();
        }
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
        private final StoreCatalog catalog;
        private Transaction txn;

        public TransactionBoundLocatableResolverDatabase(@Nonnull URI dbPath,
                                                         @Nonnull StorageCluster cluster,
                                                         @Nonnull LocatableResolver resolver,
                                                         @Nonnull StoreCatalog catalog) {
            super(NoOpMetadataOperationsFactory.INSTANCE, NoOpQueryFactory.INSTANCE, null, Options.NONE);
            this.dbPath = dbPath;
            this.transactionManager = cluster.getTransactionManager();
            this.resolver = resolver;
            this.catalog = catalog;
        }

        @Override
        public RelationalConnection connect(@Nullable Transaction sharedTransaction) throws RelationalException {
            EmbeddedRelationalConnection connection = new EmbeddedRelationalConnection(this, catalog, sharedTransaction, Options.NONE);
            setConnection(connection);
            return connection;
        }

        @Override
        public void close() throws RelationalException {
            if (txn != null) {
                txn.close();
            }
            txn = null;
        }

        @Override
        public BackingStore loadRecordStore(@Nonnull String schemaId, @Nonnull FDBRecordStoreBase.StoreExistenceCheck existenceCheck) throws RelationalException {
            return BackingLocatableResolverStore.create(resolver, getCurrentTransaction());
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
        ScopedInterningLayer interningLayer = createScopedInterningLayer();
        return new TransactionBoundLocatableResolverDatabase(dbPath, storageCluster, interningLayer, catalog);
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
                    assertMapping(statement, entry.getKey(), entry.getValue(), null);
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
            Continuation continuation = ContinuationImpl.BEGIN;

            while (!continuation.atEnd()) {
                try (RelationalStatement statement = connection.createStatement()) {
                    Options options = Options.builder()
                            .withOption(Options.Name.MAX_ROWS, pageSize)
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
                        Assertions.assertThat(continuation.getReason())
                                .as("Continuation reasons are all erroneously null due to: https://github.com/FoundationDB/fdb-record-layer/issues/3227")
                                .isNull();
                    }
                }
            }

            Assertions.assertThat(found)
                    .containsExactlyEntriesOf(mappings);
        }
    }

    @Test
    void withMetaData() throws RelationalException, SQLException {
        // Use resolver create hooks to insert the name into the meta-data field
        RelationalDatabase db = createScopedInterningDatabase();
        Map<String, Long> mappings = resolveMappings(db, 10, name -> name.getBytes(StandardCharsets.UTF_8));

        try (RelationalConnection connection = db.connect(null)) {
            connection.setSchema("dl");
            try (RelationalStatement statement = connection.createStatement()) {
                for (Map.Entry<String, Long> mapping : mappings.entrySet()) {
                    assertMapping(statement, mapping.getKey(), mapping.getValue(), mapping.getKey().getBytes(StandardCharsets.UTF_8));
                }
            }
        }

        // Change the meta-data hooks to instead put the length of the name
        Map<String, Long> newMappings = resolveMappings(db, 20, name -> ("" + name.length()).getBytes(StandardCharsets.UTF_8));
        Assertions.assertThat(newMappings)
                .containsAllEntriesOf(mappings);

        // Go through and validate that any entry which was written in the first pass retains its original
        // meta-data value, and that values written in the second pass get the new meta-data value
        try (RelationalConnection connection = db.connect(null)) {
            connection.setSchema("dl");
            try (RelationalStatement statement = connection.createStatement()) {
                for (Map.Entry<String, Long> entry : newMappings.entrySet()) {
                    String metaDataBasis = mappings.containsKey(entry.getKey()) ? entry.getKey() : ("" + entry.getKey().length());
                    byte[] expectedMetaData = metaDataBasis.getBytes(StandardCharsets.UTF_8);
                    assertMapping(statement, entry.getKey(), entry.getValue(), expectedMetaData);
                }
            }
        }
    }

    @Test
    void updateMapping() throws RelationalException, SQLException {
        // Use resolver create hooks to insert the name into the meta-data field
        RelationalDatabase db = createScopedInterningDatabase();
        Map<String, Long> mappings = resolveMappings(db, 10, name -> name.getBytes(StandardCharsets.UTF_8));

        try (RelationalConnection connection = db.connect(null)) {
            connection.setSchema("dl");
            try (RelationalStatement statement = connection.createStatement()) {
                // Update a few mappings by changing their meta-data
                for (Map.Entry<String, Long> mapping : mappings.entrySet()) {
                    if (mapping.getValue() % 2 == 0) {
                        final var struct = EmbeddedRelationalStruct.newBuilder()
                                .addString(LocatableResolverMetaDataProvider.KEY_FIELD_NAME, mapping.getKey())
                                .addBytes(LocatableResolverMetaDataProvider.META_DATA_FIELD_NAME, ("foo_" + mapping.getKey()).getBytes())
                                .build();
                        RelationalAssertions.assertThrowsSqlException(() -> statement.executeInsert(LocatableResolverMetaDataProvider.INTERNING_TYPE_NAME, struct))
                                .hasErrorCode(ErrorCode.UNIQUE_CONSTRAINT_VIOLATION);

                        Options options = Options.builder()
                                .withOption(Options.Name.REPLACE_ON_DUPLICATE_PK, true)
                                .build();
                        RelationalAssertions.assertThrowsSqlException(() -> statement.executeInsert(LocatableResolverMetaDataProvider.INTERNING_TYPE_NAME, struct, options))
                                .hasMessageContaining("Cannot update table");
                    }
                }

                // Validate meta-data has not changed as all updates failed
                for (Map.Entry<String, Long> mapping : mappings.entrySet()) {
                    assertMapping(statement, mapping.getKey(), mapping.getValue(), mapping.getKey().getBytes(StandardCharsets.UTF_8));
                }

                // Change the actual values for some mappings
                for (Map.Entry<String, Long> mapping : mappings.entrySet()) {
                    if (mapping.getValue() % 2 != 0) {
                        final var struct = EmbeddedRelationalStruct.newBuilder()
                                .addString(LocatableResolverMetaDataProvider.KEY_FIELD_NAME, mapping.getKey())
                                .addLong(LocatableResolverMetaDataProvider.VALUE_FIELD_NAME, 100L + mapping.getValue())
                                .build();

                        RelationalAssertions.assertThrowsSqlException(() -> statement.executeInsert(LocatableResolverMetaDataProvider.INTERNING_TYPE_NAME, struct))
                                .hasErrorCode(ErrorCode.UNIQUE_CONSTRAINT_VIOLATION);

                        Options options = Options.builder()
                                .withOption(Options.Name.REPLACE_ON_DUPLICATE_PK, true)
                                .build();
                        RelationalAssertions.assertThrowsSqlException(() -> statement.executeInsert(LocatableResolverMetaDataProvider.INTERNING_TYPE_NAME, struct, options))
                                .hasMessageContaining("Cannot update table");
                    }
                }

                // Validate mappings have changed
                for (Map.Entry<String, Long> mapping : mappings.entrySet()) {
                    assertMapping(statement, mapping.getKey(), mapping.getValue(), mapping.getKey().getBytes(StandardCharsets.UTF_8));
                }
            }
        }
    }

    @Test
    void insertPreAllocated() throws RelationalException, SQLException {
        // Pre-allocate a mapping from values to longs. This represents something like copying data from one locatable resolver to another
        Map<String, Long> mappings = LongStream.range(1, 20)
                .boxed()
                .collect(Collectors.toMap(v -> String.format(Locale.ROOT, "val_%d", v), Function.identity()));
        RelationalDatabase db = createScopedInterningDatabase();

        try (RelationalConnection connection = db.connect(null)) {
            connection.setSchema("dl");
            try (RelationalStatement statement = connection.createStatement()) {
                for (Map.Entry<String, Long> mapping : mappings.entrySet()) {
                    RelationalStruct struct;
                    if (mapping.getValue() % 2 == 0) {
                        struct = EmbeddedRelationalStruct.newBuilder()
                                .addString(LocatableResolverMetaDataProvider.KEY_FIELD_NAME, mapping.getKey())
                                .addLong(LocatableResolverMetaDataProvider.VALUE_FIELD_NAME, 100L + mapping.getValue())
                                .build();
                    } else {
                        struct = EmbeddedRelationalStruct.newBuilder()
                                .addString(LocatableResolverMetaDataProvider.KEY_FIELD_NAME, mapping.getKey())
                                .addLong(LocatableResolverMetaDataProvider.VALUE_FIELD_NAME, 100L + mapping.getValue())
                                .addBytes(LocatableResolverMetaDataProvider.META_DATA_FIELD_NAME, mapping.getKey().getBytes())
                                .build();
                    }
                    RelationalAssertions.assertThrowsSqlException(() -> statement.executeInsert(LocatableResolverMetaDataProvider.INTERNING_TYPE_NAME, struct))
                            .hasErrorCode(ErrorCode.UNSUPPORTED_OPERATION);
                }

                for (Map.Entry<String, Long> mapping : mappings.entrySet()) {
                    assertNoMapping(statement, mapping.getKey());
                    assertNoReverseMapping(statement, mapping.getValue());
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
                int version = interningLayer.getVersion(null).join();
                assertStateMatches(statement, version, "UNLOCKED");

                interningLayer.incrementVersion().join();
                assertStateMatches(statement, version + 1, "UNLOCKED");

                insertResolverState(statement, version + 2, "UNLOCKED");
                assertStateMatches(statement, version + 2, "UNLOCKED");

                interningLayer.enableWriteLock().join();
                assertStateMatches(statement, version + 2, "WRITE_LOCKED");

                interningLayer.disableWriteLock().join();
                assertStateMatches(statement, version + 2, "UNLOCKED");

                interningLayer.retireLayer().join();
                assertStateMatches(statement, version + 2, "RETIRED");

                insertResolverState(statement, version + 3, "RETIRED");
                assertStateMatches(statement, version + 3, "RETIRED");

                insertResolverState(statement, version + 3, "UNLOCKED");
                assertStateMatches(statement, version + 3, "UNLOCKED");

                RelationalAssertions.assertThrowsSqlException(() -> insertResolverState(statement, version + 2, "UNLOCKED"))
                        .hasMessageContaining("resolver state version must monotonically increase");
                assertStateMatches(statement, version + 3, "UNLOCKED");
            }
        }
    }

    @Test
    void listResolverState() throws RelationalException, SQLException {
        RelationalDatabase db = createScopedInterningDatabase();
        ScopedInterningLayer interningLayer = createScopedInterningLayer();

        try (RelationalConnection connection = db.connect(null)) {
            connection.setSchema("dl");
            try (RelationalStatement statement = connection.createStatement()) {
                int version = interningLayer.getVersion(null).join();

                try (RelationalResultSet resultSet = scanResolverStates(statement)) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow()
                            .row()
                            .hasValue(LocatableResolverMetaDataProvider.VERSION_FIELD_NAME, version)
                            .hasValue(LocatableResolverMetaDataProvider.LOCK_FIELD_NAME, "UNLOCKED");
                    ResultSetAssert.assertThat(resultSet)
                            .hasNoNextRow();
                    Assertions.assertThat(resultSet.getContinuation().atBeginning())
                            .isFalse();
                    Assertions.assertThat(resultSet.getContinuation().atEnd())
                            .isTrue();
                }
            }
        }
    }

    @Test
    void listResolverStateWithContinuation() throws RelationalException, SQLException {
        RelationalDatabase db = createScopedInterningDatabase();
        ScopedInterningLayer interningLayer = createScopedInterningLayer();

        try (RelationalConnection connection = db.connect(null)) {
            connection.setSchema("dl");
            try (RelationalStatement statement = connection.createStatement()) {
                int version = interningLayer.getVersion(null).join();

                Continuation continuation;
                try (RelationalResultSet resultSet = scanResolverStates(statement, 1, ContinuationImpl.BEGIN)) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow()
                            .row()
                            .hasValue(LocatableResolverMetaDataProvider.VERSION_FIELD_NAME, version)
                            .hasValue(LocatableResolverMetaDataProvider.LOCK_FIELD_NAME, "UNLOCKED");
                    ResultSetAssert.assertThat(resultSet)
                            .hasNoNextRow();
                    continuation = resultSet.getContinuation();
                    Assertions.assertThat(continuation.atBeginning())
                            .isFalse();
                    Assertions.assertThat(continuation.atEnd())
                            .isFalse();
                    Assertions.assertThat(continuation.getReason())
                            .as("Continuation reasons are all erroneously null due to: https://github.com/FoundationDB/fdb-record-layer/issues/3227")
                            .isNull();
                }
                // There's always only a single record, so just assert that there is not another row returned
                // when resumed from a continuation
                try (RelationalResultSet resultSet = scanResolverStates(statement, 1, continuation)) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNoNextRow();
                    continuation = resultSet.getContinuation();
                    Assertions.assertThat(continuation.atBeginning())
                            .isFalse();
                    Assertions.assertThat(continuation.atEnd())
                            .isTrue();
                    Assertions.assertThat(continuation.getReason())
                            .as("Continuation reasons are all erroneously null due to: https://github.com/FoundationDB/fdb-record-layer/issues/3227")
                            .isNull();
                }
            }
        }
    }

    private Map<String, Long> resolveMappings(RelationalDatabase db, int count) throws RelationalException, SQLException {
        return resolveMappings(db, count, ignore -> null);
    }

    private Map<String, Long> resolveMappings(RelationalDatabase db, int count, ResolverCreateHooks.MetadataHook metadataHook) throws RelationalException, SQLException {
        Map<String, Long> mappings = new HashMap<>();
        try (RelationalConnection connection = db.connect(null)) {
            connection.setSchema("dl");
            try (RelationalStatement statement = connection.createStatement()) {
                for (int i = 0; i < count; i++) {
                    String name = String.format(Locale.ROOT, "val_%02d", i);
                    byte[] metaData = metadataHook.apply(name);
                    RelationalStruct struct;
                    if (metaData != null) {
                        struct = EmbeddedRelationalStruct.newBuilder()
                                .addString(LocatableResolverMetaDataProvider.KEY_FIELD_NAME, name)
                                .addBytes(LocatableResolverMetaDataProvider.META_DATA_FIELD_NAME, metaData)
                                .build();
                    } else {
                        struct = EmbeddedRelationalStruct.newBuilder()
                                .addString(LocatableResolverMetaDataProvider.KEY_FIELD_NAME, name)
                                .build();
                    }
                    try {
                        statement.executeInsert("Interning", struct);
                    } catch (SQLException err) {
                        // Ignore duplicate primary keys, as previous runs can insert the data
                        if (!err.getSQLState().equals(ErrorCode.UNIQUE_CONSTRAINT_VIOLATION.getErrorCode())) {
                            throw err;
                        }
                    }

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

    private void assertNoMapping(RelationalStatement statement, String key) throws SQLException {
        KeySet keySet = new KeySet()
                .setKeyColumn(LocatableResolverMetaDataProvider.KEY_FIELD_NAME, key);
        try (RelationalResultSet resultSet = statement.executeGet(LocatableResolverMetaDataProvider.INTERNING_TYPE_NAME, keySet, Options.NONE)) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNoNextRow();
        }
    }

    private void assertNoReverseMapping(RelationalStatement statement, long value) throws SQLException {
        KeySet keySet = new KeySet()
                .setKeyColumn(LocatableResolverMetaDataProvider.VALUE_FIELD_NAME, value);
        Options reverseLookupOptions = Options.builder()
                .withOption(Options.Name.INDEX_HINT, LocatableResolverMetaDataProvider.REVERSE_INDEX_NAME)
                .build();
        try (RelationalResultSet resultSet = statement.executeGet(LocatableResolverMetaDataProvider.INTERNING_TYPE_NAME, keySet, reverseLookupOptions)) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNoNextRow();
        }
    }

    private void assertMapping(RelationalStatement statement, String key, long value, @Nullable byte[] metaData) throws SQLException {
        KeySet keySet = new KeySet()
                .setKeyColumn(LocatableResolverMetaDataProvider.KEY_FIELD_NAME, key);
        try (RelationalResultSet resultSet = statement.executeGet(LocatableResolverMetaDataProvider.INTERNING_TYPE_NAME, keySet, Options.NONE)) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .row()
                    .hasValue(LocatableResolverMetaDataProvider.KEY_FIELD_NAME, key)
                    .hasValue(LocatableResolverMetaDataProvider.VALUE_FIELD_NAME, value)
                    .hasValue(LocatableResolverMetaDataProvider.META_DATA_FIELD_NAME, metaData);
            ResultSetAssert.assertThat(resultSet)
                    .hasNoNextRow();
        }

        KeySet reverseKeySet = new KeySet()
                .setKeyColumn(LocatableResolverMetaDataProvider.VALUE_FIELD_NAME, value);
        Options reverseLookupOptions = Options.builder()
                .withOption(Options.Name.INDEX_HINT, LocatableResolverMetaDataProvider.REVERSE_INDEX_NAME)
                .build();
        try (RelationalResultSet resultSet = statement.executeGet(LocatableResolverMetaDataProvider.INTERNING_TYPE_NAME, reverseKeySet, reverseLookupOptions)) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .row()
                    .hasValue(LocatableResolverMetaDataProvider.KEY_FIELD_NAME, key)
                    .hasValue(LocatableResolverMetaDataProvider.VALUE_FIELD_NAME, value);
            ResultSetAssert.assertThat(resultSet)
                    .hasNoNextRow();
        }
    }

    private void assertStateMatches(RelationalStatement statement, int version, String lock) throws SQLException {
        try (RelationalResultSet resultSet = statement.executeGet(LocatableResolverMetaDataProvider.RESOLVER_STATE_TYPE_NAME, new KeySet(), Options.NONE)) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .row()
                    .hasValue(LocatableResolverMetaDataProvider.VERSION_FIELD_NAME, version)
                    .hasValue(LocatableResolverMetaDataProvider.LOCK_FIELD_NAME, lock);
            ResultSetAssert.assertThat(resultSet)
                    .hasNoNextRow();
        }

        try (RelationalResultSet resultSet = scanResolverStates(statement)) {
            ResultSetAssert.assertThat(resultSet)
                    .hasNextRow()
                    .row()
                    .hasValue(LocatableResolverMetaDataProvider.VERSION_FIELD_NAME, version)
                    .hasValue(LocatableResolverMetaDataProvider.LOCK_FIELD_NAME, lock);
            ResultSetAssert.assertThat(resultSet)
                    .hasNoNextRow();
        }
    }

    private void insertResolverState(RelationalStatement statement, int version, String lock) throws SQLException {
        final var struct = EmbeddedRelationalStruct.newBuilder()
                .addInt(LocatableResolverMetaDataProvider.VERSION_FIELD_NAME, version)
                .addObject(LocatableResolverMetaDataProvider.LOCK_FIELD_NAME, lock)
                .build();
        Options options = Options.builder()
                .withOption(Options.Name.REPLACE_ON_DUPLICATE_PK, true)
                .build();
        statement.executeInsert(LocatableResolverMetaDataProvider.RESOLVER_STATE_TYPE_NAME, struct, options);
    }

    @Nonnull
    private RelationalResultSet scanResolverStates(RelationalStatement statement) throws SQLException {
        return scanResolverStates(statement, 0, ContinuationImpl.BEGIN);
    }

    @Nonnull
    private RelationalResultSet scanResolverStates(RelationalStatement statement, int maxRows, Continuation continuation) throws SQLException {
        Options options = Options.builder()
                .withOption(Options.Name.MAX_ROWS, maxRows)
                .withOption(Options.Name.CONTINUATION, continuation)
                .build();
        return statement.executeScan(LocatableResolverMetaDataProvider.RESOLVER_STATE_TYPE_NAME, new KeySet(), options);
    }
}
