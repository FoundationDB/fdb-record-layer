/*
 * TransactionBoundDatabaseTest.java
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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.DirectoryLayerDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.relational.api.EmbeddedRelationalDriver;
import com.apple.foundationdb.relational.api.EmbeddedRelationalEngine;
import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.transactionbound.TransactionBoundEmbeddedRelationalEngine;
import com.apple.foundationdb.relational.utils.ConnectionUtils;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class TransactionBoundDatabaseTest {
    @RegisterExtension
    @Order(0)
    public static final RelationalExtension relational = new EmbeddedRelationalExtension();
    private static final String SIMPLE_TYPE = "myType";
    private static final String SIMPLE_FIELD = "field";

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule dbRule = new SimpleDatabaseRule(TransactionBoundDatabaseTest.class, TestSchemas.restaurant());

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connRule = new RelationalConnectionRule(dbRule::getConnectionUri)
            .withOptions(Options.NONE)
            .withSchema("TEST_SCHEMA");

    @Test
    void simpleSelect() throws RelationalException, SQLException {
        final var embeddedConnection = connRule.getUnderlyingEmbeddedConnection();
        final var store = getStore(embeddedConnection);
        final var schemaTemplate = getSchemaTemplate(embeddedConnection);

        try (FDBRecordContext context = createNewContext(embeddedConnection)) {
            final var newStore = store.asBuilder().setMetaDataProvider(store.getMetaDataProvider()).setContext(context).open();
            try (Transaction transaction = new RecordStoreAndRecordContextTransaction(newStore, context, schemaTemplate)) {

                // Then, once we have a transaction that contains both an FDBRecordStoreBase<Message> and an FDBRecordContext,
                // connect to a TransactionBoundDatabase
                EmbeddedRelationalEngine engine = new TransactionBoundEmbeddedRelationalEngine();
                EmbeddedRelationalDriver driver = new EmbeddedRelationalDriver(engine);
                try (RelationalConnection conn = driver.connect(dbRule.getConnectionUri(), transaction, Options.NONE)) {
                    conn.setSchema("TEST_SCHEMA");
                    try (RelationalStatement statement = conn.createStatement()) {
                        var record = EmbeddedRelationalStruct.newBuilder()
                                .addLong("REST_NO", 42)
                                .addString("NAME", "FOO")
                                .build();
                        statement.executeInsert("RESTAURANT", record);
                    }

                    try (RelationalStatement statement = conn.createStatement()) {
                        try (RelationalResultSet resultSet = statement.executeScan("RESTAURANT", new KeySet(), Options.NONE)) {
                            Assertions.assertThat(resultSet.next()).isTrue();
                            Assertions.assertThat(resultSet.getString("NAME")).isEqualTo("FOO");
                            Assertions.assertThat(resultSet.getLong("REST_NO")).isEqualTo(42L);
                        }
                    }
                }
            }
        }
    }

    @Test
    void selectWithIncludedPlanCache() throws RelationalException, SQLException {
        final var embeddedConnection = connRule.getUnderlyingEmbeddedConnection();
        final var store = getStore(embeddedConnection);
        final var schemaTemplate = getSchemaTemplate(embeddedConnection);

        try (FDBRecordContext context = createNewContext(embeddedConnection)) {
            final var newStore = store.asBuilder().setMetaDataProvider(store.getMetaDataProvider()).setContext(context).open();
            try (Transaction transaction = new RecordStoreAndRecordContextTransaction(newStore, context, schemaTemplate)) {

                // Then, once we have a transaction that contains both an FDBRecordStoreBase<Message> and an FDBRecordContext,
                // connect to a TransactionBoundDatabase
                TransactionBoundEmbeddedRelationalEngine engine = new TransactionBoundEmbeddedRelationalEngine(Options.builder().withOption(Options.Name.PLAN_CACHE_PRIMARY_TIME_TO_LIVE_MILLIS, 10L).build());
                Assertions.assertThat(engine.getPlanCache()).isNotNull()
                        .extracting(planCache -> planCache.getStats().numEntries()).isEqualTo(0L);
                EmbeddedRelationalDriver driver = new EmbeddedRelationalDriver(engine);
                try (RelationalConnection conn = driver.connect(dbRule.getConnectionUri(), transaction, Options.NONE)) {
                    conn.setSchema("TEST_SCHEMA");
                    try (RelationalStatement statement = conn.createStatement()) {
                        var record = EmbeddedRelationalStruct.newBuilder()
                                .addLong("REST_NO", 42)
                                .addString("NAME", "FOO")
                                .build();
                        statement.executeInsert("RESTAURANT", record);
                    }

                    try (RelationalStatement statement = conn.createStatement()) {
                        try (RelationalResultSet resultSet = statement.executeQuery("select * from RESTAURANT")) {
                            Assertions.assertThat(resultSet.next()).isTrue();
                            Assertions.assertThat(resultSet.getString("NAME")).isEqualTo("FOO");
                            Assertions.assertThat(resultSet.getLong("REST_NO")).isEqualTo(42L);
                        }
                    }
                }

                //we should have populated the plan cache;
                // Assertions.assertThat(engine.getPlanCache().getStats().numEntries()).isEqualTo(1L);
            }
        }
    }

    @Test
    void createTemporaryFunction() throws RelationalException, SQLException {
        final var embeddedConnection = connRule.getUnderlyingEmbeddedConnection();
        final var store = getStore(embeddedConnection);
        final var schemaTemplate = getSchemaTemplate(embeddedConnection);

        try (FDBRecordContext context = createNewContext(embeddedConnection)) {
            final var newStore = store.asBuilder().setMetaDataProvider(store.getMetaDataProvider()).setContext(context).open();
            try (Transaction transaction = new RecordStoreAndRecordContextTransaction(newStore, context, schemaTemplate)) {
                EmbeddedRelationalDriver driver = new EmbeddedRelationalDriver(new TransactionBoundEmbeddedRelationalEngine());
                try (RelationalConnection conn = driver.connect(dbRule.getConnectionUri(), transaction, Options.NONE)) {
                    conn.setSchema("TEST_SCHEMA");
                    try (RelationalStatement statement = conn.createStatement()) {
                        statement.executeUpdate("CREATE TEMPORARY FUNCTION REST_FUNC() ON COMMIT DROP FUNCTION AS SELECT * FROM RESTAURANT WHERE REST_NO > 1000");
                    }
                    Assertions.assertThat(transaction.getBoundSchemaTemplateMaybe()).isPresent();
                    final var routinesInBoundSchemaTemplates = transaction.getBoundSchemaTemplateMaybe().get().getInvokedRoutines();
                    Assertions.assertThat(routinesInBoundSchemaTemplates)
                            .hasSize(1)
                            .anyMatch(routine -> routine.getName().equals("REST_FUNC"));
                }
            }
        }
    }

    @Test
    void dropTemporaryFunction() throws RelationalException, SQLException {
        final var embeddedConnection = connRule.getUnderlyingEmbeddedConnection();
        final var store = getStore(embeddedConnection);
        final var schemaTemplate = getSchemaTemplate(embeddedConnection);

        try (FDBRecordContext context = createNewContext(embeddedConnection)) {
            final var newStore = store.asBuilder().setMetaDataProvider(store.getMetaDataProvider()).setContext(context).open();
            try (Transaction transaction = new RecordStoreAndRecordContextTransaction(newStore, context, schemaTemplate)) {
                EmbeddedRelationalDriver driver = new EmbeddedRelationalDriver(new TransactionBoundEmbeddedRelationalEngine());
                try (RelationalConnection conn = driver.connect(dbRule.getConnectionUri(), transaction, Options.NONE)) {
                    conn.setSchema("TEST_SCHEMA");
                    try (RelationalStatement statement = conn.createStatement()) {
                        statement.executeUpdate("CREATE TEMPORARY FUNCTION REST_FUNC() ON COMMIT DROP FUNCTION AS SELECT * FROM RESTAURANT WHERE REST_NO > 1000");
                    }
                    Assertions.assertThat(transaction.getBoundSchemaTemplateMaybe()).isPresent();
                    Assertions.assertThat(transaction.getBoundSchemaTemplateMaybe().get().getInvokedRoutines())
                            .hasSize(1)
                            .anyMatch(routine -> routine.getName().equals("REST_FUNC"));
                    try (RelationalStatement statement = conn.createStatement()) {
                        statement.executeUpdate("DROP TEMPORARY FUNCTION REST_FUNC");
                    }
                    Assertions.assertThat(transaction.getBoundSchemaTemplateMaybe()).isPresent();
                    Assertions.assertThat(transaction.getBoundSchemaTemplateMaybe().get().getInvokedRoutines())
                            .isEmpty();
                }
            }
        }
    }

    @Test
    void dropTemporaryIfExistFunction() throws RelationalException, SQLException {
        final var embeddedConnection = connRule.getUnderlyingEmbeddedConnection();
        final var store = getStore(embeddedConnection);
        final var schemaTemplate = getSchemaTemplate(embeddedConnection);

        try (FDBRecordContext context = createNewContext(embeddedConnection)) {
            final var newStore = store.asBuilder().setMetaDataProvider(store.getMetaDataProvider()).setContext(context).open();
            try (Transaction transaction = new RecordStoreAndRecordContextTransaction(newStore, context, schemaTemplate)) {
                EmbeddedRelationalDriver driver = new EmbeddedRelationalDriver(new TransactionBoundEmbeddedRelationalEngine());
                try (RelationalConnection conn = driver.connect(dbRule.getConnectionUri(), transaction, Options.NONE)) {
                    conn.setSchema("TEST_SCHEMA");
                    try (RelationalStatement statement = conn.createStatement()) {
                        statement.executeUpdate("CREATE TEMPORARY FUNCTION REST_FUNC() ON COMMIT DROP FUNCTION AS SELECT * FROM RESTAURANT WHERE REST_NO > 1000");
                    }
                    Assertions.assertThat(transaction.getBoundSchemaTemplateMaybe()).isPresent();
                    Assertions.assertThat(transaction.getBoundSchemaTemplateMaybe().get().getInvokedRoutines())
                            .hasSize(1)
                            .anyMatch(routine -> routine.getName().equals("REST_FUNC"));
                    try (RelationalStatement statement = conn.createStatement()) {
                        statement.executeUpdate("DROP TEMPORARY FUNCTION IF EXISTS REST_FUNC");
                    }
                    Assertions.assertThat(transaction.getBoundSchemaTemplateMaybe()).isPresent();
                    Assertions.assertThat(transaction.getBoundSchemaTemplateMaybe().get().getInvokedRoutines())
                            .isEmpty();
                    try (RelationalStatement statement = conn.createStatement()) {
                        statement.executeUpdate("DROP TEMPORARY FUNCTION IF EXISTS DOES_NOT_EXISTS");
                    }
                }
            }
        }
    }

    @Test
    void unsetTransactionBoundSchemaTemplate() throws RelationalException, SQLException {
        final var embeddedConnection = connRule.getUnderlyingEmbeddedConnection();
        final var store = getStore(embeddedConnection);
        final var schemaTemplate = getSchemaTemplate(embeddedConnection);

        try (FDBRecordContext context = createNewContext(embeddedConnection)) {
            final var newStore = store.asBuilder().setMetaDataProvider(store.getMetaDataProvider()).setContext(context).open();
            try (Transaction transaction = new RecordStoreAndRecordContextTransaction(newStore, context, schemaTemplate)) {
                EmbeddedRelationalDriver driver = new EmbeddedRelationalDriver(new TransactionBoundEmbeddedRelationalEngine());
                try (RelationalConnection conn = driver.connect(dbRule.getConnectionUri(), transaction, Options.NONE)) {
                    conn.setSchema("TEST_SCHEMA");
                    try (RelationalStatement statement = conn.createStatement()) {
                        statement.executeUpdate("CREATE TEMPORARY FUNCTION REST_FUNC() ON COMMIT DROP FUNCTION AS SELECT * FROM RESTAURANT WHERE REST_NO > 1000");
                    }
                    Assertions.assertThat(transaction.getBoundSchemaTemplateMaybe()).isPresent();
                    Assertions.assertThat(transaction.getBoundSchemaTemplateMaybe().get().getInvokedRoutines())
                            .hasSize(1)
                            .anyMatch(routine -> routine.getName().equals("REST_FUNC"));
                    transaction.unsetBoundSchemaTemplate();
                    Assertions.assertThat(transaction.getBoundSchemaTemplateMaybe()).isEmpty();
                }
            }
        }
    }

    static final KeySpace keySpace = new KeySpace(
            new DirectoryLayerDirectory("root", "transaction-bound-test")
                    .addSubdirectory(new KeySpaceDirectory("test", KeySpaceDirectory.KeyType.STRING)
                            .addSubdirectory(new KeySpaceDirectory("db", KeySpaceDirectory.KeyType.LONG)
                                    .addSubdirectory(new KeySpaceDirectory("schema1", KeySpaceDirectory.KeyType.NULL, null))
                                    .addSubdirectory(new KeySpaceDirectory("schema2", KeySpaceDirectory.KeyType.LONG, 1L)))));

    @Test
    void copyOtherPath() throws RelationalException, SQLException {
        final EmbeddedRelationalConnection embeddedConnection = connRule.getUnderlyingEmbeddedConnection();
        final KeySpacePath sourcePath = keySpace.path("root")
                .add("test", UUID.randomUUID().toString())
                .add("db", 17L);
        final URI sourceUri = KeySpaceUtils.pathToUri(sourcePath);
        final KeySpacePath destPath = keySpace.path("root")
                .add("test", UUID.randomUUID().toString())
                .add("db", 23L);
        final URI destUri = KeySpaceUtils.pathToUri(destPath);

        Map<Tuple, Tuple> schema1Data = Map.of(
                Tuple.from(1), Tuple.from("First"),
                Tuple.from(2), Tuple.from("Second")
        );
        Map<Tuple, Tuple> schema2Data = Map.of(
                Tuple.from(3), Tuple.from("Alpha"),
                Tuple.from("X"), Tuple.from("Beta")
        );
        final KeySpacePath sourcePath1 = sourcePath.add("schema1");
        writeDataToPath(embeddedConnection, sourcePath1, schema1Data);
        final KeySpacePath sourcePath2 = sourcePath.add("schema2");
        writeDataToPath(embeddedConnection, sourcePath2, schema2Data);

        Assertions.assertThat(getDataInPath(embeddedConnection, sourcePath1)).isEqualTo(schema1Data);
        Assertions.assertThat(getDataInPath(embeddedConnection, sourcePath2)).isEqualTo(schema2Data);

        // export the data
        final List<byte[]> data = exportDataWithCopy(embeddedConnection, sourceUri, destUri);
        Assertions.assertThat(data).hasSizeGreaterThanOrEqualTo(2);

        importDataWithCopy(embeddedConnection, destUri, data);

        Assertions.assertThat(getDataInPath(embeddedConnection, destPath.add("schema1"))).isEqualTo(schema1Data);
        Assertions.assertThat(getDataInPath(embeddedConnection, destPath.add("schema2"))).isEqualTo(schema2Data);
    }


    @Test
    void copyOtherPathWithStore() throws RelationalException, SQLException, Descriptors.DescriptorValidationException {
        final EmbeddedRelationalConnection embeddedConnection = connRule.getUnderlyingEmbeddedConnection();
        final KeySpacePath sourcePath = keySpace.path("root")
                .add("test", UUID.randomUUID().toString())
                .add("db", 17L);
        final URI sourceUri = KeySpaceUtils.pathToUri(sourcePath);
        final KeySpacePath destPath = keySpace.path("root")
                .add("test", UUID.randomUUID().toString())
                .add("db", 23L);
        final URI destUri = KeySpaceUtils.pathToUri(destPath);

        Map<Tuple, Tuple> schema1Data = Map.of(
                Tuple.from(1), Tuple.from("First"),
                Tuple.from(2), Tuple.from("Second")
        );
        Map<Tuple, Tuple> schema2Data = Map.of(
                Tuple.from(3), Tuple.from("Alpha"),
                Tuple.from("X"), Tuple.from("Beta")
        );
        final KeySpacePath sourcePath1 = sourcePath.add("schema1");
        final RecordMetaData metadata = simpleMetaData();
        withStore(embeddedConnection, sourcePath1, metadata,
                store -> {
                    saveSimpleRecord(store, "First");
                    saveSimpleRecord(store, "Second");
                });
        final KeySpacePath sourcePath2 = sourcePath.add("schema2");
        withStore(embeddedConnection, sourcePath2, metadata, store1 -> {
            saveSimpleRecord(store1, "Alpha");
            saveSimpleRecord(store1, "Beta");
        });
        // export the data
        final List<byte[]> data = exportDataWithCopy(embeddedConnection, sourceUri, destUri);
        Assertions.assertThat(data).hasSizeGreaterThanOrEqualTo(2);

        importDataWithCopy(embeddedConnection, destUri, data);

        withStore(embeddedConnection, destPath.add("schema1"), metadata,
                store -> {
                    assertSimpleRecordExists(store, "First");
                    assertSimpleRecordExists(store, "Second");
                });

        withStore(embeddedConnection, destPath.add("schema2"), metadata,
                store -> {
                    assertSimpleRecordExists(store, "Alpha");
                    assertSimpleRecordExists(store, "Beta");
                });
    }

    private static void withStore(final EmbeddedRelationalConnection embeddedConnection,
                                  final KeySpacePath path,
                                  final RecordMetaData metadata,
                                  final Consumer<FDBRecordStore> action) throws RelationalException {
        try (FDBRecordContext context = createNewContext(embeddedConnection)) {
            final FDBRecordStore store = FDBRecordStore.newBuilder()
                    .setKeySpacePath(path)
                    .setMetaDataProvider(() -> metadata)
                    .setContext(context)
                    .build();
            action.accept(store);
            context.commit();
        }
    }


    private void assertSimpleRecordExists(final FDBRecordStore store, final String field) {
        final FDBStoredRecord<Message> record = store.loadRecord(Tuple.from(field));
        Assertions.assertThat(record).isNotNull();
        final Message message = record.getRecord();
        final Descriptors.Descriptor type = getSimpleType(store);
        Assertions.assertThat(message.getField(getSimpleField(type))).isEqualTo(field);
    }

    private static Descriptors.FieldDescriptor getSimpleField(final Descriptors.Descriptor type) {
        return type.findFieldByName(SIMPLE_FIELD);
    }

    private static Descriptors.Descriptor getSimpleType(final FDBRecordStore store) {
        return store.getRecordMetaData().getRecordsDescriptor().findMessageTypeByName(SIMPLE_TYPE);
    }

    private static void saveSimpleRecord(final FDBRecordStore store, final String value) {
        final Descriptors.Descriptor type = getSimpleType(store);
        store.saveRecord(DynamicMessage.newBuilder(type)
                .setField(getSimpleField(type), value).build());
    }

    @Nonnull
    private static RecordMetaData simpleMetaData() throws Descriptors.DescriptorValidationException {
        final DescriptorProtos.FileDescriptorProto.Builder protoBuilder = DescriptorProtos.FileDescriptorProto.newBuilder();
        protoBuilder.addMessageTypeBuilder()
                .setName(SIMPLE_TYPE)
                .addField(protoField(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING, SIMPLE_FIELD, 1));
        protoBuilder.addMessageTypeBuilder()
                .setName("RecordTypeUnion")
                .addField(protoField(SIMPLE_TYPE, "_myType", 1));
        final Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(protoBuilder.build(), new Descriptors.FileDescriptor[0]);
        final RecordMetaDataBuilder recordMetaDataBuilder = RecordMetaData.newBuilder().setRecords(fileDescriptor);
        recordMetaDataBuilder.getRecordType(SIMPLE_TYPE)
                .setPrimaryKey(Key.Expressions.field(SIMPLE_FIELD));
        return recordMetaDataBuilder.build();
    }

    private static DescriptorProtos.FieldDescriptorProto protoField(DescriptorProtos.FieldDescriptorProto.Type type, String name, int number) {
        return DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setNumber(number)
                .setName(name)
                .setType(type)
                .build();
    }

    private static DescriptorProtos.FieldDescriptorProto protoField(String typeName, String name, int number) {
        return DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setNumber(number)
                .setName(name)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setTypeName(typeName)
                .build();
    }

    private void importDataWithCopy(final EmbeddedRelationalConnection embeddedConnection, final URI destUri, final List<byte[]> data) throws RelationalException, SQLException {
        withTransactionBoundConnection(embeddedConnection, conn -> {
            try (RelationalPreparedStatement stmt = conn.prepareStatement("COPY \"" + destUri + "\" FROM ?")) {
                stmt.setObject(1, data);
                final RelationalResultSet relationalResultSet = stmt.executeQuery();
                Assertions.assertThat(relationalResultSet.next()).isTrue();
                Assertions.assertThat(relationalResultSet.getInt(1)).isEqualTo(data.size());
                Assertions.assertThat(relationalResultSet.next()).isFalse();
            }
        });
    }

    @Nonnull
    private List<byte[]> exportDataWithCopy(final EmbeddedRelationalConnection embeddedConnection, final URI sourceUri, final URI destUri) throws RelationalException, SQLException {
        List<byte[]> data = new ArrayList<>();
        withTransactionBoundConnection(embeddedConnection, conn -> {
            try (RelationalStatement statement = conn.createStatement()) {
                final RelationalResultSet resultSet = statement.executeQuery("COPY \"" + sourceUri + "\"");
                while (resultSet.next()) {
                    data.add(resultSet.getBytes(1));
                }
            }
            // sanity check that the destination is already empty
            try (RelationalStatement statement = conn.createStatement()) {
                final RelationalResultSet resultSet = statement.executeQuery("COPY \"" + destUri + "\"");
                Assertions.assertThat(resultSet.next()).isFalse();
            }
        });
        return data;
    }

    private void writeDataToPath(final EmbeddedRelationalConnection embeddedConnection,
                                 final KeySpacePath path,
                                 final Map<Tuple, Tuple> data) throws RelationalException {
        try (FDBRecordContext context = createNewContext(embeddedConnection)) {
            final Subspace subspace = path.toSubspace(context);
            data.forEach((key, value) -> {
                context.ensureActive().set(subspace.pack(key), value.pack());
            });
            context.commit();
        }
    }

    private static Map<Tuple, Tuple> getDataInPath(final EmbeddedRelationalConnection embeddedConnection,
                                                   final KeySpacePath sourcePath) throws RelationalException {
        try (FDBRecordContext context = createNewContext(embeddedConnection)) {
            return context.ensureActive().getRange(sourcePath.toSubspace(context).range()).asList().join()
                    .stream().collect(Collectors.toMap(
                            keyValue -> sourcePath.toSubspace(context).unpack(keyValue.getKey()),
                            keyValue -> Tuple.fromBytes(keyValue.getValue())));
        }
    }

    private void withTransactionBoundConnection(final EmbeddedRelationalConnection embeddedConnection,
                                                ConnectionUtils.SQLConsumer<RelationalConnection> action)
            throws RelationalException, SQLException {
        final FDBRecordStore store = getStore(embeddedConnection);
        final SchemaTemplate schemaTemplate = getSchemaTemplate(embeddedConnection);
        try (FDBRecordContext context = createNewContext(embeddedConnection)) {
            final FDBRecordStore newStore = store.asBuilder().setContext(context).open();
            try (Transaction transaction = new RecordStoreAndRecordContextTransaction(newStore, context, schemaTemplate)) {
                EmbeddedRelationalEngine engine = new TransactionBoundEmbeddedRelationalEngine(Options.NONE, keySpace);
                EmbeddedRelationalDriver driver = new EmbeddedRelationalDriver(engine);
                try (RelationalConnection conn = driver.connect(dbRule.getConnectionUri(), transaction, Options.NONE)) {
                    conn.setSchema("TEST_SCHEMA");
                    action.accept(conn);
                    // Closing the connection will close the underlying transaction. This seems at odds with the spirit
                    // of RecordStoreAndRecordContextTransaction
                    context.commit();
                }
            }
        }
    }

    static FDBRecordStore getStore(EmbeddedRelationalConnection connection) throws RelationalException, SQLException {
        connection.setAutoCommit(false);
        connection.createNewTransaction();
        RecordLayerSchema schema = connection.getRecordLayerDatabase().loadSchema("TEST_SCHEMA");
        final var store = schema.loadStore().unwrap(FDBRecordStore.class);
        connection.rollback();
        connection.setAutoCommit(true);
        return store;
    }

    static SchemaTemplate getSchemaTemplate(EmbeddedRelationalConnection connection) throws RelationalException, SQLException {
        connection.setAutoCommit(false);
        connection.createNewTransaction();
        final var schemaTemplate = connection.getSchemaTemplate();
        connection.rollback();
        connection.setAutoCommit(true);
        return schemaTemplate;
    }

    static FDBRecordContext createNewContext(@Nonnull EmbeddedRelationalConnection connection) throws RelationalException {
        return connection.getRecordLayerDatabase().getTransactionManager().createTransaction(Options.NONE).unwrap(FDBRecordContext.class);
    }
}
