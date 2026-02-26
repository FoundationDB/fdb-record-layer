/*
 * TransactionBoundQueryTest.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.UnnestedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.relational.api.EmbeddedRelationalDriver;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.Metadata;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.AbstractDatabase;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.RecordStoreAndRecordContextTransaction;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.storage.BackingStore;
import com.apple.foundationdb.relational.transactionbound.TransactionBoundEmbeddedRelationalEngine;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.RelationalResultSetAssert;

import com.google.protobuf.Descriptors;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class TransactionBoundQueryTest {
    @Nonnull
    private static final String SCHEMA_TEMPLATE =
            """
            CREATE TABLE t1(id bigint, a bigint, b string, PRIMARY KEY(id))
            CREATE TYPE as STRUCT entry(key string, value bigint)
            CREATE TABLE t2(id bigint, a bigint, c string, d entry ARRAY, PRIMARY KEY (id))
            CREATE TABLE t3(id bigint, a bigint, e bigint, f bigint, PRIMARY KEY(id))
            """;
    @RegisterExtension
    @Order(0)
    @Nonnull
    final EmbeddedRelationalExtension embeddedExtension = new EmbeddedRelationalExtension();
    @RegisterExtension
    @Order(1)
    final SimpleDatabaseRule databaseRule = new SimpleDatabaseRule(TransactionBoundQueryTest.class, SCHEMA_TEMPLATE);

    @Nonnull
    private static Options engineOptions() throws SQLException {
        return Options.builder()
                .withOption(Options.Name.PLAN_CACHE_PRIMARY_MAX_ENTRIES, 100)
                .withOption(Options.Name.PLAN_CACHE_SECONDARY_MAX_ENTRIES, 10)
                .withOption(Options.Name.PLAN_CACHE_TERTIARY_TIME_TO_LIVE_MILLIS, 10L)
                .build();
    }

    @Nonnull
    private EmbeddedRelationalConnection connectEmbedded() throws SQLException {
        Connection connection = DriverManager.getConnection(databaseRule.getConnectionUri().toString());
        connection.setSchema(databaseRule.getSchemaName());
        return connection.unwrap(EmbeddedRelationalConnection.class);
    }

    @Nonnull
    private FDBRecordContext openContext() throws SQLException, RelationalException {
        try (EmbeddedRelationalConnection connection = connectEmbedded()) {
            connection.createNewTransaction();
            FDBRecordContext context = connection.getTransaction().unwrap(FDBRecordContext.class);

            // Existing context is closed when the connection is closed. Create a new one from the same database
            // to ensure it can be used
            return context.getDatabase().openContext();
        }
    }

    @Nonnull
    private EmbeddedRelationalConnection connectTransactionBound(@Nonnull FDBRecordContext context, @Nonnull RecordMetaData metaData) throws SQLException, RelationalException {
        try (EmbeddedRelationalConnection embeddedConnection = connectEmbedded()) {
            embeddedConnection.createNewTransaction();
            AbstractDatabase db = embeddedConnection.getRecordLayerDatabase();
            BackingStore store = db.loadRecordStore(databaseRule.getSchemaName(), FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NO_INFO_AND_NOT_EMPTY);

            FDBRecordStore baseStore = store.unwrap(FDBRecordStore.class);
            FDBRecordStore newStore = baseStore.asBuilder()
                    .setMetaDataProvider(metaData)
                    .setContext(context)
                    .open();

            final var originalDriver = embeddedExtension.getDriver();
            DriverManager.deregisterDriver(originalDriver);
            var newDriver = new EmbeddedRelationalDriver(new TransactionBoundEmbeddedRelationalEngine(engineOptions()));
            DriverManager.registerDriver(newDriver);
            final var driver = (EmbeddedRelationalDriver) DriverManager.getDriver(databaseRule.getConnectionUri().toString());
            final var connectionOptions = Options.none();
            try {
                final var schemaTemplate = RecordLayerSchemaTemplate.fromRecordMetadata(metaData, databaseRule.getSchemaTemplateName(),
                        metaData.getVersion());
                final var transactionBoundConnection = driver.connect(databaseRule.getConnectionUri(),
                        new RecordStoreAndRecordContextTransaction(newStore, context, schemaTemplate), connectionOptions);
                transactionBoundConnection.setSchema(databaseRule.getSchemaName());
                return transactionBoundConnection.unwrap(EmbeddedRelationalConnection.class);
            } finally {
                DriverManager.deregisterDriver(newDriver);
                DriverManager.registerDriver(originalDriver);
            }
        }
    }

    private void insertT3Data(EmbeddedRelationalConnection connection) throws SQLException {
        try (RelationalStatement statement = connection.createStatement()) {
            statement.executeUpdate(
                    "INSERT INTO T3 VALUES " +
                            "(1, 1, 10, 1), " +
                            "(2, 2,  9, 3), " +
                            "(3, 2,  8, 2), " +
                            "(4, 3,  8, 1), " +
                            "(5, 3,  7, 2), " +
                            "(6, 4,  7, 3), " +
                            "(7, 4,  6, 1), " +
                            "(8, 5,  6, 0) "
            );
        }
    }

    private RecordMetaData getUpdatedMetaData(@Nonnull Consumer<RecordMetaDataBuilder> metaDataUpdater) throws SQLException, RelationalException {
        try (EmbeddedRelationalConnection connection = connectEmbedded()) {
            connection.createNewTransaction();
            SchemaTemplate schemaTemplate = connection.getSchemaTemplate();
            RecordMetaData metaDataWithoutIndex = schemaTemplate.unwrap(RecordLayerSchemaTemplate.class).toRecordMetadata();
            RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(metaDataWithoutIndex.toProto());
            metaDataUpdater.accept(metaDataBuilder);
            return metaDataBuilder.build();
        }
    }

    @Test
    void canUseRecordLayerIndex() throws SQLException, RelationalException {
        // Define an index using Record Layer abstractions
        RecordMetaData metaDataWithIndex = getUpdatedMetaData(metaDataBuilder -> {
            var index = new Index("bWithA", Key.Expressions.keyWithValue(Key.Expressions.concatenateFields("B", "A"), 1));
            metaDataBuilder.addIndex("T1", index);
        });

        // Now re-open the store (with the new meta-data) using the record layer index
        try (FDBRecordContext context = openContext()) {
            try (RelationalConnection connection = connectTransactionBound(context, metaDataWithIndex)) {
                try (RelationalStatement statement = connection.createStatement()) {
                    try (RelationalResultSet resultSet = statement.executeQuery("explain SELECT a, b FROM t1 ORDER BY b DESC")) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNextRow();
                        Assertions.assertThat(resultSet.getString("plan"))
                                .as("Planner should select Record Layer defined index in plan")
                                .contains("COVERING(bWithA");

                        RelationalResultSetAssert.assertThat(resultSet).hasNoNextRow();
                    }
                }
            }
        }
    }

    @Test
    void ignoreSyntheticIndexes() throws SQLException, RelationalException {
        RecordMetaData metaDataOrig = getUpdatedMetaData(metaDataBuilder -> { });

        RecordMetaData metaData = getUpdatedMetaData(metaDataBuilder -> {
            Descriptors.FileDescriptor descriptor = metaDataOrig.getRecordsDescriptor();

            UnnestedRecordTypeBuilder unnestedBuilder = metaDataBuilder.addUnnestedRecordType("unnested");
            unnestedBuilder.addParentConstituent("parent", metaDataBuilder.getRecordType("T2"));
            unnestedBuilder.addNestedConstituent("child", descriptor.findMessageTypeByName("ENTRY"), "parent", Key.Expressions.field("D").nest("values", KeyExpression.FanType.FanOut));

            // Add index on unnested type
            var unnestedIndex = new Index("unnestedIndex", Key.Expressions.concat(
                    Key.Expressions.field("child").nest("KEY"),
                    Key.Expressions.field("parent").nest("C"),
                    Key.Expressions.field("child").nest("VALUE")));
            metaDataBuilder.addIndex(unnestedBuilder, unnestedIndex);

            // Add index on t2 top level
            var cIndex = new Index("cIndex", "C");
            metaDataBuilder.addIndex("T2", cIndex);

            // We will disable this index. This will result in us needing to generate the index bit set
            var aIndex = new Index("aIndex", "A");
            metaDataBuilder.addIndex("T2", aIndex);

        });

        try (FDBRecordContext context = openContext()) {
            try (EmbeddedRelationalConnection connection = connectTransactionBound(context, metaData)) {
                connection.getRecordLayerDatabase().loadRecordStore(databaseRule.getSchemaName(), FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NOT_EXISTS)
                        .unwrap(FDBRecordStore.class)
                        .markIndexDisabled("aIndex").join();

                SchemaTemplate schemaTemplate = connection.getSchemaTemplate();
                Assertions.assertThat(schemaTemplate.findTableByName("T2"))
                        .isPresent()
                        .get()
                        .satisfies(table ->
                                // The table should contain only the index that Relational can process
                                Assertions.assertThat(table.getIndexes())
                                        .map(Metadata::getName)
                                        .contains("aIndex")
                                        .contains("cIndex")
                                        .doesNotContain("unnestedIndex")
                        );

                try (RelationalStatement statement = connection.createStatement()) {
                    // This query could use the unnested index if we planned against it correctly. In particular,
                    // it could be executed as scan on the first two columns of the index
                    String sqlQuery = "SELECT e.value " +
                            "FROM t2, (SELECT key, value FROM t2.d) e " +
                            "WHERE t2.c = 'foo' AND e.key = 'bar'";
                    try (RelationalResultSet resultSet = statement.executeQuery("explain " + sqlQuery)) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNextRow();
                        Assertions.assertThat(resultSet.getString("plan"))
                                .doesNotContain("unnestedIndex")
                                .contains("ISCAN(cIndex");
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNoNextRow();
                    }
                }
            }
        }
    }

    @Test
    void addUniversalCountIndex() throws SQLException, RelationalException {
        final String countIndex = "countByType";
        RecordMetaData metaData = getUpdatedMetaData(metaDataBuilder -> {
            Index index = new Index(countIndex, Key.Expressions.recordType().ungrouped(), IndexTypes.COUNT);
            metaDataBuilder.addUniversalIndex(index);
        });

        try (FDBRecordContext context = openContext()) {
            try (EmbeddedRelationalConnection connection = connectTransactionBound(context, metaData)) {
                try (RelationalStatement statement = connection.createStatement()) {
                    // This query could use the count index if we can match against the record type correctly.
                    String sqlQuery = "SELECT count(*) FROM t1";
                    try (RelationalResultSet resultSet = statement.executeQuery("explain " + sqlQuery)) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNextRow();
                        Assertions.assertThat(resultSet.getString("plan"))
                                .doesNotContain(countIndex)
                                .contains("SCAN(<,>)");
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNoNextRow();
                    }
                }
            }
        }
    }

    @Test
    void addUniversalCountWithGrouping() throws SQLException, RelationalException {
        final String countIndexName = "groupedCountByType";
        final String valueIndexName = "valueIndex";
        RecordMetaData metaData = getUpdatedMetaData(metaDataBuilder -> {
            Index countIndex = new Index(countIndexName, new GroupingKeyExpression(Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.field("A")), 0), IndexTypes.COUNT);
            metaDataBuilder.addUniversalIndex(countIndex);

            Index valueIndex = new Index(valueIndexName, Key.Expressions.field("A"));
            metaDataBuilder.addIndex("T1", valueIndex);
        });

        try (FDBRecordContext context = openContext()) {
            try (EmbeddedRelationalConnection connection = connectTransactionBound(context, metaData)) {
                try (RelationalStatement statement = connection.createStatement()) {
                    // This query could use the count index if we can match against the record type correctly.
                    String sqlQuery = "SELECT a, count(*) FROM t1 GROUP BY a";
                    try (RelationalResultSet resultSet = statement.executeQuery("explain " + sqlQuery)) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNextRow();
                        Assertions.assertThat(resultSet.getString("plan"))
                                .doesNotContain(countIndexName)
                                .contains("ISCAN(" + valueIndexName);
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNoNextRow();
                    }
                }
            }
        }
    }

    @Test
    void addUniversalCountWithGroupingAndFilter() throws SQLException, RelationalException {
        final String countIndex = "groupedCountByType";
        final String valueIndexName = "valueIndex";
        RecordMetaData metaData = getUpdatedMetaData(metaDataBuilder -> {
            Index index = new Index(countIndex, new GroupingKeyExpression(Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.field("A")), 0), IndexTypes.COUNT);
            metaDataBuilder.addUniversalIndex(index);

            Index valueIndex = new Index(valueIndexName, Key.Expressions.field("A"));
            metaDataBuilder.addIndex("T1", valueIndex);
        });

        try (FDBRecordContext context = openContext()) {
            try (EmbeddedRelationalConnection connection = connectTransactionBound(context, metaData)) {
                try (RelationalStatement statement = connection.createStatement()) {
                    // This query could use the count index if we can match against the record type correctly.
                    String sqlQuery = "SELECT count(*) FROM t1 WHERE a = 0 GROUP BY a";
                    try (RelationalResultSet resultSet = statement.executeQuery("explain " + sqlQuery)) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNextRow();
                        Assertions.assertThat(resultSet.getString("plan"))
                                .doesNotContain(countIndex)
                                .contains("ISCAN(" + valueIndexName + " [EQUALS ");
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNoNextRow();
                    }
                }
            }
        }
    }

    @Test
    void countByAdditionIndex() throws SQLException, RelationalException {
        final String countIndexName = "countByAPlusE";
        RecordMetaData metaData = getUpdatedMetaData(metaDataBuilder -> {
            final Index countIndex = new Index(countIndexName,
                    new GroupingKeyExpression(Key.Expressions.function("add", Key.Expressions.concat(Key.Expressions.field("A"), Key.Expressions.field("E"))), 0),
                    IndexTypes.COUNT);
            metaDataBuilder.addIndex("T3", countIndex);
        });

        try (FDBRecordContext context = openContext()) {
            try (EmbeddedRelationalConnection connection = connectTransactionBound(context, metaData)) {
                insertT3Data(connection);

                try (RelationalStatement statement = connection.createStatement()) {
                    String sqlQuery = "SELECT a + e AS addition, count(*) AS c FROM t3 GROUP BY a + e";
                    try (RelationalResultSet resultSet = statement.executeQuery("explain " + sqlQuery)) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNextRow();
                        Assertions.assertThat(resultSet.getString("plan"))
                                .contains("AISCAN(" + countIndexName + " <,> BY_GROUP");
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNoNextRow();
                    }

                    try (RelationalResultSet resultSet = statement.executeQuery(sqlQuery)) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .nextRowMatches(Map.of("ADDITION", 10L, "C", 3L))
                                .nextRowMatches(Map.of("ADDITION", 11L, "C", 5L))
                                .hasNoNextRow();
                    }
                }
            }
        }
    }

    @Disabled("throws StackOverflow error: TODO (StackOverflow when trying to plan a streaming aggregate with a group by that is an arithmetic function)")
    @Test
    void countByAdditionStreamingAggregate() throws SQLException, RelationalException {
        final String valueIndex = "aPlusEValueIndex";
        RecordMetaData metaData = getUpdatedMetaData(metaDataBuilder -> {
            final Index countIndex = new Index(valueIndex,
                    Key.Expressions.function("add", Key.Expressions.concat(Key.Expressions.field("A"), Key.Expressions.field("E"))),
                    IndexTypes.VALUE);
            metaDataBuilder.addIndex("T3", countIndex);
        });

        try (FDBRecordContext context = openContext()) {
            try (EmbeddedRelationalConnection connection = connectTransactionBound(context, metaData)) {
                insertT3Data(connection);

                try (RelationalStatement statement = connection.createStatement()) {
                    String sqlQuery = "SELECT a + e AS addition, count(*) AS c FROM t3 GROUP BY a + e";
                    try (RelationalResultSet resultSet = statement.executeQuery("explain " + sqlQuery)) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNextRow();
                        Assertions.assertThat(resultSet.getString("plan"))
                                .contains("Index(" + valueIndex + " <,>)");
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNoNextRow();
                    }

                    try (RelationalResultSet resultSet = statement.executeQuery(sqlQuery)) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .nextRowMatches(Map.of("ADDITION", 10L, "C", 3L))
                                .nextRowMatches(Map.of("ADDITION", 11L, "C", 5L))
                                .hasNoNextRow();
                    }
                }
            }
        }
    }

    @Test
    void countAndFilterOnAdditionIndex() throws SQLException, RelationalException {
        final String countIndexName = "countByAPlusE";
        RecordMetaData metaData = getUpdatedMetaData(metaDataBuilder -> {
            final Index countIndex = new Index(countIndexName,
                    new GroupingKeyExpression(Key.Expressions.function("add", Key.Expressions.concat(Key.Expressions.field("A"), Key.Expressions.field("E"))), 0),
                    IndexTypes.COUNT);
            metaDataBuilder.addIndex("T3", countIndex);
        });

        try (FDBRecordContext context = openContext()) {
            try (EmbeddedRelationalConnection connection = connectTransactionBound(context, metaData)) {
                insertT3Data(connection);

                final String sqlQuery = "SELECT count(*) AS c FROM t3 WHERE a + e = ? GROUP BY a + e";
                try (RelationalPreparedStatement statement = connection.prepareStatement("explain " + sqlQuery)) {
                    statement.setLong(1, 0L);
                    try (RelationalResultSet resultSet = statement.executeQuery()) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNextRow();
                        Assertions.assertThat(resultSet.getString("plan"))
                                .contains("AISCAN(" + countIndexName + " [EQUALS ");
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNoNextRow();
                    }
                }

                try (RelationalPreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {
                    preparedStatement.setLong(1, 10L);
                    try (RelationalResultSet resultSet = preparedStatement.executeQuery()) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .nextRowMatches(Map.of("C", 3L))
                                .hasNoNextRow();
                    }

                    preparedStatement.setLong(1, 11L);
                    try (RelationalResultSet resultSet = preparedStatement.executeQuery()) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .nextRowMatches(Map.of("C", 5L))
                                .hasNoNextRow();
                    }

                    preparedStatement.setLong(1, 9L);
                    try (RelationalResultSet resultSet = preparedStatement.executeQuery()) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNoNextRow();
                    }
                }
            }
        }
    }

    @Nonnull
    static Stream<String> countByAdditionGroupWithFilterOnOtherColumn() {
        return Stream.of("", " ORDER BY f - e ASC", " ORDER BY f - e DESC");
    }

    @ParameterizedTest
    @MethodSource
    void countByAdditionGroupWithFilterOnOtherColumn(String orderingSuffix) throws SQLException, RelationalException {
        final String countIndexName = "countByAAndFEDiff";
        RecordMetaData metaData = getUpdatedMetaData(metaDataBuilder -> {
            final Index countIndex = new Index(countIndexName,
                    new GroupingKeyExpression(Key.Expressions.concat(
                            Key.Expressions.field("A"),
                            Key.Expressions.function("sub", Key.Expressions.concat(Key.Expressions.field("F"), Key.Expressions.field("E")))
                    ), 0),
                    IndexTypes.COUNT);
            metaDataBuilder.addIndex("T3", countIndex);
        });

        try (FDBRecordContext context = openContext()) {
            try (EmbeddedRelationalConnection connection = connectTransactionBound(context, metaData)) {
                insertT3Data(connection);

                final String sqlQuery = "SELECT f - e AS diff, count(*) AS c FROM t3 WHERE a = ? GROUP BY a, f - e" + orderingSuffix;
                try (RelationalPreparedStatement statement = connection.prepareStatement("explain " + sqlQuery)) {
                    statement.setLong(1, 0L);
                    try (RelationalResultSet resultSet = statement.executeQuery()) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNextRow();
                        Assertions.assertThat(resultSet.getString("plan"))
                                .contains("AISCAN(" + countIndexName + " [EQUALS ");
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNoNextRow();
                    }
                }

                try (RelationalPreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {
                    preparedStatement.setLong(1, 1L);
                    try (RelationalResultSet resultSet = preparedStatement.executeQuery()) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .nextRowMatches(Map.of("DIFF", -9L, "C", 1L))
                                .hasNoNextRow();
                    }

                    preparedStatement.setLong(1, 2L);
                    try (RelationalResultSet resultSet = preparedStatement.executeQuery()) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .nextRowMatches(Map.of("DIFF", -6L, "C", 2L))
                                .hasNoNextRow();
                    }

                    preparedStatement.setLong(1, 3L);
                    try (RelationalResultSet resultSet = preparedStatement.executeQuery()) {
                        if (orderingSuffix.contains("DESC")) {
                            RelationalResultSetAssert.assertThat(resultSet)
                                    .nextRowMatches(Map.of("DIFF", -5L, "C", 1L))
                                    .nextRowMatches(Map.of("DIFF", -7L, "C", 1L))
                                    .hasNoNextRow();
                        } else {
                            RelationalResultSetAssert.assertThat(resultSet)
                                    .nextRowMatches(Map.of("DIFF", -7L, "C", 1L))
                                    .nextRowMatches(Map.of("DIFF", -5L, "C", 1L))
                                    .hasNoNextRow();
                        }
                    }

                    preparedStatement.setLong(1, 4L);
                    try (RelationalResultSet resultSet = preparedStatement.executeQuery()) {
                        if (orderingSuffix.contains("DESC")) {
                            RelationalResultSetAssert.assertThat(resultSet)
                                    .nextRowMatches(Map.of("DIFF", -4L, "C", 1L))
                                    .nextRowMatches(Map.of("DIFF", -5L, "C", 1L))
                                    .hasNoNextRow();
                        } else {
                            RelationalResultSetAssert.assertThat(resultSet)
                                    .nextRowMatches(Map.of("DIFF", -5L, "C", 1L))
                                    .nextRowMatches(Map.of("DIFF", -4L, "C", 1L))
                                    .hasNoNextRow();
                        }
                    }

                    preparedStatement.setLong(1, 5L);
                    try (RelationalResultSet resultSet = preparedStatement.executeQuery()) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .nextRowMatches(Map.of("DIFF", -6L, "C", 1L))
                                .hasNoNextRow();
                    }

                    preparedStatement.setLong(1, 6L);
                    try (RelationalResultSet resultSet = preparedStatement.executeQuery()) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNoNextRow();
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void countByEMask2(boolean maskAsParam) throws SQLException, RelationalException {
        final String countIndexName = "countEAnd2";
        RecordMetaData metaData = getUpdatedMetaData(metaDataBuilder -> {
            final Index countIndex = new Index(countIndexName,
                    new GroupingKeyExpression(Key.Expressions.function("bitand", Key.Expressions.concat(Key.Expressions.field("E"), Key.Expressions.value(2))), 0),
                    IndexTypes.COUNT);
            metaDataBuilder.addIndex("T3", countIndex);
        });

        try (FDBRecordContext context = openContext()) {
            try (EmbeddedRelationalConnection connection = connectTransactionBound(context, metaData)) {
                insertT3Data(connection);

                final String sqlQuery = "SELECT e & " + (maskAsParam ? "?m" : "2") + " as masked , count(*) AS c FROM t3 GROUP BY e & " + (maskAsParam ? "?m" : "2");
                try (RelationalPreparedStatement statement = connection.prepareStatement("explain " + sqlQuery)) {
                    statement.setInt("m", 2);

                    try (RelationalResultSet resultSet = statement.executeQuery()) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNextRow();
                        Assertions.assertThat(resultSet.getString("plan"))
                                .contains("AISCAN(" + countIndexName + " <,> BY_GROUP");
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNoNextRow();
                    }
                }

                try (RelationalPreparedStatement statement = connection.prepareStatement(sqlQuery)) {
                    statement.setInt("m", 2);
                    try (RelationalResultSet resultSet = statement.executeQuery()) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .nextRowMatches(Map.of("MASKED", 0L, "C", 3L))
                                .nextRowMatches(Map.of("MASKED", 2L, "C", 5L))
                                .hasNoNextRow();
                    }
                }
            }
        }
    }

    static Stream<Arguments> countAndFilterOnMaskIndex() {
        return Stream.of(false, true).flatMap(maskAsParam ->
                Stream.of(false, true).map(asHaving ->
                        Arguments.of(maskAsParam, asHaving)));
    }

    @ParameterizedTest
    @MethodSource
    void countAndFilterOnMaskIndex(boolean maskAsParam, boolean asHaving) throws SQLException, RelationalException {
        final String countIndexName = "countFAnd2";
        RecordMetaData metaData = getUpdatedMetaData(metaDataBuilder -> {
            final Index countIndex = new Index(countIndexName,
                    new GroupingKeyExpression(Key.Expressions.function("bitand", Key.Expressions.concat(Key.Expressions.field("F"), Key.Expressions.value(2))), 0),
                    IndexTypes.COUNT);
            metaDataBuilder.addIndex("T3", countIndex);
        });

        try (FDBRecordContext context = openContext()) {
            try (EmbeddedRelationalConnection connection = connectTransactionBound(context, metaData)) {
                insertT3Data(connection);

                final StringBuilder sqlQueryBuilder = new StringBuilder("SELECT count(*) AS c FROM t3 ");
                if (!asHaving) {
                    sqlQueryBuilder.append("WHERE f & ");
                    if (maskAsParam) {
                        sqlQueryBuilder.append("?m");
                    } else {
                        sqlQueryBuilder.append("2");
                    }
                    sqlQueryBuilder.append(" = ? ");
                }
                sqlQueryBuilder.append("GROUP BY f & ");
                if (maskAsParam) {
                    sqlQueryBuilder.append("?m");
                } else {
                    sqlQueryBuilder.append("2");
                }
                if (asHaving) {
                    sqlQueryBuilder.append(" HAVING f & ");
                    if (maskAsParam) {
                        sqlQueryBuilder.append("?m");
                    } else {
                        sqlQueryBuilder.append("2");
                    }
                    sqlQueryBuilder.append(" = ?");
                }

                final String sqlQuery = sqlQueryBuilder.toString();
                try (RelationalPreparedStatement statement = connection.prepareStatement("explain " + sqlQuery)) {
                    statement.setInt("m", 2);
                    statement.setLong(1, 0L);
                    try (RelationalResultSet resultSet = statement.executeQuery()) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNextRow();
                        Assertions.assertThat(resultSet.getString("plan"))
                                .contains("AISCAN(" + countIndexName + " [EQUALS ");
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNoNextRow();
                    }
                }

                try (RelationalPreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {
                    preparedStatement.setInt("m", 2);

                    preparedStatement.setLong(1, 0L);
                    try (RelationalResultSet resultSet = preparedStatement.executeQuery()) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .nextRowMatches(Map.of("C", 4L))
                                .hasNoNextRow();
                    }

                    preparedStatement.setLong(1, 1L);
                    try (RelationalResultSet resultSet = preparedStatement.executeQuery()) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNoNextRow();
                    }

                    preparedStatement.setLong(1, 2L);
                    try (RelationalResultSet resultSet = preparedStatement.executeQuery()) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .nextRowMatches(Map.of("C", 4L))
                                .hasNoNextRow();
                    }
                }
            }
        }
    }

    @Test
    void countWithInsAndMask() throws SQLException, RelationalException {
        final String countIndexName = "multiGroupedCount";
        RecordMetaData metaData = getUpdatedMetaData(metaDataBuilder -> {
            final Index countIndex = new Index(countIndexName,
                    new GroupingKeyExpression(Key.Expressions.concat(
                            Key.Expressions.field("A"),
                            Key.Expressions.field("E"),
                            Key.Expressions.function("bitand", Key.Expressions.concat(Key.Expressions.field("F"), Key.Expressions.value(2L))
                            )), 0),
                    IndexTypes.COUNT);
            metaDataBuilder.addIndex("T3", countIndex);
        });

        try (FDBRecordContext context = openContext()) {
            try (EmbeddedRelationalConnection connection = connectTransactionBound(context, metaData)) {
                insertT3Data(connection);

                // Put in list predicates on the first two columns. Then have it return all (in practice, both) values for the mask value.
                final String sqlQuery = "SELECT a, e, f & ?m AS flags, count(*) AS c FROM t3 GROUP BY a, e, f & ?m HAVING a IN ?aList AND e IN ?eList";
                try (RelationalPreparedStatement statement = connection.prepareStatement("explain " + sqlQuery)) {
                    statement.setLong("m", 2L);
                    statement.setArray("aList", connection.createArrayOf("BIGINT", new Object[]{0L, 1L, 2L}));
                    statement.setArray("eList", connection.createArrayOf("BIGINT", new Object[]{0L, 1L, 2L}));

                    try (RelationalResultSet resultSet = statement.executeQuery()) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNextRow();
                        Assertions.assertThat(resultSet.getString("plan"))
                                .contains("AISCAN(" + countIndexName + " [EQUALS ");
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNoNextRow();
                    }
                }

                try (RelationalPreparedStatement statement = connection.prepareStatement(sqlQuery)) {
                    statement.setLong("m", 2L);
                    statement.setArray("aList", connection.createArrayOf("BIGINT", new Object[]{2L, 4L}));
                    statement.setArray("eList", connection.createArrayOf("BIGINT", new Object[]{6L, 7L, 8L, 9L}));

                    try (RelationalResultSet resultSet = statement.executeQuery()) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .nextRowMatches(Map.of("A", 2L, "E", 8L, "FLAGS", 2L, "C", 1L))
                                .nextRowMatches(Map.of("A", 2L, "E", 9L, "FLAGS", 2L, "C", 1L))
                                .nextRowMatches(Map.of("A", 4L, "E", 6L, "FLAGS", 0L, "C", 1L))
                                .nextRowMatches(Map.of("A", 4L, "E", 7L, "FLAGS", 2L, "C", 1L))
                                .hasNoNextRow();

                    }
                }
            }
        }
    }

    @Test
    void doNotUseCachedValuePlanWithIncorrectMask() throws SQLException, RelationalException {
        Utils.enableCascadesDebugger();
        final String fBy2IndexName = "fAnd2";
        final String fBy4IndexName = "fAnd4";
        RecordMetaData metaData = getUpdatedMetaData(metaDataBuilder -> {
            // Two very similar indexes. One is grouped by f & 2 while the other is grouped by f & 4.
            final Index countFBy2Index = new Index(fBy2IndexName,
                    Key.Expressions.function("bitand", Key.Expressions.concat(Key.Expressions.field("F"), Key.Expressions.value(2L))),
                    IndexTypes.VALUE);
            metaDataBuilder.addIndex("T3", countFBy2Index);
            final Index countFBy4Index = new Index(fBy4IndexName,
                    Key.Expressions.function("bitand", Key.Expressions.concat(Key.Expressions.field("F"), Key.Expressions.value(4L))),
                    IndexTypes.VALUE);
            metaDataBuilder.addIndex("T3", countFBy4Index);
        });

        try (FDBRecordContext context = openContext()) {
            try (EmbeddedRelationalConnection connection = connectTransactionBound(context, metaData)) {
                final String sqlQuery = "SELECT f & ?m, t3.id FROM t3 WHERE f & ?m > ?";
                try (RelationalPreparedStatement statement = connection.prepareStatement("explain " + sqlQuery)) {
                    statement.setLong("m", 2L);
                    statement.setLong(1, 0);

                    try (RelationalResultSet resultSet = statement.executeQuery()) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNextRow();
                        Assertions.assertThat(resultSet.getString("plan"))
                                .contains("ISCAN(" + fBy2IndexName + " [[GREATER_THAN ");
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNoNextRow();
                    }

                    statement.setLong("m", 4L);
                    try (RelationalResultSet resultSet = statement.executeQuery()) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNextRow();
                        Assertions.assertThat(resultSet.getString("plan"))
                                .contains("ISCAN(" + fBy4IndexName + " [[GREATER_THAN ");
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNoNextRow();
                    }
                }
            }
        }
    }

    @Test
    void doNotUseCachedCountPlanWithIncorrectMask() throws SQLException, RelationalException {
        Utils.enableCascadesDebugger();
        final String countFBy2IndexName = "countFAnd2";
        final String countFBy4IndexName = "countFAnd4";
        RecordMetaData metaData = getUpdatedMetaData(metaDataBuilder -> {
            // Two very similar indexes. One is grouped by f & 2 while the other is grouped by f & 4.
            final Index countFBy2Index = new Index(countFBy2IndexName,
                    new GroupingKeyExpression(Key.Expressions.function("bitand", Key.Expressions.concat(Key.Expressions.field("F"), Key.Expressions.value(2L))), 0),
                    IndexTypes.COUNT);
            metaDataBuilder.addIndex("T3", countFBy2Index);
            final Index countFBy4Index = new Index(countFBy4IndexName,
                    new GroupingKeyExpression(Key.Expressions.function("bitand", Key.Expressions.concat(Key.Expressions.field("F"), Key.Expressions.value(4L))), 0),
                    IndexTypes.COUNT);
            metaDataBuilder.addIndex("T3", countFBy4Index);
        });

        try (FDBRecordContext context = openContext()) {
            try (EmbeddedRelationalConnection connection = connectTransactionBound(context, metaData)) {
                final String sqlQuery = "SELECT f & ?m, count(*) FROM t3 GROUP BY f & ?m HAVING f & ?m > ?";
                try (RelationalPreparedStatement statement = connection.prepareStatement("explain " + sqlQuery)) {
                    statement.setLong("m", 2L);
                    statement.setLong(1, 0);

                    try (RelationalResultSet resultSet = statement.executeQuery()) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNextRow();
                        Assertions.assertThat(resultSet.getString("plan"))
                                .contains("AISCAN(" + countFBy2IndexName + " [[GREATER_THAN ");
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNoNextRow();
                    }

                    statement.setLong("m", 4L);
                    try (RelationalResultSet resultSet = statement.executeQuery()) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNextRow();
                        Assertions.assertThat(resultSet.getString("plan"))
                                .contains("AISCAN(" + countFBy4IndexName + " [[GREATER_THAN ");
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNoNextRow();
                    }
                }
            }
        }
    }
}
