/*
 * TransactionBoundQueryTest.java
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
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.Metadata;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.AbstractDatabase;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.RecordStoreAndRecordContextTransaction;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.storage.BackingStore;
import com.apple.foundationdb.relational.transactionbound.TransactionBoundEmbeddedRelationalEngine;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.RelationalResultSetAssert;

import com.google.protobuf.Descriptors;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.function.Consumer;


public class TransactionBoundQueryTest {
    @Nonnull
    private static final String SCHEMA_TEMPLATE = " CREATE TABLE t1(id bigint, a bigint, b string, PRIMARY KEY(id)) " +
            "CREATE TYPE as STRUCT entry(key string, value bigint) " +
            "CREATE TABLE t2(id bigint, a bigint, c string, d entry ARRAY, PRIMARY KEY (id))";
    @RegisterExtension
    @Order(0)
    @Nonnull
    final EmbeddedRelationalExtension embeddedExtension = new EmbeddedRelationalExtension();
    @RegisterExtension
    @Order(1)
    final SimpleDatabaseRule databaseRule = new SimpleDatabaseRule(embeddedExtension, TransactionBoundQueryTest.class, SCHEMA_TEMPLATE);

    @Nonnull
    private static Options engineOptions() throws SQLException {
        return Options.builder()
                .withOption(Options.Name.PLAN_CACHE_PRIMARY_MAX_ENTRIES, 100)
                .withOption(Options.Name.PLAN_CACHE_SECONDARY_MAX_ENTRIES, 10)
                .withOption(Options.Name.PLAN_CACHE_TERTIARY_TIME_TO_LIVE_MILLIS, 10L)
                .build();
    }

    @Nonnull
    private EmbeddedRelationalConnection connectEmbedded() throws SQLException, RelationalException {
        RelationalConnection connection = Relational.connect(databaseRule.getConnectionUri(), Options.NONE);
        connection.setSchema(databaseRule.getSchemaName());
        return connection.unwrap(EmbeddedRelationalConnection.class);
    }

    private FDBRecordContext openContext() throws SQLException, RelationalException {
        try (EmbeddedRelationalConnection connection = connectEmbedded()) {
            connection.beginTransaction();
            FDBRecordContext context = connection.getTransaction().unwrap(FDBRecordContext.class);

            // Existing context is closed when the connection is closed. Create a new one from the same database
            // to ensure it can be used
            return context.getDatabase().openContext();
        }
    }

    @Nonnull
    private EmbeddedRelationalConnection connectTransactionBound(@Nonnull FDBRecordContext context, @Nonnull RecordMetaData metaData) throws SQLException, RelationalException {
        try (EmbeddedRelationalConnection embeddedConnection = connectEmbedded()) {
            embeddedConnection.beginTransaction();
            AbstractDatabase db = embeddedConnection.getRecordLayerDatabase();
            BackingStore store = db.loadRecordStore(databaseRule.getSchemaName(), FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NO_INFO_AND_NOT_EMPTY);

            FDBRecordStore baseStore = store.unwrap(FDBRecordStore.class);
            FDBRecordStore newStore = baseStore.asBuilder()
                    .setMetaDataProvider(metaData)
                    .setContext(context)
                    .open();

            embeddedExtension.getEngine().deregisterDriver();
            var engine = new TransactionBoundEmbeddedRelationalEngine(engineOptions());
            engine.registerDriver();
            try {
                RelationalConnection transactionBoundConnection = Relational.connect(databaseRule.getConnectionUri(),
                        new RecordStoreAndRecordContextTransaction(newStore, context, RecordLayerSchemaTemplate.fromRecordMetadata(metaData, databaseRule.getSchemaTemplateName(), metaData.getVersion())), Options.NONE);
                transactionBoundConnection.setAutoCommit(false);
                transactionBoundConnection.setSchema(databaseRule.getSchemaName());
                return transactionBoundConnection.unwrap(EmbeddedRelationalConnection.class);
            } finally {
                engine.deregisterDriver();
                embeddedExtension.getEngine().registerDriver();
            }
        }
    }

    private RecordMetaData getUpdatedMetaData(@Nonnull Consumer<RecordMetaDataBuilder> metaDataUpdater) throws SQLException, RelationalException {
        try (EmbeddedRelationalConnection connection = connectEmbedded()) {
            connection.beginTransaction();
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
        }) ;

        // Now re-open the store (with the new meta-data) using the record layer index
        try (FDBRecordContext context = openContext()) {
            try (RelationalConnection connection = connectTransactionBound(context, metaDataWithIndex)) {
                try (RelationalStatement statement = connection.createStatement()) {
                    try (RelationalResultSet resultSet = statement.executeQuery("explain SELECT a, b FROM t1 ORDER BY b DESC")) {
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNextRow();
                        Assertions.assertThat(resultSet.getString("plan"))
                                .as("Planner should select Record Layer defined index in plan")
                                .contains("Covering(Index(bWithA");

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
                                .contains("Index(cIndex");
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
                                .contains("Scan(<,>)");
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
                                .contains("Index(" + valueIndexName);
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
                                .contains("Index(" + valueIndexName + " [EQUALS ");
                        RelationalResultSetAssert.assertThat(resultSet)
                                .hasNoNextRow();
                    }
                }
            }
        }
    }
}
