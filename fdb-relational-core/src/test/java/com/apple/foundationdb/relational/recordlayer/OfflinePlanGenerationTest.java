/*
 * OfflinePlanGenerationTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.query.PlanContext;
import com.apple.foundationdb.relational.recordlayer.query.PlanGenerator;
import com.apple.foundationdb.relational.recordlayer.query.QueryPlan;
import com.apple.foundationdb.relational.recordlayer.query.cache.NoOpMetricCollector;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class OfflinePlanGenerationTest {

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(2)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(relationalExtension, 
            OfflinePlanGenerationTest.class, TestSchemas.restaurant());

    @RegisterExtension
    @Order(3)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(relationalExtension, database::getConnectionUri)
            .withSchema("TEST_SCHEMA");

    @RegisterExtension
    @Order(4)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);

    public OfflinePlanGenerationTest() {
        Utils.enableCascadesDebugger();
    }

    private RecordLayerSchemaTemplate schemaTemplate;
    private RecordStoreState storeState;

    @BeforeEach
    void captureTemplateAndState() throws Exception {
        final var conn = (EmbeddedRelationalConnection) connection.connection;
        conn.setAutoCommit(false);
        conn.createNewTransaction();
        try {
            final var store = conn.getRecordLayerDatabase()
                    .loadSchema(connection.getSchema())
                    .loadStore().unwrap(FDBRecordStoreBase.class);
            schemaTemplate = (RecordLayerSchemaTemplate) conn.getSchemaTemplate();
            storeState = store.getRecordStoreState();
        } finally {
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    @Test
    void offlinePlanSelectPrimaryKey() throws Exception {
        final var query = "select * from restaurant where rest_no > 10";

        final var plan = PlanGenerator.create(
                Optional.empty(),
                schemaTemplate,
                storeState,
                NoOpMetricCollector.INSTANCE,
                connection.getOptions())
                .getPlan(query);

        assertThat(plan).isInstanceOf(QueryPlan.PhysicalQueryPlan.class);
        final var physicalPlan = (QueryPlan.PhysicalQueryPlan) plan;
        assertThat(physicalPlan.getRecordQueryPlan().toString()).startsWith("SCAN([IS RESTAURANT");
    }

    @Test
    void offlinePlanSelectByIndex() throws Exception {
        final var query = "select * from restaurant where name = 'foo'";

        final var plan = PlanGenerator.create(
                Optional.empty(),
                schemaTemplate,
                storeState,
                NoOpMetricCollector.INSTANCE,
                connection.getOptions())
                .getPlan(query);

        assertThat(plan).isInstanceOf(QueryPlan.PhysicalQueryPlan.class);
        final var physicalPlan = (QueryPlan.PhysicalQueryPlan) plan;
        assertThat(physicalPlan.getRecordQueryPlan().toString()).startsWith("ISCAN(RECORD_NAME_IDX");
    }

    @Test
    void offlinePlanSelectByDisabledIndex() throws Exception {
        final var query = "select * from restaurant where name = 'foo'";

        final var disabledStoreState = new RecordStoreState(
                null,
                Map.of("RECORD_NAME_IDX", IndexState.DISABLED));

        final var plan = PlanGenerator.create(
                Optional.empty(),
                schemaTemplate,
                disabledStoreState,
                NoOpMetricCollector.INSTANCE,
                connection.getOptions())
                .getPlan(query);

        assertThat(plan).isInstanceOf(QueryPlan.PhysicalQueryPlan.class);
        final var physicalPlan = (QueryPlan.PhysicalQueryPlan) plan;
        assertThat(physicalPlan.getRecordQueryPlan().toString()).startsWith("SCAN([IS RESTAURANT");
    }

    @Test
    void offlinePlanCompareOpenStorePlan() throws Exception {
        final var query = "select * from restaurant where rest_no > 10";
        final var options = connection.getOptions();

        // Plan offline first — uses only template + state, no transaction.
        final var offlinePlan = (QueryPlan.PhysicalQueryPlan) PlanGenerator
                .create(Optional.empty(), schemaTemplate, storeState,
                        NoOpMetricCollector.INSTANCE, options)
                .getPlan(query);

        // Then open a transaction and plan against the real store via the standard workflow.
        final var embeddedConnection = (EmbeddedRelationalConnection) connection.connection;
        embeddedConnection.setAutoCommit(false);
        embeddedConnection.createNewTransaction();
        try {
            final AbstractDatabase rlDatabase = embeddedConnection.getRecordLayerDatabase();
            final FDBRecordStoreBase<?> store = rlDatabase.loadSchema(connection.getSchema())
                    .loadStore().unwrap(FDBRecordStoreBase.class);

            final PlanContext openPlanContext = PlanContext.Builder.create()
                    .fromRecordStore(store, options)
                    .fromDatabase(rlDatabase)
                    .withMetricsCollector(embeddedConnection.getMetricCollector())
                    .withSchemaTemplate(embeddedConnection.getSchemaTemplate())
                    .build();
            final var openPlan = (QueryPlan.PhysicalQueryPlan) PlanGenerator
                    .create(Optional.empty(), openPlanContext, store, options)
                    .getPlan(query);

            assertThat(offlinePlan.getRecordQueryPlan().semanticHashCode()).isEqualTo(openPlan.getRecordQueryPlan().semanticHashCode());
        } finally {
            embeddedConnection.rollback();
            embeddedConnection.setAutoCommit(true);
        }
    }

    @Test
    void offlinePlanCacheForOpenStore() throws Exception {
        final var query = "select * from restaurant where rest_no > 10";
        final var options = connection.getOptions();
        final var cache = RelationalPlanCache.buildWithDefaults();

        // Plan offline first using the shared cache — writes the plan to it.
        PlanGenerator.create(Optional.of(cache), schemaTemplate, storeState,
                        NoOpMetricCollector.INSTANCE, options)
                .getPlan(query);

        // Open a transaction and plan via the standard workflow with the SAME cache instance.
        final var embeddedConnection = (EmbeddedRelationalConnection) connection.connection;
        embeddedConnection.setAutoCommit(false);
        embeddedConnection.createNewTransaction();
        try {
            final AbstractDatabase rlDatabase = embeddedConnection.getRecordLayerDatabase();
            final FDBRecordStoreBase<?> store = rlDatabase.loadSchema(connection.getSchema())
                    .loadStore().unwrap(FDBRecordStoreBase.class);

            final PlanContext openPlanContext = PlanContext.Builder.create()
                    .fromRecordStore(store, options)
                    .fromDatabase(rlDatabase)
                    .withMetricsCollector(embeddedConnection.getMetricCollector())
                    .withSchemaTemplate(embeddedConnection.getSchemaTemplate())
                    .build();
            PlanGenerator.create(Optional.of(cache), openPlanContext, store, options)
                    .getPlan(query);
        } finally {
            embeddedConnection.rollback();
            embeddedConnection.setAutoCommit(true);
        }

        // Cache must have exactly one main entry — open path hit the entry written by offline path.
        cache.cleanUp();
        assertThat(cache.getStats().numEntriesSlow()).isEqualTo(1L);
    }

    @Test
    void offlinePlanSchemaVersions() throws Exception {
        final var query = "select * from BOOKS where YEAR > 1980";
        final var emptyState = new RecordStoreState(null, Map.of());

        final var planV1 = (QueryPlan.PhysicalQueryPlan) PlanGenerator.create(
                Optional.empty(),
                booksTemplate(false, 1),
                emptyState,
                NoOpMetricCollector.INSTANCE,
                connection.getOptions())
                .getPlan(query);

        final var planV2 = (QueryPlan.PhysicalQueryPlan) PlanGenerator.create(
                Optional.empty(),
                booksTemplate(true, 2),
                emptyState,
                NoOpMetricCollector.INSTANCE,
                connection.getOptions())
                .getPlan(query);

        assertThat(planV2.getRecordQueryPlan().toString()).startsWith("ISCAN(YEAR_IDX");
        assertThat(planV1.getRecordQueryPlan().toString()).startsWith("SCAN([IS BOOKS])");
        assertThat(planV2.getRecordQueryPlan().semanticHashCode())
                .isNotEqualTo(planV1.getRecordQueryPlan().semanticHashCode());
    }

    @Nonnull
    private static RecordLayerSchemaTemplate booksTemplate(boolean withIndex, int version) {
        final var tableBuilder = RecordLayerTable.newBuilder(false)
                .setName("BOOKS")
                .addColumn(RecordLayerColumn.newBuilder()
                        .setName("ID")
                        .setDataType(DataType.Primitives.LONG.type())
                        .build())
                .addColumn(RecordLayerColumn.newBuilder()
                        .setName("YEAR")
                        .setDataType(DataType.Primitives.INTEGER.type())
                        .build())
                .addColumn(RecordLayerColumn.newBuilder()
                        .setName("TITLE")
                        .setDataType(DataType.Primitives.STRING.type())
                        .build())
                .addPrimaryKeyPart(List.of("ID"));
        if (withIndex) {
            tableBuilder.addIndex(RecordLayerIndex.newBuilder()
                    .setName("YEAR_IDX")
                    .setTableName("BOOKS")
                    .setIndexType(IndexTypes.VALUE)
                    .setKeyExpression(Key.Expressions.field("YEAR", KeyExpression.FanType.None))
                    .build());
        }
        return RecordLayerSchemaTemplate.newBuilder()
                .setName("BOOKS_TEMPLATE")
                .setVersion(version)
                .addTable(tableBuilder.build())
                .build();
    }
}
