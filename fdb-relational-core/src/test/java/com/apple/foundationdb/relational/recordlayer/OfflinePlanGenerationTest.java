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

import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.query.PlanContext;
import com.apple.foundationdb.relational.recordlayer.query.PlanGenerator;
import com.apple.foundationdb.relational.recordlayer.query.QueryPlan;
import com.apple.foundationdb.relational.recordlayer.query.cache.NoOpMetricCollector;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class OfflinePlanGenerationTest {

    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(2)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(
            OfflinePlanGenerationTest.class, TestSchemas.restaurant());

    @RegisterExtension
    @Order(3)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
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

        final var plan = PlanGenerator.createOfflineDql(
                Optional.empty(),
                schemaTemplate,
                storeState,
                NoOpMetricCollector.INSTANCE,
                connection.getOptions())
                .getPlan(query);

        assertThat(plan).isInstanceOf(QueryPlan.PhysicalQueryPlan.class);
        final var physicalPlan = (QueryPlan.PhysicalQueryPlan) plan;
        assertThat(physicalPlan.getRecordQueryPlan().toString()).hasToString("SCAN([IS RESTAURANT, [GREATER_THAN promote(@c7 AS LONG)]])");
    }

    @Test
    void offlinePlanSelectByIndex() throws Exception {
        final var query = "select * from restaurant where name = 'foo'";

        final var plan = PlanGenerator.createOfflineDql(
                Optional.empty(),
                schemaTemplate,
                storeState,
                NoOpMetricCollector.INSTANCE,
                connection.getOptions())
                .getPlan(query);

        assertThat(plan).isInstanceOf(QueryPlan.PhysicalQueryPlan.class);
        final var physicalPlan = (QueryPlan.PhysicalQueryPlan) plan;
        assertThat(physicalPlan.getRecordQueryPlan().toString()).hasToString("ISCAN(RECORD_NAME_IDX [EQUALS promote(@c7 AS STRING)])");
    }

    @Test
    void offlinePlanCompareOpenStorePlan() throws Exception {
        final var query = "select * from restaurant where rest_no > 10";
        final var options = connection.getOptions();

        // Plan offline first — uses only template + state, no transaction.
        final var offlinePlan = (QueryPlan.PhysicalQueryPlan) PlanGenerator
                .createOfflineDql(Optional.empty(), schemaTemplate, storeState,
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
}
