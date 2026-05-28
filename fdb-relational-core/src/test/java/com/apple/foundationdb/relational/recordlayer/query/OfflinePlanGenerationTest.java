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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactoryRegistryImpl;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.ddl.NoOpQueryFactory;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.ddl.NoOpMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.query.cache.NoOpMetricCollector;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class OfflinePlanGenerationTest {

    private static final String TEMPLATE_NAME = "OfflineTestTemplate";
    private static final int TEMPLATE_VERSION = 1;

    @Nonnull
    private static RecordLayerSchemaTemplate createTemplate() {
        return RecordLayerSchemaTemplate.newBuilder()
                .setName(TEMPLATE_NAME)
                .setVersion(TEMPLATE_VERSION)
                .addTable(RecordLayerTable.newBuilder(false)
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
                        .addPrimaryKeyPart(List.of("ID"))
                        .addIndex(RecordLayerIndex.newBuilder()
                                .setName("YEAR_IDX")
                                .setTableName("BOOKS")
                                .setIndexType(IndexTypes.VALUE)
                                .setKeyExpression(Key.Expressions.field("YEAR", KeyExpression.FanType.None))
                                .build())
                        .build())
                .build();
    }

    @Nonnull
    private static PlanGenerator createOfflinePlanGenerator(@Nonnull final RecordLayerSchemaTemplate schemaTemplate,
                                                            @Nonnull final RecordStoreState storeState,
                                                            @Nonnull final Options options) throws RelationalException {
        final var metaData = schemaTemplate.toRecordMetadata();
        final var planContext = PlanContext.Builder.create()
                .fromMetaDataAndState(metaData, storeState, options)
                .withSchemaTemplate(schemaTemplate)
                .withMetricsCollector(NoOpMetricCollector.INSTANCE)
                .withConstantActionFactory(NoOpMetadataOperationsFactory.INSTANCE)
                .withDdlQueryFactory(NoOpQueryFactory.INSTANCE)
                .withDbUri(URI.create("embed:offline"))
                .build();
        return PlanGenerator.create(
                Optional.empty(),
                planContext,
                metaData,
                storeState,
                IndexMaintainerFactoryRegistryImpl.instance(),
                options);
    }

    @Test
    void plansSelectAgainstIndexedColumnWithoutOpeningStore() throws RelationalException {
        final var schemaTemplate = createTemplate();
        final var storeState = new RecordStoreState(null, Map.of());

        final var planGenerator = createOfflinePlanGenerator(schemaTemplate, storeState, Options.NONE);
        final var plan = planGenerator.getPlan("SELECT * FROM BOOKS WHERE YEAR > 1980");

        Assertions.assertThat(plan).isInstanceOf(QueryPlan.PhysicalQueryPlan.class);
        final var physicalPlan = (QueryPlan.PhysicalQueryPlan) plan;
        Assertions.assertThat(physicalPlan.getRecordQueryPlan().toString()).hasToString("ISCAN(YEAR_IDX [[GREATER_THAN promote(@c7 AS INT)]])");
    }
}
