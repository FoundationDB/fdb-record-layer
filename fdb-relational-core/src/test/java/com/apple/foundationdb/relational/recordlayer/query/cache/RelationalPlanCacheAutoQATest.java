/*
 * RelationalPlanCacheAutoQATest.java
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

package com.apple.foundationdb.relational.recordlayer.query.cache;

import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metrics.MetricCollector;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.recordlayer.AbstractDatabase;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.RelationalConnectionRule;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.query.Plan;
import com.apple.foundationdb.relational.recordlayer.query.PlanContext;
import com.apple.foundationdb.relational.recordlayer.query.PlanGenerator;
import com.apple.foundationdb.relational.recordlayer.query.PlannerConfiguration;
import com.apple.foundationdb.relational.recordlayer.query.QueryPlan;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.utils.DatabaseRule;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import com.google.common.testing.FakeTicker;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.DriverManager;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Additional tests for {@link RelationalPlanCache} identity, statistics and write-only behaviour.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class RelationalPlanCacheAutoQATest {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(2)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(RelationalPlanCacheAutoQATest.class, TestSchemas.books());

    @RegisterExtension
    @Order(3)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(database::getConnectionUri)
            .withSchema("TEST_SCHEMA");

    @RegisterExtension
    @Order(4)
    public final DatabaseRule secondDatabase = new DatabaseRule(
            URI.create("/TEST/" + RelationalPlanCacheAutoQATest.class.getSimpleName() + "_SECOND"), Options.none());

    @Nonnull
    private static final String i1970 = "IDX_1970";

    @Nonnull
    private static final String i1980 = "IDX_1980";

    @Nonnull
    private static final String Scan = "SCAN";

    private static final class CountingMetricCollector implements MetricCollector {
        @Nonnull
        private final Map<RelationalMetric.RelationalCount, Integer> counts = new EnumMap<>(RelationalMetric.RelationalCount.class);

        @Override
        public void increment(@Nonnull final RelationalMetric.RelationalCount count, final int val) {
            counts.merge(count, val, Integer::sum);
        }

        @Override
        public <T> T clock(@Nonnull final RelationalMetric.RelationalEvent event,
                           final com.apple.foundationdb.relational.util.Supplier<T> supplier) throws RelationalException {
            return supplier.get();
        }

        int count(@Nonnull final RelationalMetric.RelationalCount count) {
            return counts.getOrDefault(count, 0);
        }
    }

    @Nonnull
    private RelationalPlanCache getCache(@Nonnull final FakeTicker ticker) {
        return RelationalPlanCache.newRelationalCacheBuilder()
                .setSize(2)
                .setSecondarySize(2)
                .setTertiarySize(2)
                .setTtl(20)
                .setSecondaryTtl(10)
                .setTertiaryTtl(5)
                .setExecutor(Runnable::run)
                .setSecondaryExecutor(Runnable::run)
                .setTertiaryExecutor(Runnable::run)
                .setTicker(ticker)
                .build();
    }

    @Nonnull
    private PlanGenerator getPlanGenerator(@Nonnull final RelationalPlanCache cache,
                                           @Nonnull final String schemaTemplateName,
                                           int schemaTemplateVersion,
                                           @Nonnull final Set<String> readableIndexes,
                                           @Nonnull final Options options,
                                           @Nonnull final MetricCollector metricCollector) throws Exception {
        final var schemaName = connection.getSchema();
        final var embeddedConnection = Assert.castUnchecked(connection.getUnderlyingEmbeddedConnection(), EmbeddedRelationalConnection.class);
        final var schemaTemplate = embeddedConnection.getSchemaTemplate().unwrap(RecordLayerSchemaTemplate.class).toBuilder()
                .setVersion(schemaTemplateVersion).setName(schemaTemplateName).build();
        final AbstractDatabase recordLayerDatabase = embeddedConnection.getRecordLayerDatabase();
        final var storeState = new RecordStoreState(null, readableIndexes.stream()
                .map(index -> Pair.of(index, IndexState.READABLE))
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
        final FDBRecordStoreBase<?> store = recordLayerDatabase.loadSchema(schemaName).loadStore().unwrap(FDBRecordStoreBase.class);
        final PlanContext planContext = PlanContext.Builder
                .create()
                .fromDatabase(recordLayerDatabase)
                .fromRecordStore(store, options)
                .withSchemaTemplate(schemaTemplate)
                .withMetricsCollector(metricCollector)
                .withPlannerConfiguration(PlannerConfiguration.of(Optional.of(readableIndexes), options))
                .build();
        return PlanGenerator.create(Optional.of(cache), planContext, store.getRecordMetaData(), storeState,
                store.getIndexMaintainerRegistry(), options);
    }

    @Nonnull
    private String planQuery(@Nonnull final RelationalPlanCache cache,
                             @Nonnull final String query,
                             @Nonnull final String schemaTemplateName,
                             int schemaTemplateVersion,
                             @Nonnull final Options options,
                             @Nonnull final MetricCollector metricCollector) throws Exception {
        connection.setAutoCommit(false);
        connection.getUnderlyingEmbeddedConnection().createNewTransaction();
        final Plan<?> plan;
        try {
            final var planGenerator = getPlanGenerator(cache, schemaTemplateName, schemaTemplateVersion,
                    Set.of(i1970, i1980), options, metricCollector);
            plan = planGenerator.getPlan(query);
        } finally {
            connection.rollback();
            connection.setAutoCommit(true);
        }
        return inferScanType(plan);
    }

    @Nonnull
    private static String inferScanType(@Nonnull final Plan<?> plan) {
        assertThat(plan).isInstanceOf(QueryPlan.PhysicalQueryPlan.class);
        final var desc = ((QueryPlan.PhysicalQueryPlan) plan).getRecordQueryPlan().toString();
        if (desc.contains(i1970)) {
            return i1970;
        } else if (desc.contains(i1980)) {
            return i1980;
        } else {
            return Scan;
        }
    }

    private static int numTertiaryEntries(@Nonnull final RelationalPlanCache cache) {
        int result = 0;
        for (final var key : Set.copyOf(cache.getStats().getAllKeys())) {
            for (final var secondaryKey : Set.copyOf(cache.getStats().getAllSecondaryKeys(key))) {
                result += cache.getStats().getAllTertiaryMappings(key, secondaryKey).size();
            }
        }
        return result;
    }

    private static int numSecondaryEntries(@Nonnull final RelationalPlanCache cache) {
        int result = 0;
        for (final var key : Set.copyOf(cache.getStats().getAllKeys())) {
            result += cache.getStats().getAllSecondaryKeys(key).size();
        }
        return result;
    }

    @Test
    void describeSchemaInSecondDatabaseReportsThatDatabase() throws Exception {
        // BUG AQA-19: the plan for an unqualified DESCRIBE SCHEMA embeds the connection's database, but the plan cache key
        // does not include it, so another database on the same template gets the cached plan. Spec: MetadataPlanVisitor
        // resolves an unqualified schema against the connection's database; the cache must not change results.
        // an unqualified DESCRIBE SCHEMA resolves against the connection's database, even if another database with
        // the same schema template ran the same statement first and its plan is in the cache
        final var firstPath = database.getDatabasePath().getPath();
        final var secondPath = secondDatabase.getDbUri().getPath();
        try (var conn = DriverManager.getConnection("jdbc:embed:/__SYS")) {
            conn.setSchema("CATALOG");
            try (var statement = conn.createStatement()) {
                statement.executeUpdate("CREATE SCHEMA \"" + secondPath + "/TEST_SCHEMA\" WITH TEMPLATE \"" + database.getSchemaTemplateName() + "\"");
            }
        }
        assertThat(describeTestSchemaPath(firstPath)).isEqualTo(firstPath);
        assertThat(describeTestSchemaPath(secondPath)).isEqualTo(secondPath);
    }

    @Nonnull
    private static String describeTestSchemaPath(@Nonnull final String databasePath) throws Exception {
        try (var conn = DriverManager.getConnection("jdbc:embed:" + databasePath)) {
            conn.setSchema("TEST_SCHEMA");
            try (var statement = conn.createStatement();
                    var resultSet = statement.executeQuery("DESCRIBE SCHEMA TEST_SCHEMA")) {
                assertThat(resultSet.next()).isTrue();
                return resultSet.getString("DATABASE_PATH");
            }
        }
    }
}
