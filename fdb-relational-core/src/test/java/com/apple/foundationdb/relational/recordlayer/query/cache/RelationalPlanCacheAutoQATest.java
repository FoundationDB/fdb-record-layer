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
import com.apple.foundationdb.record.query.plan.cascades.rules.ImplementInsertRule;
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
import com.google.common.collect.ImmutableSet;
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
    void reduceMetricsMatchPrimarySecondaryAndTertiaryOutcomes() throws Exception {
        // the cache metrics and primary statistics must reflect each level's hit or miss for a sequence of lookups
        final var cache = getCache(new FakeTicker());
        final var metrics = new CountingMetricCollector();
        final String q1970 = "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979";
        final String q1980 = "SELECT * FROM BOOKS WHERE YEAR > 1980 AND YEAR < 1985";

        // cold cache: every level misses
        assertThat(planQuery(cache, q1970, "SCHEMA_TEMPLATE_1", 10, Options.none(), metrics)).isEqualTo(i1970);
        assertThat(metrics.count(RelationalMetric.RelationalCount.PLAN_CACHE_PRIMARY_MISS)).isEqualTo(1);
        assertThat(metrics.count(RelationalMetric.RelationalCount.PLAN_CACHE_SECONDARY_MISS)).isEqualTo(1);
        assertThat(metrics.count(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS)).isEqualTo(1);
        assertThat(metrics.count(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT)).isZero();

        // same query: full hit
        assertThat(planQuery(cache, q1970, "SCHEMA_TEMPLATE_1", 10, Options.none(), metrics)).isEqualTo(i1970);
        assertThat(metrics.count(RelationalMetric.RelationalCount.PLAN_CACHE_PRIMARY_MISS)).isEqualTo(1);
        assertThat(metrics.count(RelationalMetric.RelationalCount.PLAN_CACHE_SECONDARY_MISS)).isEqualTo(1);
        assertThat(metrics.count(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS)).isEqualTo(1);
        assertThat(metrics.count(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT)).isEqualTo(1);

        // same shape, constants outside the cached constraint: tertiary miss only
        assertThat(planQuery(cache, q1980, "SCHEMA_TEMPLATE_1", 10, Options.none(), metrics)).isEqualTo(i1980);
        assertThat(metrics.count(RelationalMetric.RelationalCount.PLAN_CACHE_PRIMARY_MISS)).isEqualTo(1);
        assertThat(metrics.count(RelationalMetric.RelationalCount.PLAN_CACHE_SECONDARY_MISS)).isEqualTo(1);
        assertThat(metrics.count(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS)).isEqualTo(2);

        // another template version: secondary and tertiary miss
        assertThat(planQuery(cache, q1970, "SCHEMA_TEMPLATE_1", 11, Options.none(), metrics)).isEqualTo(i1970);
        assertThat(metrics.count(RelationalMetric.RelationalCount.PLAN_CACHE_PRIMARY_MISS)).isEqualTo(1);
        assertThat(metrics.count(RelationalMetric.RelationalCount.PLAN_CACHE_SECONDARY_MISS)).isEqualTo(2);
        assertThat(metrics.count(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS)).isEqualTo(3);

        // another template name: every level misses
        assertThat(planQuery(cache, q1970, "SCHEMA_TEMPLATE_2", 10, Options.none(), metrics)).isEqualTo(i1970);
        assertThat(metrics.count(RelationalMetric.RelationalCount.PLAN_CACHE_PRIMARY_MISS)).isEqualTo(2);
        assertThat(metrics.count(RelationalMetric.RelationalCount.PLAN_CACHE_SECONDARY_MISS)).isEqualTo(3);
        assertThat(metrics.count(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS)).isEqualTo(4);
        assertThat(metrics.count(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT)).isEqualTo(1);
        assertThat(metrics.count(RelationalMetric.RelationalCount.PLAN_CACHE_PRIMARY_LRU_EVICTION)).isZero();

        // the primary statistics agree: 2 primary misses (templates 1 and 2), 3 primary hits
        assertThat(cache.getStats().numMisses()).isEqualTo(2);
        assertThat(cache.getStats().numHits()).isEqualTo(3);
    }

    @Test
    void writeOnlyQueryOptionStoresWithoutLookupAndReplacesEqualConstraint() throws Exception {
        // OPTIONS (PLAN CACHE WRITE ONLY) never reads, replaces an entry under an equal constraint, and is served later
        final var cache = getCache(new FakeTicker());
        final var metrics = new CountingMetricCollector();
        final String query = "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979";
        final String writeOnly = query + " OPTIONS (PLAN CACHE WRITE ONLY)";

        assertThat(planQuery(cache, writeOnly, "SCHEMA_TEMPLATE_1", 10, Options.none(), metrics)).isEqualTo(i1970);
        assertThat(planQuery(cache, writeOnly, "SCHEMA_TEMPLATE_1", 10, Options.none(), metrics)).isEqualTo(i1970);
        assertThat(metrics.count(RelationalMetric.RelationalCount.PLAN_CACHE_WRITE_ONLY_STORE)).isEqualTo(2);
        assertThat(metrics.count(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT)).isZero();
        assertThat(metrics.count(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS)).isZero();
        // the second store replaced the first: one secondary key with a single tertiary entry
        assertThat(numSecondaryEntries(cache)).isEqualTo(1);
        assertThat(numTertiaryEntries(cache)).isEqualTo(1);

        // the plain query shares the key (OPTIONS are not part of it) and is served from the warmed entry
        assertThat(planQuery(cache, query, "SCHEMA_TEMPLATE_1", 10, Options.none(), metrics)).isEqualTo(i1970);
        assertThat(metrics.count(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT)).isEqualTo(1);
        assertThat(metrics.count(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_MISS)).isZero();
        assertThat(numTertiaryEntries(cache)).isEqualTo(1);
    }

    @Test
    void plannerConfigurationOptionsSegregateSecondaryKeys() throws Exception {
        // disabled planner rules and the index fetch method are part of the planner configuration and must not share plans
        final var cache = getCache(new FakeTicker());
        final var metrics = new CountingMetricCollector();
        final String query = "SELECT * FROM BOOKS WHERE YEAR > 1970 AND YEAR < 1979";
        final var disabledRule = Options.builder().withOption(Options.Name.DISABLED_PLANNER_RULES,
                ImmutableSet.of(ImplementInsertRule.class.getSimpleName())).build();
        final var remoteFetch = Options.builder().withOption(Options.Name.INDEX_FETCH_METHOD,
                Options.IndexFetchMethod.USE_REMOTE_FETCH).build();

        planQuery(cache, query, "SCHEMA_TEMPLATE_1", 10, Options.none(), metrics);
        planQuery(cache, query, "SCHEMA_TEMPLATE_1", 10, disabledRule, metrics);
        assertThat(numSecondaryEntries(cache)).isEqualTo(2);
        planQuery(cache, query, "SCHEMA_TEMPLATE_1", 10, remoteFetch, metrics);
        // secondary size is two, so one key may have been evicted, but no lookup may have been served from another key
        assertThat(metrics.count(RelationalMetric.RelationalCount.PLAN_CACHE_SECONDARY_MISS)).isEqualTo(3);
        assertThat(metrics.count(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT)).isZero();
        final var configurations = cache.getStats().getAllSecondaryKeys("SCHEMA_TEMPLATE_1").stream()
                .map(QueryCacheKey::getPlannerConfiguration)
                .collect(Collectors.toSet());
        assertThat(configurations).contains(PlannerConfiguration.of(Optional.of(Set.of(i1970, i1980)), remoteFetch));
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
