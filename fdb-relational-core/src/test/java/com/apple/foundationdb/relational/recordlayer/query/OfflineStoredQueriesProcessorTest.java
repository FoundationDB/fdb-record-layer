/*
 * OfflineStoredQueriesProcessorTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metrics.MetricCollector;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.metric.StoreTimerMetricCollector;
import com.apple.foundationdb.relational.recordlayer.query.cache.NoOpMetricCollector;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;
import com.apple.foundationdb.relational.util.Supplier;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests warm-up with record metadata and options passed by the caller. Needs no database.
 *
 * <p>The record metadata has an index that the template's table lacks. It stands for indexes a template cannot
 * describe, such as indexes on several record types.</p>
 */
class OfflineStoredQueriesProcessorTest {

    private static final int VERSION = 7;

    private static final String QUERY = "SELECT * FROM BOOKS WHERE YEAR > 1980";

    @Test
    void planStoredQueriesWithIndexOnlyInRecordMetaDataPlansWithThatIndex() throws Exception {
        final var template = booksTemplate(false, VERSION, QUERY, List.of());
        final var cache = RelationalPlanCache.buildWithDefaults();

        OfflineStoredQueriesProcessor.planStoredQueries(cache, template, booksMetaDataWithYearIndex(),
                NoOpMetricCollector.INSTANCE, Options.NONE);

        assertThat(cachedRecordQueryPlans(cache, template)).singleElement().asString().startsWith("ISCAN(YEAR_IDX");
    }

    /**
     * The function is compiled into a copy of the template, and its body must still see the index.
     */
    @Test
    void planStoredQueriesWithTemporaryFunctionPlansWithIndexOnlyInRecordMetaData() throws Exception {
        final var template = booksTemplate(false, VERSION, "SELECT * FROM RECENT()",
                List.of("CREATE TEMPORARY FUNCTION RECENT() ON COMMIT DROP FUNCTION AS " + QUERY));
        final var cache = RelationalPlanCache.buildWithDefaults();

        OfflineStoredQueriesProcessor.planStoredQueries(cache, template, booksMetaDataWithYearIndex(),
                NoOpMetricCollector.INSTANCE, Options.NONE);

        assertThat(cachedRecordQueryPlans(cache, template)).singleElement().asString().contains("ISCAN(YEAR_IDX");
    }

    /**
     * The options are part of the cache key.
     */
    @Test
    void planStoredQueriesWithOptionsIsFoundOnlyWithTheSameOptions() throws Exception {
        final var template = booksTemplate(false, VERSION, QUERY, List.of());
        final var metaData = booksMetaDataWithYearIndex();
        final var options = Options.builder().withOption(Options.Name.DISABLE_PLANNER_REWRITING, true).build();
        final var cache = RelationalPlanCache.buildWithDefaults();

        OfflineStoredQueriesProcessor.planStoredQueries(cache, template, metaData, NoOpMetricCollector.INSTANCE, options);

        final var metricCollector = new CountingMetricCollector();
        planWithCache(cache, template, metaData, metricCollector, options);
        assertThat(metricCollector.countOf(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT)).isEqualTo(1);

        planWithCache(cache, template, metaData, metricCollector, Options.NONE);
        assertThat(metricCollector.countOf(RelationalMetric.RelationalCount.PLAN_CACHE_TERTIARY_HIT)).isEqualTo(1);
        assertThat(metricCollector.countOf(RelationalMetric.RelationalCount.PLAN_CACHE_SECONDARY_MISS)).isEqualTo(1);
    }

    @Test
    void planStoredQueriesWithStoreTimerCollectorCountsOnThatTimer() throws Exception {
        final var template = booksTemplate(false, VERSION, QUERY, List.of());
        final var timer = new StoreTimer();

        OfflineStoredQueriesProcessor.planStoredQueries(RelationalPlanCache.buildWithDefaults(), template,
                booksMetaDataWithYearIndex(), StoreTimerMetricCollector.fromStoreTimer(timer), Options.NONE);

        assertThat(timer.getCount(RelationalMetric.RelationalCount.OFFLINE_STORED_QUERIES_TEMPLATES_PROCESSED)).isEqualTo(1);
        assertThat(timer.getCount(RelationalMetric.RelationalCount.OFFLINE_STORED_QUERIES_QUERIES_PROCESSED)).isEqualTo(1);
        assertThat(timer.getCount(RelationalMetric.RelationalCount.OFFLINE_STORED_QUERIES_PLANS_WARMED)).isEqualTo(1);
        assertThat(timer.getCount(RelationalMetric.RelationalEvent.OFFLINE_STORED_QUERIES_WARM_UP)).isEqualTo(1);
    }

    private static void planWithCache(@Nonnull final RelationalPlanCache cache,
                                      @Nonnull final RecordLayerSchemaTemplate template,
                                      @Nonnull final RecordMetaData metaData,
                                      @Nonnull final MetricCollector metricCollector,
                                      @Nonnull final Options options) throws Exception {
        PlanGenerator.create(Optional.of(cache), template, metaData, new RecordStoreState(null, null),
                        metricCollector, options, PreparedParams.empty())
                .getPlan(QUERY);
    }

    @Nonnull
    private static List<String> cachedRecordQueryPlans(@Nonnull final RelationalPlanCache cache,
                                                       @Nonnull final RecordLayerSchemaTemplate template) {
        final var plans = new ArrayList<String>();
        for (final var secondaryKey : cache.getStats().getAllSecondaryKeys(template.getName())) {
            for (final var plan : cache.getStats().getAllTertiaryMappings(template.getName(), secondaryKey).values()) {
                plans.add(((QueryPlan.PhysicalQueryPlan) plan).getRecordQueryPlan().toString());
            }
        }
        return plans;
    }

    /**
     * The template's record metadata with an extra index.
     */
    @Nonnull
    private static RecordMetaData booksMetaDataWithYearIndex() {
        return booksTemplate(true, VERSION, QUERY, List.of()).toRecordMetadata();
    }

    @Nonnull
    private static RecordLayerSchemaTemplate booksTemplate(final boolean withIndex, final int version,
                                                           @Nonnull final String storedQuery,
                                                           @Nonnull final List<String> tempFunctions) {
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
                .addStoredQuery("BY_YEAR", storedQuery, tempFunctions, Map.of(), List.of())
                .build();
    }

    /**
     * Counts plan cache hits and misses.
     */
    private static final class CountingMetricCollector implements MetricCollector {
        private final Map<RelationalMetric.RelationalCount, Integer> counts =
                new EnumMap<>(RelationalMetric.RelationalCount.class);

        @Override
        public void increment(@Nonnull final RelationalMetric.RelationalCount count, final int amount) {
            counts.merge(count, amount, Integer::sum);
        }

        @Override
        public <T> T clock(@Nonnull final RelationalMetric.RelationalEvent event,
                           @Nonnull final Supplier<T> supplier) throws RelationalException {
            return supplier.get();
        }

        int countOf(@Nonnull final RelationalMetric.RelationalCount count) {
            return counts.getOrDefault(count, 0);
        }
    }
}
