/*
 * OfflineStoredQueriesProcessor.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metrics.MetricCollector;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.recordlayer.catalog.systables.SystemTable;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metric.StoreTimerMetricCollector;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;
import com.codahale.metrics.MetricRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Pre-warms the shared {@link RelationalPlanCache} at engine startup by planning every stored
 * query declared on every schema template in the catalog.
 *
 * <p>Used as two independent static calls so the caller owns the FDB transaction lifecycle (kept
 * tight to respect FDB's transaction time bound):</p>
 *
 * <ol>
 *   <li>{@link #getSchemaTemplates(StoreCatalog, Transaction)} — runs inside a caller-supplied
 *       transaction. Reads templates from the catalog and returns those with non-empty
 *       {@code storedQueries}. Catalog-read failures are logged at {@code ERROR} level and an
 *       empty list is returned, so the caller never sees a checked exception from this method.</li>
 *   <li>{@link #planStoredQueriesForSchemaTemplates(RelationalPlanCache, MetricRegistry, List)} —
 *       offline (no FDB transaction). For each template, plans every stored query and inserts the
 *       resulting plans into the cache. Each per-template and per-query failure is logged at
 *       {@code ERROR} level, and is also reflected in the {@code OFFLINE_STORED_QUERIES_*_FAILED}
 *       counters and the summary {@code INFO} log line.</li>
 * </ol>
 */
@API(API.Status.EXPERIMENTAL)
public final class OfflineStoredQueriesProcessor {
    private static final Logger logger = LogManager.getLogger(OfflineStoredQueriesProcessor.class);

    private OfflineStoredQueriesProcessor() {
    }

    /**
     * Reads schema templates from the catalog inside the caller-supplied transaction. The
     * caller is responsible for the transaction's full lifecycle (open and commit/rollback);
     * this method only reads. Any exception from catalog access is caught and logged at error
     * level, and an empty list is returned in that case.
     */
    @Nonnull
    public static List<RecordLayerSchemaTemplate> getSchemaTemplates(@Nonnull final StoreCatalog storeCatalog,
                                                                     @Nonnull final Transaction txn) {
        final List<RecordLayerSchemaTemplate> result = new ArrayList<>();
        try (var rs = storeCatalog.getSchemaTemplateCatalog().listTemplates(txn)) {
            while (rs.next()) {
                final var template = storeCatalog.getSchemaTemplateCatalog()
                        .loadSchemaTemplate(txn, rs.getString(SystemTable.TEMPLATE_NAME))
                        .unwrap(RecordLayerSchemaTemplate.class);
                if (!template.getStoredQueries().isEmpty()) {
                    result.add(template);
                }
            }
        } catch (java.sql.SQLException | RelationalException | RuntimeException e) {
            if (logger.isErrorEnabled()) {
                logger.error(KeyValueLogMessage.of("OfflineStoredQueriesProcessor failed to read catalog"), e);
            }
            return List.of();
        }
        return result;
    }

    /**
     * Plans the stored queries for each given schema template and inserts the resulting plans
     * into {@code cache}. This is an offline operation and does not require an FDB transaction.
     *
     * <p>Stored queries are planned with {@link Options#NONE}, i.e. the planner's default
     * options &mdash; this includes the default case-sensitivity setting
     * ({@link Options.Name#CASE_SENSITIVE_IDENTIFIERS}) and every other planner-tunable. The
     * planned-and-cached plans therefore reflect the engine defaults, not any per-connection or
     * per-session overrides; adopters whose runtime queries depend on non-default options will
     * not see this warm-up cache hit. Allowing callers to inject their own {@link Options} is a
     * deliberate non-goal today but a likely future extension &mdash; if/when we do, this method
     * will take an {@code Options} parameter and the planning loop will thread it through.</p>
     *
     * <p>Failures are never propagated &mdash; a bad template or query must not abort startup.
     * Each failure is logged at {@code ERROR} level, and is also surfaced as a metric:
     * per-template failures bump
     * {@link RelationalMetric.RelationalCount#OFFLINE_STORED_QUERIES_TEMPLATES_FAILED}
     * and per-query failures bump
     * {@link RelationalMetric.RelationalCount#OFFLINE_STORED_QUERIES_QUERIES_FAILED}.</p>
     */
    public static void planStoredQueriesForSchemaTemplates(@Nonnull final RelationalPlanCache cache,
                                                           @Nonnull final MetricRegistry metricRegistry,
                                                           @Nonnull final List<RecordLayerSchemaTemplate> templates) {
        final MetricCollector metricCollector = StoreTimerMetricCollector.fromMetricRegistry(metricRegistry);
        final Counts counts;
        try {
            counts = metricCollector.clock(RelationalMetric.RelationalEvent.OFFLINE_STORED_QUERIES_WARM_UP,
                    () -> planStoredQueriesForSchemaTemplatesAll(cache, metricCollector, templates));
        } catch (RelationalException e) {
            if (logger.isErrorEnabled()) {
                logger.error(KeyValueLogMessage.of("OfflineStoredQueriesProcessor failed unexpectedly"), e);
            }
            return;
        }
        metricCollector.increment(RelationalMetric.RelationalCount.OFFLINE_STORED_QUERIES_TEMPLATES_PROCESSED, counts.templatesProcessed);
        metricCollector.increment(RelationalMetric.RelationalCount.OFFLINE_STORED_QUERIES_TEMPLATES_FAILED, counts.templatesFailed);
        metricCollector.increment(RelationalMetric.RelationalCount.OFFLINE_STORED_QUERIES_QUERIES_PROCESSED, counts.queriesProcessed);
        metricCollector.increment(RelationalMetric.RelationalCount.OFFLINE_STORED_QUERIES_QUERIES_FAILED, counts.queriesFailed);
        if (logger.isInfoEnabled()) {
            long durationMicros = -1L;
            try {
                durationMicros = (long) metricCollector.getAverageTimeMicrosForEvent(
                        RelationalMetric.RelationalEvent.OFFLINE_STORED_QUERIES_WARM_UP);
            } catch (RelationalException e) {
                assert e != null;
            }
            logger.info(KeyValueLogMessage.of("OfflineStoredQueriesProcessor finished",
                    "templatesProcessed", counts.templatesProcessed,
                    "templatesFailed", counts.templatesFailed,
                    "storedQueriesProcessed", counts.queriesProcessed,
                    "storedQueriesFailed", counts.queriesFailed,
                    "durationMicros", durationMicros));
        }
    }

    @Nonnull
    private static Counts planStoredQueriesForSchemaTemplatesAll(@Nonnull final RelationalPlanCache cache,
                                                                 @Nonnull final MetricCollector metricCollector,
                                                                 @Nonnull final List<RecordLayerSchemaTemplate> templates) {
        final Counts counts = new Counts();
        for (final RecordLayerSchemaTemplate template : templates) {
            try {
                final int queriesProcessed = planStoredQueriesForSchemaTemplate(cache, metricCollector, template);
                counts.templatesProcessed++;
                counts.queriesProcessed += queriesProcessed;
                counts.queriesFailed += template.getStoredQueries().size() - queriesProcessed;
            } catch (RelationalException | RuntimeException e) {
                counts.templatesFailed++;
                if (logger.isErrorEnabled()) {
                    logger.error(KeyValueLogMessage.of("OfflineStoredQueriesProcessor failed to process schema template",
                            "schemaTemplate", template.getName() + ":" + template.getVersion()), e);
                }
            }
        }
        return counts;
    }

    private static int planStoredQueriesForSchemaTemplate(@Nonnull final RelationalPlanCache cache,
                                                          @Nonnull final MetricCollector metricCollector,
                                                          @Nonnull final RecordLayerSchemaTemplate template) throws RelationalException {
        final var generator = PlanGenerator.create(
                Optional.of(cache),
                template,
                new RecordStoreState(null, null),
                metricCollector,
                Options.NONE);
        return generator.planStoredQueries();
    }

    private static final class Counts {
        int templatesProcessed;
        int templatesFailed;
        int queriesProcessed;
        int queriesFailed;
    }
}
