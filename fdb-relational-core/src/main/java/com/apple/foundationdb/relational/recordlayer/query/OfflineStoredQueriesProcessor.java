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
import com.apple.foundationdb.relational.api.ddl.ConstantAction;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.api.metadata.StoredQuery;
import com.apple.foundationdb.relational.api.metrics.MetricCollector;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.recordlayer.catalog.systables.SystemTable;
import com.apple.foundationdb.relational.recordlayer.ddl.AbstractMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerInvokedRoutine;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metric.StoreTimerMetricCollector;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;
import com.codahale.metrics.MetricRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Pre-warms the shared {@link RelationalPlanCache} at engine startup by planning every stored
 * query declared on every schema template in the catalog.
 *
 * <p>Used as two independent static calls so the caller owns the FDB transaction lifecycle (kept
 * tight to respect FDB's transaction time bound):</p>
 *
 * <ol>
 *   <li>{@link #getSchemaTemplates(StoreCatalog, Transaction)} &mdash; runs inside a caller-supplied
 *       transaction. Reads templates from the catalog and returns those with non-empty
 *       {@code storedQueries}. Catalog-read failures are logged at {@code ERROR} level and an
 *       empty list is returned, so the caller never sees a checked exception from this method.</li>
 *   <li>{@link #planStoredQueriesForSchemaTemplates(RelationalPlanCache, MetricRegistry, List)} &mdash;
 *       offline (no FDB transaction). For each template, plans every stored query and inserts the
 *       resulting plans into the cache. Each {@link StoredQuery}'s {@code CREATE [OR REPLACE]?
 *       TEMPORARY FUNCTION ...} declarations are compiled first via {@link MetadataTempFuncFactory}
 *       so the running schema template carries those routines when the SELECT body is planned.
 *       Each per-template and per-query failure is logged at {@code ERROR} level, and is also
 *       reflected in the {@code OFFLINE_STORED_QUERIES_*_FAILED} counters and the summary
 *       {@code INFO} log line.</li>
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
     * ({@link Options.Name#CASE_SENSITIVE_IDENTIFIERS}) and every other planner-tunable.</p>
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
        } catch (RelationalException | RuntimeException e) {
            if (logger.isErrorEnabled()) {
                logger.error(KeyValueLogMessage.of("OfflineStoredQueriesProcessor failed unexpectedly"), e);
            }
            return;
        }
        metricCollector.increment(RelationalMetric.RelationalCount.OFFLINE_STORED_QUERIES_TEMPLATES_PROCESSED, counts.templatesProcessed);
        metricCollector.increment(RelationalMetric.RelationalCount.OFFLINE_STORED_QUERIES_TEMPLATES_FAILED, counts.templatesFailed);
        metricCollector.increment(RelationalMetric.RelationalCount.OFFLINE_STORED_QUERIES_QUERIES_PROCESSED, counts.queriesProcessed);
        metricCollector.increment(RelationalMetric.RelationalCount.OFFLINE_STORED_QUERIES_QUERIES_FAILED, counts.queriesFailed);
        metricCollector.increment(RelationalMetric.RelationalCount.OFFLINE_STORED_QUERIES_TEMP_FUNCTIONS_PROCESSED, counts.tempFunctionsProcessed);
        metricCollector.increment(RelationalMetric.RelationalCount.OFFLINE_STORED_QUERIES_TEMP_FUNCTIONS_FAILED, counts.tempFunctionsFailed);
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
                    "tempFunctionsProcessed", counts.tempFunctionsProcessed,
                    "tempFunctionsFailed", counts.tempFunctionsFailed,
                    "durationMicros", durationMicros));
        }
    }

    @Nonnull
    private static Counts planStoredQueriesForSchemaTemplatesAll(@Nonnull final RelationalPlanCache cache,
                                                                 @Nonnull final MetricCollector metricCollector,
                                                                 @Nonnull final List<RecordLayerSchemaTemplate> templates) {
        final Counts counts = new Counts();
        for (final RecordLayerSchemaTemplate template : templates) {
            planStoredQueriesForSchemaTemplate(cache, metricCollector, template, counts);
            counts.templatesProcessed++;
        }
        return counts;
    }

    /**
     * Plans every stored query in {@code template}. For each {@link StoredQuery} the temp-function
     * declarations are compiled first (via {@link MetadataTempFuncFactory}) so the running
     * template carries them when the SELECT body is planned. Per-query and per-temp-function
     * outcomes are reported by {@link #planStoredQuery} directly into {@code counts}.
     */
    private static void planStoredQueriesForSchemaTemplate(@Nonnull final RelationalPlanCache cache,
                                                           @Nonnull final MetricCollector metricCollector,
                                                           @Nonnull final RecordLayerSchemaTemplate template,
                                                           @Nonnull final Counts counts) {
        final String templateKey = template.getName() + ":" + template.getVersion();
        for (final var entry : template.getStoredQueries().entrySet()) {
            planStoredQuery(cache, metricCollector, template, templateKey, entry.getKey(), entry.getValue(), counts);
        }
    }

    /**
     * Plans one stored query: first each {@code CREATE [OR REPLACE]? TEMPORARY FUNCTION ...}
     * declaration (folding the captured routine back into the template), then the SELECT body
     * with the cache wired up. Updates {@code counts} as follows:
     * <ul>
     *   <li>Each temp function that plans successfully bumps {@code tempFunctionsProcessed}.</li>
     *   <li>A temp function that fails to plan bumps {@code tempFunctionsFailed} and
     *       {@code queriesFailed}; the rest of this stored query is skipped.</li>
     *   <li>The SELECT body planning bumps {@code queriesProcessed} on success or
     *       {@code queriesFailed} on failure.</li>
     * </ul>
     * Planning errors are caught and logged inside {@code getPlan}'s {@code finally} block; this
     * method never propagates them &mdash; both {@link RelationalException} and unchecked
     * {@link RuntimeException} (e.g. {@code UncheckedRelationalException}) from {@code getPlan}
     * are swallowed so one bad query cannot abort the enclosing template's iteration.
     */
    private static void planStoredQuery(@Nonnull final RelationalPlanCache cache,
                                        @Nonnull final MetricCollector metricCollector,
                                        @Nonnull final RecordLayerSchemaTemplate template,
                                        @Nonnull final String templateKey,
                                        @Nonnull final String storedQueryName,
                                        @Nonnull final StoredQuery storedQuery,
                                        @Nonnull final Counts counts) {
        final var tempFuncFactory = new MetadataTempFuncFactory();
        RecordLayerSchemaTemplate currentTemplate = template;

        for (final var tempFunc : storedQuery.getTempFunctions()) {
            try {
                PlanGenerator.create(currentTemplate, tempFuncFactory, metricCollector, Options.NONE)
                        .getPlan(tempFunc, Map.of(
                                "schemaTemplate", templateKey,
                                "storedQueryName", storedQueryName,
                                "tempFunction", tempFunc));
                currentTemplate = tempFuncFactory.updateTemplate(currentTemplate);
                counts.tempFunctionsProcessed++;
            } catch (RelationalException | RuntimeException e) {
                // error already logged inside getPlan's finally
                counts.tempFunctionsFailed++;
                counts.queriesFailed++;
                return;
            }
        }
        try {
            final var sql = storedQuery.getQuery();
            PlanGenerator.create(
                            Optional.of(cache),
                            currentTemplate,
                            new RecordStoreState(null, null),
                            metricCollector,
                            Options.NONE)
                    .getPlan(sql, Map.of(
                            "schemaTemplate", templateKey,
                            "storedQueryName", storedQueryName,
                            "storedQuerySql", sql));
            counts.queriesProcessed++;
        } catch (RelationalException | RuntimeException e) {
            // error already logged inside getPlan's finally
            counts.queriesFailed++;
        }
    }

    private static final class Counts {
        int templatesProcessed;
        int templatesFailed;
        int queriesProcessed;
        int queriesFailed;
        int tempFunctionsProcessed;
        int tempFunctionsFailed;
    }

    /**
     * Offline-path factory for {@code CREATE TEMPORARY FUNCTION ...} compilation.
     *
     * <p>Each compile of a temp-function clause captures the resulting routine into the
     * {@code invokedRoutine} field. The caller folds it back into the running schema template
     * via {@link #updateTemplate(RecordLayerSchemaTemplate)} after planning, so the next
     * iteration (and the final SELECT plan) sees the new routine.
     *
     * <p>{@link com.apple.foundationdb.relational.recordlayer.query.visitors.DdlVisitor} invokes
     * {@link #getCreateTemporaryFunctionConstantAction} during plan generation. The returned
     * {@link ConstantAction} is intentionally a no-op &mdash; there is no transaction to mutate
     * in the offline path; the side-effect is applied via {@link #updateTemplate}.
     */
    private static final class MetadataTempFuncFactory extends AbstractMetadataOperationsFactory {
        @Nullable
        private RecordLayerInvokedRoutine invokedRoutine;

        @Nonnull
        @Override
        public ConstantAction getCreateTemporaryFunctionConstantAction(@Nonnull final SchemaTemplate templateArg,
                                                                       final boolean throwIfExists,
                                                                       @Nonnull final RecordLayerInvokedRoutine invokedRoutine) {
            this.invokedRoutine = invokedRoutine;
            return txn -> {
                // no-op: offline path applies its side-effect via updateTemplate()
            };
        }

        @Nonnull
        RecordLayerSchemaTemplate updateTemplate(@Nonnull final RecordLayerSchemaTemplate template) {
            final var routine = this.invokedRoutine;
            this.invokedRoutine = null;
            if (routine == null) {
                return template;
            }
            return template.toBuilder().replaceInvokedRoutine(routine).build();
        }
    }
}
