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
import com.apple.foundationdb.relational.recordlayer.FdbConnection;
import com.apple.foundationdb.relational.recordlayer.catalog.systables.SystemTable;
import com.apple.foundationdb.relational.recordlayer.ddl.AbstractMetadataOperationsFactory;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerInvokedRoutine;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.query.cache.OfflineMetricCollector;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;
import com.codahale.metrics.MetricRegistry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Pre-warms the shared plan cache at engine startup.
 *
 * <p>Plans every stored query declared on every schema template in the catalog and inserts the
 * results into the shared {@link RelationalPlanCache}. Two-phase to respect FDB's transaction
 * time bound:
 * <ol>
 *   <li>{@link #getSchemaTemplates()} &mdash; read-only catalog transaction. Iterates all schema
 *       templates and keeps only those with non-empty {@code storedQueries}.</li>
 *   <li>{@link #planStoredQueriesForSchemaTemplate} &mdash; for each retained template, installs
 *       its declared temporary functions via offline DDL planning, then plans every stored
 *       SELECT into the cache.</li>
 * </ol>
 *
 * <p>Per-template failures are swallowed so a single bad template cannot abort startup.</p>
 */
@API(API.Status.EXPERIMENTAL)
public final class OfflineStoredQueriesProcessor {
    private static final Logger logger = LogManager.getLogger(OfflineStoredQueriesProcessor.class);

    @Nonnull
    private final RelationalPlanCache cache;

    @Nonnull
    private final StoreCatalog storeCatalog;

    @Nonnull
    private final FdbConnection fdbConnection;

    @Nonnull
    private final OfflineMetricCollector metricCollector;

    public OfflineStoredQueriesProcessor(@Nonnull final RelationalPlanCache cache,
                                         @Nonnull final StoreCatalog storeCatalog,
                                         @Nonnull final FdbConnection fdbConnection,
                                         @Nonnull final MetricRegistry metricRegistry) {
        this.cache = cache;
        this.storeCatalog = storeCatalog;
        this.fdbConnection = fdbConnection;
        this.metricCollector = new OfflineMetricCollector(metricRegistry);
    }

    public void run() {
        try {
            final long startNanos = System.nanoTime();
            final List<RecordLayerSchemaTemplate> templates;
            try {
                templates = getSchemaTemplates();
            } catch (RelationalException e) {
                if (logger.isErrorEnabled()) {
                    logger.error(KeyValueLogMessage.of("OfflineStoredQueriesProcessor failed to read catalog"), e);
                }
                return;
            }

            int queriesPlanned = 0;
            int queriesFailed = 0;
            int templatesProcessed  = 0;
            int templatesFailed = 0;
            for (final RecordLayerSchemaTemplate template : templates) {
                try {
                    final var res = planStoredQueriesForSchemaTemplate(template);
                    queriesPlanned += res.planned;
                    queriesFailed += res.failed;
                    templatesProcessed++;
                } catch (RuntimeException e) {
                    templatesFailed++;
                    if (logger.isErrorEnabled()) {
                        logger.error(KeyValueLogMessage.of("OfflineStoredQueriesProcessor failed to process schema template",
                                "schemaTemplate", template.getName() + ":" + template.getVersion()), e);
                    }
                }
            }
            if (logger.isInfoEnabled()) {
                logger.info(KeyValueLogMessage.of("OfflineStoredQueriesProcessor finished",
                        "templatesProcessed", templatesProcessed,
                        "templatesFailed", templatesFailed,
                        "queriesPlanned", queriesPlanned,
                        "queriesFailed", queriesFailed,
                        "durationMicros", TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startNanos)));
            }
        } catch (RuntimeException e) {
            if (logger.isErrorEnabled()) {
                logger.error(KeyValueLogMessage.of("OfflineStoredQueriesProcessor aborted unexpectedly"), e);
            }
        }
    }

    @Nonnull
    private List<RecordLayerSchemaTemplate> getSchemaTemplates() throws RelationalException {
        final List<RecordLayerSchemaTemplate> result = new ArrayList<>();
        try (Transaction txn = fdbConnection.getTransactionManager().createTransaction(Options.NONE)) {
            try (var rs = storeCatalog.getSchemaTemplateCatalog().listTemplates(txn)) {
                while (rs.next()) {
                    final var template = storeCatalog.getSchemaTemplateCatalog()
                            .loadSchemaTemplate(txn, rs.getString(SystemTable.TEMPLATE_NAME))
                            .unwrap(RecordLayerSchemaTemplate.class);
                    if (!template.getStoredQueries().isEmpty()) {
                        result.add(template);
                    }
                }
            } catch (java.sql.SQLException e) {
                throw new RelationalException(e);
            }
            txn.commit();
        }
        return result;
    }

    @Nonnull
    private Counts planStoredQueriesForSchemaTemplate(@Nonnull final RecordLayerSchemaTemplate template) {
        final Counts counts = new Counts();
        final String templateKey = template.getName() + ":" + template.getVersion();
        if (cache.isPrepared(templateKey)) {
            return counts;
        }

        for (final var storedQuery : template.getStoredQueries().entrySet()) {
            if (planStoredQuery(template, storedQuery.getKey(), storedQuery.getValue())) {
                counts.planned++;
            } else {
                counts.failed++;
            }
        }

        cache.markPrepared(templateKey);
        return counts;
    }

    private boolean planStoredQuery(@Nonnull final RecordLayerSchemaTemplate template,
                                 @Nonnull final String storedQueryName,
                                 @Nonnull final StoredQuery storedQuery) {
        final String templateKey = template.getName() + ":" + template.getVersion();
        final var tempFuncFactory = new MetadataTempFuncFactory();
        RecordLayerSchemaTemplate currentTemplate = template;

        for (final var tempFunc : storedQuery.getTempFunctions()) {
            try {
                final var message = KeyValueLogMessage.build("PlanStoredQueries");
                message.addKeyAndValue("schemaTemplate", templateKey);
                message.addKeyAndValue("storedQueryName", storedQueryName);
                message.addKeyAndValue("tempFunction", tempFunc);
                PlanGenerator.create(currentTemplate, tempFuncFactory, metricCollector, Options.NONE)
                        .getPlan(tempFunc);
                currentTemplate = tempFuncFactory.updateTemplate(currentTemplate);
            } catch (RelationalException e) {
                // do nothing here, error is already logged inside getPlan's finally
                return false;
            }
        }
        try {
            final var sql = storedQuery.getStoredQuery();
            final var message = KeyValueLogMessage.build("PlanStoredQueries");
            message.addKeyAndValue("schemaTemplate", templateKey);
            message.addKeyAndValue("storedQueryName", storedQueryName);
            message.addKeyAndValue("storedQuerySql", sql);
            PlanGenerator.create(
                            Optional.of(cache),
                            currentTemplate,
                            new RecordStoreState(null, null),
                            metricCollector,
                            Options.NONE)
                    .getPlan(sql, message);
        } catch (RelationalException e) {
            // do nothing here, error is already logged inside getPlan's finally
            return false;
        }
        return true;
    }

    private static final class Counts {
        int planned;
        int failed;
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

        /**
         * Returns a copy of {@code template} with the most-recently-captured routine,
         * and clears the captured slot.
         *
         * @param template the schema template to apply the captured routine to
         * @return the updated template, or the same instance if nothing was captured
         */
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
