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
import com.apple.foundationdb.relational.recordlayer.FdbConnection;
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
import java.util.concurrent.TimeUnit;

/**
 * Pre-warms the shared {@link RelationalPlanCache} at engine startup by planning every
 * stored query declared on every schema template in the catalog.
 *
 * <p>Two-phase to respect FDB's transaction time bound:
 * <ol>
 *   <li>{@link #getSchemaTemplates()} — read-only catalog transaction. It iterates all
 *       schema templates in the catalog and stores only with
 *       non-empty {@code storedQueries}</li>
 *   <li>{@link #planStoredQueriesForSchemaTemplate} — Iterates stored schema templates,
 *       for each of them builds an offline {@link PlanGenerator} and delegates to
 *       {@link PlanGenerator#planStoredQueries()} which idempotently populates the cache.</li>
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
    private final MetricCollector metricCollector;

    public OfflineStoredQueriesProcessor(@Nonnull final RelationalPlanCache cache,
                                         @Nonnull final StoreCatalog storeCatalog,
                                         @Nonnull final FdbConnection fdbConnection,
                                         @Nonnull final MetricRegistry metricRegistry) {
        this.cache = cache;
        this.storeCatalog = storeCatalog;
        this.fdbConnection = fdbConnection;
        this.metricCollector = StoreTimerMetricCollector.fromMetricRegistry(metricRegistry);
    }

    public void run() {
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
        int templatesFailed = 0;
        for (final RecordLayerSchemaTemplate template : templates) {
            try {
                planStoredQueriesForSchemaTemplate(template);
                queriesPlanned += template.getStoredQueries().size();
            } catch (RelationalException e) {
                templatesFailed++;
                if (logger.isErrorEnabled()) {
                    logger.error(KeyValueLogMessage.of("OfflineStoredQueriesProcessor failed to process schema template",
                            "schemaTemplate", template.getName() + ":" + template.getVersion()), e);
                }
            }
        }
        if (logger.isInfoEnabled()) {
            logger.info(KeyValueLogMessage.of("OfflineStoredQueriesProcessor finished",
                    "templates", templates.size(),
                    "templatesFailed", templatesFailed,
                    "queriesPlanned", queriesPlanned,
                    "durationMicros", TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startNanos)));
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

    private void planStoredQueriesForSchemaTemplate(@Nonnull final RecordLayerSchemaTemplate template) throws RelationalException {
        final var generator = PlanGenerator.create(
                Optional.of(cache),
                template,
                new RecordStoreState(null, null),
                metricCollector,
                Options.NONE);                      // assume this is standard set of options
        generator.planStoredQueries();
    }
}
