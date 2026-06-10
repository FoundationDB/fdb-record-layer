/*
 * OfflinePrepareStatementsProcessor.java
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
import com.apple.foundationdb.relational.recordlayer.FdbConnection;
import com.apple.foundationdb.relational.recordlayer.catalog.systables.SystemTable;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.query.cache.OfflineMetricCollector;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Pre-warms the shared {@link RelationalPlanCache} at engine startup by planning every
 * prepare statement declared on every schema template in the catalog.
 *
 * <p>Two-phase to respect FDB's transaction time bound:
 * <ol>
 *   <li>{@link #getSchemaTemplates()} — read-only catalog transaction. It iterates all
 *       schema templates in the catalog and stores only with
 *       non-empty {@code prepareStatements}</li>
 *   <li>{@link #prepareStatementsForSchemaTemplate} — Iterates stored schema templates,
 *       for each of them builds an offline {@link PlanGenerator} and delegates to
 *       {@link PlanGenerator#prepareStatements()} which idempotently populates the cache.</li>
 * </ol>
 *
 * <p>Per-template failures are swallowed so a single bad template cannot abort startup.</p>
 */
@API(API.Status.EXPERIMENTAL)
public final class OfflinePrepareStatementsProcessor {
    private static final Logger logger = LogManager.getLogger(OfflinePrepareStatementsProcessor.class);

    @Nonnull
    private final RelationalPlanCache cache;

    @Nonnull
    private final StoreCatalog storeCatalog;

    @Nonnull
    private final FdbConnection fdbConnection;

    public OfflinePrepareStatementsProcessor(@Nonnull final RelationalPlanCache cache,
                                             @Nonnull final StoreCatalog storeCatalog,
                                             @Nonnull final FdbConnection fdbConnection) {
        this.cache = cache;
        this.storeCatalog = storeCatalog;
        this.fdbConnection = fdbConnection;
    }

    public void run() {
        final List<RecordLayerSchemaTemplate> templates;
        try {
            templates = getSchemaTemplates();
        } catch (RelationalException e) {
            logger.error(KeyValueLogMessage.of("OfflinePrepareStatementsProcessor failed to read catalog"), e);
            return;
        }
        for (final RecordLayerSchemaTemplate template : templates) {
            try {
                prepareStatementsForSchemaTemplate(template);
            } catch (RelationalException e) {
                logger.error(KeyValueLogMessage.of("OfflinePrepareStatementsProcessor failed to process schema template",
                        "schemaTemplate", template.getName() + ":" + template.getVersion()), e);
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
                    if (!template.getPrepareStatements().isEmpty()) {
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

    private void prepareStatementsForSchemaTemplate(@Nonnull final RecordLayerSchemaTemplate template) throws RelationalException {
        final var generator = PlanGenerator.create(
                Optional.of(cache),
                template,
                new RecordStoreState(null, null),
                OfflineMetricCollector.INSTANCE,
                Options.NONE);                      // assume this is standard set of options
        generator.prepareStatements();
    }
}
