/*
 * YamlTestCatalogInitializer.java
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

package com.apple.foundationdb.relational.yamltests;

import com.apple.foundationdb.Range;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContextConfig;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.RecordContextTransaction;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;
import com.apple.foundationdb.relational.recordlayer.catalog.StoreCatalogProvider;
import com.apple.foundationdb.relational.recordlayer.util.ConflictKeyFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * Proactive one-shot initialiser for the SYS catalog on every cluster the yaml-tests will
 * touch.
 * <p>
 * The problem this addresses: {@link com.apple.foundationdb.relational.server.FRL#FRL} and
 * the internals under {@link com.apple.foundationdb.relational.api.EmbeddedRelationalDriver}
 * both create the record store at {@code /__SYS/CATALOG} on their first construction. When
 * yaml-tests spin up (a) an in-process embedded driver, (b) an in-process JDBC server, and
 * (c) one or more external-server subprocesses — all racing to construct that record store
 * against the same FDB cluster — the concurrent init transactions commit-conflict on the
 * catalog metadata (SQLSTATE 40001) or, worse, one process ends up throwing while another
 * is mid-init.
 * <p>
 * {@link YamlTestExtension#beforeAll} calls {@link #initializeCatalog(List)} <em>before</em>
 * any of those configs run. This method opens a plain FDB transaction, invokes the same
 * {@link StoreCatalogProvider#getCatalog} primitive the FRL uses, and commits — retrying on
 * SQLSTATE 40001 up to a small attempt limit. After it returns, every subsequent FRL
 * construction against the same cluster opens an already-initialised store and does not
 * race on the init commit.
 * <p>
 * Lives inside {@code yaml-tests} rather than pulling
 * {@code fdb-relational-core}'s testFixtures onto the yaml-tests classpath — the yaml-tests
 * package already depends on the record-layer primitives this needs directly, so a private
 * helper here is cheaper than adding another cross-module dependency edge.
 */
public final class YamlTestCatalogInitializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(YamlTestCatalogInitializer.class);

    private static final int MAX_ATTEMPTS = 10;
    private static final long RETRY_BASE_MILLIS = 20L;

    private YamlTestCatalogInitializer() {
    }

    /**
     * Initialise the SYS catalog on every given cluster, in order. Idempotent under retry —
     * if the store already exists, {@link StoreCatalogProvider#getCatalog} opens it. If the
     * commit races with a concurrent initialiser on the same cluster, retries on SQLSTATE
     * 40001 with a short back-off.
     *
     * @param clusterFiles cluster files to initialise; a {@code null} entry means "the
     *                     default cluster" per {@link FDBDatabaseFactory#getDatabase(String)}
     * @throws RelationalException if any cluster's init ultimately fails after retries
     */
    public static void initializeCatalog(@Nonnull final List<String> clusterFiles) throws RelationalException {
        // Register the FRL keyspace domain once up front. It's idempotent, but doing it here
        // ensures the keyspace tree is ready before we resolve any paths against it below.
        final RelationalKeyspaceProvider keyspaceProvider = RelationalKeyspaceProvider.instance();
        keyspaceProvider.registerDomainIfNotExists("FRL");
        final KeySpace keySpace = keyspaceProvider.getKeySpace();
        for (final String clusterFile : clusterFiles) {
            initializeOne(clusterFile, keySpace);
        }
    }

    private static void initializeOne(final String clusterFile, @Nonnull final KeySpace keySpace) throws RelationalException {
        final FDBDatabase database = FDBDatabaseFactory.instance().getDatabase(clusterFile);
        // Ask FDB to record the specific conflict ranges on any commit failure so we can log
        // exactly which key(s) collided when a retry happens.
        final FDBRecordContextConfig config = FDBRecordContextConfig.newBuilder()
                .setReportConflictingKeys(true)
                .build();
        RelationalException last = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            final FDBRecordContext ctx = database.openContext(config);
            try (Transaction txn = new RecordContextTransaction(ctx)) {
                final StoreCatalog ignored = StoreCatalogProvider.getCatalog(txn, keySpace);
                txn.commit();
                return;
            } catch (RelationalException e) {
                if (e.getErrorCode() != ErrorCode.SERIALIZATION_FAILURE) {
                    throw e;
                }
                final List<Range> ranges = ctx.getNotCommittedConflictingKeys();
                if (ranges != null) {
                    final String rendered = ConflictKeyFormatter.formatRanges(ranges);
                    LOGGER.warn("YamlTestCatalogInitializer commit conflict on {} (attempt {}/{}): count={} ranges={}",
                            clusterFile == null ? "<default>" : clusterFile, attempt, MAX_ATTEMPTS, ranges.size(), rendered);
                }
                last = e;
                // Short, linear back-off; the commit-conflict window is brief and clearing it
                // just requires the other party's commit to land.
                try {
                    Thread.sleep(RETRY_BASE_MILLIS * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    e.addSuppressed(ie);
                    throw e;
                }
            }
        }
        throw last;
    }
}
