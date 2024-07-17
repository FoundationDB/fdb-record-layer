/*
 * FDBRecordStoreConcurrentTestBase.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.Range;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.debug.DebuggerWithSymbolTables;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.test.TestMdcExtension;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.UUID;

/**
 * Base class for tests of {@link FDBRecordStore}.
 * This has better support for concurrently interacting with a single store in the database than {@link FDBRecordStoreTestBase}.
 */
@Tag(Tags.RequiresFDB)
public class FDBRecordStoreConcurrentTestBase {
    @RegisterExtension
    protected final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    protected final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);

    private static final Logger LOGGER = LoggerFactory.getLogger(FDBRecordStoreConcurrentTestBase.class);

    protected boolean useCascadesPlanner = false;
    protected FDBDatabase fdb;
    protected FDBStoreTimer timer = new FDBStoreTimer();

    public FDBRecordStoreConcurrentTestBase() {
    }

    public boolean isUseCascadesPlanner() {
        return useCascadesPlanner;
    }

    public FDBRecordStoreConcurrentTestBase setUseCascadesPlanner(final boolean useCascadesPlanner) {
        this.useCascadesPlanner = useCascadesPlanner;
        return this;
    }

    @BeforeEach
    void initDatabaseAndPath() {
        fdb = dbExtension.getDatabase();
    }

    protected Pair<FDBRecordStore, QueryPlanner> createOrOpenRecordStore(@Nonnull FDBRecordContext context,
                                                                         @Nonnull RecordMetaData metaData,
                                                                         @Nonnull final KeySpacePath path) {
        FDBRecordStore store = getStoreBuilder(context, metaData, path).createOrOpen();
        return Pair.of(store, setupPlanner(store, null));
    }

    public QueryPlanner setupPlanner(@Nonnull FDBRecordStore recordStore, @Nullable PlannableIndexTypes indexTypes) {
        final QueryPlanner planner;
        if (useCascadesPlanner) {
            planner = new CascadesPlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState());
            if (Debugger.getDebugger() == null) {
                Debugger.setDebugger(DebuggerWithSymbolTables.withSanityChecks());
            }
            Debugger.setup();
        } else {
            if (indexTypes == null) {
                indexTypes = PlannableIndexTypes.DEFAULT;
            }
            planner = new RecordQueryPlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState(), indexTypes, recordStore.getTimer());
        }
        return planner;
    }

    @Nonnull
    protected FDBRecordStore.Builder getStoreBuilder(@Nonnull FDBRecordContext context, @Nonnull RecordMetaData metaData,
                                                     @Nonnull final KeySpacePath path) {
        return FDBRecordStore.newBuilder()
                .setFormatVersion(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION) // set to max to test newest features (unsafe for real deployments)
                .setKeySpacePath(path)
                .setContext(context)
                .setMetaDataProvider(metaData);
    }

    public FDBRecordContext openContext(@Nonnull final RecordLayerPropertyStorage props) {
        return openContext(props.toBuilder());
    }

    public FDBRecordContext openContext(@Nonnull final RecordLayerPropertyStorage.Builder props) {
        final FDBRecordContextConfig config = contextConfig(props)
                .setTimer(timer)
                .build();

        return fdb.openContext(config);
    }

    FDBRecordContextConfig.Builder contextConfig(@Nonnull final RecordLayerPropertyStorage.Builder props) {
        UUID transactionUuid = UUID.randomUUID();
        @Nullable String testMethod = MDC.get(TestMdcExtension.TEST_METHOD);
        String transactionId = (testMethod == null) ? transactionUuid.toString() : (testMethod + "_" + transactionUuid);
        ImmutableMap<String, String> mdcContext = ImmutableMap.<String, String>builder()
                .put("uuid", transactionId)
                .putAll(MDC.getCopyOfContextMap())
                .build();

        return FDBRecordContextConfig.newBuilder()
                .setTransactionId(transactionId)
                .setTimer(timer)
                .setMdcContext(mdcContext)
                .setTrackOpen(true)
                .setSaveOpenStackTrace(true)
                .setRecordContextProperties(addDefaultProps(props).build())
                .setReportConflictingKeys(true);
    }

    protected RecordLayerPropertyStorage.Builder addDefaultProps(final RecordLayerPropertyStorage.Builder props) {
        return props;
    }

    public void commit(FDBRecordContext context) {
        try {
            context.commit();
            if (LOGGER.isInfoEnabled()) {
                KeyValueLogMessage msg = KeyValueLogMessage.build("committing transaction");
                msg.addKeysAndValues(timer.getKeysAndValues());
                msg.addKeyAndValue(LogMessageKeys.TRANSACTION_ID, context.getTransactionId());
                LOGGER.info(msg.toString());
            }
        } catch (FDBExceptions.FDBStoreTransactionConflictException conflictException) {
            List<Range> conflictRanges = context.getNotCommittedConflictingKeys();
            if (conflictRanges != null && !conflictRanges.isEmpty()) {
                conflictException.addLogInfo("conflict_ranges", conflictRanges);
            }
            throw conflictException;
        } finally {
            timer.reset();
        }
    }
}
