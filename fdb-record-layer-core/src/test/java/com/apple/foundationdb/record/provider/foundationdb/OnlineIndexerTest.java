/*
 * OnlineIndexerTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase.RecordMetaDataHook;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.test.Tags;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link OnlineIndexer}.
 */
@Tag(Tags.RequiresFDB)
public abstract class OnlineIndexerTest extends FDBTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(OnlineIndexerTest.class);

    RecordMetaData metaData;
    RecordQueryPlanner planner;
    FDBRecordStore recordStore;
    FDBDatabase fdb;
    Subspace subspace;
    private IndexMaintenanceFilter indexMaintenanceFilter;

    private static long oldMaxDelayMillis;
    private static long oldInitialDelayMillis;
    private static int oldMaxAttempts;

    @BeforeAll
    public static void setUpForClass() {
        oldInitialDelayMillis = FDBDatabaseFactory.instance().getInitialDelayMillis();
        FDBDatabaseFactory.instance().setInitialDelayMillis(2L);
        oldMaxDelayMillis = FDBDatabaseFactory.instance().getMaxDelayMillis();
        FDBDatabaseFactory.instance().setMaxDelayMillis(4L);
        oldMaxAttempts = FDBDatabaseFactory.instance().getMaxAttempts();
        FDBDatabaseFactory.instance().setMaxAttempts(100);
    }

    @AfterAll
    public static void tearDownForClass() {
        FDBDatabaseFactory.instance().setMaxDelayMillis(oldMaxDelayMillis);
        FDBDatabaseFactory.instance().setInitialDelayMillis(oldInitialDelayMillis);
        FDBDatabaseFactory.instance().setMaxAttempts(oldMaxAttempts);
    }

    public void setIndexMaintenanceFilter(@Nullable IndexMaintenanceFilter indexMaintenanceFilter) {
        this.indexMaintenanceFilter = indexMaintenanceFilter;
    }

    @Nonnull
    public IndexMaintenanceFilter getIndexMaintenanceFilter() {
        if (indexMaintenanceFilter == null) {
            return IndexMaintenanceFilter.NORMAL;
        } else {
            return indexMaintenanceFilter;
        }
    }

    @BeforeEach
    public void setUp() {
        if (fdb == null) {
            fdb = FDBDatabaseFactory.instance().getDatabase();
            fdb.setAsyncToSyncTimeout(5, TimeUnit.MINUTES);
        }
        if (subspace == null) {
            subspace = DirectoryLayer.getDefault().createOrOpen(fdb.database(), Arrays.asList("record-test", "unit", "oib")).join();
        }
        fdb.run(context -> {
            FDBRecordStore.deleteStore(context, subspace);
            return null;
        });
    }

    void clearIndexData(@Nonnull Index index) {
        fdb.database().run(tr -> {
            tr.clear(Range.startsWith(recordStore.indexSubspace(index).pack()));
            tr.clear(recordStore.indexSecondarySubspace(index).range());
            tr.clear(recordStore.indexRangeSubspace(index).range());
            tr.clear(recordStore.indexBuildSubspace(index).range());
            return null;
        });
    }

    private void openMetaData(@Nonnull Descriptors.FileDescriptor descriptor, @Nonnull RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(descriptor);
        hook.apply(metaDataBuilder);
        metaData = metaDataBuilder.getRecordMetaData();
    }

    private void openMetaData(@Nonnull Descriptors.FileDescriptor descriptor) {
        openMetaData(descriptor, (metaDataBuilder) -> {
        });
    }

    void openSimpleMetaData() {
        openMetaData(TestRecords1Proto.getDescriptor());
    }

    void openSimpleMetaData(RecordMetaDataHook hook) {
        openMetaData(TestRecords1Proto.getDescriptor(), hook);
    }

    OnlineIndexer.Builder newIndexerBuilder() {
        return OnlineIndexer.newBuilder()
                .setDatabase(fdb)
                .setMetaData(metaData)
                .setSubspace(subspace)
                .setIndexMaintenanceFilter(getIndexMaintenanceFilter())
                .setFormatVersion(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION);
    }

    OnlineIndexScrubber.Builder newScrubberBuilder() {
        return OnlineIndexScrubber.newBuilder()
                .setDatabase(fdb)
                .setMetaData(metaData)
                .setSubspace(subspace)
                .setIndexMaintenanceFilter(getIndexMaintenanceFilter())
                .setFormatVersion(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION);
    }

    FDBRecordContext openContext(boolean checked) {
        FDBRecordContext context = fdb.openContext();
        FDBRecordStore.Builder builder = FDBRecordStore.newBuilder()
                .setMetaDataProvider(metaData)
                .setContext(context)
                .setFormatVersion(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION)
                .setSubspace(subspace)
                .setIndexMaintenanceFilter(getIndexMaintenanceFilter());
        if (checked) {
            recordStore = builder.createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
        } else {
            recordStore = builder.uncheckedOpen();
        }
        metaData = recordStore.getRecordMetaData();
        planner = new RecordQueryPlanner(metaData, recordStore.getRecordStoreState(), recordStore.getTimer());
        return context;
    }

    FDBRecordContext openContext() {
        return openContext(true);
    }
}
