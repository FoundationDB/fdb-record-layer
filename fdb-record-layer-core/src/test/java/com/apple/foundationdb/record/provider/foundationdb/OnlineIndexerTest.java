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
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase.RecordMetaDataHook;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.test.Tags;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link OnlineIndexer}.
 */
@Tag(Tags.RequiresFDB)
public abstract class OnlineIndexerTest extends FDBTestBase {

    RecordMetaData metaData;
    RecordQueryPlanner planner;
    FDBRecordStore recordStore;
    FDBDatabase fdb;
    Subspace subspace;
    private IndexMaintenanceFilter indexMaintenanceFilter;
    int formatVersion = FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION;

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

    void openMetaData(@Nonnull Descriptors.FileDescriptor descriptor, @Nonnull RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(descriptor);
        hook.apply(metaDataBuilder);
        metaData = metaDataBuilder.getRecordMetaData();
    }

    void openMetaData(@Nonnull Descriptors.FileDescriptor descriptor) {
        openMetaData(descriptor, (metaDataBuilder) -> {
        });
    }

    void openSimpleMetaData() {
        openMetaData(TestRecords1Proto.getDescriptor());
    }

    void openSimpleMetaData(@Nonnull RecordMetaDataHook hook) {
        openMetaData(TestRecords1Proto.getDescriptor(), hook);
    }

    FDBRecordContext openContext(boolean checked) {
        FDBRecordContext context = fdb.openContext();
        FDBRecordStore.Builder builder = FDBRecordStore.newBuilder()
                .setMetaDataProvider(metaData)
                .setContext(context)
                .setFormatVersion(formatVersion)
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

    OnlineIndexer.Builder newIndexerBuilder() {
        return OnlineIndexer.newBuilder()
                .setDatabase(fdb)
                .setMetaData(metaData)
                .setSubspace(subspace)
                .setIndexMaintenanceFilter(getIndexMaintenanceFilter())
                .setFormatVersion(formatVersion);
    }

    OnlineIndexer.Builder newIndexerBuilder(List<Index> indexes) {
        return newIndexerBuilder().setTargetIndexes(indexes);
    }

    OnlineIndexer.Builder newIndexerBuilder(List<Index> indexes, FDBStoreTimer timer) {
        return newIndexerBuilder(indexes).setTimer(timer);
    }

    OnlineIndexer.Builder newIndexerBuilder(Index index) {
        return newIndexerBuilder().addTargetIndex(index);
    }

    OnlineIndexer.Builder newIndexerBuilder(Index index, FDBStoreTimer timer) {
        return newIndexerBuilder(index).setTimer(timer);
    }

    OnlineIndexScrubber.Builder newScrubberBuilder() {
        return OnlineIndexScrubber.newBuilder()
                .setDatabase(fdb)
                .setMetaData(metaData)
                .setSubspace(subspace)
                .setIndexMaintenanceFilter(getIndexMaintenanceFilter())
                .setFormatVersion(formatVersion);
    }

    OnlineIndexScrubber.Builder newScrubberBuilder(Index index) {
        return newScrubberBuilder().setIndex(index);
    }

    OnlineIndexScrubber.Builder newScrubberBuilder(Index index, FDBStoreTimer timer) {
        return newScrubberBuilder(index).setTimer(timer);
    }

    protected void disableAll(List<Index> indexes) {
        try (FDBRecordContext context = openContext()) {
            // disable all
            for (Index index : indexes) {
                recordStore.markIndexDisabled(index).join();
            }
            context.commit();
        }
    }

    protected void assertAllValidated(List<Index> indexes) {
        final FDBStoreTimer timer = new FDBStoreTimer();
        for (Index index: indexes) {
            if (index.getType().equals(IndexTypes.VALUE)) {
                try (OnlineIndexScrubber indexScrubber = newScrubberBuilder(index, timer)
                        .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                                .setLogWarningsLimit(Integer.MAX_VALUE)
                                .setAllowRepair(false)
                                .build())
                        .build()) {
                    indexScrubber.scrubDanglingIndexEntries();
                    indexScrubber.scrubMissingIndexEntries();
                }
                assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_DANGLING_ENTRIES));
                assertEquals(0, timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES));
            }
        }
    }

    protected void populateData(final long numRecords) {
        openSimpleMetaData();
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, numRecords).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(val)
                        .setNumValue2((int)val * 19)
                        .setNumValue3Indexed((int) val * 77)
                        .setNumValueUnique((int)val * 1139)
                        .build()
        ).collect(Collectors.toList());

        try (FDBRecordContext context = openContext())  {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }
    }

    protected static FDBRecordStoreTestBase.RecordMetaDataHook allIndexesHook(List<Index> indexes) {
        return metaDataBuilder -> {
            for (Index index: indexes) {
                metaDataBuilder.addIndex("MySimpleRecord", index);
            }
        } ;
    }

    protected void assertReadable(List<Index> indexes) {
        openSimpleMetaData(allIndexesHook(indexes));
        try (FDBRecordContext context = openContext()) {
            for (Index index : indexes) {
                assertTrue(recordStore.isIndexReadable(index));
            }
            context.commit();
        }
    }

    protected void assertReadable(Index index) {
        assertReadable(List.of(index));
    }

    protected void buildIndexClean(Index index) {
        try (OnlineIndexer indexer = newIndexerBuilder(index).build()) {
            indexer.buildIndex(true);
        }
        try (FDBRecordContext context = openContext()) {
            recordStore.vacuumReadableIndexesBuildData();
            context.commit();
        }
    }
}
