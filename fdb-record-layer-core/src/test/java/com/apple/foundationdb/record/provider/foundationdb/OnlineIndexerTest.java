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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsJoinIndexProto;
import com.apple.foundationdb.record.TestRecordsNestedMapProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase.RecordMetaDataHook;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link OnlineIndexer}.
 */
@Tag(Tags.RequiresFDB)
public abstract class OnlineIndexerTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);

    FDBDatabase fdb;
    KeySpacePath path;
    RecordMetaData metaData;
    RecordQueryPlanner planner;
    FDBRecordStore recordStore;
    private IndexMaintenanceFilter indexMaintenanceFilter;
    int formatVersion = FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION;

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
        final FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        factory.setInitialDelayMillis(2L);
        factory.setMaxDelayMillis(4L);
        factory.setMaxAttempts(100);

        fdb = dbExtension.getDatabase();
        fdb.setAsyncToSyncTimeout(5, TimeUnit.MINUTES);
        path = pathManager.createPath(TestKeySpace.RECORD_STORE);
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
                .setKeySpacePath(path)
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
                .setSubspaceProvider(new SubspaceProviderByKeySpacePath(path))
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
                .setSubspaceProvider(new SubspaceProviderByKeySpacePath(path))
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

    @Nonnull
    protected List<Message> populateNestedMapData(final long numRecords) {
        assertNotNull(metaData, "meta-data must be opened to populate data");
        final List<Message> data = new ArrayList<>();
        for (long i = 0; i < numRecords; i++) {
            data.add(TestRecordsNestedMapProto.OuterRecord.newBuilder()
                    .setRecId(2 * i)
                    .setOtherId(i % 2)
                    .setMap(TestRecordsNestedMapProto.MapRecord.newBuilder()
                            .addEntry(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder()
                                    .setKey("a")
                                    .setIntValue(i))
                            .addEntry(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder()
                                    .setKey("b")
                                    .setIntValue(i))
                            .addEntry(TestRecordsNestedMapProto.MapRecord.Entry.newBuilder()
                                    .setKey("c")
                                    .setIntValue(i))
                    )
                    .build()
            );
            data.add(TestRecordsNestedMapProto.OtherRecord.newBuilder()
                    .setRecId(2 * i + 1)
                    .setOtherId(i % 2)
                    .build()
            );
        }

        try (FDBRecordContext context = openContext()) {
            data.forEach(recordStore::saveRecord);
            context.commit();
        }
        return data;
    }

    @Nonnull
    protected List<Message> populateJoinedData(final long numRecords) {
        assertNotNull(metaData, "meta-data must be opened to populate data");
        final List<Message> data = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            final int numValue = i % 5;
            data.add(TestRecordsJoinIndexProto.MySimpleRecord.newBuilder()
                    .setRecNo(i)
                    .setNumValue(numValue)
                    .setNumValue2(i % 3)
                    .setStrValue(i % 2 == 0 ? "even" : "odd")
                    .setOtherRecNo(1000 + i)
                    .build());

            data.add(TestRecordsJoinIndexProto.MyOtherRecord.newBuilder()
                    .setRecNo(1000 + i)
                    .setNumValue(numValue)
                    .setNumValue3(i % 5)
                    .build());
        }

        try (FDBRecordContext context = openContext()) {
            data.forEach(recordStore::saveRecord);
            context.commit();
        }
        return data;
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

    protected List<Tuple> getBoundariesList(final long numRecords, final long step) {
        List<Tuple> boundaries = new ArrayList<>();
        boundaries.add(null);
        for (long i = step; i < numRecords; i += step) {
            final TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(i).build();
            final RecordType recordType = metaData.getRecordTypeForDescriptor(rec.getDescriptorForType());
            final KeyExpression primaryKeyExpression = recordType.getPrimaryKey();
            final FDBStoredRecordBuilder<TestRecords1Proto.MySimpleRecord> recordBuilder = FDBStoredRecord.newBuilder(rec).setRecordType(recordType);
            final Tuple primaryKey = primaryKeyExpression.evaluateSingleton(recordBuilder).toTuple();
            boundaries.add(primaryKey);
        }
        boundaries.add(null);
        return boundaries;
    }

    protected <M extends Message> List<Tuple> getBoundariesList(List<M> records, final int step) {
        // Assumption: the records are sorted by RecNo
        List<Tuple> boundaries = new ArrayList<>();
        boundaries.add(null);
        if (TestRecords1Proto.MySimpleRecord.class.isAssignableFrom(records.get(0).getClass())) {
            for (int i = step; i < records.size(); i += step) {
                final TestRecords1Proto.MySimpleRecord rec = (TestRecords1Proto.MySimpleRecord) records.get(i);
                final RecordType recordType = metaData.getRecordTypeForDescriptor(rec.getDescriptorForType());
                final KeyExpression primaryKeyExpression = recordType.getPrimaryKey();
                final FDBStoredRecordBuilder<TestRecords1Proto.MySimpleRecord> recordBuilder = FDBStoredRecord.newBuilder(rec).setRecordType(recordType);
                final Tuple primaryKey = primaryKeyExpression.evaluateSingleton(recordBuilder).toTuple();
                boundaries.add(primaryKey);
            }
        }
        boundaries.add(null);
        return boundaries;
    }
}
