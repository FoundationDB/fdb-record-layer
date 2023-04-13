/*
 * FDBVersionsQueryTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.query;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TestRecords1Proto.MySimpleRecord;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.provider.common.RecordSerializer;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBTypedRecordStore;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.version;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.filter;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.unbounded;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of queries involving predicates on {@link FDBRecordVersion}s. These tests are around to facilitate testing
 * the planners' version queries, including things like making sure if the version is
 * For additional tests, see {@link com.apple.foundationdb.record.provider.foundationdb.indexes.VersionIndexTest}.
 */
@Tag(Tags.RequiresFDB)
public class FDBVersionsQueryTest extends FDBRecordStoreQueryTestBase {
    private static final Index VERSION_INDEX = new Index("versionIndex", version(), IndexTypes.VERSION);
    private static final Index VERSION_BY_NUM_VALUE_2_INDEX = new Index("versionByNumValue2Index", concat(field("num_value_2"), version()), IndexTypes.VERSION);

    private static final RecordMetaDataHook VERSIONS_HOOK = metaDataBuilder -> {
        metaDataBuilder.setStoreRecordVersions(true);

        final RecordTypeBuilder simple = metaDataBuilder.getRecordType("MySimpleRecord");
        metaDataBuilder.addIndex(simple, VERSION_INDEX);
        metaDataBuilder.addIndex(simple, VERSION_BY_NUM_VALUE_2_INDEX);
    };

    private void openStore(FDBRecordContext context) {
        openSimpleRecordStore(context, VERSIONS_HOOK);
    }

    @Nonnull
    private FDBTypedRecordStore<MySimpleRecord> getNarrowedStore() {
        RecordSerializer<Message> baseSerializer = recordStore.getSerializer();
        return recordStore.getTypedRecordStore(new RecordSerializer<>() {
            @Nonnull
            @Override
            public byte[] serialize(@Nonnull final RecordMetaData metaData, @Nonnull final RecordType recordType, @Nonnull final MySimpleRecord rec, @Nullable final StoreTimer timer) {
                return baseSerializer.serialize(metaData, recordType, rec, timer);
            }

            @Nonnull
            @Override
            public MySimpleRecord deserialize(@Nonnull final RecordMetaData metaData, @Nonnull final Tuple primaryKey, @Nonnull final byte[] serialized, @Nullable final StoreTimer timer) {
                Message msg = baseSerializer.deserialize(metaData, primaryKey, serialized, timer);

                if (!msg.getDescriptorForType().equals(MySimpleRecord.getDescriptor())) {
                    throw new RecordCoreException("invalid type to deserialize");
                }
                return MySimpleRecord.newBuilder().mergeFrom(msg).build();
            }

            @Nonnull
            @Override
            public RecordSerializer<Message> widen() {
                return baseSerializer.widen();
            }
        });
    }

    @Nonnull
    private List<FDBStoredRecord<MySimpleRecord>> populateRecords() {
        List<FDBStoredRecord<MySimpleRecord>> saved = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            try (FDBRecordContext context = openContext()) {
                openStore(context);
                FDBTypedRecordStore<MySimpleRecord> typedStore = getNarrowedStore();

                List<FDBStoredRecord<MySimpleRecord>> savedInTransaction = new ArrayList<>();
                for (int j = 0; j < 10; j++) {
                    MySimpleRecord record = MySimpleRecord.newBuilder()
                            .setRecNo(j * 100 + i)
                            .setStrValueIndexed(j % 2 == 0 ? "even" : "odd")
                            .setNumValue2(j % 3)
                            .setNumValue3Indexed(j)
                            .setNumValueUnique(i * 100 + j)
                            .build();

                    savedInTransaction.add(typedStore.saveRecord(record));
                }

                context.commit();
                byte[] globalVersion = context.getVersionStamp();
                savedInTransaction.forEach(rec -> saved.add(rec.withCommittedVersion(globalVersion)));
            }
        }
        return saved;
    }

    @DualPlannerTest
    void orderByVersion() {
        List<FDBStoredRecord<MySimpleRecord>> records = populateRecords();

        try (FDBRecordContext context = openContext()) {
            openStore(context);
            FDBTypedRecordStore<MySimpleRecord> typedStore = getNarrowedStore();

            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setSort(version())
                    .build();

            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, indexScan(allOf(indexName(VERSION_INDEX.getName()), unbounded())));

            List<FDBQueriedRecord<MySimpleRecord>> queried = typedStore.executeQuery(plan)
                    .asList()
                    .join();
            assertThat(queried, hasSize(records.size()));
            assertInVersionOrder(queried);
        }
    }

    @DualPlannerTest
    void orderByVersionWithSelectiveResults() {
        List<FDBStoredRecord<MySimpleRecord>> records = populateRecords();

        try (FDBRecordContext context = openContext()) {
            openStore(context);
            FDBTypedRecordStore<MySimpleRecord> typedStore = getNarrowedStore();

            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setRequiredResults(List.of(field("rec_no"), version()))
                    .setSort(version())
                    .build();

            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, indexScan(allOf(indexName(VERSION_INDEX.getName()), unbounded())));

            List<FDBQueriedRecord<MySimpleRecord>> queried = typedStore.executeQuery(plan)
                    .asList()
                    .join();
            assertThat(queried, hasSize(records.size()));
            assertInVersionOrder(queried);
        }
    }

    @DualPlannerTest
    void filterByVersion() {
        List<FDBStoredRecord<MySimpleRecord>> records = populateRecords();

        try (FDBRecordContext context = openContext()) {
            openStore(context);
            FDBTypedRecordStore<MySimpleRecord> typedStore = getNarrowedStore();

            FDBRecordVersion versionForQuery = records.get(records.size() / 2).getVersion();
            assertNotNull(versionForQuery);
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.version().greaterThan(versionForQuery))
                    .build();

            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, indexScan(allOf(indexName(VERSION_INDEX.getName()), bounds(hasTupleString("([" + versionForQuery.toVersionstamp(false) + "],>")))));
            List<FDBStoredRecord<MySimpleRecord>> queried = typedStore.executeQuery(plan)
                    .map(FDBQueriedRecord::getStoredRecord)
                    .asList()
                    .join();

            List<FDBStoredRecord<MySimpleRecord>> expected = records.stream()
                    .filter(rec -> {
                        FDBRecordVersion recordVersion = rec.getVersion();
                        assertNotNull(recordVersion);
                        return recordVersion.compareTo(versionForQuery) > 0;
                    })
                    .collect(Collectors.toList());
            assertEquals(expected, queried);
        }
    }

    @DualPlannerTest
    void residualVersionFilter() {
        List<FDBStoredRecord<MySimpleRecord>> records = populateRecords();

        try (FDBRecordContext context = openContext()) {
            openStore(context);
            FDBTypedRecordStore<MySimpleRecord> typedStore = getNarrowedStore();

            FDBRecordVersion versionForQuery = records.get(records.size() / 2).getVersion();
            assertNotNull(versionForQuery);
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.version().greaterThan(versionForQuery))
                    .setSort(field("num_value_unique")) // use sort to force execution of predicate as a residual filter
                    .build();

            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, filter(Query.version().greaterThan(versionForQuery), indexScan(allOf(indexName("MySimpleRecord$num_value_unique"), unbounded()))));
            List<FDBStoredRecord<MySimpleRecord>> queried = typedStore.executeQuery(plan)
                    .map(FDBQueriedRecord::getStoredRecord)
                    .asList()
                    .join();

            List<FDBStoredRecord<MySimpleRecord>> expected = records.stream()
                    .filter(rec -> {
                        FDBRecordVersion recordVersion = rec.getVersion();
                        assertNotNull(recordVersion);
                        return recordVersion.compareTo(versionForQuery) > 0;
                    })
                    .sorted(Comparator.comparingInt(rec -> rec.getRecord().getNumValueUnique()))
                    .collect(Collectors.toList());
            assertEquals(expected, queried);
        }
    }

    @DualPlannerTest
    void residualVersionFilterWithSelectiveResults() {
        List<FDBStoredRecord<MySimpleRecord>> records = populateRecords();

        try (FDBRecordContext context = openContext()) {
            openStore(context);
            FDBTypedRecordStore<MySimpleRecord> typedStore = getNarrowedStore();

            FDBRecordVersion versionForQuery = records.get(records.size() / 2).getVersion();
            assertNotNull(versionForQuery);
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.version().greaterThan(versionForQuery))
                    .setSort(field("num_value_unique")) // use sort to force execution of predicate as a residual filter
                    .setRequiredResults(List.of(field("num_value_unique"), field("rec_no")))
                    .build();

            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, filter(Query.version().greaterThan(versionForQuery), indexScan(allOf(indexName("MySimpleRecord$num_value_unique"), unbounded()))));
            List<Long> queried = typedStore.executeQuery(plan)
                    .map(rec -> rec.getRecord().getRecNo())
                    .asList()
                    .join();

            List<Long> expected = records.stream()
                    .filter(rec -> {
                        FDBRecordVersion recordVersion = rec.getVersion();
                        assertNotNull(recordVersion);
                        return recordVersion.compareTo(versionForQuery) > 0;
                    })
                    .sorted(Comparator.comparingInt(rec -> rec.getRecord().getNumValueUnique()))
                    .map(rec -> rec.getRecord().getRecNo())
                    .collect(Collectors.toList());
            assertEquals(expected, queried);
        }
    }

    @DualPlannerTest
    void sortAndFilterWithSingleIndex() {
        List<FDBStoredRecord<MySimpleRecord>> records = populateRecords();

        try (FDBRecordContext context = openContext()) {
            openStore(context);
            FDBTypedRecordStore<MySimpleRecord> typedStore = getNarrowedStore();

            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.field("num_value_2").equalsValue(1))
                    .setSort(version())
                    .build();

            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, indexScan(allOf(indexName(VERSION_BY_NUM_VALUE_2_INDEX.getName()), bounds(hasTupleString("[[1],[1]]")))));
            List<FDBStoredRecord<MySimpleRecord>> queried = typedStore.executeQuery(plan)
                    .map(FDBQueriedRecord::getStoredRecord)
                    .asList()
                    .join();
            assertInVersionOrder(queried);

            List<FDBStoredRecord<MySimpleRecord>> expected = records.stream()
                    .filter(rec -> rec.getRecord().getNumValue2() == 1)
                    .collect(Collectors.toList());
            assertEquals(expected, queried);
        }
    }

    @DualPlannerTest
    void sortFilterOnVersionIndexEntries() {
        List<FDBStoredRecord<MySimpleRecord>> records = populateRecords();

        try (FDBRecordContext context = openContext()) {
            openStore(context);
            FDBTypedRecordStore<MySimpleRecord> typedStore = getNarrowedStore();

            FDBRecordVersion excludedVersion = records.stream()
                    .filter(rec -> rec.getRecord().getNumValue2() == 1)
                    .map(FDBStoredRecord::getVersion)
                    .findAny()
                    .get();
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.and(Query.field("num_value_2").equalsValue(1), Query.version().notEquals(excludedVersion)))
                    .setSort(version())
                    .build();

            RecordQueryPlan plan = planner.plan(query);
            // Should be able to push down version filter onto index the index entries when planner can better reason about version field
            assertThat(plan, filter(Query.version().notEquals(excludedVersion), indexScan(allOf(indexName(VERSION_BY_NUM_VALUE_2_INDEX.getName()), bounds(hasTupleString("[[1],[1]]"))))));
            List<FDBStoredRecord<MySimpleRecord>> queried = typedStore.executeQuery(plan)
                    .map(FDBQueriedRecord::getStoredRecord)
                    .asList()
                    .join();
            assertInVersionOrder(queried);

            List<FDBStoredRecord<MySimpleRecord>> expected = records.stream()
                    .filter(rec -> rec.getRecord().getNumValue2() == 1)
                    .filter(rec -> rec.hasVersion() && !rec.getVersion().equals(excludedVersion))
                    .collect(Collectors.toList());
            assertEquals(expected, queried);
        }
    }

    @DualPlannerTest
    void requestVersionWhenQueryIsOnOtherFields() {
        List<FDBStoredRecord<MySimpleRecord>> records = populateRecords();

        try (FDBRecordContext context = openContext()) {
            openStore(context);
            FDBTypedRecordStore<MySimpleRecord> typedStore = getNarrowedStore();

            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.field("str_value_indexed").equalsValue("even"))
                    .setRequiredResults(List.of(field("rec_no"), version()))
                    .build();

            RecordQueryPlan plan = planner.plan(query);
            // Should be able to push down version filter onto index the index entries when planner can better reason about version field
            assertThat(plan, indexScan(allOf(indexName("MySimpleRecord$str_value_indexed"), bounds(hasTupleString("[[even],[even]]")))));
            List<FDBStoredRecord<MySimpleRecord>> queried = typedStore.executeQuery(plan)
                    .map(FDBQueriedRecord::getStoredRecord)
                    .asList()
                    .join();
            assertTrue(queried.stream().allMatch(FDBRecord::hasVersion), "records should all have non-null versions");

            List<FDBStoredRecord<MySimpleRecord>> expected = records.stream()
                    .filter(rec -> rec.getRecord().getStrValueIndexed().equals("even"))
                    .sorted(Comparator.comparing(FDBStoredRecord::getPrimaryKey))
                    .collect(Collectors.toList());
            assertEquals(expected, queried);
        }
    }

    private static void assertInVersionOrder(List<? extends FDBRecord<?>> records) {
        FDBRecordVersion lastVersion = null;
        for (FDBRecord<?> record : records) {
            FDBRecordVersion nextVersion = record.getVersion();
            assertNotNull(nextVersion, () -> String.format("version for record with primary key %s should not be null", record.getPrimaryKey()));
            if (lastVersion != null) {
                assertThat(nextVersion, greaterThan(lastVersion));
            }
            lastVersion = nextVersion;
        }
    }
}
