/*
 * OnlineIndexerUniqueIndexTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.record.RecordCoreRetriableTransactionException;
import com.apple.foundationdb.record.RecordIndexUniquenessViolation;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.tuple.Tuple;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for building unique indexes with {@link OnlineIndexer}.
 */
public class OnlineIndexerUniqueIndexTest extends OnlineIndexerTest {
    @Test
    public void uniquenessViolations() {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 10).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(((int)val) % 5).build()
        ).collect(Collectors.toList());
        Index index = new Index("simple$value_2", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);

        // Case 1: Entirely in build.
        openSimpleMetaData();
        try (FDBRecordContext context = openContext())  {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            context.commit();
        }
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .build()) {
            indexBuilder.buildIndexAsync().handle((ignore, e) -> {
                assertNotNull(e);
                RuntimeException runE = FDBExceptions.wrapException(e);
                assertNotNull(runE);
                assertThat(runE, instanceOf(RecordIndexUniquenessViolation.class));
                return null;
            }).join();

            // Case 2: While in write-only mode.
            try (FDBRecordContext context = openContext()) {
                recordStore.deleteAllRecords();
                recordStore.markIndexWriteOnly(index).join();
                context.commit();
            }
            try (FDBRecordContext context = openContext()) {
                records.forEach(recordStore::saveRecord);
                context.commit();
            }
            try (FDBRecordContext context = openContext()) {
                assertEquals(10, (int)recordStore.scanUniquenessViolations(index).getCount().join());
                context.commit();
            }
            indexBuilder.buildIndexAsync().handle((ignore, e) -> {
                assertNotNull(e);
                RuntimeException runE = FDBExceptions.wrapException(e);
                assertNotNull(runE);
                assertThat(runE, instanceOf(RecordIndexUniquenessViolation.class));
                return null;
            }).join();
        }

        // Case 3: Some in write-only mode.
        fdb.run(context -> {
            FDBRecordStore.deleteStore(context, subspace);
            return null;
        });
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < 5; i++) {
                recordStore.saveRecord(records.get(i));
            }
            context.commit();
        }
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            for (int i = 5; i < records.size(); i++) {
                recordStore.saveRecord(records.get(i));
            }
            context.commit();
        }
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .build()) {
            indexBuilder.buildIndexAsync().handle((ignore, e) -> {
                assertNotNull(e);
                RuntimeException runE = FDBExceptions.wrapException(e);
                assertNotNull(runE);
                assertThat(runE, instanceOf(RecordIndexUniquenessViolation.class));
                return null;
            }).join();
        }

        // Case 4: Some in write-only mode with an initial range build that shouldn't affect anything.
        fdb.run(context -> {
            FDBRecordStore.deleteStore(context, subspace);
            return null;
        });
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            for (int i = 5; i < records.size(); i++) {
                recordStore.saveRecord(records.get(i));
            }
            context.commit();
        }
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            for (int i = 0; i < 5; i++) {
                recordStore.saveRecord(records.get(i));
            }
            context.commit();
        }
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .build()) {
            indexBuilder.buildUnbuiltRange(Key.Evaluated.scalar(0L), Key.Evaluated.scalar(5L)).join();
            try (FDBRecordContext context = openContext()) {
                assertEquals(0, (int)recordStore.scanUniquenessViolations(index).getCount().join());
                context.commit();
            }
            indexBuilder.buildUnbuiltRange(Key.Evaluated.scalar(5L), Key.Evaluated.scalar(10L)).join();
            try (FDBRecordContext context = openContext()) {
                assertEquals(10, (int)recordStore.scanUniquenessViolations(index).getCount().join());
                context.commit();
            }
            indexBuilder.buildIndexAsync().handle((ignore, e) -> {
                assertNotNull(e);
                RuntimeException runE = FDBExceptions.wrapException(e);
                assertNotNull(runE);
                assertThat(runE, instanceOf(RecordIndexUniquenessViolation.class));
                return null;
            }).join();
        }

        // Case 5: Should be caught by write-only writes after build.
        fdb.run(context -> {
            FDBRecordStore.deleteStore(context, subspace);
            return null;
        });
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < 5; i++) {
                recordStore.saveRecord(records.get(i));
            }
            context.commit();
        }
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            context.commit();
        }
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .build()) {
            indexBuilder.buildIndex();
        }
        try (FDBRecordContext context = openContext()) {
            for (int i = 5; i < records.size(); i++) {
                recordStore.saveRecord(records.get(i));
            }
            context.commit();
            fail("Did not catch uniqueness violation when done after build by write-only writes");
        } catch (RecordIndexUniquenessViolation e) {
            // passed.
        }

        // Case 6: Should be caught by write-only writes after partial build.
        fdb.run(context -> {
            FDBRecordStore.deleteStore(context, subspace);
            return null;
        });
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < 5; i++) {
                recordStore.saveRecord(records.get(i));
            }
            context.commit();
        }
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            context.commit();
        }
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .build()) {
            indexBuilder.buildUnbuiltRange(Key.Evaluated.scalar(0L), Key.Evaluated.scalar(5L)).join();
            try (FDBRecordContext context = openContext()) {
                for (int i = 5; i < records.size(); i++) {
                    recordStore.saveRecord(records.get(i));
                }
                context.commit();
            }
            try (FDBRecordContext context = openContext()) {
                assertEquals(10, (int)recordStore.scanUniquenessViolations(index).getCount().join());
                context.commit();
            }
            indexBuilder.buildIndexAsync().handle((ignore, e) -> {
                assertNotNull(e);
                RuntimeException runE = FDBExceptions.wrapException(e);
                assertNotNull(runE);
                assertThat(runE, instanceOf(RecordIndexUniquenessViolation.class));
                return null;
            }).join();
        }

        // Case 7: The second of these two transactions should fail on not_committed, and then
        // there should be a uniqueness violation.
        fdb.run(context -> {
            FDBRecordStore.deleteStore(context, subspace);
            return null;
        });
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < 5; i++) {
                recordStore.saveRecord(records.get(i));
            }
            context.commit();
        }
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            context.commit();
        }
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .build()) {
            try (FDBRecordContext context = openContext()) {
                context.getReadVersion();
                try (FDBRecordContext context2 = fdb.openContext()) {
                    context2.getReadVersion();
                    FDBRecordStore recordStore2 = recordStore.asBuilder().setContext(context2).build();

                    indexBuilder.buildUnbuiltRange(recordStore, null, Key.Evaluated.scalar(5L)).join();
                    recordStore2.saveRecord(records.get(8));

                    context.commit();
                    context2.commitAsync().handle((ignore, e) -> {
                        assertNotNull(e);
                        RuntimeException runE = FDBExceptions.wrapException(e);
                        assertThat(runE, instanceOf(RecordCoreRetriableTransactionException.class));
                        assertNotNull(runE.getCause());
                        assertThat(runE.getCause(), instanceOf(FDBException.class));
                        FDBException fdbE = (FDBException)runE.getCause();
                        assertEquals(1020, fdbE.getCode()); // not_committed
                        return null;
                    }).join();
                }
            }
            try (FDBRecordContext context = openContext()) {
                for (int i = 5; i < records.size(); i++) {
                    recordStore.saveRecord(records.get(i));
                }
                context.commit();
            }
            try (FDBRecordContext context = openContext()) {
                assertEquals(10, (int)recordStore.scanUniquenessViolations(index).getCount().join());
                context.commit();
            }
            indexBuilder.buildIndexAsync().handle((ignore, e) -> {
                assertNotNull(e);
                RuntimeException runE = FDBExceptions.wrapException(e);
                assertNotNull(runE);
                assertThat(runE, instanceOf(RecordIndexUniquenessViolation.class));
                return null;
            }).join();
        }
    }

    @Test
    public void resolveUniquenessViolations() {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, 10).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val).setNumValue2(((int)val) % 5).build()
        ).collect(Collectors.toList());
        Index index = new Index("simple$value_2", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", index);

        openSimpleMetaData();
        try (FDBRecordContext context = openContext())  {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            context.commit();
        }
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setIndex(index).setSubspace(subspace)
                .build()) {
            indexBuilder.buildIndexAsync().handle((ignore, e) -> {
                assertNotNull(e);
                RuntimeException runE = FDBExceptions.wrapException(e);
                assertNotNull(runE);
                assertThat(runE, instanceOf(RecordIndexUniquenessViolation.class));
                return null;
            }).join();
        }

        try (FDBRecordContext context = openContext()) {
            Set<Tuple> indexEntries = new HashSet<>(recordStore.scanUniquenessViolations(index)
                    .map( v -> v.getIndexEntry().getKey() )
                    .asList().join());

            for (Tuple indexKey : indexEntries) {
                List<Tuple> primaryKeys = recordStore.scanUniquenessViolations(index, indexKey).map(RecordIndexUniquenessViolation::getPrimaryKey).asList().join();
                assertEquals(2, primaryKeys.size());
                recordStore.resolveUniquenessViolation(index, indexKey, primaryKeys.get(0)).join();
                assertEquals(0, (int)recordStore.scanUniquenessViolations(index, indexKey).getCount().join());
            }

            for (int i = 0; i < 5; i++) {
                assertNotNull(recordStore.loadRecord(Tuple.from(i)));
            }
            for (int i = 5; i < records.size(); i++) {
                assertNull(recordStore.loadRecord(Tuple.from(i)));
            }

            recordStore.markIndexReadable(index).join();
            context.commit();
        }
    }
}
