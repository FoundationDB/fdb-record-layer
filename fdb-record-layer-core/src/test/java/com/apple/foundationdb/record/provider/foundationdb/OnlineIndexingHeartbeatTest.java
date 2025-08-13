/*
 * OnlineIndexingHeartbeatTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verify indexing heartbeat activity (query & clear).
 */
class OnlineIndexingHeartbeatTest extends OnlineIndexerTest {

    @Test
    void testHeartbeatLowLevel() {
        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);

        final int count = 10;
        IndexingHeartbeat[] heartbeats = new IndexingHeartbeat[count];
        for (int i = 0; i < count; i++) {
            heartbeats[i] = new IndexingHeartbeat(UUID.randomUUID(), "Test", 100 + i, true);
        }

        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            for (var heartbeat : heartbeats) {
                heartbeat.updateHeartbeat(recordStore, indexes.get(0));
                heartbeat.updateHeartbeat(recordStore, indexes.get(1));
            }
            context.commit();
        }

        // Verify query/clear operation
        try (OnlineIndexer indexer = newIndexerBuilder(indexes.get(0)).build()) {
            // Query, unlimited
            Map<UUID, IndexBuildProto.IndexBuildHeartbeat> queried = indexer.getIndexingHeartbeats(0);
            Assertions.assertThat(queried).hasSize(count);
            Assertions.assertThat(queried.keySet())
                    .containsExactlyInAnyOrderElementsOf(Arrays.stream(heartbeats).map(heartbeat -> heartbeat.indexerId).collect(Collectors.toList()));

            // Query, partial
            queried = indexer.getIndexingHeartbeats(5);
            Assertions.assertThat(queried).hasSize(5);

            // clear, partial
            int countDeleted = indexer.clearIndexingHeartbeats(0, 7);
            Assertions.assertThat(countDeleted).isEqualTo(7);
            queried = indexer.getIndexingHeartbeats(5);
            Assertions.assertThat(queried).hasSize(3);
        }

        // Verify that the previous clear does not affect other index
        try (OnlineIndexer indexer = newIndexerBuilder(indexes.get(1)).build()) {
            Map<UUID, IndexBuildProto.IndexBuildHeartbeat> queried = indexer.getIndexingHeartbeats(100);
            Assertions.assertThat(queried).hasSize(count);
            Assertions.assertThat(queried.keySet())
                    .containsExactlyInAnyOrderElementsOf(Arrays.stream(heartbeats).map(ht -> ht.indexerId).collect(Collectors.toList()));

            // clear all
            int countDeleted = indexer.clearIndexingHeartbeats(0, 0);
            Assertions.assertThat(countDeleted).isEqualTo(count);

            // verify empty
            queried = indexer.getIndexingHeartbeats(0);
            Assertions.assertThat(queried).isEmpty();
        }
    }

    @ParameterizedTest
    @BooleanSource
    void testIndexersHeartbeatsClearAfterBuild(boolean mutualIndexing) {
        // Assert that the heartbeats are cleared after building
        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        int numRecords = 77;
        populateData(numRecords);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);

        if (mutualIndexing) {
            int boundarySize = 23;
            final List<Tuple> boundariesList = getBoundariesList(numRecords, boundarySize);
            IntStream.rangeClosed(1, 5).parallel().forEach(i -> {
                try (OnlineIndexer indexer = newIndexerBuilder(indexes)
                        .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                                .setMutualIndexingBoundaries(boundariesList))
                        .build()) {
                    indexer.buildIndex();
                }
            });
        } else {
            try (OnlineIndexer indexer = newIndexerBuilder(indexes)
                    .build()) {
                indexer.buildIndex();
            }
        }

        for (Index index : indexes) {
            try (OnlineIndexer indexer = newIndexerBuilder(index).build()) {
                Assertions.assertThat(indexer.getIndexingHeartbeats(0)).isEmpty();
            }
        }
    }

    @ParameterizedTest
    @BooleanSource
    void testIndexersHeartbeatsClearAfterCrash(boolean mutualIndexing) {
        // Assert that the heartbeats are cleared after crash
        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        int numRecords = 98;
        populateData(numRecords);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);

        final String testThrowMsg = "Intentionally crash during test";
        final AtomicLong counter = new AtomicLong(0);
        if (mutualIndexing) {
            int boundarySize = 20;
            final List<Tuple> boundariesList = getBoundariesList(numRecords, boundarySize);
            IntStream.rangeClosed(1, 9).parallel().forEach(i -> {
                try (OnlineIndexer indexer = newIndexerBuilder(indexes)
                        .setLimit(10)
                        .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                                .setMutualIndexingBoundaries(boundariesList))
                        .setConfigLoader(old -> {
                            // Unfortunately, we cannot verify that at least one heartbeat exists from this
                            // block, as it would have been nesting "asyncToSync" functions. But there are other tests
                            // that verify the "sync lock" functionality.
                            if (counter.incrementAndGet() > 2) {
                                throw new RecordCoreException(testThrowMsg);
                            }
                            return old;
                        })
                        .build()) {
                    RecordCoreException e = assertThrows(RecordCoreException.class, indexer::buildIndex);
                    assertTrue(e.getMessage().contains(testThrowMsg));
                }
            });
        } else {
            try (OnlineIndexer indexer = newIndexerBuilder(indexes)
                    .setLimit(10)
                    .setConfigLoader(old -> {
                        // Unfortunately, we cannot verify that at least one heartbeat exists from this
                        // block, as it would have been nesting "asyncToSync" functions. But there are other tests
                        // that verify the "sync lock" functionality.
                        if (counter.incrementAndGet() > 2) {
                            throw new RecordCoreException(testThrowMsg);
                        }
                        return old;
                    })
                    .build()) {
                RecordCoreException e = assertThrows(RecordCoreException.class, indexer::buildIndex);
                assertTrue(e.getMessage().contains(testThrowMsg));
            }
        }

        for (Index index : indexes) {
            try (OnlineIndexer indexer = newIndexerBuilder(index).build()) {
                Assertions.assertThat(indexer.getIndexingHeartbeats(0)).isEmpty();
            }
        }
    }

    @Test
    void testMutualIndexersHeartbeatsClearAfterBuild() throws InterruptedException {
        // Check heartbeats count during mutual indexing
        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        int numRecords = 77;
        populateData(numRecords);
        int boundarySize = 5;
        final List<Tuple> boundariesList = getBoundariesList(numRecords, boundarySize);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);

        Semaphore pauseSemaphore = new Semaphore(1);
        Semaphore startSemaphore = new Semaphore(1);
        final AtomicInteger count = new AtomicInteger(0);
        pauseSemaphore.acquire();
        startSemaphore.acquire();
        AtomicReference<Map<UUID, IndexBuildProto.IndexBuildHeartbeat>> heartbeats = new AtomicReference<>();
        IntStream.rangeClosed(1, 4).parallel().forEach(i -> {
            if (i == 4) {
                try {
                    startSemaphore.acquire();
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                try (OnlineIndexer indexer = newIndexerBuilder(indexes).build()) {
                    heartbeats.set(indexer.getIndexingHeartbeats(0));
                }
                startSemaphore.release();
                pauseSemaphore.release();
            } else {
                AtomicInteger counter = new AtomicInteger(0);
                try (OnlineIndexer indexer = newIndexerBuilder(indexes)
                        .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                                .setMutualIndexingBoundaries(boundariesList))
                        .setConfigLoader(old -> {
                            if (counter.incrementAndGet() > 0) {
                                if (count.incrementAndGet() == 2) {
                                    startSemaphore.release();
                                }
                                try {
                                    pauseSemaphore.acquire();
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                } finally {
                                    pauseSemaphore.release();
                                }
                            }
                            return old;
                        })
                        .build()) {
                    indexer.buildIndex();
                }
            }
        });
        // While building, heartbeats count should have been 3
        Assertions.assertThat(heartbeats.get()).hasSize(3);

        // After building, heartbeats count should be 0
        try (OnlineIndexer indexer = newIndexerBuilder(indexes).build()) {
            heartbeats.set(indexer.getIndexingHeartbeats(0));
        }
    }


    @Test
    void testHeartbeatsRenewal() throws InterruptedException {
        // make sure that the heartbeats behave as expected during indexing:
        // single item
        // same indexerId, genesis time
        // monotonically increasing heartbeats
        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        int numRecords = 74;
        populateData(numRecords);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);
        final List<Map<UUID, IndexBuildProto.IndexBuildHeartbeat>> heartbeatsQueries = new ArrayList<>();

        Semaphore indexerGo = new Semaphore(1);
        Semaphore colectorGo = new Semaphore(1);
        AtomicBoolean indexerDone = new AtomicBoolean(false);
        colectorGo.acquire();
        Thread indexerThread = new Thread( () -> {
            try (OnlineIndexer indexer = newIndexerBuilder(indexes)
                    .setLimit(10)
                    .setConfigLoader(old -> {
                        colectorGo.release();
                        try {
                            indexerGo.acquire();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        return old;
                    })
                    .build()) {
                indexer.buildIndex();
            }
            colectorGo.release();
            indexerDone.set(true);
        });

        Thread collectorThread = new Thread(() -> {
            while (!indexerDone.get()) {
                try {
                    colectorGo.acquire();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                try (FDBRecordContext context = openContext()) {
                    final Map<UUID, IndexBuildProto.IndexBuildHeartbeat> heartbeats = IndexingHeartbeat.getIndexingHeartbeats(recordStore, indexes.get(0), 0).join();
                    heartbeatsQueries.add(heartbeats);
                    context.commit();
                }
                indexerGo.release();
            }
        });
        indexerThread.start();
        collectorThread.start();
        collectorThread.join();
        indexerThread.join();

        Assertions.assertThat(heartbeatsQueries).hasSizeGreaterThan(5);
        Assertions.assertThat(heartbeatsQueries.get(0)).hasSize(1);
        final Map.Entry<UUID, IndexBuildProto.IndexBuildHeartbeat> first = heartbeatsQueries.get(0).entrySet().iterator().next();
        Map.Entry<UUID, IndexBuildProto.IndexBuildHeartbeat> previous = first;
        for (int i = 1; i < heartbeatsQueries.size() - 1; i++) {
            Assertions.assertThat(heartbeatsQueries.get(i)).hasSize(1);
            final Map.Entry<UUID, IndexBuildProto.IndexBuildHeartbeat> item = heartbeatsQueries.get(i).entrySet().iterator().next();
            Assertions.assertThat(item.getKey()).isEqualTo(first.getKey());
            Assertions.assertThat(item.getValue().getGenesisTimeMilliseconds()).isEqualTo(first.getValue().getGenesisTimeMilliseconds());
            Assertions.assertThat(item.getValue().getInfo()).isEqualTo(first.getValue().getInfo());
            Assertions.assertThat(item.getValue().getHeartbeatTimeMilliseconds())
                    .isGreaterThan(previous.getValue().getHeartbeatTimeMilliseconds());
            previous = item;
        }
    }
}
