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
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

/**
 * Verify indexing heartbeat activity (query & clear).
 */
public class OnlineIndexingHeartbeatTest extends OnlineIndexerTest {
    @Test
    void testHeartbeatLowLevel() {
        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);

        final int count = 10;
        IndexingHeartbeat[] heartbeats = new IndexingHeartbeat[count];
        for (int i = 0; i < count; i++) {
            heartbeats[i] = new IndexingHeartbeat(UUID.randomUUID(), IndexBuildProto.IndexBuildIndexingStamp.Method.BY_INDEX, 100 + i);
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
                    .containsExactlyInAnyOrderElementsOf(Arrays.stream(heartbeats).map(ht -> ht.sessionId).collect(Collectors.toList()));

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
                    .containsExactlyInAnyOrderElementsOf(Arrays.stream(heartbeats).map(ht -> ht.sessionId).collect(Collectors.toList()));

            // clear all
            int countDeleted = indexer.clearIndexingHeartbeats(0, 0);
            Assertions.assertThat(countDeleted).isEqualTo(count);

            // verify empty
            queried = indexer.getIndexingHeartbeats(0);
            Assertions.assertThat(queried).isEmpty();
        }
    }
}
