/*
 * LuceneRepartitionPlannerTest.java
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * tests for the lucene repartition planner.
 */
@Tag(Tags.RequiresFDB)
public class LuceneRepartitionPlannerTest {
    @ParameterizedTest
    @MethodSource(value = "com.apple.foundationdb.record.lucene.LuceneIndexTest#simplePartitionConsolidationTest")
    public void luceneRepartitionPlannerTest(int lowWatermark,
                                             int highWatermark,
                                             int repartitionDocumentCount,
                                             int[] allPartitionCounts,
                                             int[] countExpectations) {
        Tuple groupingKey = Tuple.from(1L);
        LuceneRepartitionPlanner repartitionPlanner = new LuceneRepartitionPlanner(lowWatermark, highWatermark);
        int maxPartitionId = allPartitionCounts.length;

        // shuffle partition ids so they're not sorted relative to their respective partition keys
        List<Integer> partitionIds = IntStream.range(0, allPartitionCounts.length).boxed().collect(Collectors.toList());
        Collections.shuffle(partitionIds);

        List<LucenePartitionInfoProto.LucenePartitionInfo> allPartitions = new ArrayList<>();
        for (int i = allPartitionCounts.length - 1; i >= 0; i--) { // the planner expects list in descending order
            allPartitions.add(LucenePartitionInfoProto.LucenePartitionInfo.newBuilder()
                    .setCount(allPartitionCounts[i])
                    .setId(partitionIds.get(i))
                    .setFrom(ByteString.copyFrom(Tuple.from(i * 1000).pack()))
                    .setTo(ByteString.copyFrom(Tuple.from(i * 1000 + 999).pack()))
                    .build());
        }

        int totalMoved;
        do {
            totalMoved = 0;
            for (int i = 0; i < allPartitions.size(); i++) {
                LucenePartitionInfoProto.LucenePartitionInfo currentPartitionInfo = allPartitions.get(i);

                if (currentPartitionInfo.getCount() == 0) {
                    continue;
                }

                Pair<LucenePartitionInfoProto.LucenePartitionInfo, LucenePartitionInfoProto.LucenePartitionInfo> neighborPartitions =
                        LucenePartitioner.getPartitionNeighbors(allPartitions, i);

                LucenePartitionInfoProto.LucenePartitionInfo newerPartition = neighborPartitions.getLeft();
                LucenePartitionInfoProto.LucenePartitionInfo olderPartition = neighborPartitions.getRight();

                LuceneRepartitionPlanner.RepartitioningContext repartitioningContext = repartitionPlanner.determineRepartitioningAction(groupingKey,
                        allPartitions, i, repartitionDocumentCount);

                if (repartitioningContext.action == LuceneRepartitionPlanner.RepartitioningAction.NOT_REQUIRED ||
                        repartitioningContext.action == LuceneRepartitionPlanner.RepartitioningAction.NO_CAPACITY_FOR_MERGE) {
                    continue;
                }

                int actualCountToMove = repartitioningContext.countToMove;
                if (!repartitioningContext.emptyingPartition && repartitioningContext.newBoundaryRecordPresent) {
                    actualCountToMove--;
                }

                long currentFrom = Tuple.fromBytes(currentPartitionInfo.getFrom().toByteArray()).getLong(0);
                switch (repartitioningContext.action) {
                    case MERGE_INTO_BOTH:
                    case MERGE_INTO_OLDER:
                        assertNotNull(olderPartition);
                        assertFalse(olderPartition.getCount() + actualCountToMove > highWatermark);
                        // "move" docs to older partition
                        allPartitions.set(i + 1, olderPartition.toBuilder().setCount(olderPartition.getCount() + actualCountToMove).build());
                        totalMoved += actualCountToMove;
                        break;
                    case MERGE_INTO_NEWER:
                        assertNotNull(newerPartition);
                        assertFalse(newerPartition.getCount() + actualCountToMove > highWatermark);
                        // "move" docs to newer partition
                        allPartitions.set(i - 1, newerPartition.toBuilder().setCount(newerPartition.getCount() + actualCountToMove).build());
                        totalMoved += actualCountToMove;
                        break;
                    case OVERFLOW:
                        if (olderPartition != null && olderPartition.getCount() + actualCountToMove <= highWatermark) {
                            assertFalse(olderPartition.getCount() + actualCountToMove > highWatermark);
                            // "move" to older partition
                            allPartitions.set(i + 1, olderPartition.toBuilder().setCount(olderPartition.getCount() + actualCountToMove).build());
                        } else {
                            assertFalse(actualCountToMove > highWatermark);
                            // "create" new overflow partition
                            LucenePartitionInfoProto.LucenePartitionInfo newOverflowPartition = LucenePartitionInfoProto.LucenePartitionInfo
                                    .newBuilder()
                                    .setCount(actualCountToMove)
                                    .setId(maxPartitionId++)
                                    .setFrom(ByteString.copyFrom(Tuple.from(currentFrom).pack())) // dummy
                                    .setTo(ByteString.copyFrom(Tuple.from(currentFrom + actualCountToMove).pack())) // dummy
                                    .build();
                            allPartitions.add(newOverflowPartition);
                            // re-sort
                            allPartitions.sort(Collections.reverseOrder(Comparator.comparing(p -> Tuple.fromBytes(p.getFrom().toByteArray()))));
                        }
                        totalMoved += actualCountToMove;
                        break;
                    default:
                        break;
                }
                // assert we never move out of the current partition more than its size
                assertTrue(actualCountToMove <= currentPartitionInfo.getCount());
                if (actualCountToMove > 0) {
                    allPartitions.set(i, currentPartitionInfo.toBuilder()
                            .setCount(currentPartitionInfo.getCount() - actualCountToMove)
                            .setFrom(ByteString.copyFrom(Tuple.from(currentFrom + actualCountToMove).pack()))
                            .build());
                    allPartitions = allPartitions.stream().filter(pInfo -> pInfo.getCount() > 0).collect(Collectors.toList());
                    break;
                }
            }
        } while (totalMoved > 0);

        // back to Arguments order
        Collections.reverse(allPartitions);

        int[] actualCounts = allPartitions.stream().mapToInt(LucenePartitionInfoProto.LucenePartitionInfo::getCount).toArray();
        assertArrayEquals(countExpectations, actualCounts);
    }

}
