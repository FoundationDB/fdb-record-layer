/*
 * LucenePartitionPlanner.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * Manage repartitioning details (merging small partitions and splitting large ones).
 */
@API(API.Status.EXPERIMENTAL)
public class LuceneRepartitionPlanner {
    private final int indexPartitionLowWatermark;
    private final int indexPartitionHighWatermark;

    public LuceneRepartitionPlanner(int indexPartitionLowWatermark, int indexPartitionHighWatermark) {
        this.indexPartitionLowWatermark = indexPartitionLowWatermark;
        this.indexPartitionHighWatermark = indexPartitionHighWatermark;
    }

    /**
     * Determine the repartitioning action and parameters for a given partition.
     *
     * @param groupingKey grouping key
     * @param allPartitions all partition info, sorted in descending key order
     * @param currentPartitionPosition position of current partition in list
     * @param repartitionDocumentCount max allowed documents to move per iteration
     * @return repartitioning context instance ({@link RepartitioningContext}
     */
    @Nonnull
    RepartitioningContext determineRepartitioningAction(@Nonnull final Tuple groupingKey,
                                                        @Nonnull final List<LucenePartitionInfoProto.LucenePartitionInfo> allPartitions,
                                                        int currentPartitionPosition,
                                                        int repartitionDocumentCount) {
        int maxPartitionId = allPartitions.stream().mapToInt(LucenePartitionInfoProto.LucenePartitionInfo::getId).max().orElse(0);
        LucenePartitionInfoProto.LucenePartitionInfo candidatePartition = allPartitions.get(currentPartitionPosition);

        Pair<LucenePartitionInfoProto.LucenePartitionInfo, LucenePartitionInfoProto.LucenePartitionInfo> neighborPartitions =
                LucenePartitioner.getPartitionNeighbors(allPartitions, currentPartitionPosition);

        // partitions sorted by key descending
        LucenePartitionInfoProto.LucenePartitionInfo olderPartition = neighborPartitions.getRight();
        LucenePartitionInfoProto.LucenePartitionInfo newerPartition = neighborPartitions.getLeft();

        RepartitioningContext repartitioningContext = new RepartitioningContext(groupingKey,
                maxPartitionId,
                candidatePartition,
                olderPartition,
                newerPartition);

        // candidate partition's current doc count
        final int currentPartitionCount = candidatePartition.getCount();
        if (currentPartitionCount >= indexPartitionLowWatermark && currentPartitionCount <= indexPartitionHighWatermark) {
            // repartitioning not required
            return repartitioningContext;
        }

        if (currentPartitionCount < indexPartitionLowWatermark) {
            if (currentPartitionCount > repartitionDocumentCount) {
                repartitioningContext.countToMove = repartitionDocumentCount + 1;
                repartitioningContext.newBoundaryRecordPresent = true;
            } else {
                repartitioningContext.countToMove = currentPartitionCount;
                repartitioningContext.emptyingPartition = true;
            }

            if (olderPartition != null && olderPartition.getCount() + currentPartitionCount <= indexPartitionHighWatermark) {
                repartitioningContext.action = RepartitioningAction.MERGE_INTO_OLDER;
            } else if (newerPartition != null && newerPartition.getCount() + currentPartitionCount <= indexPartitionHighWatermark) {
                repartitioningContext.action = RepartitioningAction.MERGE_INTO_NEWER;
            } else if (olderPartition != null && newerPartition != null && (2 * indexPartitionHighWatermark - olderPartition.getCount() - newerPartition.getCount()) >= currentPartitionCount ) {
                repartitioningContext.action = RepartitioningAction.MERGE_INTO_BOTH;
                // split current and merge into newerPartition and olderPartition.
                // for this iteration, we do a partial move into olderPartition; next iteration(s) would eventually
                // move the rest to newerPartition
                int olderPartitionCapacity = indexPartitionHighWatermark - olderPartition.getCount();
                repartitioningContext.countToMove = Math.min(olderPartitionCapacity, repartitionDocumentCount);
                if (currentPartitionCount > repartitioningContext.countToMove) {
                    repartitioningContext.countToMove++;
                    repartitioningContext.newBoundaryRecordPresent = true;
                }
                repartitioningContext.emptyingPartition = false;
            } else {
                repartitioningContext.action = RepartitioningAction.NO_CAPACITY_FOR_MERGE;
            }
        } else {
            // partition is above high watermark
            repartitioningContext.countToMove = 1 + Math.min(repartitionDocumentCount, indexPartitionHighWatermark);
            // sanity: in case someone passes a high enough repartitionDocumentCount such that removing
            // this many docs would flip the partition from being above the high watermark to being below
            // the low watermark
            if (currentPartitionCount - repartitioningContext.countToMove < indexPartitionLowWatermark) {
                repartitioningContext.countToMove = Math.max(1, ((indexPartitionHighWatermark - indexPartitionLowWatermark) / 2));
            }
            repartitioningContext.newBoundaryRecordPresent = true;
            repartitioningContext.action = RepartitioningAction.OVERFLOW;
        }

        return repartitioningContext;
    }


    enum RepartitioningAction {
        /**
         * repartitioning is not required.
         */
        NOT_REQUIRED,
        /**
         * partition is over the high watermark.
         */
        OVERFLOW,
        /**
         * partition is under the low watermark, and the next newer partition has capacity to absorb it.
         */
        MERGE_INTO_NEWER,
        /**
         * partition is under the low watermark, and the next older partition has capacity to absorb it.
         */
        MERGE_INTO_OLDER,
        /**
         * partition is under the low watermark, and the next older and newer partitions, together, have capacity
         * to absorb it. The caller will choose either the older or the newer to merge partially into and then
         * continue the iterations.
         */
        MERGE_INTO_BOTH,
        /**
         * partition is under the low watermark, but its immediate neighbors have no capacity to absorb it.
         */
        NO_CAPACITY_FOR_MERGE
    }

    /**
     * Convenience collection of data needed for repartitioning.
     */
    @VisibleForTesting
    public static class RepartitioningContext {
        @Nonnull Tuple groupingKey;
        @Nonnull final LucenePartitionInfoProto.LucenePartitionInfo sourcePartition;
        @Nullable final LucenePartitionInfoProto.LucenePartitionInfo olderPartition;
        @Nullable final LucenePartitionInfoProto.LucenePartitionInfo newerPartition;
        boolean emptyingPartition;
        int countToMove;
        int maxPartitionId;
        boolean newBoundaryRecordPresent;
        RepartitioningAction action;

        RepartitioningContext(@Nonnull final Tuple groupingKey,
                              int maxPartitionId,
                              @Nonnull final LucenePartitionInfoProto.LucenePartitionInfo sourcePartition,
                              @Nullable final LucenePartitionInfoProto.LucenePartitionInfo olderPartition,
                              @Nullable final LucenePartitionInfoProto.LucenePartitionInfo newerPartition) {
            this.groupingKey = groupingKey;
            this.maxPartitionId = maxPartitionId;
            this.sourcePartition = sourcePartition;
            this.olderPartition = olderPartition;
            this.newerPartition = newerPartition;
            this.action = RepartitioningAction.NOT_REQUIRED;
        }

        @Override
        public String toString() {
            return "RepartitioningContext{" +
                    "groupingKey=" + groupingKey +
                    ", sourcePartition=" + sourcePartition +
                    ", olderPartition=" + olderPartition +
                    ", newerPartition=" + newerPartition +
                    ", emptyingPartition=" + emptyingPartition +
                    ", countToMove=" + countToMove +
                    ", maxPartitionId=" + maxPartitionId +
                    ", newBoundaryRecordPresent=" + newBoundaryRecordPresent +
                    ", action=" + action +
                    '}';
        }
    }
}
