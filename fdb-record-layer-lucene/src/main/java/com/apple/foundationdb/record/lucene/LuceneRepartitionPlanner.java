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
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
     * @param maxPartitionId max partition id (in case a new partition should be created)
     * @param candidatePartition current partition
     * @param olderPartition next older partition of current partition
     * @param newerPartition next newer partition of current partition
     * @param repartitionDocumentCount max allowed documents to move per iteration
     * @return repartitioning context instance ({@link RepartitioningContext}
     */
    @Nonnull
    RepartitioningContext determinePartitionRepartitioningAction(@Nonnull final Tuple groupingKey,
                                                                 int maxPartitionId,
                                                                 @Nonnull final LucenePartitionInfoProto.LucenePartitionInfo candidatePartition,
                                                                 @Nullable LucenePartitionInfoProto.LucenePartitionInfo olderPartition,
                                                                 @Nullable LucenePartitionInfoProto.LucenePartitionInfo newerPartition,
                                                                 int repartitionDocumentCount) {
        // reset to defaults
        RepartitioningContext repartitioningContext = new RepartitioningContext(groupingKey, maxPartitionId, candidatePartition);

        // candidate partition's current doc count
        final int currentPartitionCount = candidatePartition.getCount();
        if (currentPartitionCount >= indexPartitionLowWatermark && currentPartitionCount <= indexPartitionHighWatermark) {
            // repartitioning not required
            return repartitioningContext;
        }

        // here: repartitioning is required
        repartitioningContext.sourcePartition = candidatePartition;

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
                repartitioningContext.countToMove = (indexPartitionHighWatermark - indexPartitionLowWatermark) / 2;
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
        LucenePartitionInfoProto.LucenePartitionInfo sourcePartition;
        boolean emptyingPartition;
        int countToMove;
        int maxPartitionId;
        boolean newBoundaryRecordPresent;
        RepartitioningAction action;

        RepartitioningContext(@Nonnull final Tuple groupingKey,
                              int maxPartitionId,
                              @Nonnull final LucenePartitionInfoProto.LucenePartitionInfo sourcePartition) {
            this.groupingKey = groupingKey;
            this.maxPartitionId = maxPartitionId;
            this.sourcePartition = sourcePartition;
            this.action = RepartitioningAction.NOT_REQUIRED;
        }
    }
}
