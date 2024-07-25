/*
 * LuceneGetMetadataInfo.java
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
import com.apple.foundationdb.record.provider.foundationdb.IndexOperation;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Get metadata information about a given lucene index.
 * <p>
 *     This is currently intended to be used for debugging purposes.
 * </p>
 */
@API(API.Status.INTERNAL)
public class LuceneGetMetadataInfo extends IndexOperation {

    @Nonnull
    private final Tuple groupingKey;
    @Nullable
    private final Integer partitionId;
    private final boolean justPartitionInfo;

    /**
     * Create a new operation to get metadata about the lucene index.
     * <p>
     *     The parameters can be used to limit the scope of information gathered so that it can fit in a single
     *     transaction.
     * </p>
     * @param groupingKey the grouping key, or an empty {@code Tuple} if this index is not grouped
     * @param partitionId if {@code null}, this will return information for all partitions, otherwise just the given partition
     * @param justPartitionInfo if {@code true} then only the partition info will be fetched, otherwise information from
     * lucene itself will be fetched
     */
    public LuceneGetMetadataInfo(@Nonnull Tuple groupingKey,
                                 @Nullable Integer partitionId,
                                 boolean justPartitionInfo) {
        this.groupingKey = groupingKey;
        this.partitionId = partitionId;
        this.justPartitionInfo = justPartitionInfo;
    }

    /**
     * The grouping key to inspect.
     * @return the grouping key or an empty {@code Tuple} if this index is not grouped
     */
    @Nonnull
    public Tuple getGroupingKey() {
        return groupingKey;
    }

    /**
     * The partition id to inpsect.
     * @return the partition id to inspect or {@code null} to inspect all partitions.
     */
    @Nullable
    public Integer getPartitionId() {
        return partitionId;
    }

    /**
     * Just fetch the partition info, and not the information for each partition.
     * @return if {@code true} only the partition info will be fetched, otherwise it will fetch the information for
     * the lucene indexes
     */
    public boolean isJustPartitionInfo() {
        return justPartitionInfo;
    }

    @Override
    public String toString() {
        return "LuceneGetMetadataInfo{" +
                "groupingKey=" + groupingKey +
                ", partitionId=" + partitionId +
                ", justPartitionInfo=" + justPartitionInfo +
                '}';
    }
}
