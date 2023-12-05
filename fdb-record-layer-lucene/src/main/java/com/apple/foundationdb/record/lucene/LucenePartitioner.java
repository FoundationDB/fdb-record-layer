/*
 * LucenePartitioner.java
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer.Waits.WAIT_LOAD_LUCENE_PARTITION_METADATA;

/**
 * Manage partitioning info for a <b>logical</b>, partitioned lucene index, in which each partition is a separate physical lucene index.
 */
@API(API.Status.EXPERIMENTAL)
public class LucenePartitioner {

    public static final int PARTITION_META_SUBSPACE = 0;
    public static final int PARTITION_DATA_SUBSPACE = 1;
    private final IndexMaintainerState state;
    private final boolean partitioningEnabled;
    private final String partitionTimestampFieldName;
    private final Map<Tuple, List<LucenePartitionInfoProto.LucenePartitionInfo>> partitionMetaInfoCache = new HashMap<>();

    public LucenePartitioner(@Nonnull IndexMaintainerState state) {
        this.state = state;
        partitionTimestampFieldName = state.index.getOption(com.apple.foundationdb.record.metadata.IndexOptions.TEXT_DOCUMENT_PARTITION_TIMESTAMP);
        this.partitioningEnabled = partitionTimestampFieldName != null;
    }

    /**
     * return the partition ID on which to run a query, given a grouping key.
     * For now, the most recent partition is returned.
     *
     * @param grouping grouping key
     * @return partition id, or null if partitioning isn't enabled
     */
    public Integer selectQueryPartitionId(Tuple grouping) {
        Integer partitionId = null;
        if (isPartitioningEnabled()) {
            partitionId = 0;
            List<LucenePartitionInfoProto.LucenePartitionInfo> partitions = getSortedPartitionMetadata(grouping);
            if (partitions != null && !partitions.isEmpty()) {
                partitionId = partitions.get(partitions.size() - 1).getId();
            }
        }
        return partitionId;
    }

    /**
     * return the partition to which a document with a given timestamp should be assigned.
     * <b>Note:</b> {@link #loadPartitioningMetadata(Set)} should be called
     * before the first call to this method.
     * If no partition currently exists, a new one is created.
     *
     * @param grouping document grouping key
     * @param timestamp document timestamp
     * @return assigned partition metadata
     */
    public LucenePartitionInfoProto.LucenePartitionInfo assignWritePartition(Tuple grouping, long timestamp) {
        List<LucenePartitionInfoProto.LucenePartitionInfo> partitions = partitionMetaInfoCache.get(grouping);
        if (partitions == null) {
            // if loadPartitioningMetadata(...) was called earlier, then this should never happen
            throw new RuntimeException("Internal Error");
        }

        if (partitions.isEmpty()) {
            // very first partition
            partitions.add(LucenePartitionInfoProto.LucenePartitionInfo.newBuilder()
                    .setId(0)
                    .setCount(0)
                    .setFrom(timestamp)
                    .setTo(timestamp)
                    .build());
            return partitions.get(0);
        }

        return selectWritePartition(partitions, timestamp);
        // TODO: check and split the assigned partition if it's too big
    }

    /**
     * add a timestamp to the metadata of a given partition and save to db.
     * The <code>count</code> will be incremented, and the <code>from</code> or <code>to</code> timestamps will
     * be adjusted if applicable.
     *
     * @param grouping  grouping key
     * @param partition partition metadata
     * @param timestamp document timestamp
     */
    public void addToAndSavePartitionMetadata(final Tuple grouping, final LucenePartitionInfoProto.LucenePartitionInfo partition, final Long timestamp) {
        LucenePartitionInfoProto.LucenePartitionInfo.Builder builder = partition.toBuilder();
        builder.setCount(partition.getCount() + 1);
        if (timestamp < partition.getFrom()) {
            builder.setFrom(timestamp);
        }
        if (timestamp > partition.getTo()) {
            builder.setTo(timestamp);

        }
        savePartitionMetadataAndUpdateCache(grouping, builder);
    }

    /**
     * remove a document from a partition metadata and save to db.
     * Note that only the document count is changed (decremented). <code>from</code> and <code>to</code> are unchanged.
     *
     * @param grouping grouping key
     * @param partition partition metadata object
     */
    public void removeFromAndSavePartitionMetadata(final Tuple grouping, final LucenePartitionInfoProto.LucenePartitionInfo partition) {
        LucenePartitionInfoProto.LucenePartitionInfo.Builder builder = partition.toBuilder();
        // note that the to/from of the partition do not get updated, since that would require us to know what the next potential boundary
        // value(s) are.
        builder.setCount(partition.getCount() - 1);

        if (builder.getCount() < 0) {
            // should never happen
            throw new RecordCoreException("Issue updating Lucene partition metadata (resulting count < 0)", "partitionId", partition.getId());
        }
        savePartitionMetadataAndUpdateCache(grouping, builder);
    }

    /**
     * given a grouping key and a partition metadata object builder, build and save the partition metadata object in the db, and update the local cache.
     *
     * @param grouping grouping key
     * @param builder proto builder
     */
    private void savePartitionMetadataAndUpdateCache(final Tuple grouping, final LucenePartitionInfoProto.LucenePartitionInfo.Builder builder) {
        byte[] partitionKey = state.indexSubspace.subspace(grouping).pack(Tuple.from(PARTITION_META_SUBSPACE, builder.getId()));
        LucenePartitionInfoProto.LucenePartitionInfo updatedPartition = builder.build();
        state.context.ensureActive().set(partitionKey, updatedPartition.toByteArray());

        // update cache
        List<LucenePartitionInfoProto.LucenePartitionInfo> partitions = partitionMetaInfoCache.get(grouping);

        // partition ids are in order, in the list
        for (int i = 0; i < partitions.size(); i++) {
            if (partitions.get(i).getId() == updatedPartition.getId()) {
                partitions.set(i, updatedPartition);
                return;
            }
        }

        // should be unreachable
    }

    /**
     * Selects the correct partition to assign a document to.
     *
     * @param partitions candidate partitions
     * @param timestamp timestamp of the document
     * @return assigned partition
     */
    private LucenePartitionInfoProto.LucenePartitionInfo selectWritePartition(final List<LucenePartitionInfoProto.LucenePartitionInfo> partitions, final long timestamp) {

        // candidate partitions must include at least one
        Preconditions.checkNotNull(partitions);
        Preconditions.checkArgument(!partitions.isEmpty());

        // ----------------------------------------------------------
        // The partitioning rules are as follows (per requirements doc):
        //
        // When adding an email
        //
        // If its date is within a range of an existing partition it will get added to that partition.
        // If it is between two partitions, it will get added to the smaller one, expanding that partitions date range
        // If it is older than the oldest partition’s min date it will get added there.
        // If it is newer than the newest partition’s max date it will get added there.
        //
        // If the partition that we just added to is larger than “max” size we will probabilistically split it, with a growing probability as the size increases.
        // The splitting will happen asynchronously in the background as it moves a lot of data around
        //
        // If a partition grows to the point where we need to split it we will queue work to split that partition in two.
        //
        // For getting new emails this should generally result in filling a partition to the max size, and then start adding to a new partition.
        //
        // For import, we may find ourselves getting into a situation where partitions are getting too big that are not the most recent (i.e. we got their most recent email first, and are going backwards).
        // It may make sense to change it so that getting an email older than the oldest email will create a new partition, but need to verify the implications of that.
        //
        // Just starting a new partition with a single email would mean that we would have to combine relevancy search results or autocomplete search results, otherwise, they may just get results from 1 email, which would be bad.
        // ----------------------------------------------------------

        // the current implementation below partially implements the above (minus splitting partitions). The rest will be added later (TODO)

        // shortcut
        if (partitions.size() == 1) {
            return partitions.get(0);
        }

        // note that the partitions may not necessarily be in timestamp order (due to later splits etc.)
        partitions.sort(Comparator.comparingLong(LucenePartitionInfoProto.LucenePartitionInfo::getFrom));

        // timestamp is older than the oldest partition
        if (timestamp < partitions.get(0).getFrom()) {
            return partitions.get(0);
        }

        // timestamp is newer than the newest partition
        if (timestamp > partitions.get(partitions.size() - 1).getTo()) {
            return partitions.get(partitions.size() - 1);
        }

        // the value is within a partition or between partitions
        for (int i = 0; i < partitions.size() - 1; i++) {
            LucenePartitionInfoProto.LucenePartitionInfo currentPartition = partitions.get(i);
            LucenePartitionInfoProto.LucenePartitionInfo nextPartition = partitions.get(i + 1);

            if (timestamp >= currentPartition.getFrom() && timestamp <= currentPartition.getTo()) {
                return currentPartition;
            } else if (timestamp > currentPartition.getTo() && timestamp < nextPartition.getFrom()) {
                // Value falls between two partitions, choose the one with the smaller count
                return currentPartition.getCount() < nextPartition.getCount() ? currentPartition : nextPartition;
            }
        }

        // unreachable
        return partitions.get(partitions.size() - 1);
    }

    /**
     * get whether this index has partitioning enabled.
     *
     * @return true if partitioning is enabled
     */
    public boolean isPartitioningEnabled() {
        return partitioningEnabled;
    }

    /**
     * get the record field name that contains the document timestamp, which will be used to determine
     * partition assignment. The record field name may be qualified, when nested (e.g. <code>nestedRecord.fieldN</code>
     *
     * @return field name
     */
    public String getPartitionTimestampFieldName() {
        return partitionTimestampFieldName;
    }

    /**
     * sync get the partition metadata for a given grouping key, sorted by <code>from</code> timestamp.
     *
     * @param grouping grouping key
     * @return partition metadata
     */
    public List<LucenePartitionInfoProto.LucenePartitionInfo> getSortedPartitionMetadata(Tuple grouping) {
        List<LucenePartitionInfoProto.LucenePartitionInfo> result = state.context
                .asyncToSync(WAIT_LOAD_LUCENE_PARTITION_METADATA, loadPartitioningMetadata(grouping));

        Objects.requireNonNull(result).sort(Comparator.comparingLong(LucenePartitionInfoProto.LucenePartitionInfo::getFrom));
        return result;
    }

    /**
     * load all partition metadata for a set of grouping keys and cache them in this instance.
     * This call needs to be made first, before other assignment and update methods (e.g.
     * {@link #assignWritePartition(Tuple, long)} and {@link #addToAndSavePartitionMetadata(Tuple, LucenePartitionInfoProto.LucenePartitionInfo, Long)})
     *
     * @param groupings set of grouping keys for which to load partition metadata
     * @return void future
     */
    public CompletableFuture<Void> loadPartitioningMetadata(Set<Tuple> groupings) {
        List<CompletableFuture<List<LucenePartitionInfoProto.LucenePartitionInfo>>> futures =
                groupings.stream().map(this::loadPartitioningMetadata).collect(Collectors.toList());
        return AsyncUtil.whenAll(futures);
    }

    /**
     * load partitioning metadata for a given grouping key, and cache it.
     *
     * @param grouping grouping key
     * @return future list of partitioning metadata
     */
    private CompletableFuture<List<LucenePartitionInfoProto.LucenePartitionInfo>> loadPartitioningMetadata(Tuple grouping) {
        if (partitionMetaInfoCache.containsKey(grouping)) {
            return CompletableFuture.completedFuture(partitionMetaInfoCache.get(grouping));
        }

        List<LucenePartitionInfoProto.LucenePartitionInfo> result = new ArrayList<>();

        // get all partition metadata records
        final AsyncIterable<KeyValue> rangeIterable = state.context.ensureActive()
                .getRange(state.indexSubspace.subspace(Tuple.from(PARTITION_META_SUBSPACE)).range(), ReadTransaction.ROW_LIMIT_UNLIMITED, false, StreamingMode.WANT_ALL);
        return AsyncUtil.forEach(rangeIterable, kv -> {
            try {
                result.add(LucenePartitionInfoProto.LucenePartitionInfo.parseFrom(kv.getValue()));
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }).thenApply($ -> {
            partitionMetaInfoCache.put(grouping, result);
            return result;
        });
    }

    /**
     * find the partition whose range contains the given timestamp, for a given grouping key.
     *
     * @param grouping  grouping key
     * @param timestamp timestamp
     * @return partition, or null if no suitable partition found
     */
    public LucenePartitionInfoProto.LucenePartitionInfo locateInPartition(final Tuple grouping, final Long timestamp) {
        Preconditions.checkNotNull(timestamp);

        List<LucenePartitionInfoProto.LucenePartitionInfo> partitionsForGroup = partitionMetaInfoCache.get(grouping);
        if (partitionsForGroup == null) {
            return null;
        }

        Optional<LucenePartitionInfoProto.LucenePartitionInfo> partitionOpt = partitionsForGroup
                .stream()
                .filter(p -> p.getFrom() <= timestamp && p.getTo() >= timestamp)
                .findAny();

        return partitionOpt.orElse(null);
    }
}
