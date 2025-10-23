/*
 * IndexingHeartbeat.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexing;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.IndexingSubspaces;
import com.apple.foundationdb.synchronizedsession.SynchronizedSessionLockedException;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Indexing Shared Heartbeats can be used to define and handle "active" indexing processes.
 * Every indexer should update its unique heartbeat during its indexing iteration. If the indexing session is optimized for
 * non-mutual (as defined by the indexing type, see {@link IndexBuildProto.IndexBuildIndexingStamp}), detecting an existing
 * active heartbeat will help preventing concurrent, conflicting, indexing attempts.
 * In addition, the heartbeats can be used by users to query activity status of ongoing indexing sessions.
 */
@API(API.Status.INTERNAL)
public class IndexingHeartbeat {
    // [prefix, indexerId] -> [creator info, create time, heartbeat time]
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(IndexingHeartbeat.class);
    public static final String INVALID_HEARTBEAT_INFO = "<< Invalid Heartbeat >>";

    final UUID indexerId;
    final String info;
    final long createTimeMilliseconds;
    final long leaseLength;
    final boolean allowMutual;

    public IndexingHeartbeat(final UUID indexerId, String info, long leaseLength, boolean allowMutual) {
        this.indexerId = indexerId;
        this.info = info;
        this.leaseLength = leaseLength;
        this.allowMutual = allowMutual;
        this.createTimeMilliseconds = nowMilliseconds();
    }

    public UUID getIndexerId() {
        return indexerId;
    }

    public void updateHeartbeat(@Nonnull FDBRecordStore store, @Nonnull Index index) {
        byte[] key = IndexingSubspaces.indexHeartbeatSubspaceBytes(store, index, indexerId);
        byte[] value = IndexBuildProto.IndexBuildHeartbeat.newBuilder()
                .setInfo(info)
                .setCreateTimeMilliseconds(createTimeMilliseconds)
                .setHeartbeatTimeMilliseconds(nowMilliseconds())
                .build().toByteArray();
        store.ensureContextActive().set(key, value);
    }

    public CompletableFuture<Void> checkAndUpdateHeartbeat(@Nonnull FDBRecordStore store, @Nonnull Index index) {
        // complete exceptionally if non-mutual, other exists
        if (allowMutual) {
            updateHeartbeat(store, index);
            return AsyncUtil.DONE;
        }

        final AsyncIterator<KeyValue> iterator = heartbeatsIterator(store, index);
        final long now = nowMilliseconds();
        return AsyncUtil.forEachRemaining(iterator, kv -> checkSingleHeartbeat(store, index, kv, now), store.getExecutor())
                .thenRun(() -> updateHeartbeat(store, index));
    }

    private void checkSingleHeartbeat(final @Nonnull FDBRecordStore store, final @Nonnull Index index, final KeyValue kv, final long now) {
        try {
            final UUID otherIndexerId = heartbeatKeyToIndexerId(store, index, kv.getKey());
            if (!otherIndexerId.equals(this.indexerId)) {
                final IndexBuildProto.IndexBuildHeartbeat otherHeartbeat = IndexBuildProto.IndexBuildHeartbeat.parseFrom(kv.getValue());
                final long age = now - otherHeartbeat.getHeartbeatTimeMilliseconds();
                if (age > TimeUnit.DAYS.toMillis(-1) && age < leaseLength) {
                    // Note that if this heartbeat's age is more than a day in the future, it is considered as bad data. A day seems to be
                    // long enough to tolerate reasonable clock skews between nodes.
                    // For practical reasons, this exception is backward compatible to the Synchronized Lock one
                    throw new SynchronizedSessionLockedException("Failed to initialize the session because of an existing session in progress")
                            .addLogInfo(LogMessageKeys.INDEXER_ID, indexerId)
                            .addLogInfo(LogMessageKeys.EXISTING_INDEXER_ID, otherIndexerId)
                            .addLogInfo(LogMessageKeys.AGE_MILLISECONDS, age)
                            .addLogInfo(LogMessageKeys.TIME_LIMIT_MILLIS, leaseLength);
                }
            }
        } catch (InvalidProtocolBufferException e) {
            if (logger.isWarnEnabled()) {
                logger.warn(KeyValueLogMessage.of("Bad indexing heartbeat item",
                        LogMessageKeys.KEY, kv.getKey(),
                        LogMessageKeys.VALUE, kv.getValue()));
            }
        }
    }

    public void clearHeartbeat(@Nonnull FDBRecordStore store, @Nonnull Index index) {
        store.ensureContextActive().clear(IndexingSubspaces.indexHeartbeatSubspaceBytes(store, index, indexerId));
    }

    public static void clearAllHeartbeats(@Nonnull FDBRecordStore store, @Nonnull Index index) {
        store.ensureContextActive().clear(IndexingSubspaces.indexHeartbeatSubspace(store, index).range());
    }

    public static CompletableFuture<Map<UUID, IndexBuildProto.IndexBuildHeartbeat>> getIndexingHeartbeats(FDBRecordStore store, Index index, int maxCount) {
        final Map<UUID, IndexBuildProto.IndexBuildHeartbeat> ret = new HashMap<>();
        final AsyncIterator<KeyValue> iterator = heartbeatsIterator(store, index);
        final AtomicInteger iterationCount = new AtomicInteger(0);
        return AsyncUtil.whileTrue(() -> iterator.onHasNext()
                        .thenApply(hasNext -> {
                            if (!hasNext) {
                                return false;
                            }
                            if (maxCount > 0 && maxCount < iterationCount.incrementAndGet()) {
                                return false;
                            }
                            final KeyValue kv = iterator.next();
                            final UUID otherIndexerId = heartbeatKeyToIndexerId(store, index, kv.getKey());
                            try {
                                final IndexBuildProto.IndexBuildHeartbeat otherHeartbeat = IndexBuildProto.IndexBuildHeartbeat.parseFrom(kv.getValue());
                                ret.put(otherIndexerId, otherHeartbeat);
                            } catch (InvalidProtocolBufferException e) {
                                // Let the caller know about this invalid heartbeat.
                                ret.put(otherIndexerId, IndexBuildProto.IndexBuildHeartbeat.newBuilder()
                                        .setInfo(INVALID_HEARTBEAT_INFO)
                                                .setCreateTimeMilliseconds(0)
                                                .setHeartbeatTimeMilliseconds(0)
                                        .build());
                            }
                            return true;
                        }), store.getExecutor())
                .thenApply(ignore -> ret);
    }

    public static CompletableFuture<Integer> clearIndexingHeartbeats(@Nonnull FDBRecordStore store, @Nonnull Index index, long minAgeMilliseconds, int maxIteration) {
        final AsyncIterator<KeyValue> iterator = heartbeatsIterator(store, index);
        final AtomicInteger deleteCount = new AtomicInteger(0);
        final AtomicInteger iterationCount = new AtomicInteger(0);
        final long now = nowMilliseconds();
        return AsyncUtil.whileTrue(() -> iterator.onHasNext()
                        .thenApply(hasNext -> {
                            if (!hasNext) {
                                return false;
                            }
                            if (maxIteration > 0 && maxIteration < iterationCount.incrementAndGet()) {
                                return false;
                            }
                            final KeyValue kv = iterator.next();
                            boolean shouldRemove;
                            try {
                                final IndexBuildProto.IndexBuildHeartbeat otherHeartbeat = IndexBuildProto.IndexBuildHeartbeat.parseFrom(kv.getValue());
                                // remove heartbeat if too old
                                shouldRemove = now >= otherHeartbeat.getHeartbeatTimeMilliseconds() +  minAgeMilliseconds;
                            } catch (InvalidProtocolBufferException e) {
                                // remove heartbeat if invalid
                                shouldRemove = true;
                            }
                            if (shouldRemove) {
                                store.ensureContextActive().clear(kv.getKey());
                                deleteCount.incrementAndGet();
                            }
                            return true;
                        }), store.getExecutor())
                .thenApply(ignore -> deleteCount.get());
    }

    private static AsyncIterator<KeyValue> heartbeatsIterator(FDBRecordStore store, Index index) {
        return store.getContext().ensureActive().getRange(IndexingSubspaces.indexHeartbeatSubspace(store, index).range()).iterator();
    }

    private static UUID heartbeatKeyToIndexerId(FDBRecordStore store, Index index, byte[] key) {
        return IndexingSubspaces.indexHeartbeatSubspace(store, index).unpack(key).getUUID(0);
    }

    private static long nowMilliseconds() {
        return System.currentTimeMillis();
    }
}
