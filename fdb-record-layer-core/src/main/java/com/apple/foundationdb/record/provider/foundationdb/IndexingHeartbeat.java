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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.synchronizedsession.SynchronizedSessionLockedException;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexingHeartbeat {
    // [prefix, indexerId] -> [indexing-type, genesis time, heartbeat time]
    final UUID indexerId;
    final String info;
    final long genesisTimeMilliseconds;
    final long leaseLength;
    final boolean allowMutual;

    public IndexingHeartbeat(final UUID indexerId, String info, long leaseLength, boolean allowMutual) {
        this.indexerId = indexerId;
        this.info = info;
        this.leaseLength = leaseLength;
        this.allowMutual = allowMutual;
        this.genesisTimeMilliseconds = nowMilliseconds();
    }

    public void updateHeartbeat(@Nonnull FDBRecordStore store, @Nonnull Index index) {
        byte[] key = IndexingSubspaces.indexheartbeatSubspace(store, index, indexerId).pack();
        byte[] value = IndexBuildProto.IndexBuildHeartbeat.newBuilder()
                .setInfo(info)
                .setGenesisTimeMilliseconds(genesisTimeMilliseconds)
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
        return AsyncUtil.whileTrue(() -> iterator.onHasNext()
                        .thenApply(hasNext -> {
                            if (!hasNext) {
                                return false;
                            }
                            final KeyValue kv = iterator.next();
                            try {
                                final UUID otherIndexerId = heartbeatKeyToIndexerId(store, index, kv.getKey());
                                if (!otherIndexerId.equals(this.indexerId)) {
                                    final IndexBuildProto.IndexBuildHeartbeat otherHeartbeat = IndexBuildProto.IndexBuildHeartbeat.parseFrom(kv.getValue());
                                    final long age = now - otherHeartbeat.getHeartbeatTimeMilliseconds();
                                    if (age > 0 && age < leaseLength) {
                                        // For practical reasons, this exception is backward compatible to the Synchronized Lock one
                                        throw new SynchronizedSessionLockedException("Failed to initialize the session because of an existing session in progress")
                                                .addLogInfo(LogMessageKeys.INDEXER_ID, indexerId)
                                                .addLogInfo(LogMessageKeys.EXISTING_INDEXER_ID, otherIndexerId)
                                                .addLogInfo(LogMessageKeys.AGE_MILLISECONDS, age)
                                                .addLogInfo(LogMessageKeys.TIME_LIMIT_MILLIS, leaseLength);
                                    }
                                }
                            } catch (InvalidProtocolBufferException e) {
                                throw new RuntimeException(e);
                            }
                            return true;
                        }))
                .thenApply(ignore -> {
                    updateHeartbeat(store, index);
                    return null;
                });
    }

    public void clearHeartbeat(@Nonnull FDBRecordStore store, @Nonnull Index index) {
        store.ensureContextActive().clear(IndexingSubspaces.indexheartbeatSubspace(store, index, indexerId).pack());
    }

    public static void clearAllHeartbeats(@Nonnull FDBRecordStore store, @Nonnull Index index) {
        store.ensureContextActive().clear(IndexingSubspaces.indexheartbeatSubspace(store, index).range());
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
                                        .setInfo("<< Invalid Heartbeat >>")
                                        .build());
                            }
                            return true;
                        }))
                .thenApply(ignore -> ret);
    }

    public static CompletableFuture<Integer> clearIndexingHeartbeats(@Nonnull FDBRecordStore store, @Nonnull Index index, long minAgenMilliseconds, int maxIteration) {
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
                                shouldRemove = now + minAgenMilliseconds >= otherHeartbeat.getHeartbeatTimeMilliseconds();
                            } catch (InvalidProtocolBufferException e) {
                                // remove heartbeat if invalid
                                shouldRemove = true;
                            }
                            if (shouldRemove) {
                                store.ensureContextActive().clear(kv.getKey());
                                deleteCount.incrementAndGet();
                            }
                            return true;
                        }))
                .thenApply(ignore -> deleteCount.get());
    }

    private static AsyncIterator<KeyValue> heartbeatsIterator(FDBRecordStore store, Index index) {
        return store.getContext().ensureActive().snapshot().getRange(IndexingSubspaces.indexheartbeatSubspace(store, index).range()).iterator();
    }

    private static UUID heartbeatKeyToIndexerId(FDBRecordStore store, Index index, byte[] key) {
        return IndexingSubspaces.indexheartbeatSubspace(store, index).unpack(key).getUUID(0);
    }

    private static long nowMilliseconds() {
        return System.currentTimeMillis();
    }
}
