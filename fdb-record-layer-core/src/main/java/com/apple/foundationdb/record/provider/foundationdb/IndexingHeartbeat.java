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
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexingHeartbeat {
    // [prefix, xid] -> [indexing-type, genesis time, heartbeat time]
    final UUID indexerId;
    final IndexBuildProto.IndexBuildIndexingStamp.Method indexingMethod;
    final long genesisTimeMilliseconds;
    final long leaseLength;

    public IndexingHeartbeat(final UUID indexerId, IndexBuildProto.IndexBuildIndexingStamp.Method indexingMethod, long leaseLength) {
        this.indexerId = indexerId;
        this.indexingMethod = indexingMethod;
        this.leaseLength = leaseLength;
        this.genesisTimeMilliseconds = nowMilliseconds();
    }

    public void updateHeartbeat(@Nonnull FDBRecordStore store, @Nonnull Index index) {
        byte[] key = IndexingSubspaces.indexheartbeatSubspace(store, index, indexerId).pack();
        byte[] value = IndexBuildProto.IndexBuildHeartbeat.newBuilder()
                .setMethod(indexingMethod)
                .setGenesisTimeMilliseconds(genesisTimeMilliseconds)
                .setHeartbeatTimeMilliseconds(nowMilliseconds())
                .build().toByteArray();
        store.ensureContextActive().set(key, value);
    }

    public CompletableFuture<Void> checkAndUpdateHeartbeat(@Nonnull FDBRecordStore store, @Nonnull Index index) {
        // complete exceptionally if non-mutual, other exists
        switch (indexingMethod) {
            case SCRUB_REPAIR:
            case MUTUAL_BY_RECORDS:
                return AsyncUtil.DONE;

            case BY_RECORDS:
            case MULTI_TARGET_BY_RECORDS:
            case BY_INDEX:
                final AsyncIterator<KeyValue> iterator = heartbeatsIterator(store, index);
                return AsyncUtil.whileTrue(() -> iterator.onHasNext()
                        .thenApply(hasNext -> {
                            if (!hasNext) {
                                return false;
                            }
                            validateNonCompetingHeartbeat(iterator.next(), nowMilliseconds());
                            return true;
                        }));

            default:
                throw new IndexingBase.ValidationException("invalid indexing method",
                        LogMessageKeys.INDEXING_METHOD, indexingMethod);
        }
    }

    private void validateNonCompetingHeartbeat(KeyValue kv, long now) {
        final Tuple keyTuple = Tuple.fromBytes(kv.getKey());
        if (keyTuple.size() < 2) { // expecting 8
            return;
        }
        final UUID otherSessionId = keyTuple.getUUID(keyTuple.size() - 1);
        if (!otherSessionId.equals(this.indexerId)) {
            try {
                final IndexBuildProto.IndexBuildHeartbeat otherHeartbeat = IndexBuildProto.IndexBuildHeartbeat.parseFrom(kv.getValue());
                final long age = now - otherHeartbeat.getHeartbeatTimeMilliseconds();
                if (age > 0 && age < leaseLength) {
                    throw new SynchronizedSessionLockedException("Failed to initialize the session because of an existing session in progress")
                            .addLogInfo(LogMessageKeys.SESSION_ID, indexerId)
                            .addLogInfo(LogMessageKeys.EXISTING_SESSION_ID, otherSessionId)
                            .addLogInfo(LogMessageKeys.AGE_MILLISECONDS, age)
                            .addLogInfo(LogMessageKeys.TIME_LIMIT_MILLIS, leaseLength);
                }
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void clearHeartbeat(@Nonnull FDBRecordStore store, @Nonnull Index index) {
        store.ensureContextActive().clear(IndexingSubspaces.indexheartbeatSubspace(store, index, indexerId).pack());
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
                            final Tuple keyTuple = Tuple.fromBytes(kv.getKey());
                            if (keyTuple.size() < 2) { // expecting 8
                                return true; // ignore, next
                            }
                            final UUID otherSessionId = keyTuple.getUUID(keyTuple.size() - 1);
                            try {
                                final IndexBuildProto.IndexBuildHeartbeat otherHeartbeat = IndexBuildProto.IndexBuildHeartbeat.parseFrom(kv.getValue());
                                ret.put(otherSessionId, otherHeartbeat);
                            } catch (InvalidProtocolBufferException e) {
                                // put a NONE heartbeat to indicate an invalid item
                                ret.put(otherSessionId, IndexBuildProto.IndexBuildHeartbeat.newBuilder()
                                        .setMethod(IndexBuildProto.IndexBuildIndexingStamp.Method.NONE)
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

    public static AsyncIterator<KeyValue> heartbeatsIterator(FDBRecordStore store, Index index) {
        return store.getContext().ensureActive().snapshot().getRange(IndexingSubspaces.indexheartbeatSubspace(store, index).range()).iterator();
    }


    private static long nowMilliseconds() {
        return System.currentTimeMillis();
    }
}
