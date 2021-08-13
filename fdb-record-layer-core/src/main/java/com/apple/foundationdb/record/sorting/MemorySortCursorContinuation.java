/*
 * MemorySortCursorContinuation.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.sorting;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ByteArrayContinuation;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorStartContinuation;
import com.apple.foundationdb.record.RecordSortingProto;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

@API(API.Status.EXPERIMENTAL)
class MemorySortCursorContinuation<K, V> implements RecordCursorContinuation {
    @Nonnull
    private final MemorySortAdapter<K, V> adapter;
    private final boolean exhausted;
    @Nonnull
    private final Collection<V> records;
    @Nullable
    private final K minimumKey;
    @Nonnull
    private final RecordCursorContinuation childContinuation;

    @Nullable
    private RecordSortingProto.MemorySortContinuation cachedProto;
    @Nullable
    private byte[] cachedBytes;

    MemorySortCursorContinuation(@Nonnull MemorySortAdapter<K, V> adapter, boolean exhausted, @Nonnull Collection<V> records,
                                 @Nullable K minimumKey, @Nonnull RecordCursorContinuation childContinuation) {
        this.adapter = adapter;
        this.exhausted = exhausted;
        this.records = records;
        this.minimumKey = minimumKey;
        this.childContinuation = childContinuation;
    }

    @Nonnull
    RecordSortingProto.MemorySortContinuation toProto() {
        if (cachedProto == null) {
            RecordSortingProto.MemorySortContinuation.Builder builder = RecordSortingProto.MemorySortContinuation.newBuilder();
            for (V record : records) {
                builder.addRecords(ByteString.copyFrom(adapter.serializeValue(record)));
            }
            if (minimumKey != null) {
                builder.setMinimumKey(ByteString.copyFrom(adapter.serializeKey(minimumKey)));
            }
            byte[] childBytes = childContinuation.toBytes();
            if (childBytes != null) {
                builder.setContinuation(ByteString.copyFrom(childBytes));
            }
            cachedProto = builder.build();
        }
        return cachedProto;
    }

    @Override
    @Nullable
    public byte[] toBytes() {
        if (cachedBytes == null) {
            cachedBytes = toProto().toByteArray();
        }
        return cachedBytes;
    }

    @Nonnull
    static <K, V> MemorySortCursorContinuation<K, V> from(@Nonnull RecordSortingProto.MemorySortContinuation parsed,
                                                          @Nonnull MemorySortAdapter<K, V> adapter) {
        MemorySortCursorContinuation<K, V> result = new MemorySortCursorContinuation<>(
                adapter, false,
                parsed.getRecordsList().stream().map(bs -> adapter.deserializeValue(bs.toByteArray())).collect(Collectors.toList()),
                parsed.hasMinimumKey() ? adapter.deserializeKey(parsed.getMinimumKey().toByteArray()) : null,
                parsed.hasContinuation() ? ByteArrayContinuation.fromNullable(parsed.getContinuation().toByteArray()) : RecordCursorStartContinuation.START
        );
        result.cachedProto = parsed;
        return result;
    }

    @Nonnull
    static <K, V> MemorySortCursorContinuation<K, V> from(@Nullable byte[] unparsed,
                                                          @Nonnull MemorySortAdapter<K, V> adapter) {
        MemorySortCursorContinuation<K, V> result;
        if (unparsed == null) {
            result = new MemorySortCursorContinuation<>(adapter, false, Collections.emptyList(), null, RecordCursorStartContinuation.START);
        } else {
            try {
                result = MemorySortCursorContinuation.from(RecordSortingProto.MemorySortContinuation.parseFrom(unparsed), adapter);
            } catch (InvalidProtocolBufferException ex) {
                throw new RecordCoreException("invalid continuation", ex)
                        .addLogInfo(LogMessageKeys.RAW_BYTES, ByteArrayUtil2.loggable(unparsed));
            }
            result.cachedBytes = unparsed;
        }
        return result;
    }

    @Nonnull
    public Collection<V> getRecords() {
        return records;
    }

    @Nullable
    public K getMinimumKey() {
        return minimumKey;
    }

    @Nonnull
    RecordCursorContinuation getChild() {
        return childContinuation;
    }

    @Override
    public boolean isEnd() {
        return exhausted;
    }
}
