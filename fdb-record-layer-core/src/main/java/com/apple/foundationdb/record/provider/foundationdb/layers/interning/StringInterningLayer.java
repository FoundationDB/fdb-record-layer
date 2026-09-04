/*
 * StringInterningLayer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.layers.interning;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.StringInterningProto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverResult;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ZeroCopyByteString;

import org.jspecify.annotations.Nullable;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * A persistent bidirectional unique mapping between strings and integers.
 */
@API(API.Status.INTERNAL)
public class StringInterningLayer {
    private final Subspace mappingSubspace;
    private final Subspace reverseMappingSubspace;
    private final Subspace counterSubspace;
    private final boolean isRootLevel;

    public StringInterningLayer(Subspace baseSubspace) {
        this(baseSubspace, false);
    }

    public StringInterningLayer(Subspace baseSubspace, boolean isRootLevel) {
        this(baseSubspace.get(2),
                baseSubspace.get(1),
                baseSubspace.get(0),
                isRootLevel);
    }

    private StringInterningLayer(Subspace mappingSubspace,
                                 Subspace reverseMappingSubspace,
                                 Subspace counterSubspace,
                                 boolean isRootLevel) {
        this.mappingSubspace = mappingSubspace;
        this.reverseMappingSubspace = reverseMappingSubspace;
        this.counterSubspace = counterSubspace;
        this.isRootLevel = isRootLevel;
    }

    @SuppressWarnings("NullAway") // NullAway/JSpecify does not currently track @Nullable on array (byte[])
    // parameters; createMapping's metadata parameter is correctly declared @Nullable just below.
    protected CompletableFuture<ResolverResult> intern(FDBRecordContext context, final String toIntern) {
        CompletableFuture<Optional<ResolverResult>> readResult;
        synchronized (this) {
            readResult = read(context, toIntern);
        }
        return readResult
                .thenApply(maybeRead -> maybeRead.map(CompletableFuture::completedFuture))
                .thenCompose(value -> value.orElseGet(() -> createMapping(context, toIntern, null)));
    }

    protected CompletableFuture<Boolean> exists(FDBRecordContext context, final String toRead) {
        return context.ensureActive().get(mappingSubspace.pack(toRead))
                .thenApply(Objects::nonNull);
    }

    protected CompletableFuture<Optional<ResolverResult>> read(FDBRecordContext context, final String toRead) {
        return context.ensureActive().get(mappingSubspace.pack(toRead))
                .thenApply(Optional::ofNullable)
                .thenApply(maybeValue -> maybeValue.map(StringInterningLayer::deserializeValue));
    }


    protected CompletableFuture<Optional<String>> readReverse(FDBRecordContext context, final Long internedValue) {
        return context.ensureActive().get(reverseMappingSubspace.pack(internedValue))
                .thenApply(Optional::ofNullable)
                .thenApply(maybeValue -> maybeValue.map(bytes -> Tuple.fromBytes(bytes).getString(0)));
    }

    @VisibleForTesting
    protected void deleteReverseForTesting(FDBRecordContext context, final Long internedValue) {
        context.ensureActive().clear(reverseMappingSubspace.pack(internedValue));
    }

    protected void putReverse(FDBRecordContext context, final Long internedValue, String key) {
        context.ensureActive().set(reverseMappingSubspace.pack(internedValue), Tuple.from(key).pack());
    }

    protected CompletableFuture<ResolverResult> create(FDBRecordContext context,
                                                       final String toIntern,
                                                       @Nullable final byte[] metadata) {
        return exists(context, toIntern)
                .thenCompose(present -> {
                    if (present) {
                        throw new RecordCoreException("value already exists in interning layer")
                                .addLogInfo("value", toIntern);
                    }
                    return createMapping(context, toIntern, metadata);
                });
    }

    protected CompletableFuture<Void> updateMetadata(final FDBRecordContext context,
                                                     final String key,
                                                     @Nullable final byte[] metadata) {
        return read(context, key).thenApply(maybeRead ->
                maybeRead.map(read -> new ResolverResult(read.getValue(), metadata))
                        .orElseThrow(() -> new NoSuchElementException("updateMetadata must reference key that already exists")))
                .thenAccept(newResult -> context.ensureActive().set(mappingSubspace.pack(key), serializeValue(newResult)));
    }

    protected CompletableFuture<Void> setMapping(final FDBRecordContext context,
                                                 final String key,
                                                 final ResolverResult value) {
        return read(context, key).thenCombine(readReverse(context, value.getValue()), (maybeRead, maybeReverseRead) -> {
            maybeRead.ifPresent(read -> {
                if (!read.equals(value)) {
                    throw new RecordCoreException("mapping already exists with different value")
                            .addLogInfo("keyToSet", key)
                            .addLogInfo("valueToSet", value)
                            .addLogInfo("valueFound", read);
                }
            });
            maybeReverseRead.ifPresent(reverseRead -> {
                if (!reverseRead.equals(key)) {
                    throw new RecordCoreException("reverse mapping already exists with different key")
                            .addLogInfo("keyToSet", key)
                            .addLogInfo("valueToSet", value)
                            .addLogInfo("keyForValueFound", reverseRead);
                }
            });

            if (!maybeRead.isPresent() && !maybeReverseRead.isPresent()) {
                context.ensureActive().set(mappingSubspace.pack(key), serializeValue(value));
                getHca(context).forceAllocate(key, value.getValue());
            }
            return null;
        });
    }

    protected CompletableFuture<Void> setWindow(final FDBRecordContext context, long count) {
        getHca(context).setWindow(count);
        return AsyncUtil.DONE;
    }

    protected Subspace getMappingSubspace() {
        return mappingSubspace;
    }

    private CompletableFuture<ResolverResult> createMapping(FDBRecordContext context,
                                                            final String toIntern,
                                                            @Nullable final byte[] metadata) {
        final HighContentionAllocator hca = getHca(context);
        final byte[] mappingKey = mappingSubspace.pack(toIntern);
        return hca.allocate(toIntern)
                .thenApply(allocated -> {
                    ResolverResult result = new ResolverResult(allocated, metadata);
                    context.ensureActive().set(mappingKey, serializeValue(result));
                    return result;
                });
    }

    private HighContentionAllocator getHca(FDBRecordContext context) {
        return isRootLevel ?
                HighContentionAllocator.forRoot(context, counterSubspace, reverseMappingSubspace) :
                new HighContentionAllocator(context, counterSubspace, reverseMappingSubspace);
    }

    private byte[] serializeValue(ResolverResult allocated) {
        @Nullable byte[] metadata = allocated.getMetadata();
        StringInterningProto.Data.Builder builder = StringInterningProto.Data.newBuilder();
        builder.setInternedValue(allocated.getValue());
        if (metadata != null) {
            builder.setMetadata(ZeroCopyByteString.wrap(metadata));
        }
        return builder.build().toByteArray();
    }

    @SuppressWarnings("NullAway") // NullAway/JSpecify does not currently track @Nullable on array (byte[])
    // parameters; ResolverResult's metadata parameter is correctly declared @Nullable.
    protected static ResolverResult deserializeValue(byte[] bytes) {
        try {
            StringInterningProto.Data interned = StringInterningProto.Data.parseFrom(bytes);
            return new ResolverResult(
                    interned.getInternedValue(),
                    interned.hasMetadata() ? interned.getMetadata().toByteArray() : null);
        } catch (InvalidProtocolBufferException exception) {
            throw new RecordCoreException("invalid interned value", exception)
                    .addLogInfo("internedBytes", ByteArrayUtil2.loggable(bytes));
        }
    }
}
