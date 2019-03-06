/*
 * ResolverMappingDigest.java
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

package com.apple.foundationdb.record.provider.foundationdb.keyspace;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.KeyValueCursor;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CompletableFuture;

/**
 * Computes a message digest for all of the string to integer mappings in the provided {@link LocatableResolver}. This
 * gives a way to easily verify that a copy operation performed by {@link ResolverMappingReplicator} succeeded. The digest
 * is computed based on the {@link Tuple} serialization of the string and integer rather than the serialization used by
 * {@link LocatableResolver} as that can vary depending on implementation.
 */
@API(API.Status.EXPERIMENTAL)
public class ResolverMappingDigest implements AutoCloseable {
    private static final String ALGORITHM = "SHA-256";
    @Nonnull
    private final LocatableResolver resolver;
    @Nonnull
    private final FDBDatabaseRunner runner;
    private final int transactionRowLimit;

    public ResolverMappingDigest(@Nonnull LocatableResolver directoryScope) {
        this(directoryScope, 10_000);
    }

    public ResolverMappingDigest(@Nonnull LocatableResolver directoryScope,
                                 int transactionRowLimit) {
        this.runner = directoryScope.getDatabase().newRunner();
        this.resolver = directoryScope;
        this.transactionRowLimit = transactionRowLimit;
    }

    @Override
    public void close() {
        runner.close();
    }

    public CompletableFuture<byte[]> computeDigest() {
        MessageDigest messageDigest;
        try {
            messageDigest = MessageDigest.getInstance(ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("invalid directory layer digest algorithm", e);
        }

        FDBRecordContext context = runner.openContext();
        return computeInternal(context, null, messageDigest)
                .thenCompose(continuation -> {
                    context.close();
                    if (continuation != null) {
                        return computeInternal(runner.openContext(), continuation, messageDigest);
                    }
                    return CompletableFuture.completedFuture(null);
                }).thenApply(ignore -> messageDigest.digest());
    }

    private CompletableFuture<byte[]> computeInternal(@Nonnull FDBRecordContext context,
                                                      @Nullable byte[] continuation,
                                                      @Nonnull MessageDigest messageDigest) {

        return resolver.getMappingSubspaceAsync().thenCompose(mappingSubspace -> {
            final RecordCursor<KeyValue> cursor = KeyValueCursor.Builder.withSubspace(mappingSubspace)
                    .setScanProperties(new ScanProperties(ExecuteProperties.newBuilder().setReturnedRowLimit(transactionRowLimit).setIsolationLevel(IsolationLevel.SNAPSHOT).build()))
                    .setContext(context)
                    .setContinuation(continuation)
                    .build();

            return cursor.forEachResult(result -> {
                KeyValue kv = result.get();
                String key = mappingSubspace.unpack(kv.getKey()).getString(0);
                ResolverResult value = resolver.deserializeValue(kv.getValue());

                messageDigest.update(Tuple.from(key, value.getValue(), value.getMetadata()).pack());
            }).thenApply(result -> result.getContinuation().toBytes());
        });
    }
}
