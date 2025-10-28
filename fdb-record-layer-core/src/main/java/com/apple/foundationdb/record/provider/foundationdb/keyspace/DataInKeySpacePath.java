/*
 * DataInKeySpacePath.java
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

package com.apple.foundationdb.record.provider.foundationdb.keyspace;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

/**
 * Class representing a {@link KeyValue} pair within in {@link KeySpacePath}.
 */
@API(API.Status.EXPERIMENTAL)
public class DataInKeySpacePath {

    @Nonnull
    private final KeySpacePath path;
    @Nullable
    private final Tuple remainder;
    @Nonnull
    private final byte[] value;

    public DataInKeySpacePath(@Nonnull final KeySpacePath path, @Nullable final Tuple remainder,
                              @Nullable final byte[] value) {
        this.path = path;
        this.remainder = remainder;
        if (value == null) {
            throw new RecordCoreArgumentException("Value cannot be null")
                    .addLogInfo(LogMessageKeys.KEY, path);
        }
        this.value = value;
    }

    public byte[] getValue() {
        return this.value;
    }

    @Nonnull
    public KeySpacePath getPath() {
        return path;
    }

    @Nullable
    public Tuple getRemainder() {
        return remainder;
    }

    /**
     * Converts this data item to a {@link ResolvedKeySpacePath} by resolving the path and applying the remainder.
     * @param context the context to use for resolving the path
     * @return a future that completes with the resolved path including the remainder
     */
    @Nonnull
    public CompletableFuture<ResolvedKeySpacePath> getResolvedPath(@Nonnull FDBRecordContext context) {
        return path.toResolvedPathAsync(context)
                .thenApply(resolvedPath -> remainder == null ? resolvedPath : resolvedPath.withRemainder(remainder));
    }
}
