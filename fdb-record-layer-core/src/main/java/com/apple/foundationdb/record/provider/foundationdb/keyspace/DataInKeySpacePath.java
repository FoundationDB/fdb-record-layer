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
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.tuple.ByteArrayUtil2;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

/**
 * Class representing a {@link KeyValue} pair within in {@link KeySpacePath}.
 */
public class DataInKeySpacePath {

    @Nonnull
    private final CompletableFuture<ResolvedKeySpacePath> resolvedPath;
    @Nonnull
    private final byte[] value;

    public DataInKeySpacePath(KeySpacePath path, KeyValue rawKeyValue, FDBRecordContext context) {
        this.resolvedPath = path.toResolvedPathAsync(context, rawKeyValue.getKey());
        this.value = rawKeyValue.getValue();
        if (this.value == null) {
            throw new RecordCoreArgumentException("Value cannot be null")
                    .addLogInfo(LogMessageKeys.KEY, ByteArrayUtil2.loggable(rawKeyValue.getKey()));
        }
    }

    public CompletableFuture<ResolvedKeySpacePath> getResolvedPath() {
        return resolvedPath;
    }

    public byte[] getValue() {
        return this.value;
    }
}
