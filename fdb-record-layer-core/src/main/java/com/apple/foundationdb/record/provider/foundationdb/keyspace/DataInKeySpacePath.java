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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;

import java.util.concurrent.CompletableFuture;

/**
 * Class representing a {@link KeyValue} pair within in {@link KeySpacePath}.
 */
public class DataInKeySpacePath {

    final CompletableFuture<ResolvedKeySpacePath> resolvedPath;
    final KeyValue rawKeyValue;

    public DataInKeySpacePath(KeySpacePath path, KeyValue rawKeyValue, FDBRecordContext context) {
        this.rawKeyValue = rawKeyValue;

        // Convert the raw key to a Tuple and resolve it starting from the provided path
        Tuple keyTuple = Tuple.fromBytes(rawKeyValue.getKey());
        
        // First resolve the provided path to get its resolved form
        this.resolvedPath = path.toResolvedPathAsync(context).thenCompose(resolvedPath -> {
            // Now use the resolved path to find the child for the key
            // We need to figure out how much of the key corresponds to the resolved path
            Tuple pathTuple = resolvedPath.toTuple();
            int pathLength = pathTuple.size();
            
            // The remaining part of the key should be resolved from the resolved path's directory
            if (keyTuple.size() > pathLength) {
                // There's more in the key than just the path, so resolve the rest
                if (resolvedPath.getDirectory().getSubdirectories().isEmpty()) {
                    return CompletableFuture.completedFuture(
                            new ResolvedKeySpacePath(resolvedPath.getParent(), resolvedPath.toPath(),
                                    resolvedPath.getResolvedPathValue(),
                                    TupleHelpers.subTuple(keyTuple, pathTuple.size(), keyTuple.size())));
                } else {
                    return resolvedPath.getDirectory().findChildForKey(context, resolvedPath, keyTuple, keyTuple.size(), pathLength);
                }
            } else {
                // The key exactly matches the path
                return CompletableFuture.completedFuture(resolvedPath);
            }
        });
    }

    public CompletableFuture<ResolvedKeySpacePath> getResolvedPath() {
        return resolvedPath;
    }

    public KeyValue getRawKeyValue() {
        return rawKeyValue;
    }
}
