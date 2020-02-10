/*
 * KeySpaceCountTree.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.clientlog;

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolvedKeySpacePath;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Count keys and resolve back to key space paths.
 */
public class KeySpaceCountTree extends TupleKeyCountTree {
    @Nullable
    private KeySpaceDirectory directory;
    @Nullable
    private ResolvedKeySpacePath resolvedPath;

    public KeySpaceCountTree(@Nonnull KeySpace keySpace) {
        super();
        this.directory = keySpace.getRoot();
    }

    public KeySpaceCountTree(@Nullable KeySpaceCountTree parent, @Nonnull byte[] bytes, @Nullable Object object) {
        super(parent, bytes, object);
    }

    @Override
    @Nonnull
    protected TupleKeyCountTree newChild(@Nonnull byte[] bytes, @Nonnull Object object) {
        return new KeySpaceCountTree(this, bytes, object);
    }

    @Override
    @Nonnull
    protected TupleKeyCountTree newPrefixChild(@Nonnull Object prefix) {
        TupleKeyCountTree result = super.newPrefixChild(prefix);
        ((KeySpaceCountTree)result).directory = directory;
        return result;
    }

    public CompletableFuture<Void> resolveVisibleChildren(@Nonnull FDBRecordContext context) {
        if (directory != null) {
            final Iterator<TupleKeyCountTree> children = getChildren().iterator();
            return AsyncUtil.whileTrue(() -> {
                if (!children.hasNext()) {
                    return AsyncUtil.READY_FALSE;
                }
                KeySpaceCountTree child = (KeySpaceCountTree)children.next();
                return child.resolvePathValue(context, directory, resolvedPath)
                        .thenCompose(vignore -> child.resolveVisibleChildren(context))
                        .thenApply(vignore -> true);
            });
        } else {
            return AsyncUtil.DONE;
        }
    }

    protected CompletableFuture<Void> resolvePathValue(@Nonnull FDBRecordContext context,
                                                       @Nonnull KeySpaceDirectory parentDirectory, @Nullable ResolvedKeySpacePath parentPath) {
        if (resolvedPath != null) {
            return AsyncUtil.DONE;
        }
        try {
            return parentDirectory.findChildForValue(context, parentPath, getObject()).handle((resolved, ex) -> {
                if (resolved != null) {
                    // Null case includes swallowing async error (ex).
                    this.resolvedPath = resolved;
                    this.directory = resolved.getDirectory();
                }
                return null;
            });
        } catch (RecordCoreException ex) {
            return AsyncUtil.DONE;
        }
    }

    @Override
    public String toString() {
        if (resolvedPath != null) {
            StringBuilder str = new StringBuilder();
            str.append(resolvedPath.getDirectoryName()).append(':');
            ResolvedKeySpacePath.appendValue(str, resolvedPath.getLogicalValue());
            if (!Objects.equals(resolvedPath.getLogicalValue(), resolvedPath.getResolvedValue())) {
                str.append('[').append(resolvedPath.getResolvedValue()).append(']');
            }
            return str.toString();
        } else {
            return super.toString();
        }
    }
}
