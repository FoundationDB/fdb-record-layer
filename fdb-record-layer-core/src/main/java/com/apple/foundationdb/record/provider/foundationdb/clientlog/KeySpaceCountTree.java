/*
 * KeySpaceCountTree.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.clientlog.TupleKeyCountTree;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceTreeResolver;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

/**
 * Count keys and resolve back to key space paths.
 */
public class KeySpaceCountTree extends TupleKeyCountTree {
    @Nullable
    private KeySpaceTreeResolver.Resolved resolved;

    public KeySpaceCountTree(@Nonnull KeySpace keySpace) {
        super();
        this.resolved = new KeySpaceTreeResolver.ResolvedRoot(keySpace);
    }

    public KeySpaceCountTree(@Nullable KeySpaceCountTree parent, @Nonnull byte[] bytes, @Nullable Object object) {
        super(parent, bytes, object);
    }

    @Override
    @Nonnull
    protected TupleKeyCountTree newChild(@Nonnull byte[] childBytes, @Nonnull Object object) {
        return new KeySpaceCountTree(this, childBytes, object);
    }

    @Override
    @Nonnull
    protected TupleKeyCountTree newPrefixChild(@Nonnull byte[] prefixBytes, @Nonnull Object prefix) {
        TupleKeyCountTree result = super.newPrefixChild(prefixBytes, prefix);
        ((KeySpaceCountTree)result).resolved = new KeySpaceTreeResolver.ResolvedPrefixRoot(resolved, prefix);
        return result;
    }

    public CompletableFuture<Void> resolveVisibleChildren(@Nonnull KeySpaceTreeResolver resolver) {
        if (resolved != null) {
            final Iterator<TupleKeyCountTree> children = getChildren().iterator();
            return AsyncUtil.whileTrue(() -> {
                if (!children.hasNext()) {
                    return AsyncUtil.READY_FALSE;
                }
                KeySpaceCountTree child = (KeySpaceCountTree)children.next();
                if (!child.isVisible()) {
                    return AsyncUtil.READY_TRUE;
                }
                return child.resolve(resolver, resolved)
                        .thenCompose(vignore -> child.resolveVisibleChildren(resolver))
                        .thenApply(vignore -> true);
            });
        } else {
            return AsyncUtil.DONE;
        }
    }

    protected CompletableFuture<Void> resolve(@Nonnull KeySpaceTreeResolver resolver, @Nonnull KeySpaceTreeResolver.Resolved resolvedParent) {
        if (resolved != null || !hasObject()) {
            return AsyncUtil.DONE;
        }
        return resolver.resolve(resolvedParent, getObject()).thenAccept(resolved -> {
            if (resolved != null) {
                this.resolved = resolved;
            }
        });
    }

    @Override
    public String toString() {
        if (resolved != null) {
            return resolved.toString();
        } else {
            return super.toString();
        }
    }
}
