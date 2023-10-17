/*
 * AbstractChangeSet.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.async.rtree;

import com.apple.foundationdb.Transaction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Abstract base implementations for all {@link com.apple.foundationdb.async.rtree.Node.ChangeSet}s.
 * @param <S>
 * @param <N>
 */
public abstract class AbstractChangeSet<S extends NodeSlot, N extends AbstractNode<S, N>> implements Node.ChangeSet {
    @Nullable
    private final Node.ChangeSet previousChangeSet;

    @Nonnull
    private final N node;

    private final int level;

    AbstractChangeSet(@Nullable final Node.ChangeSet previousChangeSet, @Nonnull final N node, final int level) {
        this.previousChangeSet = previousChangeSet;
        this.node = node;
        this.level = level;
    }

    public void apply(@Nonnull final Transaction transaction) {
        if (previousChangeSet != null) {
            previousChangeSet.apply(transaction);
        }
    }

    @Nullable
    public Node.ChangeSet getPreviousChangeSet() {
        return previousChangeSet;
    }

    @Nonnull
    public N getNode() {
        return node;
    }

    public int getLevel() {
        return level;
    }

    public boolean isUpdateNodeSlotIndex() {
        return level >= 0;
    }
}
