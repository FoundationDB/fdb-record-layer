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
import com.apple.foundationdb.async.rtree.Node.ChangeSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Abstract base implementations for all {@link ChangeSet}s.
 * @param <S> slot type class
 * @param <N> node type class (self type)
 */
public abstract class AbstractChangeSet<S extends NodeSlot, N extends AbstractNode<S, N>> implements ChangeSet {
    @Nullable
    private final ChangeSet previousChangeSet;

    @Nonnull
    private final N node;

    private final int level;

    AbstractChangeSet(@Nullable final ChangeSet previousChangeSet, @Nonnull final N node, final int level) {
        this.previousChangeSet = previousChangeSet;
        this.node = node;
        this.level = level;
    }

    @Override
    public void apply(@Nonnull final Transaction transaction) {
        if (previousChangeSet != null) {
            previousChangeSet.apply(transaction);
        }
    }

    /**
     * Previous change set in the chain of change sets. Can be {@code null} if there is no previous change set.
     * @return the previous change set in the chain of change sets
     */
    @Nullable
    public ChangeSet getPreviousChangeSet() {
        return previousChangeSet;
    }

    /**
     * The node this change set applies to.
     * @return the node this change set applies to
     */
    @Nonnull
    public N getNode() {
        return node;
    }

    /**
     * The level we should use when maintaining the node slot index. If {@code level < 0}, do not maintain the node slot
     * index.
     * @return the level used when maintaing the node slot index
     */
    public int getLevel() {
        return level;
    }

    /**
     * Returns whether this change set needs to also update the node slot index. There are scenarios where we
     * do not need to update such an index in general. For instance, the user may not want to use such an index.
     * In addition to that, there are change set implementations that should not update the index even if such and index
     * is maintained in general. For instance, the moved-in slots were already persisted in the database before the
     * move-in operation. We should not update the node slot index in such a case.
     * @return {@code true} if we need to update the node slot index, {@code false} otherwise
     */
    public boolean isUpdateNodeSlotIndex() {
        return level >= 0;
    }
}
