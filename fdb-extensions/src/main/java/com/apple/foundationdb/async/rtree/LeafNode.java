/*
 * LeafNode.java
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

import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * A leaf node of the R-tree. A leaf node holds the actual data in {@link ItemSlot}s.
 */
class LeafNode extends AbstractNode<ItemSlot, LeafNode> {
    public LeafNode(@Nonnull final byte[] id,
                    @Nonnull final List<ItemSlot> itemSlots) {
        this(id, itemSlots, null, -1);
    }

    public LeafNode(@Nonnull final byte[] id,
                    @Nonnull final List<ItemSlot> itemSlots,
                    @Nullable final IntermediateNode parentNode,
                    final int slotIndexInParent) {
        super(id, itemSlots, parentNode, slotIndexInParent);
    }

    @Override
    public LeafNode getThis() {
        return this;
    }

    @Nonnull
    @Override
    public ItemSlot narrowSlot(@Nonnull final NodeSlot slot) {
        return (ItemSlot)slot;
    }

    @Nonnull
    @Override
    public NodeKind getKind() {
        return NodeKind.LEAF;
    }

    @Nonnull
    @Override
    public LeafNode newOfSameKind(@Nonnull final byte[] nodeId) {
        return new LeafNode(nodeId, Lists.newArrayList());
    }
}
