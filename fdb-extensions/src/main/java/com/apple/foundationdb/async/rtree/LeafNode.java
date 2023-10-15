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

import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A leaf node of the tree. A leaf node holds the actual data in {@link ItemSlot}s.
 */
public class LeafNode extends Node {
    @Nonnull
    private List<ItemSlot> itemSlots;
    @Nonnull
    private final List<ItemSlot> insertedItemSlots;
    @Nonnull
    private final List<ItemSlot> deletedItemSlots;

    public LeafNode(@Nonnull final byte[] id) {
        this(id, Lists.newArrayList());
    }

    public LeafNode(@Nonnull final byte[] id,
                    @Nonnull final List<ItemSlot> itemSlots) {
        this(id, itemSlots, null, -1);
    }

    public LeafNode(@Nonnull final byte[] id,
                    @Nonnull final List<ItemSlot> itemSlots,
                    @Nullable final IntermediateNode parentNode,
                    final int slotIndexInParent) {
        super(id, parentNode, slotIndexInParent);
        this.itemSlots = itemSlots;
        this.insertedItemSlots = Lists.newArrayList();
        this.deletedItemSlots = Lists.newArrayList();
    }

    @Nonnull
    @Override
    public Kind getKind() {
        return Node.Kind.LEAF;
    }

    @Nonnull
    @Override
    public List<ItemSlot> getSlots() {
        return itemSlots;
    }

    @Nonnull
    @Override
    public List<? extends NodeSlot> getInsertedSlots() {
        return insertedItemSlots;
    }

    @Nonnull
    @Override
    public List<? extends NodeSlot> getDeletedSlots() {
        return deletedItemSlots;
    }

    @Override
    public LeafNode replaceSlots(@Nonnull final List<? extends NodeSlot> newSlots) {
        Verify.verify(deletedItemSlots.isEmpty());
        deletedItemSlots.addAll(getSlots());

        this.itemSlots =
                newSlots.stream()
                        .map(slot -> (ItemSlot)slot)
                        .collect(Collectors.toList());

        insertedItemSlots.clear();
        insertedItemSlots.addAll(itemSlots);
        return this;
    }

    @Override
    public LeafNode insertSlot(final int slotIndex, @Nonnull final NodeSlot slot) {
        Preconditions.checkArgument(slot instanceof ItemSlot);
        insertedItemSlots.add((ItemSlot)slot);
        itemSlots.add(slotIndex, (ItemSlot)slot);
        return this;
    }

    @Override
    public LeafNode deleteSlot(final int slotIndex) {
        deletedItemSlots.add(itemSlots.get(slotIndex));
        itemSlots.remove(slotIndex);
        return this;
    }

    @Nonnull
    @Override
    public LeafNode newOfSameKind(@Nonnull final byte[] nodeId) {
        return new LeafNode(nodeId, Lists.newArrayList());
    }
}
