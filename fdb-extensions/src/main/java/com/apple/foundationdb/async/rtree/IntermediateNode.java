/*
 * IntermediateNode.java
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
 * An intermediate node of the tree. An intermediate node holds the holds information about its children nodes that
 * be intermediate nodes or leaf nodes. The secondary attributes such as {@code largestHilbertValue},
 * {@code largestKey} can be derived (and recomputed) if the children of this node are available to be introspected.
 */
public class IntermediateNode extends Node {
    @Nonnull
    private List<ChildSlot> childSlots;
    @Nonnull
    private final List<ChildSlot> insertedChildSlots;
    @Nonnull
    private final List<ChildSlot> deletedChildSlots;

    public IntermediateNode(@Nonnull final byte[] id) {
        this(id, Lists.newArrayList());
    }

    public IntermediateNode(@Nonnull final byte[] id,
                            @Nonnull final List<ChildSlot> childSlots) {
        this(id, childSlots, null, -1);
    }

    public IntermediateNode(@Nonnull final byte[] id,
                            @Nonnull final List<ChildSlot> childSlots,
                            @Nullable final IntermediateNode parentNode,
                            final int slotIndexInParent) {
        super(id, parentNode, slotIndexInParent);
        this.childSlots = childSlots;
        this.insertedChildSlots = Lists.newArrayList();
        this.deletedChildSlots = Lists.newArrayList();
    }

    @Nonnull
    @Override
    public Kind getKind() {
        return Node.Kind.INTERMEDIATE;
    }

    @Nonnull
    @Override
    public List<ChildSlot> getSlots() {
        return childSlots;
    }

    @Nonnull
    @Override
    public List<? extends NodeSlot> getInsertedSlots() {
        return insertedChildSlots;
    }

    @Nonnull
    @Override
    public List<? extends NodeSlot> getDeletedSlots() {
        return deletedChildSlots;
    }

    @Override
    public IntermediateNode replaceSlots(@Nonnull final List<? extends NodeSlot> newSlots) {
        Verify.verify(deletedChildSlots.isEmpty());
        deletedChildSlots.addAll(getSlots());

        this.childSlots =
                newSlots.stream()
                        .map(slot -> (ChildSlot)slot)
                        .collect(Collectors.toList());

        insertedChildSlots.clear();
        insertedChildSlots.addAll(childSlots);
        return this;
    }

    @Override
    public IntermediateNode insertSlot(final int slotIndex, @Nonnull final NodeSlot slot) {
        Preconditions.checkArgument(slot instanceof ChildSlot);
        insertedChildSlots.add((ChildSlot)slot);
        childSlots.add(slotIndex, (ChildSlot)slot);
        return this;
    }

    @Override
    public IntermediateNode deleteSlot(final int slotIndex) {
        deletedChildSlots.add(childSlots.get(slotIndex));
        childSlots.remove(slotIndex);
        return this;
    }

    @Nonnull
    @Override
    public IntermediateNode newOfSameKind(@Nonnull final byte[] nodeId) {
        return new IntermediateNode(nodeId, Lists.newArrayList());
    }
}
