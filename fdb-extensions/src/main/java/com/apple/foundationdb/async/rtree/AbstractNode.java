/*
 * Node.java
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

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * Abstract base class to define common attributed and to provide common implementations of
 * {@link LeafNode} and {@link IntermediateNode}.
 * @param <S> slot type class
 * @param <N> node type class
 */
abstract class AbstractNode<S extends NodeSlot, N extends AbstractNode<S, N>> implements Node {

    @Nonnull
    private final byte[] id;

    @Nonnull
    private List<S> nodeSlots;

    @Nullable
    private IntermediateNode parentNode;
    private int slotIndexInParent;

    @Nullable
    private AbstractChangeSet<S, N> changeSet;

    @SpotBugsSuppressWarnings("EI_EXPOSE_REP2")
    protected AbstractNode(@Nonnull final byte[] id, @Nonnull final List<S> nodeSlots,
                           @Nullable final IntermediateNode parentNode, final int slotIndexInParent) {
        this.id = id;
        this.nodeSlots = nodeSlots;
        this.parentNode = parentNode;
        this.slotIndexInParent = slotIndexInParent;
        this.changeSet = null;
    }

    public abstract N getThis();

    @Nonnull
    @SpotBugsSuppressWarnings("EI_EXPOSE_REP")
    public byte[] getId() {
        return id;
    }

    @Nonnull
    @Override
    public List<S> getSlots() {
        return nodeSlots;
    }

    @Nonnull
    @Override
    public List<S> getSlots(final int startIndexInclusive, final int endIndexExclusive) {
        return nodeSlots.subList(startIndexInclusive, endIndexExclusive);
    }

    public int size() {
        return nodeSlots.size();
    }

    /**
     * Return if this node does not hold any slots. Note that a node can ony be temporarily empty, for instance when
     * slots are moved out of a node before other slots get moved in. Such a node must not be persisted as it violates
     * the invariants of ancR-tree.
     * @return {@code true} if the node currently does not hold any node slots.
     */
    public boolean isEmpty() {
        return nodeSlots.isEmpty();
    }

    @Nonnull
    @Override
    public S getSlot(final int index) {
        return getSlots().get(index);
    }

    @Nonnull
    @Override
    public Stream<? extends NodeSlot> slotsStream() {
        return nodeSlots.stream();
    }

    @Nullable
    @Override
    public AbstractChangeSet<S, N> getChangeSet() {
        return changeSet;
    }

    @Nonnull
    public abstract S narrowSlot(@Nonnull final NodeSlot slot);

    @Nonnull
    @Override
    public N moveInSlots(@Nonnull final StorageAdapter storageAdapter, @Nonnull final Iterable<? extends NodeSlot> slots) {
        final N self = getThis();
        final List<S> narrowedSlots = Streams.stream(slots).map(this::narrowSlot).collect(ImmutableList.toImmutableList());
        nodeSlots.addAll(narrowedSlots);
        this.changeSet = storageAdapter.newInsertChangeSet(self, -1, narrowedSlots);
        return self;
    }

    @Nonnull
    @Override
    public N moveOutAllSlots(@Nonnull final StorageAdapter storageAdapter) {
        return deleteAllSlots(storageAdapter, -1);
    }

    @Nonnull
    @Override
    public N insertSlot(@Nonnull final StorageAdapter storageAdapter, final int level, final int slotIndex,
                        @Nonnull final NodeSlot slot) {
        final N self = getThis();
        final S narrowedSlot = narrowSlot(slot);
        nodeSlots.add(slotIndex, narrowedSlot);
        this.changeSet = storageAdapter.newInsertChangeSet(self, level, ImmutableList.of(narrowedSlot));
        return self;
    }

    @Nonnull
    @Override
    public Node updateSlot(@Nonnull final StorageAdapter storageAdapter, final int level, final int slotIndex,
                           @Nonnull final NodeSlot updatedSlot) {
        final N self = getThis();
        final S narrowedSlot = narrowSlot(updatedSlot);
        final S originalSlot = nodeSlots.set(slotIndex, narrowedSlot);
        this.changeSet = storageAdapter.newUpdateChangeSet(self, level, originalSlot, narrowedSlot);
        return self;
    }

    @Nonnull
    @Override
    public Node deleteSlot(@Nonnull final StorageAdapter storageAdapter, final int level, final int slotIndex) {
        final N self = getThis();
        final S narrowedSlot = nodeSlots.get(slotIndex);
        nodeSlots.remove(slotIndex);
        this.changeSet = storageAdapter.newDeleteChangeSet(self, level, ImmutableList.of(narrowedSlot));
        return self;
    }

    @Nonnull
    @Override
    public N deleteAllSlots(@Nonnull final StorageAdapter storageAdapter, final int level) {
        final N self = getThis();
        this.changeSet = storageAdapter.newDeleteChangeSet(self, level, this.nodeSlots);
        this.nodeSlots = Lists.newArrayList();
        return self;
    }

    public boolean isRoot() {
        return Arrays.equals(RTree.rootId, id);
    }

    @Nonnull
    public abstract NodeKind getKind();

    @Nullable
    public IntermediateNode getParentNode() {
        return parentNode;
    }

    public int getSlotIndexInParent() {
        return slotIndexInParent;
    }

    public void linkToParent(@Nonnull final IntermediateNode parentNode, final int slotInParent) {
        this.parentNode = parentNode;
        this.slotIndexInParent = slotInParent;
    }

    @Nonnull
    public abstract N newOfSameKind(@Nonnull byte[] nodeId);

    @Override
    public String toString() {
        return "[" + getKind().name() + ": id = " + NodeHelpers.bytesToHex(getId()) + "; parent = " +
               (getParentNode() == null ? "null" : NodeHelpers.bytesToHex(getParentNode().getId())) + "; slotInParent = " +
               getSlotInParent() + "]";
    }
}
