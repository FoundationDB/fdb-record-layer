/*
 * AbstractNode.java
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
import java.util.List;
import java.util.stream.Stream;

/**
 * Abstract base class to define common attributed and to provide common implementations of
 * {@link LeafNode} and {@link IntermediateNode}.
 * @param <S> slot type class
 * @param <N> node type class. This is also called the self type.
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

    /**
     * Method that returns {@code this}. This method needs to be overridden in each leaf class (leaf as in final).
     * The reason this method exists is to trick the Java compiler in treating {@code this} to be of type {@code N}
     * instead of {@code AbstractNode<>}.
     * @return {@code this}
     */
    protected abstract N getThis();

    @Nonnull
    @Override
    @SpotBugsSuppressWarnings("EI_EXPOSE_REP")
    public byte[] getId() {
        return id;
    }

    /**
     * Return the slots of this node as a list. Note that the result type is covariant.
     * @return a list of node slots
     */
    @Nonnull
    @Override
    public List<S> getSlots() {
        return nodeSlots;
    }

    /**
     * Return a sub range of the slots of this node as a list. Note that the result type is covariant.
     * @return a list of node slots
     */
    @Nonnull
    @Override
    public List<S> getSlots(final int startIndexInclusive, final int endIndexExclusive) {
        return nodeSlots.subList(startIndexInclusive, endIndexExclusive);
    }

    @Override
    public int size() {
        return nodeSlots.size();
    }

    @Override
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

    /**
     * Return the (optional) change set associated with this node as a {@link AbstractChangeSet} instead of
     * a {@link com.apple.foundationdb.async.rtree.Node.ChangeSet}. Note that the result type is covariant.
     * @return the change set associated with this node
     */
    @Nullable
    @Override
    public AbstractChangeSet<S, N> getChangeSet() {
        return changeSet;
    }

    /**
     * Returns the slot handed in as a slot of type {@code S}. Needs to be implemented in a leaf (final) class.
     * @param slot a slot
     * @return the same slot that was passed in, but of type {@code S}
     */
    @Nonnull
    public abstract S narrowSlot(@Nonnull NodeSlot slot);

    @Nonnull
    @Override
    public N moveInSlots(@Nonnull final StorageAdapter storageAdapter, @Nonnull final Iterable<? extends NodeSlot> slots) {
        final AbstractStorageAdapter abstractStorageAdapter = (AbstractStorageAdapter)storageAdapter;
        final N self = getThis();
        final List<S> narrowedSlots = Streams.stream(slots).map(this::narrowSlot).collect(ImmutableList.toImmutableList());
        nodeSlots.addAll(narrowedSlots);
        this.changeSet = abstractStorageAdapter.newInsertChangeSet(self, -1, narrowedSlots);
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
        final AbstractStorageAdapter abstractStorageAdapter = (AbstractStorageAdapter)storageAdapter;
        final N self = getThis();
        final S narrowedSlot = narrowSlot(slot);
        nodeSlots.add(slotIndex, narrowedSlot);
        this.changeSet = abstractStorageAdapter.newInsertChangeSet(self, level, ImmutableList.of(narrowedSlot));
        return self;
    }

    @Nonnull
    @Override
    public Node updateSlot(@Nonnull final StorageAdapter storageAdapter, final int level, final int slotIndex,
                           @Nonnull final NodeSlot updatedSlot) {
        final AbstractStorageAdapter abstractStorageAdapter = (AbstractStorageAdapter)storageAdapter;
        final N self = getThis();
        final S narrowedSlot = narrowSlot(updatedSlot);
        final S originalSlot = nodeSlots.set(slotIndex, narrowedSlot);
        this.changeSet = abstractStorageAdapter.newUpdateChangeSet(self, level, originalSlot, narrowedSlot);
        return self;
    }

    @Nonnull
    @Override
    public Node deleteSlot(@Nonnull final StorageAdapter storageAdapter, final int level, final int slotIndex) {
        final AbstractStorageAdapter abstractStorageAdapter = (AbstractStorageAdapter)storageAdapter;
        final N self = getThis();
        final S narrowedSlot = nodeSlots.get(slotIndex);
        nodeSlots.remove(slotIndex);
        this.changeSet = abstractStorageAdapter.newDeleteChangeSet(self, level, ImmutableList.of(narrowedSlot));
        return self;
    }

    @Nonnull
    @Override
    public N deleteAllSlots(@Nonnull final StorageAdapter storageAdapter, final int level) {
        final AbstractStorageAdapter abstractStorageAdapter = (AbstractStorageAdapter)storageAdapter;
        final N self = getThis();
        this.changeSet = abstractStorageAdapter.newDeleteChangeSet(self, level, this.nodeSlots);
        this.nodeSlots = Lists.newArrayList();
        return self;
    }

    @Nullable
    @Override
    public IntermediateNode getParentNode() {
        return parentNode;
    }

    @Override
    public int getSlotIndexInParent() {
        return slotIndexInParent;
    }

    @Override
    public void linkToParent(@Nonnull final IntermediateNode parentNode, final int slotInParent) {
        this.parentNode = parentNode;
        this.slotIndexInParent = slotInParent;
    }

    /**
     * Method to return a new node of the same {@link NodeKind} as this node. This method's result type is
     * covariant to return a node of type {@code N}.
     * @param nodeId node id for the new node
     * @return a new node of type {@code N}
     */
    @Nonnull
    @Override
    public abstract N newOfSameKind(@Nonnull byte[] nodeId);

    @Override
    @Nonnull
    public String toString() {
        return "[" + getKind().name() + ": id = " + NodeHelpers.bytesToHex(getId()) + "; parent = " +
               (getParentNode() == null ? "null" : NodeHelpers.bytesToHex(getParentNode().getId())) + "; slotInParent = " +
               getSlotInParent() + "]";
    }
}
