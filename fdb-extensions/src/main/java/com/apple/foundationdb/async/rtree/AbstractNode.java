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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;

import org.jspecify.annotations.Nullable;
import java.util.List;
import java.util.stream.Stream;

/**
 * Abstract base class to define common attributed and to provide common implementations of
 * {@link LeafNode} and {@link IntermediateNode}.
 * @param <S> slot type class
 * @param <N> node type class. This is also called the self type.
 */
abstract class AbstractNode<S extends NodeSlot, N extends AbstractNode<S, N>> implements Node {
    private final byte[] id;

    private List<S> nodeSlots;

    @Nullable
    private IntermediateNode parentNode;
    private int slotIndexInParent;

    @Nullable
    private AbstractChangeSet<S, N> changeSet;

    protected AbstractNode(final byte[] id, final List<S> nodeSlots,
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

    @Override
    public byte[] getId() {
        return id;
    }

    /**
     * Return the slots of this node as a list. Note that the result type is covariant.
     * @return a list of node slots
     */
    @Override
    public List<S> getSlots() {
        return nodeSlots;
    }

    /**
     * Return a sub range of the slots of this node as a list. Note that the result type is covariant.
     * @return a list of node slots
     */
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

    @Override
    public S getSlot(final int index) {
        return getSlots().get(index);
    }

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
    public abstract S narrowSlot(NodeSlot slot);

    @Override
    public N moveInSlots(final StorageAdapter storageAdapter, final Iterable<? extends NodeSlot> slots) {
        final AbstractStorageAdapter abstractStorageAdapter = (AbstractStorageAdapter)storageAdapter;
        final N self = getThis();
        final List<S> narrowedSlots = Streams.stream(slots).map(this::narrowSlot).collect(ImmutableList.toImmutableList());
        nodeSlots.addAll(narrowedSlots);
        this.changeSet = abstractStorageAdapter.newInsertChangeSet(self, -1, narrowedSlots);
        return self;
    }

    @Override
    public N moveOutAllSlots(final StorageAdapter storageAdapter) {
        return deleteAllSlots(storageAdapter, -1);
    }

    @Override
    public N insertSlot(final StorageAdapter storageAdapter, final int level, final int slotIndex,
                        final NodeSlot slot) {
        final AbstractStorageAdapter abstractStorageAdapter = (AbstractStorageAdapter)storageAdapter;
        final N self = getThis();
        final S narrowedSlot = narrowSlot(slot);
        nodeSlots.add(slotIndex, narrowedSlot);
        this.changeSet = abstractStorageAdapter.newInsertChangeSet(self, level, ImmutableList.of(narrowedSlot));
        return self;
    }

    @Override
    public Node updateSlot(final StorageAdapter storageAdapter, final int level, final int slotIndex,
                           final NodeSlot updatedSlot) {
        final AbstractStorageAdapter abstractStorageAdapter = (AbstractStorageAdapter)storageAdapter;
        final N self = getThis();
        final S narrowedSlot = narrowSlot(updatedSlot);
        final S originalSlot = nodeSlots.set(slotIndex, narrowedSlot);
        this.changeSet = abstractStorageAdapter.newUpdateChangeSet(self, level, originalSlot, narrowedSlot);
        return self;
    }

    @Override
    public Node deleteSlot(final StorageAdapter storageAdapter, final int level, final int slotIndex) {
        final AbstractStorageAdapter abstractStorageAdapter = (AbstractStorageAdapter)storageAdapter;
        final N self = getThis();
        final S narrowedSlot = nodeSlots.get(slotIndex);
        nodeSlots.remove(slotIndex);
        this.changeSet = abstractStorageAdapter.newDeleteChangeSet(self, level, ImmutableList.of(narrowedSlot));
        return self;
    }

    @Override
    public N deleteAllSlots(final StorageAdapter storageAdapter, final int level) {
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
    public void linkToParent(final IntermediateNode parentNode, final int slotInParent) {
        this.parentNode = parentNode;
        this.slotIndexInParent = slotInParent;
    }

    /**
     * Method to return a new node of the same {@link NodeKind} as this node. This method's result type is
     * covariant to return a node of type {@code N}.
     * @param nodeId node id for the new node
     * @return a new node of type {@code N}
     */
    @Override
    public abstract N newOfSameKind(byte[] nodeId);

    @Override
    public String toString() {
        // Captured once rather than calling getParentNode() twice (once to check for null, once to dereference);
        // SpotBugs (NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE) cannot tell that two separate calls would agree.
        final IntermediateNode parent = getParentNode();
        return "[" + getKind().name() + ": id = " + NodeHelpers.bytesToHex(getId()) + "; parent = " +
               (parent == null ? "null" : NodeHelpers.bytesToHex(parent.getId())) + "; slotInParent = " +
               getSlotInParent() + "]";
    }
}
