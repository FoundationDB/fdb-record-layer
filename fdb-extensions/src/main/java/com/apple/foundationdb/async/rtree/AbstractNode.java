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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Abstract base class to capture the common aspects of {@link LeafNode} and {@link IntermediateNode}. All
 * nodes
 * have a node id and slots and can be linked up to a parent. Note that while the root node mostly is an
 * intermediate node it can also be a leaf node if the tree is nearly empty.
 * @param <S> slot type class
 * @param <N> node type class
 */
public abstract class AbstractNode<S extends NodeSlot, N extends AbstractNode<S, N>> implements Node {
    private static final int nodeIdLength = 16;

    private static final char[] hexArray = "0123456789ABCDEF".toCharArray();

    /**
     * Only used for debugging to keep node ids readable.
     */
    private static final AtomicLong nodeIdState = new AtomicLong(1); // skip the root which is always 0

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

    @Override
    public int size() {
        return getSlots().size();
    }

    @Nonnull
    @Override
    public S getSlot(final int index) {
        return getSlots().get(index);
    }

    public boolean isEmpty() {
        return getSlots().isEmpty();
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
    public N moveInSlots(@Nonnull final StorageAdapter storageAdapter, @Nonnull final List<? extends NodeSlot> slots) {
        final N self = getThis();
        final List<S> narrowedSlots = slots.stream().map(this::narrowSlot).collect(Collectors.toList());
        nodeSlots.addAll(narrowedSlots);
        this.changeSet = storageAdapter.newInsertChangeSet(self, -1, narrowedSlots);
        return self;
    }

    @Nonnull
    @Override
    public N moveOutSlots(@Nonnull final StorageAdapter storageAdapter) {
        final N self = getThis();
        this.changeSet = storageAdapter.newDeleteChangeSet(self, -1, this.nodeSlots);
        this.nodeSlots = Lists.newArrayList();
        return self;
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
    public List<S> getSlots() {
        return nodeSlots;
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
        return "[" + getKind().name() + ": id = " + bytesToHex(getId()) + "; parent = " +
               (getParentNode() == null ? "null" : bytesToHex(getParentNode().getId())) + "; slotInParent = " +
               getSlotInParent() + "]";
    }

    /**
     * Method to create a new node identifier. This method uses {@link UUID#randomUUID()} and should be used in
     * production to avoid conflicts.
     * @return a new 16-byte byte array containing a new unique node identifier
     */
    @Nonnull
    public static byte[] newRandomNodeId() {
        final UUID uuid = UUID.randomUUID();
        final byte[] uuidBytes = new byte[nodeIdLength];
        ByteBuffer.wrap(uuidBytes)
                .order(ByteOrder.BIG_ENDIAN)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits());
        return uuidBytes;
    }

    /**
     * Method to create a new node identifier. This method uses an internal static {@link AtomicLong} that is
     * incremented. This method creates monotonically increasing node identifiers which can be shortened when printed
     * or logged. This way of creating node identifiers should only be used for testing and debugging purposes.
     * @return a new 16-byte byte array containing a new unique node identifier
     */
    @Nonnull
    public static byte[] newSequentialNodeId() {
        final long nodeIdAsLong = nodeIdState.getAndIncrement();
        final byte[] uuidBytes = new byte[nodeIdLength];
        ByteBuffer.wrap(uuidBytes)
                .order(ByteOrder.BIG_ENDIAN)
                .putLong(0L)
                .putLong(nodeIdAsLong);
        return uuidBytes;
    }

    /**
     * Helper method to format bytes as hex strings for logging and debugging.
     * @param bytes an array of bytes
     * @return a {@link String} containing the hexadecimal representation of the byte array passed in
     */
    @Nonnull
    private static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return "0x" + new String(hexChars).replaceFirst("^0+(?!$)", "");
    }

    /**
     * Helper method to format the node ids of an insert/update path as a string.
     * @param node a node that is usually linked up to its parents to form an insert/update path
     * @return a {@link String} containing the string presentation of the insert/update path starting at {@code node}
     */
    @Nonnull
    static String nodeIdPath(@Nullable Node node) {
        final List<String> nodeIds = Lists.newArrayList();
        do {
            if (node != null) {
                nodeIds.add(bytesToHex(node.getId()));
                node = node.getParentNode();
            } else {
                nodeIds.add("<null>");
            }
        } while (node != null);
        Collections.reverse(nodeIds);
        return String.join(", ", nodeIds);
    }
}
