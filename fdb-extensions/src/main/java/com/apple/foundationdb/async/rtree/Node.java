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
import com.google.common.base.Verify;
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

/**
 * Abstract base class to capture the common aspects of {@link LeafNode} and {@link IntermediateNode}. All
 * nodes
 * have a node id and slots and can be linked up to a parent. Note that while the root node mostly is an
 * intermediate node it can also be a leaf node if the tree is nearly empty.
 */
public abstract class Node {
    private static final int nodeIdLength = 16;

    private static final char[] hexArray = "0123456789ABCDEF".toCharArray();

    /**
     * Only used for debugging to keep node ids readable.
     */
    private static final AtomicLong nodeIdState = new AtomicLong(1); // skip the root which is always 0

    @Nonnull
    private final byte[] id;

    @Nullable
    private IntermediateNode parentNode;
    private int slotIndexInParent;

    @SpotBugsSuppressWarnings("EI_EXPOSE_REP2")
    public Node(@Nonnull final byte[] id, @Nullable final IntermediateNode parentNode, final int slotIndexInParent) {
        this.id = id;
        this.parentNode = parentNode;
        this.slotIndexInParent = slotIndexInParent;
    }

    @Nonnull
    @SpotBugsSuppressWarnings("EI_EXPOSE_REP")
    public byte[] getId() {
        return id;
    }

    public int size() {
        return getSlots().size();
    }

    public boolean isEmpty() {
        return getSlots().isEmpty();
    }

    public abstract Node replaceSlots(@Nonnull List<? extends NodeSlot> newSlots);

    @Nonnull
    public abstract List<? extends NodeSlot> getSlots();

    public abstract Node insertSlot(int slotIndex, @Nonnull NodeSlot slot);

    @Nonnull
    public abstract List<? extends NodeSlot> getInsertedSlots();

    @Nonnull
    public abstract List<? extends NodeSlot> getDeletedSlots();

    public abstract Node deleteSlot(int slotIndex);

    public boolean isRoot() {
        return Arrays.equals(RTree.rootId, id);
    }

    @Nonnull
    public abstract Kind getKind();

    @Nullable
    public IntermediateNode getParentNode() {
        return parentNode;
    }

    public int getSlotIndexInParent() {
        return slotIndexInParent;
    }

    @Nullable
    public ChildSlot getSlotInParent() {
        if (parentNode == null) {
            return null;
        }
        Verify.verify(slotIndexInParent >= 0);
        return parentNode.getSlots().get(slotIndexInParent);
    }

    public void linkToParent(@Nonnull final IntermediateNode parentNode, final int slotInParent) {
        this.parentNode = parentNode;
        this.slotIndexInParent = slotInParent;
    }

    @Nonnull
    public abstract Node newOfSameKind(@Nonnull byte[] nodeId);

    @Override
    public String toString() {
        return "[" + getKind().name() + ": id = " + bytesToHex(getId()) + "; parent = " +
               (getParentNode() == null ? "null" : bytesToHex(getParentNode().getId())) + "; slotInParent = " +
               getSlotInParent() + "]";
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
     * Enum to capture the kind of node.
     */
    public enum Kind {
        LEAF((byte)0x00),
        INTERMEDIATE((byte)0x01);

        private final byte serialized;

        Kind(final byte serialized) {
            this.serialized = serialized;
        }

        public byte getSerialized() {
            return serialized;
        }

        @Nonnull
        static Kind fromSerializedNodeKind(byte serializedNodeKind) {
            final Kind nodeKind;
            switch (serializedNodeKind) {
                case 0x00:
                    nodeKind = Kind.LEAF;
                    break;
                case 0x01:
                    nodeKind = Kind.INTERMEDIATE;
                    break;
                default:
                    throw new IllegalArgumentException("unknown node kind");
            }
            Verify.verify(nodeKind.getSerialized() == serializedNodeKind);
            return nodeKind;
        }
    }
}
