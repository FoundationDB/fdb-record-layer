/*
 * NodeKind.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.async.hnsw;

import com.google.common.base.Verify;

import javax.annotation.Nonnull;

/**
 * Represents the different kinds of nodes, each associated with a unique byte value for serialization and
 * deserialization.
 */
public enum NodeKind {
    /**
     * Compact node. Serialization and deserialization is implemented in {@link CompactNode}.
     * <p>
     * Compact nodes store their own vector and their neighbors-list only contain the primary key for each neighbor.
     */
    COMPACT((byte)0x00),

    /**
     * Inlining node. Serialization and deserialization is implemented in {@link InliningNode}.
     * <p>
     * Inlining nodes do not store their own vector and their neighbors-list contain the both the primary key and the
     * neighbor vector for each neighbor. Each neighbor is stored in its own key/value pair.
     */
    INLINING((byte)0x01),

    /**
     * Appendable compact node. This is a storage format marker for {@link CompactNode}s; the in-memory node of this
     * kind reports {@link #COMPACT} as its kind and is indistinguishable from any other compact node.
     * <p>
     * It differs from {@link #COMPACT} in that the neighbors are not wrapped in a nested tuple but are the trailing
     * elements of the node's tuple: {@code (kind, vector, additionalValues or null, neighbor, neighbor, ...)}. As
     * tuple elements are self-delimiting, a neighbor can be added by appending the packed neighbor to the stored value
     * (using an {@code APPEND_IF_FITS} atomic mutation), rather than by rewriting the vector and all other neighbors.
     */
    COMPACT_APPENDABLE((byte)0x02),
    ;

    private final byte serialized;

    /**
     * Constructs a new {@code NodeKind} instance with its serialized representation.
     * @param serialized the byte value used for serialization
     */
    NodeKind(final byte serialized) {
        this.serialized = serialized;
    }

    /**
     * Whether this kind is a storage format of {@link CompactNode}s, i.e. {@link #COMPACT} or
     * {@link #COMPACT_APPENDABLE}.
     * @return {@code true} iff nodes of this kind are compact nodes
     */
    public boolean isCompact() {
        return this == COMPACT || this == COMPACT_APPENDABLE;
    }

    /**
     * Gets the serialized byte value.
     * @return the serialized byte value
     */
    public byte getSerialized() {
        return serialized;
    }

    /**
     * Deserializes a byte into the corresponding {@link NodeKind}.
     * @param serializedNodeKind the byte representation of the node kind.
     * @return the corresponding {@link NodeKind}, never {@code null}.
     * @throws IllegalArgumentException if the {@code serializedNodeKind} does not
     * correspond to a known node kind.
     */
    @Nonnull
    static NodeKind fromSerializedNodeKind(byte serializedNodeKind) {
        final NodeKind nodeKind = switch (serializedNodeKind) {
            case 0x00 -> NodeKind.COMPACT;
            case 0x01 -> NodeKind.INLINING;
            case 0x02 -> NodeKind.COMPACT_APPENDABLE;
            default -> throw new IllegalArgumentException("unknown node kind");
        };
        Verify.verify(nodeKind.getSerialized() == serializedNodeKind);
        return nodeKind;
    }
}
