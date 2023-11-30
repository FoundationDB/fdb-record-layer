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

import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * An intermediate node of the R-tree. An intermediate node holds the holds information about its children nodes that
 * be intermediate nodes or leaf nodes. The secondary attributes such as {@code largestHilbertValue},
 * {@code largestKey} can be derived (and recomputed) if the children of this node are available to be introspected.
 */
class IntermediateNode extends AbstractNode<ChildSlot, IntermediateNode> {
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
        super(id, childSlots, parentNode, slotIndexInParent);
    }

    @Override
    public IntermediateNode getThis() {
        return this;
    }

    @Nonnull
    @Override
    public ChildSlot narrowSlot(@Nonnull final NodeSlot slot) {
        return (ChildSlot)slot;
    }

    @Nonnull
    @Override
    public NodeKind getKind() {
        return NodeKind.INTERMEDIATE;
    }

    @Nonnull
    @Override
    public IntermediateNode newOfSameKind(@Nonnull final byte[] nodeId) {
        return new IntermediateNode(nodeId, Lists.newArrayList());
    }
}
