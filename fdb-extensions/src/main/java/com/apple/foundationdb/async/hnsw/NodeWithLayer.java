/*
 * NodeWithLayer.java
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

import javax.annotation.Nonnull;

class NodeWithLayer<N extends Neighbor> {
    private final int layer;
    @Nonnull
    private final Node<N> node;

    public NodeWithLayer(final int layer, @Nonnull final Node<N> node) {
        this.layer = layer;
        this.node = node;
    }

    public int getLayer() {
        return layer;
    }

    @Nonnull
    public Node<N> getNode() {
        return node;
    }
}
