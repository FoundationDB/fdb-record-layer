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

import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;

class GreedyResult {
    private final int layer;
    @Nonnull
    private final Tuple primaryKey;
    private final double distance;

    public GreedyResult(final int layer, @Nonnull final Tuple primaryKey, final double distance) {
        this.layer = layer;
        this.primaryKey = primaryKey;
        this.distance = distance;
    }

    public int getLayer() {
        return layer;
    }

    @Nonnull
    public Tuple getPrimaryKey() {
        return primaryKey;
    }

    public double getDistance() {
        return distance;
    }

    public NodeReferenceWithDistance toElement() {
        return new NodeReferenceWithDistance(getPrimaryKey(), getDistance());
    }
}
