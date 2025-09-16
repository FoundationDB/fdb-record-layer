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
import com.christianheina.langx.half4j.Half;

import javax.annotation.Nonnull;
import java.util.Objects;

class EntryNodeReference extends NodeReferenceWithVector {
    private final int layer;

    public EntryNodeReference(@Nonnull final Tuple primaryKey, @Nonnull final Vector<Half> vector, final int layer) {
        super(primaryKey, vector);
        this.layer = layer;
    }

    public int getLayer() {
        return layer;
    }

    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof EntryNodeReference)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        return layer == ((EntryNodeReference)o).layer;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), layer);
    }
}
