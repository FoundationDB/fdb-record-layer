/*
 * EntryNodeReference.java
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

/**
 * Represents an entry reference to a node within a hierarchical graph structure.
 * <p>
 * This class extends {@link NodeReferenceWithVector} by adding a {@code layer}
 * attribute. It is used to encapsulate all the necessary information for an
 * entry point into a specific layer of the graph, including its unique identifier
 * (primary key), its vector representation, and its hierarchical level.
 */
class EntryNodeReference extends NodeReferenceWithVector {
    private final int layer;

    /**
     * Constructs a new reference to an entry node.
     * <p>
     * This constructor initializes the node with its primary key, its associated vector,
     * and the specific layer it belongs to within a hierarchical graph structure. It calls the
     * superclass constructor to set the {@code primaryKey} and {@code vector}.
     *
     * @param primaryKey the primary key identifying the node. Must not be {@code null}.
     * @param vector the vector data associated with the node. Must not be {@code null}.
     * @param layer the layer number where this entry node is located.
     */
    public EntryNodeReference(@Nonnull final Tuple primaryKey, @Nonnull final Vector<Half> vector, final int layer) {
        super(primaryKey, vector);
        this.layer = layer;
    }

    /**
     * Gets the layer value for this object.
     * @return the integer representing the layer
     */
    public int getLayer() {
        return layer;
    }

    /**
     * Compares this {@code EntryNodeReference} to the specified object for equality.
     * <p>
     * The result is {@code true} if and only if the argument is an instance of {@code EntryNodeReference}, the
     * superclass's {@link #equals(Object)} method returns {@code true}, and the {@code layer} fields of both objects
     * are equal.
     * @param o the object to compare this {@code EntryNodeReference} against.
     * @return {@code true} if the given object is equal to this one; {@code false} otherwise.
     */
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

    /**
     * Generates a hash code for this object.
     * <p>
     * The hash code is computed by combining the hash code of the superclass with the hash code of the {@code layer}
     * field. This implementation is consistent with the contract of {@link Object#hashCode()}.
     * @return a hash code value for this object.
     */
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), layer);
    }
}
