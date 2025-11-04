/*
 * NodeReferenceWithVector.java
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

import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Represents a reference to a node that includes an associated vector.
 * <p>
 * This class extends {@link NodeReference} by adding a {@link RealVector} field. It encapsulates both the primary key
 * of a node and its corresponding vector data, which is particularly useful in vector-based search and
 * indexing scenarios. Primarily, node references are used to refer to {@link Node}s in a storage-independent way, i.e.
 * a node reference always contains the vector of a node while the node itself (depending on the storage adapter)
 * may not.
 */
public class NodeReferenceWithVector extends NodeReference {
    @Nonnull
    private final Transformed<RealVector> vector;

    /**
     * Constructs a new {@code NodeReferenceWithVector} with a specified primary key and vector.
     * <p>
     * The primary key is used to initialize the parent class via a call to {@code super()},
     * while the vector is stored as a field in this instance. Both parameters are expected
     * to be non-null.
     *
     * @param primaryKey the primary key of the node, must not be null
     * @param vector the vector associated with the node, must not be null
     */
    public NodeReferenceWithVector(@Nonnull final Tuple primaryKey, @Nonnull final Transformed<RealVector> vector) {
        super(primaryKey);
        this.vector = vector;
    }

    /**
     * Gets the vector of {@code Half} objects.
     * <p>
     * This method provides access to the internal vector. The returned vector is guaranteed
     * not to be null, as indicated by the {@code @Nonnull} annotation.
     *
     * @return the vector of {@code Half} objects; will never be {@code null}.
     */
    @Nonnull
    public Transformed<RealVector> getVector() {
        return vector;
    }

    /**
     * Override to declare that this class in fact is a {@link NodeReferenceWithVector}.
     * @return {@code true}
     */
    @Override
    boolean isNodeReferenceWithVector() {
        return true;
    }

    /**
     * Returns this instance cast as a {@code NodeReferenceWithVector}.
     * @return this instance as a {@code NodeReferenceWithVector}, which is never {@code null}.
     */
    @Nonnull
    @Override
    public NodeReferenceWithVector asNodeReferenceWithVector() {
        return this;
    }

    /**
     * Compares this {@code NodeReferenceWithVector} to the specified object for equality.
     * @param o the object to compare with this {@code NodeReferenceWithVector}.
     * @return {@code true} if the objects are equal; {@code false} otherwise.
     */
    @Override
    public boolean equals(final Object o) {
        if (!super.equals(o)) {
            return false;
        }
        return Objects.equals(vector, ((NodeReferenceWithVector)o).vector);
    }

    /**
     * Computes the hash code for this object.
     * @return a hash code value for this object.
     */
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), vector);
    }

    /**
     * Returns a string representation of this object.
     * @return a concise string representation of this object.
     */
    @Override
    public String toString() {
        return "NRV[primaryKey=" + getPrimaryKey() + ";vector=" + vector + "]";
    }
}
