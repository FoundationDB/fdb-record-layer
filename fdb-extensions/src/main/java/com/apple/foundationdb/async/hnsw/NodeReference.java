/*
 * NodeReference.java
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
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Represents a reference to a node, uniquely identified by its primary key. It provides fundamental operations such as
 * equality comparison, hashing, and string representation based on this key. It also serves as a base class for more
 * specialized node references.
 */
public class NodeReference {
    @Nonnull
    private final Tuple primaryKey;

    /**
     * Constructs a new {@code NodeReference} with the specified primary key.
     * @param primaryKey the primary key of the node to reference; must not be {@code null}.
     */
    public NodeReference(@Nonnull final Tuple primaryKey) {
        this.primaryKey = primaryKey;
    }

    /**
     * Gets the primary key for this object.
     * @return the primary key as a {@code Tuple} object, which is guaranteed to be non-null.
     */
    @Nonnull
    public Tuple getPrimaryKey() {
        return primaryKey;
    }

    /**
     * Method to indicate if the method {@link #asNodeReferenceWithVector()} can be safely called.
     * @return {@code true} iff this instance is in fact at least a {@link NodeReferenceWithVector}.
     */
    boolean isNodeReferenceWithVector() {
        return false;
    }

    /**
     * Casts this object to a {@link NodeReferenceWithVector}.
     * <p>
     * This method is intended to be used on subclasses that actually represent a node reference with a vector. For this
     * base class or specific implementation, it is not a valid operation.
     * @return this instance cast as a {@code NodeReferenceWithVector}
     * @throws IllegalStateException always, to indicate that this object cannot be
     *         represented as a {@link NodeReferenceWithVector}.
     */
    @Nonnull
    public NodeReferenceWithVector asNodeReferenceWithVector() {
        throw new IllegalStateException("method should not be called");
    }

    /**
     * Compares this {@code NodeReference} to the specified object for equality.
     * <p>
     * The result is {@code true} if and only if the argument is not {@code null} and is a {@code NodeReference} object
     * that has the same {@code primaryKey} as this object.
     *
     * @param o the object to compare with this {@code NodeReference} for equality.
     * @return {@code true} if the given object is equal to this one;
     *         {@code false} otherwise.
     */
    @Override
    public boolean equals(final Object o) {
        if (o == null) {
            return false;
        }
        if (this == o) {
            return true;
        }
        if (o.getClass() != this.getClass()) {
            return false;
        }
        final NodeReference that = (NodeReference)o;
        return Objects.equals(primaryKey, that.primaryKey);
    }

    /**
     * Generates a hash code for this object based on the primary key.
     * @return a hash code value for this object.
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(primaryKey);
    }

    /**
     * Returns a string representation of the object.
     * @return a string representation of this object.
     */
    @Override
    public String toString() {
        return "NR[primaryKey=" + primaryKey + "]";
    }

    /**
     * Helper to extract the primary keys from a given collection of node references.
     * @param neighbors an iterable of {@link NodeReference} objects from which to extract primary keys.
     * @return a lazily-evaluated {@code Iterable} of {@link Tuple}s, representing the primary keys of the input nodes.
     */
    @Nonnull
    public static Iterable<Tuple> primaryKeys(@Nonnull Iterable<? extends NodeReference> neighbors) {
        return () -> Streams.stream(neighbors)
                .map(nodeReference ->
                        Objects.requireNonNull(nodeReference).getPrimaryKey())
                .iterator();
    }
}
