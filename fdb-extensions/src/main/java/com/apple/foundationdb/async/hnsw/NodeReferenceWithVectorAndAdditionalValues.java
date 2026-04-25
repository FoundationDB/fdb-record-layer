/*
 * NodeReferenceWithVectorAndAdditionalValues.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Represents a reference to a node within a hierarchical graph structure that also flows a vector and additional
 * values that are also present in the node.
 * <p>
 * This class extends {@link NodeReferenceWithVector} by adding a {@code additionalValues} attribute.
 */
class NodeReferenceWithVectorAndAdditionalValues extends NodeReferenceWithVector {
    @Nullable
    private final Tuple additionaValues;

    /**
     * Constructs a new reference.
     *
     * @param primaryKey the primary key identifying the node. Must not be {@code null}.
     * @param vector the vector data associated with the node. Must not be {@code null}.
     * @param additionalValues additional values that are stored with and associated with the node this reference
     *        refers to.
     */
    public NodeReferenceWithVectorAndAdditionalValues(@Nonnull final Tuple primaryKey, @Nonnull final Transformed<RealVector> vector,
                                                      @Nullable final Tuple additionalValues) {
        super(primaryKey, vector);
        this.additionaValues = additionalValues;
    }

    /**
     * Gets the additional values for this object.
     * @return the additional values
     */
    @Nullable
    public Tuple getAdditionalValues() {
        return additionaValues;
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
        if (!super.equals(o)) {
            return false;
        }
        final NodeReferenceWithVectorAndAdditionalValues that = (NodeReferenceWithVectorAndAdditionalValues)o;
        return Objects.equals(additionaValues, that.getAdditionalValues());
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
        return Objects.hash(super.hashCode(), getAdditionalValues());
    }
}
