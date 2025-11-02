/*
 * NodeReferenceWithDistance.java
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
 * Represents a reference to a node that includes its vector and its distance from a query vector.
 * <p>
 * This class extends {@link NodeReferenceWithVector} by additionally associating a distance value, typically the result
 * of a distance calculation in a nearest neighbor search. Objects of this class are immutable.
 */
public class NodeReferenceWithDistance extends NodeReferenceWithVector {
    private final double distance;

    /**
     * Constructs a new instance of {@code NodeReferenceWithDistance}.
     * <p>
     * This constructor initializes the reference with the node's primary key, its vector, and the calculated distance
     * from some origin vector (e.g., a query vector). It calls the superclass constructor to set the {@code primaryKey}
     * and {@code vector}.
     * @param primaryKey the primary key of the referenced node, represented as a {@link Tuple}. Must not be null.
     * @param vector the vector associated with the referenced node. Must not be null.
     * @param distance the calculated distance of this node reference to some query vector or similar.
     */
    public NodeReferenceWithDistance(@Nonnull final Tuple primaryKey, @Nonnull final Transformed<RealVector> vector,
                                     final double distance) {
        super(primaryKey, vector);
        this.distance = distance;
    }

    /**
     * Gets the distance.
     * @return the current distance value
     */
    public double getDistance() {
        return distance;
    }

    /**
     * Compares this object against the specified object for equality.
     * <p>
     * The result is {@code true} if and only if the argument is not {@code null},
     * is a {@code NodeReferenceWithDistance} object, has the same properties as
     * determined by the superclass's {@link #equals(Object)} method, and has
     * the same {@code distance} value.
     * @param o the object to compare with this instance for equality.
     * @return {@code true} if the specified object is equal to this {@code NodeReferenceWithDistance};
     *         {@code false} otherwise.
     */
    @Override
    public boolean equals(final Object o) {
        if (!super.equals(o)) {
            return false;
        }
        final NodeReferenceWithDistance that = (NodeReferenceWithDistance)o;
        return Double.compare(distance, that.distance) == 0;
    }

    /**
     * Generates a hash code for this object.
     * @return a hash code value for this object.
     */
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), distance);
    }
}
