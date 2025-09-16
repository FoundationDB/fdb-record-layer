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

import com.apple.foundationdb.tuple.Tuple;
import com.christianheina.langx.half4j.Half;
import com.google.common.base.Objects;

import javax.annotation.Nonnull;

public class NodeReferenceWithVector extends NodeReference {
    @Nonnull
    private final Vector<Half> vector;

    public NodeReferenceWithVector(@Nonnull final Tuple primaryKey, @Nonnull final Vector<Half> vector) {
        super(primaryKey);
        this.vector = vector;
    }

    @Nonnull
    public Vector<Half> getVector() {
        return vector;
    }

    @Nonnull
    public Vector<Double> getDoubleVector() {
        return vector.toDoubleVector();
    }

    @Nonnull
    @Override
    public NodeReferenceWithVector asNodeReferenceWithVector() {
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof NodeReferenceWithVector)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        return Objects.equal(vector, ((NodeReferenceWithVector)o).vector);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), vector);
    }

    @Override
    public String toString() {
        return "NRV[primaryKey=" + getPrimaryKey() +
                ";vector=" + vector.toString(3) +
                "]";
    }
}
