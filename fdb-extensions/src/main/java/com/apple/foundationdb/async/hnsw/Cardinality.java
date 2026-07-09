/*
 * Cardinality.java
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

import com.apple.foundationdb.ReadTransaction;

import javax.annotation.Nonnull;

/**
 * A coarse classification of how many nodes live on a layer of the graph. It is deliberately coarse — only the
 * cases {@link #EMPTY}, {@link #SINGLE} and {@link #MULTIPLE} are distinguished — so that the underlying scan can
 * stop reading after the second node and need not count an entire layer.
 *
 * @see HNSW#cardinality(ReadTransaction)
 */
public enum Cardinality {
    /** The layer contains no nodes. */
    EMPTY,
    /** The layer contains exactly one node. */
    SINGLE,
    /** The layer contains two or more nodes. */
    MULTIPLE;

    /**
     * Maps a (possibly capped) node count to a {@link Cardinality}. Any count of two or more maps to
     * {@link #MULTIPLE}, so it is safe to pass the size of a list that was capped at two.
     *
     * @param count the number of nodes observed, which may be capped at two
     *
     * @return the matching {@link Cardinality}
     */
    @Nonnull
    static Cardinality fromCount(final int count) {
        if (count <= 0) {
            return EMPTY;
        } else if (count == 1) {
            return SINGLE;
        } else {
            return MULTIPLE;
        }
    }
}
