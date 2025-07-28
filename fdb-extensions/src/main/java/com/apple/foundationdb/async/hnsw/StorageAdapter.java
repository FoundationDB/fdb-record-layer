/*
 * StorageAdapter.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.christianheina.langx.half4j.Half;
import com.google.common.base.Verify;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Storage adapter used for serialization and deserialization of nodes.
 */
interface StorageAdapter {
    /**
     * Get the {@link HNSW.Config} associated with this storage adapter.
     * @return the configuration used by this storage adapter
     */
    @Nonnull
    HNSW.Config getConfig();

    /**
     * Get the subspace used to store this r-tree.
     *
     * @return r-tree subspace
     */
    @Nonnull
    Subspace getSubspace();

    /**
     * Get the subspace used to store a node slot index if in warranted by the {@link HNSW.Config}.
     *
     * @return secondary subspace or {@code null} if we do not maintain a node slot index
     */
    @Nullable
    Subspace getSecondarySubspace();

    @Nonnull
    Subspace getEntryNodeSubspace();

    @Nonnull
    Subspace getDataSubspace();

    /**
     * Get the on-write listener.
     *
     * @return the on-write listener.
     */
    @Nonnull
    OnWriteListener getOnWriteListener();

    /**
     * Get the on-read listener.
     *
     * @return the on-read listener.
     */
    @Nonnull
    OnReadListener getOnReadListener();

    CompletableFuture<EntryNodeReference> fetchEntryNodeReference(@Nonnull ReadTransaction readTransaction);

    void writeEntryNodeReference(@Nonnull final Transaction transaction,
                                 @Nonnull final EntryNodeReference entryNodeReference);

    @Nonnull
    <N extends NodeReference> CompletableFuture<Node<N>> fetchNode(@Nonnull NodeFactory<N> nodeFactory,
                                                                   @Nonnull ReadTransaction readTransaction,
                                                                   int layer,
                                                                   @Nonnull Tuple primaryKey);

    <N extends NodeReference> void writeNode(@Nonnull final Transaction transaction, @Nonnull final Node<N> node,
                                             int layer);

    @Nonnull
    static Vector<Half> vectorFromTuple(final Tuple vectorTuple) {
        final Half[] vectorHalfs = new Half[vectorTuple.size()];
        for (int i = 0; i < vectorTuple.size(); i ++) {
            vectorHalfs[i] = Half.shortBitsToHalf(shortFromBytes(vectorTuple.getBytes(i)));
        }
        return new Vector.HalfVector(vectorHalfs);
    }

    @Nonnull
    static Tuple tupleFromVector(final Vector<Half> vector) {
        final List<byte[]> vectorBytes = Lists.newArrayListWithExpectedSize(vector.size());
        for (int i = 0; i < vector.size(); i ++) {
            vectorBytes.add(bytesFromShort(Half.halfToShortBits(vector.getComponent(i))));
        }
        return Tuple.fromList(vectorBytes);
    }

    static short shortFromBytes(byte[] bytes) {
        Verify.verify(bytes.length == 2);
        int high = bytes[0] & 0xFF;   // Convert to unsigned int
        int low  = bytes[1] & 0xFF;

        return (short) ((high << 8) | low);
    }

    static byte[] bytesFromShort(short value) {
        byte[] result = new byte[2];
        result[0] = (byte) ((value >> 8) & 0xFF);  // high byte first
        result[1] = (byte) (value & 0xFF);         // low byte second
        return result;
    }
}
