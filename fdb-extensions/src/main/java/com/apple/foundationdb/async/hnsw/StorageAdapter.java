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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

/**
 * Storage adapter used for serialization and deserialization of nodes.
 */
interface StorageAdapter<N extends NodeReference> {
    byte SUBSPACE_PREFIX_ENTRY_NODE = 0x01;
    byte SUBSPACE_PREFIX_DATA = 0x02;

    /**
     * Get the {@link HNSW.Config} associated with this storage adapter.
     * @return the configuration used by this storage adapter
     */
    @Nonnull
    HNSW.Config getConfig();

    @Nonnull
    NodeFactory<N> getNodeFactory();

    @Nonnull
    NodeKind getNodeKind();

    @Nonnull
    StorageAdapter<NodeReference> asCompactStorageAdapter();

    @Nonnull
    StorageAdapter<NodeReferenceWithVector> asInliningStorageAdapter();

    /**
     * Get the subspace used to store this r-tree.
     *
     * @return r-tree subspace
     */
    @Nonnull
    Subspace getSubspace();

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

    @Nonnull
    CompletableFuture<Node<N>> fetchNode(@Nonnull ReadTransaction readTransaction,
                                         int layer,
                                         @Nonnull Tuple primaryKey);

    void writeNode(@Nonnull Transaction transaction, @Nonnull Node<N> node, int layer,
                   @Nonnull NeighborsChangeSet<N> changeSet);

    Iterable<Node<N>> scanLayer(@Nonnull ReadTransaction readTransaction, int layer, @Nullable Tuple lastPrimaryKey,
                                int maxNumRead);

    @Nonnull
    static CompletableFuture<EntryNodeReference> fetchEntryNodeReference(@Nonnull final ReadTransaction readTransaction,
                                                                         @Nonnull final Subspace subspace,
                                                                         @Nonnull final OnReadListener onReadListener) {
        final Subspace entryNodeSubspace = subspace.subspace(Tuple.from(SUBSPACE_PREFIX_ENTRY_NODE));
        final byte[] key = entryNodeSubspace.pack();

        return readTransaction.get(key)
                .thenApply(valueBytes -> {
                    if (valueBytes == null) {
                        return null; // not a single node in the index
                    }
                    onReadListener.onKeyValueRead(-1, key, valueBytes);

                    final Tuple entryTuple = Tuple.fromBytes(valueBytes);
                    final int lMax = (int)entryTuple.getLong(0);
                    final Tuple primaryKey = entryTuple.getNestedTuple(1);
                    final Tuple vectorTuple = entryTuple.getNestedTuple(2);
                    return new EntryNodeReference(primaryKey, StorageAdapter.vectorFromTuple(vectorTuple), lMax);
                });
    }

    static void writeEntryNodeReference(@Nonnull final Transaction transaction,
                                        @Nonnull final Subspace subspace,
                                        @Nonnull final EntryNodeReference entryNodeReference,
                                        @Nonnull final OnWriteListener onWriteListener) {
        final Subspace entryNodeSubspace = subspace.subspace(Tuple.from(SUBSPACE_PREFIX_ENTRY_NODE));
        final byte[] key = entryNodeSubspace.pack();
        final byte[] value = Tuple.from(entryNodeReference.getLayer(),
                entryNodeReference.getPrimaryKey(),
                StorageAdapter.tupleFromVector(entryNodeReference.getVector())).pack();
        transaction.set(key,
                value);
        onWriteListener.onKeyValueWritten(entryNodeReference.getLayer(), key, value);
    }

    @Nonnull
    static Vector.HalfVector vectorFromTuple(final Tuple vectorTuple) {
        return vectorFromBytes(vectorTuple.getBytes(0));
    }

    @Nonnull
    static Vector.HalfVector vectorFromBytes(final byte[] vectorBytes) {
        final int bytesLength = vectorBytes.length;
        Verify.verify(bytesLength % 2 == 0);
        final int componentSize = bytesLength >>> 1;
        final Half[] vectorHalfs = new Half[componentSize];
        for (int i = 0; i < componentSize; i ++) {
            vectorHalfs[i] = Half.shortBitsToHalf(shortFromBytes(vectorBytes, i << 1));
        }
        return new Vector.HalfVector(vectorHalfs);
    }


    @Nonnull
    @SuppressWarnings("PrimitiveArrayArgumentToVarargsMethod")
    static Tuple tupleFromVector(final Vector<Half> vector) {
        return Tuple.from(bytesFromVector(vector));
    }

    @Nonnull
    static byte[] bytesFromVector(final Vector<Half> vector) {
        final byte[] vectorBytes = new byte[2 * vector.size()];
        for (int i = 0; i < vector.size(); i ++) {
            final byte[] componentBytes = bytesFromShort(Half.halfToShortBits(vector.getComponent(i)));
            final int indexTimesTwo = i << 1;
            vectorBytes[indexTimesTwo] = componentBytes[0];
            vectorBytes[indexTimesTwo + 1] = componentBytes[1];
        }
        return vectorBytes;
    }

    static short shortFromBytes(final byte[] bytes, final int offset) {
        Verify.verify(offset % 2 == 0);
        int high = bytes[offset] & 0xFF;   // Convert to unsigned int
        int low  = bytes[offset + 1] & 0xFF;

        return (short) ((high << 8) | low);
    }

    static byte[] bytesFromShort(final short value) {
        byte[] result = new byte[2];
        result[0] = (byte) ((value >> 8) & 0xFF);  // high byte first
        result[1] = (byte) (value & 0xFF);         // low byte second
        return result;
    }
}
