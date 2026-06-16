/*
 * StorageHelpers.java
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

package com.apple.foundationdb.async.common;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.linear.AffineOperator;
import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.linear.VectorType;
import com.apple.foundationdb.rabitq.EncodedRealVector;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Static helpers for the storage layer shared by the vector index implementations: (de)serializing vectors to and
 * from their {@link com.apple.foundationdb.tuple.Tuple} and byte representations, and appending to, consuming,
 * aggregating, and clearing the set of sampled vectors held in a subspace. Not instantiable.
 */
public final class StorageHelpers {
    private StorageHelpers() {
        // nothing
    }

    /**
     * Reads up to {@code numMaxVectors} sampled vectors from the given subspace and removes them as it goes,
     * draining that portion of the sample buffer. Each consumed key is read on a snapshot, registered as a read
     * conflict, cleared, and reported to the listener, so the transaction conflicts only on the keys actually
     * consumed rather than on the whole range.
     *
     * @param transaction the transaction to read and clear within
     * @param prefixSubspace the subspace holding the sampled vectors
     * @param numMaxVectors the maximum number of sampled vectors to consume
     * @param onReadListener the listener notified of each key/value read
     *
     * @return a future completing with the consumed sampled vectors
     */
    @Nonnull
    public static CompletableFuture<List<AggregatedVector>> consumeSampledVectors(@Nonnull final Transaction transaction,
                                                                                  @Nonnull final Subspace prefixSubspace,
                                                                                  final int numMaxVectors,
                                                                                  @Nonnull final OnKeyValueReadListener onReadListener) {
        final byte[] prefixKey = prefixSubspace.pack();
        final ReadTransaction snapshot = transaction.snapshot();
        final Range range = Range.startsWith(prefixKey);

        return AsyncUtil.collect(snapshot.getRange(range, numMaxVectors, true, StreamingMode.ITERATOR),
                        snapshot.getExecutor())
                .thenApply(keyValues -> {
                    final ImmutableList.Builder<AggregatedVector> resultBuilder = ImmutableList.builder();
                    for (final KeyValue keyValue : keyValues) {
                        final byte[] key = keyValue.getKey();
                        final byte[] value = keyValue.getValue();
                        resultBuilder.add(aggregatedVectorFromRaw(prefixSubspace, key, value));
                        // this is done to not lock the entire range we just read but jst the keys we did read
                        transaction.addReadConflictKey(key);
                        transaction.clear(key);
                        onReadListener.onKeyValueRead(key, value);
                    }
                    return resultBuilder.build();
                });
    }

    /**
     * Combines a collection of {@link AggregatedVector}s into a single aggregate by summing their partial vectors
     * and counts.
     *
     * @param vectors the partial aggregates to combine
     *
     * @return the combined aggregate, or {@code null} if {@code vectors} contributes no elements
     */
    @Nullable
    public static AggregatedVector aggregateVectors(@Nonnull final Iterable<AggregatedVector> vectors) {
        Transformed<RealVector> partialVector = null;
        int partialCount = 0;
        for (final AggregatedVector vector : vectors) {
            partialVector = partialVector == null
                            ? vector.partialVector() : partialVector.add(vector.partialVector());
            partialCount += vector.partialCount();
        }
        return partialCount == 0 ? null : new AggregatedVector(partialCount, partialVector);
    }

    /**
     * Appends a single sampled vector to the sample buffer in the given subspace, under a key derived from its
     * partial count and a fresh (optionally deterministic) random id.
     *
     * @param transaction the transaction to write within
     * @param deterministicRandomness whether to derive the key's id deterministically rather than randomly
     * @param prefixSubspace the subspace holding the sampled vectors
     * @param partialCount the number of vectors this sample aggregates
     * @param vector the sampled (partial) vector to store
     * @param onWriteListener the listener notified of the written key/value
     */
    public static void appendSampledVector(@Nonnull final Transaction transaction,
                                           final boolean deterministicRandomness,
                                           @Nonnull final Subspace prefixSubspace,
                                           final int partialCount,
                                           @Nonnull final Transformed<RealVector> vector,
                                           @Nonnull final OnKeyValueWriteListener onWriteListener) {
        final Subspace keySubspace = prefixSubspace.subspace(Tuple.from(partialCount,
                RandomHelpers.randomUuid(deterministicRandomness)));
        final byte[] prefixKey = keySubspace.pack();
        // getting underlying is okay as it is only written to the database
        final byte[] value = tupleFromVector(vector.getUnderlyingVector().toDoubleRealVector()).pack();
        transaction.set(prefixKey, value);
        onWriteListener.onKeyValueWritten(prefixKey, value);
    }

    /**
     * Clears all sampled vectors held in the given subspace.
     *
     * @param transaction the transaction to clear within
     * @param prefixSubspace the subspace holding the sampled vectors
     * @param onWriteListener the listener notified of the cleared range
     */
    public static void deleteAllSampledVectors(@Nonnull final Transaction transaction, @Nonnull final Subspace prefixSubspace,
                                               @Nonnull final OnKeyValueWriteListener onWriteListener) {
        final byte[] prefixKey = prefixSubspace.pack();
        final Range range = Range.startsWith(prefixKey);
        transaction.clear(range);
        onWriteListener.onRangeDeleted(range);
    }

    @Nonnull
    private static AggregatedVector aggregatedVectorFromRaw(@Nonnull final Subspace prefixSubspace,
                                                            @Nonnull final byte[] key,
                                                            @Nonnull final byte[] value) {
        final Tuple keyTuple = prefixSubspace.unpack(key);
        final int partialCount = Math.toIntExact(keyTuple.getLong(0));
        final RealVector vector = DoubleRealVector.fromBytes(Tuple.fromBytes(value).getBytes(0));

        return new AggregatedVector(partialCount, AffineOperator.identity().transform(vector));
    }

    /**
     * Converts a transformed vector into a tuple.
     * @param vector a transformed vector
     * @return a new, non-null {@code Tuple} instance representing the contents of the underlying vector.
     */
    @Nonnull
    public static Tuple tupleFromVector(@Nonnull final Transformed<RealVector> vector) {
        return tupleFromVector(vector.getUnderlyingVector());
    }

    /**
     * Converts a {@link RealVector} into a {@link Tuple}.
     * <p>
     * This method first serializes the given vector into a byte array using the {@link RealVector#getRawData()} getter
     * method. It then creates a {@link Tuple} from the resulting byte array.
     * @param vector the {@link RealVector} to convert. Cannot be null.
     * @return a new, non-null {@code Tuple} instance representing the contents of the vector.
     */
    @Nonnull
    @SuppressWarnings("PrimitiveArrayArgumentToVarargsMethod")
    public static Tuple tupleFromVector(@Nonnull final RealVector vector) {
        return Tuple.from(vector.getRawData());
    }

    /**
     * Returns the raw byte representation of a transformed vector's underlying data, as stored in the database.
     *
     * @param transformedVector the transformed vector whose underlying raw bytes are returned
     *
     * @return the raw bytes of the underlying vector
     */
    @Nonnull
    public static byte[] bytesFromVector(@Nonnull final Transformed<RealVector> transformedVector) {
        return transformedVector.getUnderlyingVector().getRawData();
    }

    /**
     * Creates a {@link RealVector} from a given {@link Tuple}.
     * <p>
     * This method assumes the vector data is stored as a byte array at the first. position (index 0) of the tuple. It
     * extracts this byte array and then delegates to the {@link #vectorFromBytes(VectorEncodingConfig, byte[])} method for the
     * actual conversion.
     * @param config an HNSW configuration
     * @param vectorTuple the tuple containing the vector data as a byte array at index 0. Must not be {@code null}.
     * @return a new {@link RealVector} instance created from the tuple's data.
     *         This method never returns {@code null}.
     */
    @Nonnull
    public static RealVector vectorFromTuple(@Nonnull final VectorEncodingConfig config, @Nonnull final Tuple vectorTuple) {
        return vectorFromBytes(config, vectorTuple.getBytes(0));
    }

    /**
     * Creates a {@link RealVector} from a byte array.
     * <p>
     * This method reads the leading vector-type byte and, based on it, delegates to the appropriate decoder:
     * {@link EncodedRealVector} for RaBitQ-encoded vectors, otherwise {@link RealVector#fromBytes(byte[])}.
     * @param config a vector-encoding configuration
     * @param vectorBytes the non-null byte array to convert.
     * @return a new {@link RealVector} instance created from the byte array.
     */
    @Nonnull
    public static RealVector vectorFromBytes(@Nonnull final VectorEncodingConfig config, @Nonnull final byte[] vectorBytes) {
        final byte vectorTypeOrdinal = vectorBytes[0];
        return switch (RealVector.fromVectorTypeOrdinal(vectorTypeOrdinal)) {
            case RABITQ -> {
                Verify.verify(config.useRaBitQ());
                yield EncodedRealVector.fromBytes(vectorBytes, config.numDimensions(), config.raBitQNumExBits());
            }
            case HALF, SINGLE, DOUBLE -> RealVector.fromBytes(vectorBytes);
        };
    }
}
