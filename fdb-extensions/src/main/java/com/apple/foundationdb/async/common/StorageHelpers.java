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
import java.util.SplittableRandom;
import java.util.concurrent.CompletableFuture;

public final class StorageHelpers {
    private StorageHelpers() {
        // nothing
    }

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
                        onReadListener.onKeyValueRead(-1, key, value);
                    }
                    return resultBuilder.build();
                });
    }

    @Nullable
    public static AggregatedVector aggregateVectors(@Nonnull final Iterable<AggregatedVector> vectors) {
        Transformed<RealVector> partialVector = null;
        int partialCount = 0;
        for (final AggregatedVector vector : vectors) {
            partialVector = partialVector == null
                            ? vector.getPartialVector() : partialVector.add(vector.getPartialVector());
            partialCount += vector.getPartialCount();
        }
        return partialCount == 0 ? null : new AggregatedVector(partialCount, partialVector);
    }

    public static void appendSampledVector(@Nonnull final Transaction transaction,
                                           @Nonnull final SplittableRandom random,
                                           @Nonnull final Subspace prefixSubspace,
                                           final int partialCount,
                                           @Nonnull final Transformed<RealVector> vector,
                                           @Nonnull final OnKeyValueWriteListener onWriteListener) {
        final Subspace keySubspace = prefixSubspace.subspace(Tuple.from(partialCount, RandomHelpers.randomUUID(random)));
        final byte[] prefixKey = keySubspace.pack();
        // getting underlying is okay as it is only written to the database
        final byte[] value = tupleFromVector(vector.getUnderlyingVector().toDoubleRealVector()).pack();
        transaction.set(prefixKey, value);
        onWriteListener.onKeyValueWritten(-1, prefixKey, value);
    }

    public static void deleteAllSampledVectors(@Nonnull final Transaction transaction, @Nonnull final Subspace prefixSubspace,
                                               @Nonnull final OnKeyValueWriteListener onWriteListener) {
        final byte[] prefixKey = prefixSubspace.pack();
        final Range range = Range.startsWith(prefixKey);
        transaction.clear(range);
        onWriteListener.onRangeDeleted(-1, range);
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

    @Nonnull
    public static byte[] bytesFromVector(@Nonnull final Transformed<RealVector> transformedVector) {
        return transformedVector.getUnderlyingVector().getRawData();
    }

    /**
     * Creates a {@link RealVector} from a given {@link Tuple}.
     * <p>
     * This method assumes the vector data is stored as a byte array at the first. position (index 0) of the tuple. It
     * extracts this byte array and then delegates to the {@link #vectorFromBytes(BaseConfig, byte[])} method for the
     * actual conversion.
     * @param config an HNSW configuration
     * @param vectorTuple the tuple containing the vector data as a byte array at index 0. Must not be {@code null}.
     * @return a new {@link RealVector} instance created from the tuple's data.
     *         This method never returns {@code null}.
     */
    @Nonnull
    public static RealVector vectorFromTuple(@Nonnull final BaseConfig config, @Nonnull final Tuple vectorTuple) {
        return vectorFromBytes(config, vectorTuple.getBytes(0));
    }

    /**
     * Creates a {@link RealVector} from a byte array.
     * <p>
     * This method interprets the input byte array by interpreting the first byte of the array.
     * It the delegates to {@link RealVector#fromBytes(VectorType, byte[])}.
     * @param config an HNSW config
     * @param vectorBytes the non-null byte array to convert.
     * @return a new {@link RealVector} instance created from the byte array.
     */
    @Nonnull
    public static RealVector vectorFromBytes(@Nonnull final BaseConfig config, @Nonnull final byte[] vectorBytes) {
        final byte vectorTypeOrdinal = vectorBytes[0];
        switch (RealVector.fromVectorTypeOrdinal(vectorTypeOrdinal)) {
            case RABITQ:
                Verify.verify(config.isUseRaBitQ());
                return EncodedRealVector.fromBytes(vectorBytes, config.getNumDimensions(), config.getRaBitQNumExBits());
            case HALF:
            case SINGLE:
            case DOUBLE:
                return RealVector.fromBytes(vectorBytes);
            default:
                throw new RuntimeException("unable to serialize vector");
        }
    }
}
