/*
 * ProbableIntersectionCursorState.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.cursors;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Verify;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import com.google.protobuf.ByteString;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * A cursor state for merge cursors that also remembers information about which comparison key values it has seen.
 * In particular, it maintains a Bloom filter computed from all of the results that have been processed by the
 * cursor so far. This Bloom filter is then included as part of this cursor's continuation so that the cursor
 * can be resumed only from its continuation. It also maintains a hash set of all results it has seen since it was
 * last resumed from its continuation, but this set is <em>not</em> included in the continuation and only serves to
 * lessen the number of false positives.
 *
 * <p>
 * This currently uses the Guava {@link BloomFilter} implementation internally. This could be changed in the future,
 * but if it is, care should be taken to make sure the serialized continuation is either compatible with that
 * implementation's serialization logic or is versioned in some way to prevent unpredictable behavior during
 * upgrades.
 * </p>
 *
 * @param <T> the type of elements returned by the wrapping cursor
 */
class ProbableIntersectionCursorState<T> extends KeyedMergeCursorState<T> {
    @Nonnull
    private final BloomFilter<List<Object>> bloomFilter;
    @Nonnull
    private final Set<List<Object>> seenSet;
    private final boolean firstIteration;

    private ProbableIntersectionCursorState(@Nonnull RecordCursor<T> cursor, @Nonnull BloomFilterCursorContinuation continuation,
                                    @Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction,
                                    @Nonnull BloomFilter<List<Object>> bloomFilter,
                                    @Nonnull Set<List<Object>> seenSet, boolean firstIteration) {
        super(cursor, continuation.getChild(), comparisonKeyFunction);
        this.bloomFilter = bloomFilter;
        this.seenSet = seenSet;
        this.firstIteration = firstIteration;
    }

    @Override
    public void consume() {
        // When consuming, insert the key of the most recent thing returned by this cursor.
        bloomFilter.put(getComparisonKey());
        seenSet.add(getComparisonKey());
        super.consume();
    }

    @Override
    @Nonnull
    public BloomFilterCursorContinuation getContinuation() {
        try (ByteString.Output bloomOutput = ByteString.newOutput()) {
            bloomFilter.writeTo(bloomOutput);
            return new BloomFilterCursorContinuation(super.getContinuation(), bloomOutput.toByteString());
        } catch (IOException e) {
            throw new RecordCoreException("unable to serialize bloom filter", e);
        }
    }

    @VisibleForTesting
    BloomFilter<List<Object>> getBloomFilter() {
        return bloomFilter;
    }

    /**
     * Return whether this cursor state might have seen the given comparison key. If the
     * key has been seen before, this will return {@code true}. If the key has <em>not</em>
     * seen the key before, then this will <em>probably</em> return {@code false}, but
     * it might return {@code true}.
     *
     * @param otherComparisonKey comparison key from another cursor
     * @return whether this key might have seen that comparison key before
     */
    boolean mightContain(@Nonnull List<Object> otherComparisonKey) {
        // If the comparison key is in this state's list of seen elements, then
        // it is definitely contained. If this is the first iteration (i.e., this
        // cursor has not been resumed after a continuation), then it might be
        // necessary to consult the bloom filter.
        return seenSet.contains(otherComparisonKey) || (!firstIteration && bloomFilter.mightContain(otherComparisonKey));
    }

    boolean isDefiniteDuplicate() {
        return seenSet.contains(getComparisonKey());
    }

    // This is an enum as is the suggestion of the BloomFilter documentation
    private enum KeyFunnel implements Funnel<List<Object>> {
        VERSION_0, // For backwards compatibility reasons.
        ;

        @Override
        public void funnel(List<Object> objects, PrimitiveSink primitiveSink) {
            for (Object o : Verify.verifyNotNull(objects)) {
                if (o == null) {
                    primitiveSink.putByte((byte)0x00);
                } else if (o instanceof byte[]) {
                    primitiveSink.putBytes((byte[])o);
                } else if (o instanceof ByteString) {
                    primitiveSink.putBytes(((ByteString)o).toByteArray());
                } else if (o instanceof ByteBuffer) {
                    primitiveSink.putBytes((ByteBuffer) o);
                } else if (o instanceof String) {
                    primitiveSink.putString((String)o, StandardCharsets.UTF_8);
                } else if (o instanceof Float) {
                    primitiveSink.putFloat((float)o);
                } else if (o instanceof Double) {
                    primitiveSink.putDouble((double)o);
                } else if (o instanceof Integer) {
                    primitiveSink.putInt((int)o);
                } else if (o instanceof Long) {
                    primitiveSink.putLong((long) o);
                } else if (o instanceof Boolean) {
                    primitiveSink.putBoolean((boolean)o);
                } else if (o instanceof Enum) {
                    primitiveSink.putInt(((Enum)o).ordinal());
                } else {
                    primitiveSink.putBytes(Tuple.from(o).pack());
                }
            }
        }
    }

    @Nonnull
    static <T> ProbableIntersectionCursorState<T> from(
            @Nonnull Function<byte[], RecordCursor<T>> cursorFunction,
            @Nonnull BloomFilterCursorContinuation continuation,
            @Nonnull Function<? super T, ? extends List<Object>> comparisonKeyFunction,
            long expectedInsertions, double falsePositiveRate) {
        BloomFilter<List<Object>> bloomFilter;
        if (continuation.getBloomBytes() == null) {
            bloomFilter = BloomFilter.create(KeyFunnel.VERSION_0, expectedInsertions, falsePositiveRate);
        } else {
            try {
                bloomFilter = BloomFilter.readFrom(continuation.getBloomBytes().newInput(), KeyFunnel.VERSION_0);
            } catch (IOException e) {
                throw new RecordCoreException("unable to deserialize bloom filter", e);
            }
        }
        if (continuation.isChildEnd()) {
            return new ProbableIntersectionCursorState<>(RecordCursor.empty(), continuation, comparisonKeyFunction, bloomFilter, Collections.emptySet(), false);
        } else {
            return new ProbableIntersectionCursorState<>(cursorFunction.apply(continuation.getChild().toBytes()), continuation, comparisonKeyFunction, bloomFilter, new HashSet<>(), continuation.getBloomBytes() == null);
        }
    }
}
