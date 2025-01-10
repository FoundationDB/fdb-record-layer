/*
 * AtomicMutation.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.BiFunction;

/**
 * The particular operation to be performed by an {@link AtomicMutationIndexMaintainer} index.
 */
@API(API.Status.UNSTABLE)
public interface AtomicMutation {

    /**
     * Get the underlying mutation performed by the FDB API.
     * @return the underlying mutation type
     */
    @Nonnull
    MutationType getMutationType();

    /**
     * Get the underlying argument to the FDB API.
     * @param value the {@link Tuple} form of the value being stored into the index
     * @param remove {@code true} if the entry is being removed from the index
     * @return a byte array to pass to the FDB API or {@code null} to do nothing for this mutation
     */
    @Nullable
    byte[] getMutationParam(IndexEntry value, boolean remove);

    /**
     * Get a function to aggregate multiple index entries.
     * For example, summing subtotals or maxing individual maxima.
     * @return a function that combines a running aggregate with a single entry to produce a new aggregate
     * @see com.apple.foundationdb.record.RecordCursor#reduce
     */
    @Nonnull
    BiFunction<Tuple, Tuple, Tuple> getAggregator();

    /**
     * Get the initial value for aggregating multiple index entries.
     * @return the initial value of a running aggregate for this type
     * @see com.apple.foundationdb.record.RecordCursor#reduce
     */
    @Nullable
    Tuple getIdentity();

    /**
     * Determine whether this type aggregates values (as opposed to something like counting records).
     *
     * The values are specified in the grouped part of the {@link com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression}.
     * @return {@code true} if values are allowed
     */
    boolean hasValues();

    /**
     * Determine whether this type aggregates exactly one value.
     * @return {@code true} if only a single value is allowed
     */
    boolean hasSingleValue();

    /**
     * Determine whether this type aggregates long (integer) values.
     * @return {@code true} if only a long value is allowed
     */
    boolean hasLongValue();

    /**
     * Determine whether this type allows negative values.
     *
     * If negative values are not allowed, an exception is thrown when a record is saved that contains a negative
     * value in the indexed field.
     * @return {@code true} if negative values are allowed
     */
    boolean allowsNegative();

    /**
     * Determine whether this type is idempotent.
     * Max and min type operations are idempotent; sum and count type operations are not.
     * @return {@code true} if updating the index multiple times with the same value yields the same result
     */
    boolean isIdempotent();

    /**
     * Get a value that causes the index entry to be removed if the result of the mutation matches.
     * @return a byte array to pass to {@code COMPARE_AND_CLEAR} or {@code null} to do nothing for this mutation
     */
    @Nullable
    byte[] getCompareAndClearParam();

    /**
     * The atomic mutations implemented straightforwardly by the FDB API.
     */
    @API(API.Status.UNSTABLE)
    enum Standard implements AtomicMutation {
        COUNT(MutationType.ADD),
        COUNT_UPDATES(MutationType.ADD),
        COUNT_NOT_NULL(MutationType.ADD),
        SUM_LONG(MutationType.ADD),
        MAX_EVER_TUPLE(MutationType.BYTE_MAX),
        MIN_EVER_TUPLE(MutationType.BYTE_MIN),
        // MutationType.MAX and MIN do little-endian unsigned comparison, which only works for long (and only if you disallow negatives).
        MAX_EVER_LONG(MutationType.MAX),
        MIN_EVER_LONG(MutationType.MIN),
        MAX_EVER_VERSION(MutationType.SET_VERSIONSTAMPED_VALUE),
        COUNT_CLEAR_WHEN_ZERO(MutationType.ADD),
        COUNT_NOT_NULL_CLEAR_WHEN_ZERO(MutationType.ADD),
        SUM_LONG_CLEAR_WHEN_ZERO(MutationType.ADD);
        
        @Nonnull
        private final MutationType mutationType;

        Standard(@Nonnull MutationType mutationType) {
            this.mutationType = mutationType;
        }

        @Override
        @Nonnull
        public MutationType getMutationType() {
            return mutationType;
        }

        @Override
        @Nullable
        @SuppressWarnings("fallthrough")
        public byte[] getMutationParam(IndexEntry entry, boolean remove) {
            // Not quite sure how to handle the case where an index entry has a value. One possibility might be
            // to pack the key and the value together into a single tuple.  For now, though, just check that it
            // has nothing in it.
            if (entry.getValue().size() != 0) {
                throw new RecordCoreException("Explicit value in atomic index mutation not supported")
                        .addLogInfo("value_size", entry.getValue().size());
            }

            Number numVal;
            long lval;
            switch (this) {
                case COUNT_NOT_NULL:
                case COUNT_NOT_NULL_CLEAR_WHEN_ZERO:
                    if (entry.keyContainsNonUniqueNull()) {
                        return null;
                    } else {
                        return getMutationParamForCount(remove);
                    }
                case COUNT:
                case COUNT_CLEAR_WHEN_ZERO:
                    return getMutationParamForCount(remove);
                case COUNT_UPDATES:
                    if (remove) {
                        return null;
                    } else {
                        return FDBRecordStore.LITTLE_ENDIAN_INT64_ONE;
                    }
                case SUM_LONG:
                case SUM_LONG_CLEAR_WHEN_ZERO:
                    numVal = (Number)entry.getKey().get(0);
                    if (numVal == null) {
                        return null;
                    } else {
                        lval = numVal.longValue();
                        return encodeUnsignedLong(remove ? -lval : lval);
                    }
                case MAX_EVER_LONG:
                case MIN_EVER_LONG:
                    if (remove) {
                        return null; // _EVER, so cannot undo.
                    }
                    numVal = (Number)entry.getKey().get(0);
                    if (numVal == null) {
                        return null;
                    } else {
                        lval = numVal.longValue();
                        return encodeUnsignedLong(lval);
                    }
                case MAX_EVER_VERSION:
                    if (remove) {
                        return null; // _EVER, so cannot undo
                    }
                    if (entry.getKey().hasIncompleteVersionstamp()) {
                        return entry.getKey().packWithVersionstamp();
                    } else {
                        return entry.getKey().pack();
                    }
                default:
                    return entry.getKey().pack();
            }
        }

        @Nonnull
        private byte[] getMutationParamForCount(boolean remove) {
            if (remove) {
                return FDBRecordStore.LITTLE_ENDIAN_INT64_MINUS_ONE;
            } else {
                return FDBRecordStore.LITTLE_ENDIAN_INT64_ONE;
            }
        }

        @Nonnull
        public static byte[] encodeUnsignedLong(long value) {
            return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array();
        }

        public static long decodeUnsignedLong(byte[] bytes) {
            return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
        }

        @Nonnull
        public static byte[] encodeSignedLong(long value) {
            return encodeUnsignedLong(value - Long.MIN_VALUE);
        }

        public static long decodeSignedLong(byte[] bytes) {
            return decodeUnsignedLong(bytes) + Long.MIN_VALUE;
        }

        @Override
        @Nonnull
        public BiFunction<Tuple, Tuple, Tuple> getAggregator() {
            switch (this) {
                case COUNT:
                case COUNT_UPDATES:
                case COUNT_NOT_NULL:
                case SUM_LONG:
                case COUNT_CLEAR_WHEN_ZERO:
                case COUNT_NOT_NULL_CLEAR_WHEN_ZERO:
                case SUM_LONG_CLEAR_WHEN_ZERO:
                    return (total, tuple) -> Tuple.from(total.getLong(0) + tuple.getLong(0));
                case MIN_EVER_LONG:
                case MIN_EVER_TUPLE:
                    return (min, tuple) -> min == null || min.compareTo(tuple) > 0 ? tuple : min;
                case MAX_EVER_LONG:
                case MAX_EVER_TUPLE:
                case MAX_EVER_VERSION:
                    return (max, tuple) -> max == null || max.compareTo(tuple) < 0 ? tuple : max;
                default:
                    return (accum, tuple) -> {
                        if (accum != null) {
                            throw new RecordCoreException("No aggregation, can only have one value");
                        }
                        return tuple;
                    };
            }
        }

        @Override
        @Nullable
        public Tuple getIdentity() {
            switch (this) {
                case COUNT:
                case COUNT_UPDATES:
                case COUNT_NOT_NULL:
                case SUM_LONG:
                case COUNT_CLEAR_WHEN_ZERO:
                case COUNT_NOT_NULL_CLEAR_WHEN_ZERO:
                case SUM_LONG_CLEAR_WHEN_ZERO:
                    return Tuple.from(0L);
                default:
                    return null;
            }
        }

        @Override
        public boolean isIdempotent() {
            switch (this) {
                case COUNT:
                case COUNT_UPDATES:
                case COUNT_NOT_NULL:
                case SUM_LONG:
                case COUNT_CLEAR_WHEN_ZERO:
                case COUNT_NOT_NULL_CLEAR_WHEN_ZERO:
                case SUM_LONG_CLEAR_WHEN_ZERO:
                    return false;
                default:
                    return true;
            }
        }

        @Override
        public boolean hasValues() {
            switch (this) {
                case COUNT:
                case COUNT_UPDATES:
                case COUNT_CLEAR_WHEN_ZERO:
                case COUNT_NOT_NULL_CLEAR_WHEN_ZERO:
                    return false;
                default:
                    return true;
            }
        }

        @Override
        public boolean hasSingleValue() {
            switch (this) {
                case COUNT_NOT_NULL:
                case MAX_EVER_TUPLE:
                case MIN_EVER_TUPLE:
                case MAX_EVER_VERSION:
                case COUNT_NOT_NULL_CLEAR_WHEN_ZERO:
                    return false;
                default:
                    return true;
            }
        }

        @Override
        public boolean hasLongValue() {
            switch (this) {
                case SUM_LONG:
                case MAX_EVER_LONG:
                case MIN_EVER_LONG:
                case SUM_LONG_CLEAR_WHEN_ZERO:
                    return true;
                default:
                    return false;
            }
        }

        @Override
        public boolean allowsNegative() {
            switch (this) {
                case MIN_EVER_LONG:
                case MAX_EVER_LONG:
                    return false;
                default:
                    return true;
            }
        }

        @Override
        @Nullable
        public byte[] getCompareAndClearParam() {
            switch (this) {
                case COUNT_NOT_NULL_CLEAR_WHEN_ZERO:
                case COUNT_CLEAR_WHEN_ZERO:
                case SUM_LONG_CLEAR_WHEN_ZERO:
                    return FDBRecordStore.INT64_ZERO;
                default:
                    return null;
            }
        }
    }
}
