/*
 * TupleRange.java
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.Range;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;

/**
 * A range within a subspace specified by two {@link Tuple} endpoints.
 */
@API(API.Status.UNSTABLE)
public class TupleRange {
    public static final TupleRange ALL = new TupleRange(null, null, EndpointType.TREE_START, EndpointType.TREE_END);
    @Nullable
    private final Tuple low;
    @Nullable
    private final Tuple high;
    @Nonnull
    private final EndpointType lowEndpoint;
    @Nonnull
    private final EndpointType highEndpoint;

    public TupleRange(@Nullable Tuple low, @Nullable Tuple high,
                      @Nonnull EndpointType lowEndpoint, @Nonnull EndpointType highEndpoint) {
        this.low = low;
        this.high = high;
        this.lowEndpoint = lowEndpoint;
        this.highEndpoint = highEndpoint;
    }

    @Nullable
    public Tuple getLow() {
        return low;
    }

    @Nullable
    public Tuple getHigh() {
        return high;
    }

    @Nonnull
    public EndpointType getLowEndpoint() {
        return lowEndpoint;
    }

    @Nonnull
    public EndpointType getHighEndpoint() {
        return highEndpoint;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TupleRange that = (TupleRange) o;

        if (low != null ? !low.equals(that.low) : that.low != null) {
            return false;
        }
        if (high != null ? !high.equals(that.high) : that.high != null) {
            return false;
        }
        if (lowEndpoint != that.lowEndpoint) {
            return false;
        }
        return highEndpoint == that.highEndpoint;

    }

    @Override
    public int hashCode() {
        int result = low != null ? low.hashCode() : 0;
        result = 31 * result + (high != null ? high.hashCode() : 0);
        result = 31 * result + lowEndpoint.hashCode();
        result = 31 * result + highEndpoint.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return lowEndpoint.toString(false) + tupleToString(low) +
                "," + tupleToString(high) + highEndpoint.toString(true);
    }

    @Nonnull
    protected static String tupleToString(@Nullable Tuple t) {
        if (t == null) {
            return "";
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            for (Object o : t) {
                if (o instanceof byte[]) {
                    sb.append(Arrays.toString((byte[]) o));
                } else {
                    sb.append(o);
                }
                sb.append(", ");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.setCharAt(sb.length() - 1, ']');
            return sb.toString();
        }
    }

    @SuppressWarnings("PMD.UnusedNullCheckInEquals") // uses TupleHelpers::equals for efficiency reasons
    public boolean isEquals() {
        return low != null && TupleHelpers.equals(low, high) &&
                lowEndpoint == EndpointType.RANGE_INCLUSIVE && highEndpoint == EndpointType.RANGE_INCLUSIVE;
    }

    /**
     * Create a <code>TupleRange</code> over the same keys as this range but prepended
     * by the supplied {@link Tuple}. For example, if this range is over all <code>Tuple</code>s
     * from <code>("a", 3)</code> exclusive to <code>("b", 4)</code> inclusive and one
     * calls this method with <code>(0, null)</code> as the argument, this will create a
     * range from <code>(0, null, "a", 3)</code> exclusive to <code>(0, null, "b", 4)</code>
     * inclusive.
     *
     * @param beginning a {@link Tuple} to prepend to the beginning of this range
     * @return a new <code>TupleRange</code> over all keys in this range but prepended with <code>beginning</code>
     */
    @Nonnull
    @SuppressWarnings("PMD.UnusedNullCheckInEquals") // uses TupleHelpers::equals for efficiency reasons
    public TupleRange prepend(@Nonnull Tuple beginning) {
        Tuple newLow;
        EndpointType newLowEndpoint;
        if (low == null) {
            // assert TREE_START
            newLow = beginning;
            newLowEndpoint = EndpointType.RANGE_INCLUSIVE;
        } else {
            newLow = beginning.addAll(low);
            newLowEndpoint = lowEndpoint;
        }
        Tuple newHigh;
        EndpointType newHighEndpoint;
        if (high == null) {
            // assert TREE_END
            newHigh = beginning;
            newHighEndpoint = EndpointType.RANGE_INCLUSIVE;
        } else {
            if (TupleHelpers.equals(low, high)) {
                newHigh = newLow;
            } else {
                newHigh = beginning.addAll(high);
            }
            newHighEndpoint = highEndpoint;
        }
        return new TupleRange(newLow, newHigh, newLowEndpoint, newHighEndpoint);
    }

    /**
     * Create a {@link TupleRange} over a prefix of the keys of this range. For example, if this range is over all
     * {@link Tuple}s from <code>("a", 3)</code> exclusive to <code>("b", 4)</code> inclusive and one calls this method
     * with {@code prefixCount} of {@code 1}, this will create a range from <code>("a")</code> inclusive to
     * <code>("b")</code> inclusive. Note that the newly returned {@link TupleRange} is guaranteed to encompass
     * the old range.
     *
     * @param prefixCount the number of prefix parts to consider
     * @return a new {@link TupleRange} of a prefix of {@code predixCount} parts of this tuple range
     */
    @Nonnull
    public TupleRange prefix(final int prefixCount) {
        final Tuple newLow;
        final EndpointType newLowEndpoint;
        if (low == null) {
            // assert TREE_START
            newLow = null;
            newLowEndpoint = lowEndpoint;
        } else {
            if (low.size() > prefixCount) {
                newLow = Tuple.fromList(low.getItems().subList(0, prefixCount));
            } else {
                newLow = low;
            }
            newLowEndpoint = EndpointType.RANGE_INCLUSIVE;
        }
        final Tuple newHigh;
        final EndpointType newHighEndpoint;
        if (high == null) {
            // assert TREE_END
            newHigh = null;
            newHighEndpoint = highEndpoint;
        } else {
            if (TupleHelpers.equals(low, high)) {
                newHigh = newLow;
            } else {
                if (high.size() > prefixCount) {
                    newHigh = Tuple.fromList(high.getItems().subList(0, prefixCount));
                } else {
                    newHigh = high;
                }
            }
            newHighEndpoint = EndpointType.RANGE_INCLUSIVE;
        }
        return new TupleRange(newLow, newHigh, newLowEndpoint, newHighEndpoint);
    }

    /**
     * Method to compute if an inclusive/inclusive range given by two {@link Tuple}s overlap with this tuple range.
     * @param lowTuple low tuple
     * @param highTuple high tuple
     * @return {@code true} if and only if the range {@code [lowTuple, highTuple]} overlaps this tuple range
     */
    public boolean overlaps(@Nonnull final Tuple lowTuple, @Nonnull final Tuple highTuple) {
        switch (getLowEndpoint()) {
            case TREE_START:
                break;
            case RANGE_INCLUSIVE:
            case RANGE_EXCLUSIVE:
                final Tuple checkedLow = Objects.requireNonNull(getLow());
                if (getLowEndpoint() == EndpointType.RANGE_INCLUSIVE &&
                        TupleHelpers.compare(highTuple, checkedLow) < 0) {
                    return false;
                }
                if (getLowEndpoint() == EndpointType.RANGE_EXCLUSIVE &&
                        TupleHelpers.compare(highTuple, checkedLow) <= 0) {
                    return false;
                }
                break;
            case TREE_END:
            case CONTINUATION:
            case PREFIX_STRING:
            default:
                throw new RecordCoreException("do not support endpoint " + getLowEndpoint());
        }

        switch (getHighEndpoint()) {
            case TREE_END:
                break;
            case RANGE_INCLUSIVE:
            case RANGE_EXCLUSIVE:
                final Tuple checkedHigh = Objects.requireNonNull(getHigh());
                if (getHighEndpoint() == EndpointType.RANGE_INCLUSIVE &&
                        TupleHelpers.compare(trimTupleForHighComparison(lowTuple), checkedHigh) > 0) {
                    return false;
                }
                if (getHighEndpoint() == EndpointType.RANGE_EXCLUSIVE &&
                        TupleHelpers.compare(lowTuple, checkedHigh) >= 0) {
                    return false;
                }
                break;
            case TREE_START:
            case CONTINUATION:
            case PREFIX_STRING:
            default:
                throw new RecordCoreException("do not support endpoint " + getHighEndpoint());
        }
        return true;
    }

    /**
     * Method to compute if a given {@link Tuple} is contained within this tuple range.
     * @param tuple tuple
     * @return {@code true} if and only if {@code tuple} is contained within this tuple range
     */
    public boolean contains(@Nonnull final Tuple tuple) {
        switch (getLowEndpoint()) {
            case TREE_START:
                break;
            case RANGE_INCLUSIVE:
            case RANGE_EXCLUSIVE:
                final Tuple checkedLow = Objects.requireNonNull(getLow());
                if (getLowEndpoint() == EndpointType.RANGE_INCLUSIVE &&
                        TupleHelpers.compare(tuple, checkedLow) < 0) {
                    return false;
                }
                if (getLowEndpoint() == EndpointType.RANGE_EXCLUSIVE &&
                        TupleHelpers.compare(tuple, checkedLow) <= 0) {
                    return false;
                }
                break;
            case TREE_END:
            case CONTINUATION:
            case PREFIX_STRING:
            default:
                throw new RecordCoreException("do not support endpoint " + getLowEndpoint());
        }

        switch (getHighEndpoint()) {
            case TREE_END:
                break;
            case RANGE_INCLUSIVE:
            case RANGE_EXCLUSIVE:
                final Tuple checkedHigh = Objects.requireNonNull(getHigh());
                if (getHighEndpoint() == EndpointType.RANGE_INCLUSIVE) {
                    if (TupleHelpers.compare(trimTupleForHighComparison(tuple), checkedHigh) > 0) {
                        return false;
                    }
                }
                if (getHighEndpoint() == EndpointType.RANGE_EXCLUSIVE &&
                        TupleHelpers.compare(tuple, checkedHigh) >= 0) {
                    return false;
                }
                break;
            case TREE_START:
            case CONTINUATION:
            case PREFIX_STRING:
            default:
                throw new RecordCoreException("do not support endpoint " + getHighEndpoint());
        }

        return true;
    }

    /**
     * Method to account for range-inclusive comparisons on shortened highs. Suppose, the high is
     * {@code ('abc'))} of endpoint type {@link EndpointType#RANGE_INCLUSIVE}. A tuple {@code ('abc', 5)} should be
     * within the bounds of that high but the high sorts lower. We need to cut of the second element {@code 5} of the
     * tuple prior of doing any comparisons.
     * @param tuple {@link Tuple} to trim
     * @return the trimmed {@link Tuple} if necessary or the original {@link Tuple} if no adjustments were necessary.
     */
    @Nonnull
    private Tuple trimTupleForHighComparison(@Nonnull final Tuple tuple) {
        Verify.verify(high != null);
        Verify.verify(highEndpoint == EndpointType.RANGE_INCLUSIVE);

        if (tuple.size() > high.size()) {
            return TupleHelpers.subTuple(tuple, 0, high.size());
        }
        return tuple;
    }

    /**
     * Create a <code>TupleRange</code> over all keys beginning with a given {@link Tuple}.
     * This is a shortcut for creating a <code>TupleRange</code> with <code>prefix</code>
     * as both the low- and high-endpoint and setting both endpoint types to
     * {@link EndpointType#RANGE_INCLUSIVE RANGE_INCLUSIVE}.
     *
     * @param prefix the {@link Tuple} all keys in the returned range should begin with
     * @return a <code>TupleRange</code> corresponding to keys prefixed by <code>prefix</code>
     */
    @Nonnull
    public static TupleRange allOf(@Nullable Tuple prefix) {
        if (prefix == null) {
            return ALL;
        } else {
            return new TupleRange(prefix, prefix, EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE);
        }
    }

    /**
     * Create a <code>TupleRange</code> over all keys between the given {@link Tuple}s.
     * @param low the inclusive start of the range
     * @param high the exclusive end of the range
     * @return a <code>TupleRange</code> between <code>start</code> and <code>end</code>
     */
    public static TupleRange between(@Nullable Tuple low, @Nullable Tuple high) {
        EndpointType lowEndpoint = (low == null) ? EndpointType.TREE_START : EndpointType.RANGE_INCLUSIVE;
        EndpointType highEndpoint = (high == null) ? EndpointType.TREE_END : EndpointType.RANGE_EXCLUSIVE;
        return new TupleRange(low, high, lowEndpoint, highEndpoint);
    }

    /**
     * Create a <code>TupleRange</code> over all keys between the given {@link Tuple}s, including both endpoints.
     * @param low the inclusive start of the range
     * @param high the inclusive end of the range
     * @return a <code>TupleRange</code> between <code>start</code> and <code>end</code>, inclusive
     */
    public static TupleRange betweenInclusive(@Nullable Tuple low, @Nullable Tuple high) {
        EndpointType lowEndpoint = (low == null) ? EndpointType.TREE_START : EndpointType.RANGE_INCLUSIVE;
        EndpointType highEndpoint = (high == null) ? EndpointType.TREE_END : EndpointType.RANGE_INCLUSIVE;
        return new TupleRange(low, high, lowEndpoint, highEndpoint);
    }

    /**
     * Create a <code>TupleRange</code> over all keys prefixed by some {@link String}. This
     * is a shortcut for creating a <code>TupleRange</code> where both the low- and high-endpoints
     * are <code>Tuple</code>s containing a single <code>String</code> where both the low
     * and high endpoint types are {@link EndpointType#PREFIX_STRING PREFIX_STRING}.
     *
     * @param prefixString the string that the keys in the returned range will have as a prefix
     * @return a <code>TupleRange</code> corresponding to keys prefixed by <code>prefixString</code>
     */
    @Nonnull
    public static TupleRange prefixedBy(@Nonnull String prefixString) {
        return new TupleRange(Tuple.from(prefixString), Tuple.from(prefixString), EndpointType.PREFIX_STRING, EndpointType.PREFIX_STRING);
    }

    /**
     * Convert into a FoundationDB {@link Range}. This behaves just like the version of
     * {@link #toRange(Subspace) toRange()} that takes a {@link Subspace}, but this version does not
     * prefix the resulting range with anything. If either endpoint is <code>null</code>, then
     * the <code>null</code> endpoints will be replaced with <code>byte</code> arrays representing
     * the beginning or ending of the user-readable keys in FoundationDB (that is, it will not
     * include the system keyspace).
     *
     * @return a FoundationDB {@link Range} over the same keys as this <code>TupleRange</code>
     */
    @Nonnull
    public Range toRange() {
        return toRange(
                low == null ? null : low.pack(),
                high == null ? null : high.pack(),
                lowEndpoint,
                highEndpoint
        );
    }

    /**
     * Convert into a FoundationDB {@link Range}. This adjusts the endpoints of this <code>TupleRange</code> and
     * creates a <code>Range</code> object that spans over the same range of keys. This range can be passed
     * to {@link com.apple.foundationdb.Transaction#getRange(Range) Trasaction.getRange()}, for example.
     * As with all other ranges in FoundationDB, the resulting range will include its beginning endpoint
     * but exclude its ending endpoint. The range produced will be prefixed by the {@link Subspace}
     * provided.
     *
     * @param subspace the {@link Subspace} this range should be prefixed by
     * @return a FoundationDB {@link Range} over the same keys as this <code>TupleRange</code>
     */
    @Nonnull
    public Range toRange(@Nonnull Subspace subspace) {
        return toRange(
                low == null ? subspace.pack() : subspace.pack(low),
                high == null ? subspace.pack() : subspace.pack(high),
                lowEndpoint,
                highEndpoint
        );
    }

    /**
     * Convert a pair of endpoints into a FoundationDB {@link Range}. If both <code>lowBytes</code>
     * and <code>highBytes</code> could be unpacked into {@link Tuple}s, this would be equivalent
     * to unpacking them, creating a <code>TupleRange</code> object out of them, and then calling
     * {@link #toRange()} on the resulting <code>TupleRange</code>.
     *
     * @param lowBytes the beginning of the range
     * @param highBytes the end of the range
     * @param lowEndpoint the type (inclusive, exclusive, etc.) of the low endpoint
     * @param highEndpoint the type (inclusive, exclusive, etc.) of the high endpoint
     * @return a FoundationDB {@link Range} over the same keys as the provided parameters
     */
    @Nonnull
    public static Range toRange(@Nullable byte[] lowBytes, @Nullable byte[] highBytes,
                                @Nonnull EndpointType lowEndpoint, @Nonnull EndpointType highEndpoint) {
        // Ensure that PREFIX_STRING semantics are honored
        if (lowEndpoint == EndpointType.PREFIX_STRING || highEndpoint == EndpointType.PREFIX_STRING) {
            verifyPrefixStringSemantics(lowBytes, highBytes, lowEndpoint, highEndpoint);
        }
        switch (lowEndpoint) {
            case TREE_START:
            case RANGE_INCLUSIVE:
                break;
            case RANGE_EXCLUSIVE:
                if (lowBytes == null) {
                    throw new RecordCoreException("Exclusive low endpoint with null low bytes");
                }
                lowBytes = ByteArrayUtil.strinc(lowBytes);
                break;
            case CONTINUATION:
                // The very next key, including prefix matches.
                lowBytes = ByteArrayUtil.join(lowBytes, new byte[] { 0 });
                break;
            case PREFIX_STRING:
                lowBytes = Arrays.copyOfRange(lowBytes, 0, lowBytes.length - 1);
                break;
            default:
                throw new RecordCoreException("Incorrect low endpoint: " + lowEndpoint);
        }
        switch (highEndpoint) {
            case RANGE_EXCLUSIVE:
            case CONTINUATION:
                break;
            case TREE_END:
            case RANGE_INCLUSIVE:
                if (highBytes != null) {
                    // FF is not a valid tuple code, but is used as an escape following 00 inside string / bytes,
                    // against 00 otherwise terminating the string / bytes.
                    // We do not want such suffixed strings / bytes to be included in a tuple-oriented scan.
                    // They are after the high tuple, so outside the range.
                    highBytes = ByteArrayUtil.join(highBytes, new byte[] { (byte)0xff });
                } else {
                    if (highEndpoint != EndpointType.TREE_END) {
                        throw new RecordCoreException("Inclusive high endpoint with null high bytes");
                    }
                }
                break;
            case PREFIX_STRING:
                int newLength = highBytes.length - 1;
                while (newLength >= 1 && highBytes[newLength - 1] == (byte)0xff) {
                    newLength--;
                }
                if (newLength == 0) {
                    highBytes = new byte[]{(byte)0xff};
                } else {
                    byte[] dest = Arrays.copyOfRange(highBytes, 0, newLength);
                    dest[newLength - 1]++;
                    highBytes = dest;
                }
                break;
            default:
                throw new RecordCoreException("Incorrect high endpoint: " + highEndpoint);
        }
        return new Range(lowBytes == null ? new byte[0] : lowBytes, highBytes == null ? new byte[]{(byte)0xff} : highBytes);
    }

    private static void verifyPrefixStringSemantics(@Nullable byte[] lowBytes, @Nullable byte[] highBytes,
                                @Nonnull EndpointType lowEndpoint, @Nonnull EndpointType highEndpoint) {
        if (lowBytes == null || highBytes == null) {
            throw new RecordCoreException("PREFIX_STRING must specify non-null endpoints",
                    LogMessageKeys.LOW_BYTES, ByteArrayUtil2.loggable(lowBytes),
                    LogMessageKeys.HIGH_BYTES, ByteArrayUtil2.loggable(highBytes));
        }
        if ((lowEndpoint != EndpointType.PREFIX_STRING && lowEndpoint != EndpointType.CONTINUATION) || (highEndpoint != EndpointType.PREFIX_STRING && highEndpoint != EndpointType.CONTINUATION)) {
            throw new RecordCoreException("PREFIX_STRING must be specified for both low and high endpoints",
                    "lowEndpoint", lowEndpoint,
                    "highEndpoint", highEndpoint);
        }
        if (lowEndpoint == EndpointType.CONTINUATION) {
            // highBytes (except last byte) should be prefix of lowBytes
            if (!ByteArrayUtil2.hasCommonPrefix(lowBytes, highBytes, highBytes.length - 1)) {
                throw new RecordCoreException("PREFIX_STRING continuation is not consistent with low endpoint",
                        LogMessageKeys.LOW_BYTES, ByteArrayUtil2.loggable(lowBytes),
                        LogMessageKeys.HIGH_BYTES, ByteArrayUtil2.loggable(highBytes));
            }
            if (highBytes[highBytes.length - 1] != 0) {
                throw new ByteStringBoundException(lowBytes);
            }
        } else if (highEndpoint == EndpointType.CONTINUATION) {
            // lowBytes (except last byte) should be prefix of highBytes
            if (!ByteArrayUtil2.hasCommonPrefix(highBytes, lowBytes, lowBytes.length - 1)) {
                throw new RecordCoreException("PREFIX_STRING continuation is not consistent with high endpoint",
                        LogMessageKeys.LOW_BYTES, ByteArrayUtil2.loggable(lowBytes),
                        LogMessageKeys.HIGH_BYTES, ByteArrayUtil2.loggable(highBytes));
            }
            if (lowBytes[lowBytes.length - 1] != 0) {
                throw new ByteStringBoundException(lowBytes);
            }
        } else {
            if (!Arrays.equals(lowBytes, highBytes)) {
                throw new RecordCoreException("PREFIX_STRING must be provide identical string prefixes",
                        LogMessageKeys.LOW_BYTES, ByteArrayUtil2.loggable(lowBytes),
                        LogMessageKeys.HIGH_BYTES, ByteArrayUtil2.loggable(highBytes));
            }
            if (lowBytes[lowBytes.length - 1] != 0) {
                throw new ByteStringBoundException(lowBytes);
            }
        }
    }

    /**
     * Exception thrown when range endpoint is not consistent with {@code CONTINUATION} endpoint type.
     */
    @SuppressWarnings("serial")
    public static class ByteStringBoundException extends RecordCoreException {
        public ByteStringBoundException(@Nullable byte[] rangeBytes) {
            super("Expected a [byte] string bound", LogMessageKeys.RANGE_BYTES, ByteArrayUtil2.loggable(rangeBytes));
        }
    }
}
