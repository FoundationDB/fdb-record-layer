/*
 * TupleTypeUtil.java
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

package com.apple.foundationdb.record.metadata;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;
import com.google.protobuf.Internal;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for dealing with {@link Tuple} types. In theory, these methods should live in
 * {@link com.apple.foundationdb.tuple.TupleHelpers TupleHelpers} except that they use some Protobuf specific things
 * like the {@link ByteString} class, and {@code TupleHelpers} is defined in the
 * <a href="https://javadoc.io/doc/org.foundationdb/fdb-extensions/">fdb-extensions</a> sub-project
 * which does not (and probably should not) take Protobuf as a dependency.
 */
class TupleTypeUtil {
    @Nonnull
    private static final BigInteger BIG_INT_MAX_LONG = BigInteger.valueOf(Long.MAX_VALUE);
    @Nonnull
    private static final BigInteger BIG_INT_MIN_LONG = BigInteger.valueOf(Long.MIN_VALUE);

    /**
     * Normalize a list of values so that it can be checked for equality with other lists sharing
     * the same {@link Tuple} representation. In other words, it should be the case that:
     *
     * <pre> {@code
     *   toTupleEquivalentValue(list1).equals(toTupleEquivalentValue)
     *      == Arrays.equals(Tuple.fromList(toTupleAppropriateList(list1)).pack(), Tuple.fromList(toTupleAppropriateList(list2)).pack())
     * }</pre>
     *
     * <p>
     * for any two lists {@code list1} and {@code list2}.
     * </p>
     *
     * @param values the list of values to normalized
     * @return a new list containing the normalized elements of {@code values}
     */
    @Nonnull
    static List<Object> toTupleEquivalentList(@Nonnull List<?> values) {
        List<Object> tupleEquivalentList = new ArrayList<>(values.size());
        for (Object o : values) {
            tupleEquivalentList.add(toTupleEquivalentValue(o));
        }
        return tupleEquivalentList;
    }

    /**
     * Normalize a value so that it compares equal to anything with the same {@link Tuple} representation.
     * The value that is returned cannot necessarily be packed by a {@code Tuple} (for example,
     * a <code>byte[]</code> is returned as a {@link ByteString}), but it does implement {@link Object#equals(Object)}
     * and {@link Object#hashCode()}, so the value can be used in hash-based data structures like
     * {@link java.util.HashSet HashSet}s and {@link java.util.HashMap HashMap}s. In other words, it should
     * bethe case that:
     *
     * <pre> {@code
     *   Objects.equals(toTupleEquivalentValue(value1), toTupleEquivalentValue(value2))
     *     == Arrays.equals(Tuple.from(value1).pack(), Tuple.from(value2).pack())
     * }</pre>
     *
     * <p>
     * for any two values {@code value1} and {@code value2}.
     * </p>
     *
     * <p>
     * This will only return {@code null} if {@link #toTupleAppropriateValue(Object)} would return {@code null}
     * on the same input. If the object is already in
     * </p>
     *
     * @param obj the value to normalize
     * @return a value that has the same representation when {@link Tuple}-encoded
     */
    @Nullable
    static Object toTupleEquivalentValue(@Nullable Object obj) {
        if (obj == null || obj instanceof Key.Evaluated.NullStandin) {
            return null;
        } else if (obj instanceof List<?>) {
            List<?> list = (List<?>)obj;
            return toTupleEquivalentList(list);
        } else if (obj instanceof Tuple) {
            return toTupleEquivalentList(((Tuple)obj).getItems());
        } else if (obj instanceof byte[]) {
            return ByteString.copyFrom((byte[]) obj);
        } else if ((obj instanceof Byte) || (obj instanceof Short) || (obj instanceof Integer)) {
            return ((Number)obj).longValue();
        } else if (obj instanceof BigInteger) {
            BigInteger bigInt = (BigInteger)obj;
            if (bigInt.compareTo(BIG_INT_MIN_LONG) > 0 && bigInt.compareTo(BIG_INT_MAX_LONG) < 0) {
                return bigInt.longValue();
            } else {
                return bigInt;
            }
        } else if (obj instanceof Internal.EnumLite) {
            return (long)((Internal.EnumLite)obj).getNumber();
        } else if (obj instanceof FDBRecordVersion) {
            return ((FDBRecordVersion)obj).toVersionstamp(false);
        } else {
            return obj;
        }
    }

    /**
     * Convert a list of values into items that can all be stored within a {@link Tuple}.
     *
     * @param values a list of values
     * @return a new list with {@link Tuple}-encodable versions of the elements of {@code values}
     */
    @Nonnull
    static List<Object> toTupleAppropriateList(@Nonnull List<?> values) {
        List<Object> tupleAppropriateList = new ArrayList<>(values.size());
        for (Object o : values) {
            tupleAppropriateList.add(toTupleAppropriateValue(o));
        }
        return tupleAppropriateList;
    }

    /**
     * Convert a value into a type that can be stored within a {@link Tuple}.
     *
     * @param obj the value to convert
     * @return the value converted to some {@link Tuple}-encodable type
     */
    @Nullable
    static Object toTupleAppropriateValue(@Nullable Object obj) {
        if (obj instanceof Key.Evaluated.NullStandin) {
            return null;
        } else if (obj instanceof ByteString) {
            return ((ByteString) obj).toByteArray();
        } else if (obj instanceof List) {
            return toTupleAppropriateList((List<?>) obj);
        } else if (obj instanceof Internal.EnumLite) {
            return ((Internal.EnumLite) obj).getNumber();
        } else if (obj instanceof FDBRecordVersion) {
            return ((FDBRecordVersion) obj).toVersionstamp(false);
        } else {
            return obj;
        }
    }

    private TupleTypeUtil() {
    }
}
